# PRODUCT SIMILARITY JOB
# INPUT (MinIO): s3a://datalake/processed/
#       - products.csv
#       - orders.csv
#       - order_items.csv
# OUTPUT (MongoDB): ecommerce.product_similarity

# 1 IMPORT thư viện
# 1.1 SparkSession
from pyspark.sql import SparkSession

# 1.2 Spark SQL functions
from pyspark.sql.functions import (
    col,
    abs,
    lit,
    current_timestamp,
    max as spark_max,
    min as spark_min,
    row_number

)
from pyspark.sql.window import Window

# 1.3 Python libraries
from datetime import datetime
import logging
import sys

# 2 CONFIG

# 2.1 INPUT PATH (MinIO)
INPUT_PATH = "s3a://datalake/processed/"

# 2.2 MINIO CONFIG
MINIO_CONF = {
    "endpoint": "http://minio-service:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
}

# 2.3 MONGODB CONFIG
MONGODB_CONF = {
    "uri": "mongodb://mongodb-service.default.svc.cluster.local:27017/",
    "database": "ecommerce",
    "collection": "product_similarity"
}

# 2.4 LOGGING CONFIG
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# 3 CREATE SPARK
def create_spark():
    #mongo_uri = f"{MONGODB_CONF['uri']}/{MONGODB_CONF['database']}.{MONGODB_CONF['collection']}"

    return SparkSession.builder \
        .appName("ProductSimilarityJob") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


# 4 CALCULATE CONTENT-BASED SIMILARITY
# dựa trên tên, mô tả, category, giá
def calculate_content_based_similarity(products):

    logger.info("4. Calculating content-based similarity...")

    # Tìm giá trị lớn nhất và nhỏ nhất của cột price để chuẩn hóa dữ liệu (Z-normalization)
    price_bounds = products.agg(spark_max("price").alias("max_p"), spark_min("price").alias("min_p")).collect()[0]
    max_price = price_bounds["max_p"]
    min_price = price_bounds["min_p"]
    price_range = max_price - min_price if max_price > min_price else 1.0

    # 4.1 Tạo product pairs - mỗi product đối với tất cả các product khác
    #Tối ưu hóa: Thay vì Cross Join mù quáng (N x N), ta chỉ bắt cặp các sản phẩm cùng Category 
    # và áp dụng điều kiện product_id_1 < product_id_2 để lọc trùng lặp lẫn cặp ngược
    p1 = products.select(col("product_id").alias("product_id_1"), col("category").alias("category_1"), col("price").alias("price_1"))
    p2 = products.select(col("product_id").alias("product_id_2"), col("category").alias("category_2"), col("price").alias("price_2"))

    product_pairs = p1.join(
        p2, 
        on=(col("category_1") == col("category_2")) & (col("product_id_1") != col("product_id_2")), 
        how="inner"
    )

    # Tính toán độ tương đồng về giá
    price_similarity = 1.0 - (abs(col("price_1") - col("price_2")) / price_range)

    # Vì đã join cùng category nên mặc định category_match là True (0.5 điểm) + 50% trọng số giá
    content_score = 0.5 + (0.5 * price_similarity)
    
    final_content = product_pairs.withColumn("similarity_score", content_score) \
                                 .withColumn("category_match", lit(True)) \
                                 .withColumn("similarity_type", lit("content_based")) \
                                 .withColumn("computed_date", current_timestamp())

    # sau đó chỉ lấy tối đa 20 sản phẩm có điểm cao nhất
    # 1. Định nghĩa window: Gom nhóm theo product_id_1, sắp xếp điểm similarity_score giảm dần
    window_spec = Window.partitionBy("product_id_1").orderBy(col("similarity_score").desc())

    # 2. đánh số hàng để lọc lấy 1->20
    final_content = final_content.withColumn("rank", row_number().over(window_spec)) \
                                 .filter(col("rank") <= 20) \
                                 .drop("rank")
    logger.info("4. Content-based similarity calculated")
    
    return final_content.select(
        "product_id_1", 
        "product_id_2", 
        "similarity_score", 
        "similarity_type", 
        "category_match",
        "computed_date"
    )


# 5 SAVE TO MONGODB
def save_to_mongodb(df):

    logger.info("Saving to Mongo...")

    df.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("spark.mongodb.write.connection.uri", MONGODB_CONF["uri"]) \
        .option("database", MONGODB_CONF["database"]) \
        .option("collection", MONGODB_CONF["collection"]) \
        .save()

    logger.info("5. Saved to MongoDB successfully")



# 6 RUN PIPELINE
def run_pipeline():

    logger.info("=" * 50)
    logger.info("START PRODUCT SIMILARITY PIPELINE")
    logger.info("=" * 50)

    start_time = datetime.now()

    spark = None

    try:

        # 6.1 CREATE SPARK
        spark = create_spark()
        spark.sparkContext.setLogLevel("ERROR")

        # 6.2 LOAD PRODUCTS
        logger.info("Loading products data...")
        products = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(INPUT_PATH + "products.csv")

        # 6.3 Tính toán độ tương đồng trực tiếp
        final_output = calculate_content_based_similarity(products)

        # 6.4 SAVE TO MONGODB
        save_to_mongodb(final_output)

        # 6.5 SUCCESS
        duration = (datetime.now() - start_time).total_seconds()

        logger.info(f"PIPELINE SUCCESS ({duration}s)")

    except Exception as ex:
        logger.error("PIPELINE FAILED")
        logger.error(str(ex))
        raise

    finally:
        if spark:
            spark.stop()

        logger.info("=" * 50)


# MAIN
if __name__ == "__main__":
    run_pipeline()
