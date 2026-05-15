# PRODUCT SIMILARITY JOB
# INPUT (MinIO): s3a://datalake/processed/
#       - products.csv
#       - orders.csv
#       - order_items.csv
# OUTPUT (Cassandra): ecommerce.product_similarity

# 1 IMPORT thư viện
# 1.1 SparkSession
from pyspark.sql import SparkSession

# 1.2 Spark SQL functions
from pyspark.sql.functions import (
    col,
    row_number,
    abs,
    when,
    count,
    collect_set,
    size,
    broadcast,
    coalesce,
    lit,
    current_timestamp
)

# 1.3 Window function
from pyspark.sql.window import Window

# 1.4 Python libraries
from datetime import datetime
import logging
import sys

# 2 CONFIG

# 2.1 INPUT PATH (MinIO)
INPUT_PATH = "s3a://datalake/processed/"

# 2.2 MINIO CONFIG
MINIO_CONF = {
    "endpoint": "minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
}

# 2.3 CASSANDRA CONFIG
CASSANDRA_CONF = {
    "host": "cassandra",
    "keyspace": "ecommerce",
    "table": "product_similarity"
}

# 2.4 LOGGING CONFIG
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("product_similarity.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# 3 CREATE SPARK
def create_spark():

    return SparkSession.builder \
        .appName("ProductSimilarityJob") \
        .master("spark://spark-master:7077") \
        .config(
            "spark.jars",
            "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ) \
        .config("spark.cassandra.connection.host",CASSANDRA_CONF["host"]) \
        .getOrCreate()


# 4 CALCULATE CONTENT-BASED SIMILARITY
# dựa trên tên, mô tả, category, giá
def calculate_content_based_similarity(products):

    logger.info("4. Calculating content-based similarity...")

    # 4.1 Tạo product pairs - mỗi product đối với tất cả các product khác
    # dùng cross join tạo ra tất cả các cặp sản phẩm
    product_pairs = products.select(
        col("product_id").alias("product_id_1"),
        col("category").alias("category_1"),
        col("price").alias("price_1")
    ).join(
        products.select(
            col("product_id").alias("product_id_2"),
            col("category").alias("category_2"),
            col("price").alias("price_2")
        ),
        how="cross"
    )

    logger.info("4.1 Product pairs created")

    # 4.2 Lọc bỏ cặp trùng nhau và cặp ngược
    # chỉ giữ lại cặp mà product_id_1 < product_id_2
    product_pairs = product_pairs.filter(col("product_id_1") < col("product_id_2"))

    logger.info("4.2 Duplicate pairs removed")

    # 4.3 Tính category match
    category_match = when(
        col("category_1") == col("category_2"), True
    ).otherwise(False)

    # 4.4 Tính price similarity (normalized)
    # nếu giá khác nhiều thì điểm thấp, giá gần nhau thì điểm cao
    max_price = products.agg({"price": "max"}).collect()[0][0]
    min_price = products.agg({"price": "min"}).collect()[0][0]
    price_range = max_price - min_price if max_price > min_price else 1

    price_similarity = 1 - (abs(col("price_1") - col("price_2")) / price_range)

    # 4.5 Content-based score = 50% category match + 50% price similarity
    content_score = when(
        col("category_1") == col("category_2"),
        0.5 + (0.5 * price_similarity)
    ).otherwise(
        0.3 * price_similarity
    )

    product_pairs = product_pairs.withColumn("similarity_score", content_score)
    product_pairs = product_pairs.withColumn("category_match", category_match)
    product_pairs = product_pairs.withColumn("similarity_type", lit("content_based"))

    logger.info("4.5 Content-based similarity calculated")

    return product_pairs.select(
        "product_id_1",
        "product_id_2",
        "similarity_score",
        "similarity_type",
        "category_match"
    )


# 5 CALCULATE COLLABORATIVE SIMILARITY
# dựa vào sản phẩm được mua cùng nhau
def calculate_collaborative_similarity(products, orders, order_items):

    logger.info("5. Calculating collaborative similarity...")

    # 5.1 Join orders + order_items để lấy product_id từng order
    order_products = orders.select("order_id").join(
        order_items.select("order_id", "product_id"),
        on="order_id",
        how="inner"
    )

    logger.info("5.1 Order products joined")

    # 5.2 Tạo product pairs dựa trên co-purchase
    # nếu 2 sản phẩm xuất hiện trong cùng 1 đơn hàng => mua cùng nhau
    co_purchase = order_products.select(
        col("product_id").alias("product_id_1")
    ).join(
        order_products.select(
            col("product_id").alias("product_id_2")
        ).join(
            order_products.select("order_id"),
            how="cross"
        ),
        on="order_id",
        how="inner"
    )

    logger.info("5.2 Co-purchase pairs identified")

    # 5.3 Lọc bỏ cặp trùng và cặp ngược
    co_purchase = co_purchase.filter(col("product_id_1") < col("product_id_2"))

    # 5.4 Tính co-purchase score (normalized by number of orders)
    total_orders = orders.count()

    co_purchase_score = co_purchase.groupBy(
        "product_id_1",
        "product_id_2"
    ).agg(
        count("order_id").alias("co_purchase_count")
    ).withColumn(
        "similarity_score",
        col("co_purchase_count") / total_orders
    ).drop("co_purchase_count")

    co_purchase_score = co_purchase_score.withColumn(
        "similarity_type",
        lit("collaborative")
    ).withColumn(
        "category_match",
        lit(False)
    )

    logger.info("5.4 Collaborative similarity calculated")

    return co_purchase_score.select(
        "product_id_1",
        "product_id_2",
        "similarity_score",
        "similarity_type",
        "category_match"
    )


# 6 MERGE CONTENT + COLLABORATIVE SIMILARITY
def merge_similarities(content_sim, collab_sim):

    logger.info("6. Merging similarities...")

    # 6.1 Union both similarities
    all_similarities = content_sim.union(collab_sim)

    # 6.2 Group by product pairs and calculate hybrid score
    # nếu có cả content và collaborative => hybrid
    # lấy max score của 2
    hybrid_sim = all_similarities.groupBy(
        "product_id_1",
        "product_id_2"
    ).agg(
        # lấy max similarity score
        col("similarity_score").alias("max_score"),
        # nếu có 2 loại => hybrid, nếu 1 loại => giữ nguyên type
        when(
            count("similarity_type") > 1,
            lit("hybrid")
        ).otherwise(col("similarity_type")).alias("similarity_type")
    )

    logger.info("6.2 Hybrid similarities merged")

    return hybrid_sim


# 7 BUILD FINAL OUTPUT
def build_product_similarity(products, orders, order_items):

    logger.info("7. Building product similarity output...")

    # 7.1 Calculate content-based
    content_sim = calculate_content_based_similarity(products)

    # 7.2 Calculate collaborative
    collab_sim = calculate_collaborative_similarity(products, orders, order_items)

    # 7.3 Merge both
    final_output = merge_similarities(content_sim, collab_sim)

    # 7.4 Add computed date
    final_output = final_output.withColumn(
        "computed_date",
        current_timestamp()
    )

    logger.info("7.4 Final output completed")

    return final_output


# 8 SAVE TO CASSANDRA
def save_to_cassandra(df):

    logger.info("Saving to Cassandra...")

    final_df = df.select(
        "product_id_1",
        "product_id_2",
        "similarity_score",
        "similarity_type",
        "category_match",
        "computed_date"
    )

    final_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("overwrite") \
        .options(
            table=CASSANDRA_CONF["table"],
            keyspace=CASSANDRA_CONF["keyspace"]
        ) \
        .save()

    logger.info("8. Saved to Cassandra successfully")


# 9 RUN PIPELINE
def run_pipeline():

    logger.info("=" * 50)
    logger.info("START PRODUCT SIMILARITY PIPELINE")
    logger.info("=" * 50)

    start_time = datetime.now()

    spark = None

    try:

        # 9.1 CREATE SPARK
        spark = create_spark()
        spark.sparkContext.setLogLevel("ERROR")

        # 9.2 LOAD PRODUCTS
        logger.info("Loading products data...")
        products = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(INPUT_PATH + "products.csv")

        logger.info(f"Products count: {products.count()}")

        # 9.3 LOAD ORDERS
        logger.info("Loading orders data...")

        orders = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(INPUT_PATH + "orders.csv")

        logger.info(f"Orders count: {orders.count()}")

        # 9.4 LOAD ORDER ITEMS
        logger.info("Loading order_items data...")

        order_items = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(INPUT_PATH + "order_items.csv")

        logger.info(f"Order items count: {order_items.count()}")

        # 9.5 BUILD SIMILARITY
        final_output = build_product_similarity(
            products,
            orders,
            order_items
        )

        logger.info(f"Final rows: {final_output.count()}")

        # 9.6 SAVE TO CASSANDRA
        save_to_cassandra(final_output)

        # 9.7 SUCCESS
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
