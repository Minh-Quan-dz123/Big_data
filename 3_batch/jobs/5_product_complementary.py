# PRODUCT COMPLEMENTARY JOB (FP-GROWTH - SPARK VERSION)
# INPUT (MinIO): s3a://datalake/processed/
#       - order_items.csv
#       - products.csv
# OUTPUT (MongoDB): ecommerce.product_complementary

# 1. IMPORT THƯ VIỆN

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    collect_set,
    size,
    lit,
    unix_timestamp,
    when,
    round
)

from pyspark.ml.fpm import FPGrowth
from pyspark.sql.window import Window

from datetime import datetime
import logging
import sys


# 2. CONFIG

INPUT_PATH = "s3a://datalake/processed/"

MINIO_CONF = {
    "endpoint": "http://minio-service:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
}

MONGODB_CONF = {
    "uri": "mongodb://mongodb-service.default.svc.cluster.local:27017/",
    "database": "ecommerce",
    "collection": "product_complementary"
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# 3. CREATE SPARK SESSION
def create_spark():

    #mongo_uri = f"{MONGODB_CONF['uri']}/{MONGODB_CONF['database']}.{MONGODB_CONF['collection']}"
    return SparkSession.builder \
        .appName("ProductComplementaryJob") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


# 4. BUILD FP-GROWTH LOGIC

def build_product_complementary(order_items, products):

    logger.info("4. Building product complementary model...")

    # 4.1 tạo basket theo order_id
    baskets_df = order_items.groupBy("order_id").agg(
        collect_set("product_id").alias("items")
    )

    # lọc giỏ hàng >= 2 sản phẩm 
    baskets_df = baskets_df.filter(size(col("items")) > 1)

    # Tối ưu: Cache lại baskets_df vì nó được dùng cho cả Train FP-growth và hàm .count() bên dưới
    baskets_df.cache()
    total_baskets = baskets_df.count()

    # 4.2 FP-GROWTH TRAINING (SPARK NATIVE)
    fp = FPGrowth(
        itemsCol="items",
        minSupport=0.0001,
        minConfidence=0.005
    )

    model = fp.fit(baskets_df)
    rules = model.associationRules

    logger.info("FP-Growth training completed")

    # 4.3 FILTER 1-1 RULES (A -> B)
    rules_1_1 = rules.filter(
        (size(col("antecedent")) == 1) & (size(col("consequent")) == 1)
        # 2 cột này được sinh sẵn ở  rules = model.associationRules
        # antecedent nghĩa là vế trái A trong luật A->B (mua A thì mua kèm B)
        # consequent nghĩa là vế phải B
    )

    # tách product id
    rules_1_1 = rules_1_1 \
        .withColumn("product_id_1", col("antecedent")[0]) \
        .withColumn("product_id_2", col("consequent")[0])

    # 4.4 ENRICH CATEGORY
    prod_category = products.select("product_id", "category")

    rules_1_1 = rules_1_1.join(
        prod_category.withColumnRenamed("product_id", "product_id_1").withColumnRenamed("category", "cat_1"),
        on="product_id_1",
        how="left"
    )

    rules_1_1 = rules_1_1.join(
        prod_category.withColumnRenamed("product_id", "product_id_2").withColumnRenamed("category", "cat_2"),
        on="product_id_2",
        how="left"
    )

    # 4.5 LOGIC METRICS 
    rules_1_1 = rules_1_1.withColumn(
        "relationship_type",
        lit("co_purchase")
    )

    # support * total baskets
    rules_1_1 = rules_1_1.withColumn(
        "co_purchase_count",
        (col("support") * lit(total_baskets)).cast("int")
    )

    rules_1_1 = rules_1_1.withColumn(
        "confidence",
        col("confidence").cast("double")
    )

    rules_1_1 = rules_1_1.withColumn(
        "category_cross_sell",
        col("cat_1") != col("cat_2")
    )

    # complementary score = 0.7 * confidence + 0.3 * (lift/10)
    rules_1_1 = rules_1_1.withColumn(
        "complementary_score",
        round(
            (col("confidence") * 0.7) + ((col("lift") / 10) * 0.3), 2
        )
    )

    rules_1_1 = rules_1_1.withColumn("computed_date", unix_timestamp() * 1000)

    logger.info("Feature engineering completed")
    baskets_df.unpersist()

    return rules_1_1


# 5. SAVE TO MONGODB 
def save_to_mongodb(df):

    logger.info("Saving to mongoDB...")

    final_df = df.select(
        "product_id_1",
        "product_id_2",
        "relationship_type",
        "co_purchase_count",
        "confidence",
        "category_cross_sell",
        "complementary_score",
        "computed_date"
    )

    final_df.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("spark.mongodb.write.connection.uri", MONGODB_CONF["uri"]) \
        .option("database", MONGODB_CONF["database"]) \
        .option("collection", MONGODB_CONF["collection"]) \
        .save()

    logger.info("Saved to MongoDB successfully")


# 6. PIPELINE RUN

def run_pipeline():

    logger.info("=" * 60)
    logger.info("START PRODUCT COMPLEMENTARY PIPELINE")
    logger.info("=" * 60)

    spark = None
    start_time = datetime.now()

    try:

        spark = create_spark()
        spark.sparkContext.setLogLevel("ERROR")

        # LOAD ORDER ITEMS
        logger.info("Loading order_items...")
        order_items = spark.read.option("header", True).csv(
            INPUT_PATH + "order_items.csv"
        )

        # LOAD PRODUCTS
        logger.info("Loading products...")
        products = spark.read.option("header", True).csv(
            INPUT_PATH + "products.csv"
        )

        logger.info(f"order_items: ")
        logger.info(f"products: ")

        # BUILD MODEL
        result_df = build_product_complementary(order_items, products)

        # SAVE
        save_to_mongodb(result_df)

        duration = (datetime.now() - start_time).total_seconds()

        logger.info(f"PIPELINE SUCCESS in {duration}s")

    except Exception as e:
        logger.error("PIPELINE FAILED")
        logger.error(str(e), exc_info=True)
        raise

    finally:
        if spark:
            spark.stop()

        logger.info("=" * 60)


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":
    run_pipeline()