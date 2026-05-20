# PRODUCT COMPLEMENTARY JOB (FP-GROWTH - SPARK VERSION)
# INPUT (MinIO): s3a://datalake/processed/
#       - order_items.csv
#       - products.csv
# OUTPUT (Cassandra): ecommerce.product_complementary

# =========================================================
# 1. IMPORT THƯ VIỆN
# =========================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    collect_set,
    size,
    lit,
    current_timestamp,
    when,
    round
)

from pyspark.ml.fpm import FPGrowth
from pyspark.sql.window import Window

from datetime import datetime
from common.utils import current_time
import logging
import sys


# =========================================================
# 2. CONFIG
# =========================================================

INPUT_PATH = "s3a://datalake/processed/"

MINIO_CONF = {
    "endpoint": "minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
}

CASSANDRA_CONF = {
    "host": "cassandra",
    "keyspace": "ecommerce",
    "table": "product_complementary"
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("product_complementary.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# =========================================================
# 3. CREATE SPARK SESSION
# =========================================================

def create_spark():

    return SparkSession.builder \
        .appName("ProductComplementaryJob") \
        .master("spark://spark-master:7077") \
        .config(
            "spark.jars",
            "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
        ) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ) \
        .config("spark.cassandra.connection.host", CASSANDRA_CONF["host"]) \
        .getOrCreate()


# =========================================================
# 4. BUILD FP-GROWTH LOGIC
# =========================================================

def build_product_complementary(order_items, products):

    logger.info("4. Building product complementary model...")

    # 4.1 tạo basket theo order_id
    baskets_df = order_items.groupBy("order_id").agg(
        collect_set("product_id").alias("items")
    )

    # lọc giỏ hàng >= 2 sản phẩm (giống bản pandas)
    baskets_df = baskets_df.filter(size(col("items")) > 1)

    logger.info(f"Total baskets: {baskets_df.count()}")

    # =====================================================
    # 4.2 FP-GROWTH TRAINING (SPARK NATIVE)
    # =====================================================

    fp = FPGrowth(
        itemsCol="items",
        minSupport=0.0001,
        minConfidence=0.005
    )

    model = fp.fit(baskets_df)

    freq_itemsets = model.freqItemsets
    rules = model.associationRules

    logger.info("FP-Growth training completed")

    # =====================================================
    # 4.3 FILTER 1-1 RULES (A -> B)
    # =====================================================

    rules_1_1 = rules.filter(
        (size(col("antecedent")) == 1) &
        (size(col("consequent")) == 1)
    )

    # tách product id
    rules_1_1 = rules_1_1 \
        .withColumn("Product_id_1", col("antecedent")[0]) \
        .withColumn("Product_id_2", col("consequent")[0])

    # =====================================================
    # 4.4 ENRICH CATEGORY
    # =====================================================

    prod_category = products.select("product_id", "category")

    rules_1_1 = rules_1_1.join(
        prod_category.withColumnRenamed("product_id", "Product_id_1")
                     .withColumnRenamed("category", "cat_1"),
        on="Product_id_1",
        how="left"
    )

    rules_1_1 = rules_1_1.join(
        prod_category.withColumnRenamed("product_id", "Product_id_2")
                     .withColumnRenamed("category", "cat_2"),
        on="Product_id_2",
        how="left"
    )

    # =====================================================
    # 4.5 LOGIC METRICS (GIỮ NGUYÊN BẢN PANDAS LOGIC)
    # =====================================================

    total_baskets = baskets_df.count()

    rules_1_1 = rules_1_1.withColumn(
        "Relationship_type",
        lit("co_purchase")
    )

    # support * total baskets
    rules_1_1 = rules_1_1.withColumn(
        "Co_purchase_count",
        (col("support") * lit(total_baskets)).cast("int")
    )

    rules_1_1 = rules_1_1.withColumn(
        "Confidence",
        col("confidence").cast("double")
    )

    rules_1_1 = rules_1_1.withColumn(
        "Category_cross_sell",
        col("cat_1") != col("cat_2")
    )

    # complementary score = 0.7 * confidence + 0.3 * (lift/10)
    rules_1_1 = rules_1_1.withColumn(
        "Complementary_score",
        round(
            (col("confidence") * 0.7) +
            ((col("lift") / 10) * 0.3),
            2
        )
    )

    rules_1_1 = rules_1_1.withColumn(
        "Computed_date",
        lit(int(current_time().timestamp() * 1000))
    )

    logger.info("Feature engineering completed")

    return rules_1_1


# =========================================================
# 5. SAVE TO CASSANDRA
# =========================================================

def save_to_cassandra(df):

    logger.info("Saving to Cassandra...")

    final_df = df.select(
        "Product_id_1",
        "Product_id_2",
        "Relationship_type",
        "Co_purchase_count",
        "Confidence",
        "Category_cross_sell",
        "Complementary_score",
        "Computed_date"
    )

    final_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("overwrite") \
        .options(
            table=CASSANDRA_CONF["table"],
            keyspace=CASSANDRA_CONF["keyspace"]
        ) \
        .save()

    logger.info("Saved to Cassandra successfully")


# =========================================================
# 6. PIPELINE RUN
# =========================================================

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

        logger.info(f"order_items: {order_items.count()}")
        logger.info(f"products: {products.count()}")

        # BUILD MODEL
        result_df = build_product_complementary(order_items, products)

        logger.info(f"Final rules: {result_df.count()}")

        # SAVE
        save_to_cassandra(result_df)

        duration = (datetime.now() - start_time).total_seconds()

        logger.info(f"PIPELINE SUCCESS in {duration}s")

    except Exception as e:
        logger.error("PIPELINE FAILED")
        logger.error(str(e))
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