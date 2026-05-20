# TRENDING PRODUCTS JOB
# INPUT (MinIO): s3a://datalake/processed/
#       - events.csv
#       - orders.csv
#       - order_items.csv
#       - products.csv
# OUTPUT (Cassandra): ecommerce.trending_products

# 1 IMPORT THƯ VIỆN
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, when, current_date, datediff, to_timestamp, lit, coalesce, current_timestamp
)
from datetime import datetime
from common.utils import current_time

import logging
import sys

# 2 CONFIG
INPUT_PATH = "s3a://datalake/processed/"

MINIO_CONF = {
    "endpoint": "minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
}

CASSANDRA_CONF = {
    "host": "cassandra",
    "keyspace": "ecommerce",
    "table": "trending_products"
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("trending_products.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# 3 CREATE SPARK
def create_spark():
    return SparkSession.builder \
        .appName("TrendingProductsJob") \
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
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ) \
        .config("spark.cassandra.connection.host", CASSANDRA_CONF["host"]) \
        .getOrCreate()


# 4 BUILD TRENDING PRODUCTS
def build_trending_products(events, orders, order_items, products):
    logger.info("4. Building trending products...")

    # Define windows (e.g. 7 days)
    # Cửa sổ hiện tại: 0-7 ngày trước
    # Cửa sổ trước đó: 8-14 ngày trước
    
    # 4.1 Xử lý bảng events để tính view_growth
    # Lọc chỉ lấy các event_type = 'view'
    views = events.filter(col("event_type") == "view")
    views = views.withColumn("event_date", to_timestamp(col("event_timestamp")))
    ref_date = current_time().strftime('%Y-%m-%d')
    views = views.withColumn("days_ago", datediff(lit(ref_date), col("event_date")))

    # Đếm số lượng view hiện tại (current_window) và trước đó (previous_window)
    view_stats = views.groupBy("product_id").agg(
        count(when((col("days_ago") >= 0) & (col("days_ago") <= 7), True)).alias("current_views"),
        count(when((col("days_ago") > 7) & (col("days_ago") <= 14), True)).alias("previous_views")
    )

    # Tính view_growth = (current - prev) / (prev + 1)
    view_stats = view_stats.withColumn(
        "view_growth",
        (col("current_views") - col("previous_views")) / (col("previous_views") + 1)
    )
    logger.info("4.1. View stats completed")

    # 4.2 Xử lý orders & order_items để tính order_growth
    # Chuyển đổi order_date (hoặc order_data nếu file sai chính tả) thành timestamp
    orders = orders.withColumn("order_time", to_timestamp(col("order_date")))
    ref_date = current_time().strftime('%Y-%m-%d')
    orders = orders.withColumn("days_ago", datediff(lit(ref_date), col("order_time")))

    # Join orders với order_items
    order_details = orders.join(order_items, on="order_id", how="inner")

    # Đếm số lượng bán ra (hoặc lượt mua) hiện tại và trước đó
    order_stats = order_details.groupBy("product_id").agg(
        sum(when((col("days_ago") >= 0) & (col("days_ago") <= 7), col("quantity")).otherwise(0)).alias("current_orders"),
        sum(when((col("days_ago") > 7) & (col("days_ago") <= 14), col("quantity")).otherwise(0)).alias("previous_orders")
    )

    # Tính order_growth = (current - prev) / (prev + 1)
    order_stats = order_stats.withColumn(
        "order_growth",
        (col("current_orders") - col("previous_orders")) / (col("previous_orders") + 1)
    )
    logger.info("4.2. Order stats completed")

    # 4.3 Kết hợp view và order để tính trend_score
    # Full outer join để lấy được sản phẩm dù chỉ có view hoặc chỉ có order
    trending_df = view_stats.join(order_stats, on="product_id", how="outer")

    # Điền null = 0 cho các cột vừa join
    trending_df = trending_df.fillna({
        "current_views": 0, "previous_views": 0, "view_growth": 0.0,
        "current_orders": 0, "previous_orders": 0, "order_growth": 0.0
    })

    # Tính trend_score: Công thức giả định
    # Kết hợp giữa số lượng hiện tại và sự tăng trưởng (đặt trọng số cho order cao hơn view)
    trending_df = trending_df.withColumn(
        "trend_score",
        (col("current_views") * 0.2 + col("current_orders") * 0.8) + 
        (col("view_growth") * 5.0 + col("order_growth") * 15.0)
    )

    # 4.4 Thêm thông tin required (trend_window, trend_date)
    trending_df = trending_df.withColumn("trend_window", lit("7d"))
    # Chuyển thời gian hiện tại thành timestamp millis cho Cassandra
    ref_time_ms = int(current_time().timestamp() * 1000)
    trending_df = trending_df.withColumn(
        "trend_date", 
        lit(ref_time_ms)
    )

    # Join để đảm bảo chỉ lấy các sản phẩm tồn tại trong bảng products (Nếu cần thiết)
    # Ở đây đề bài chỉ yêu cầu product_id nên có thể pass.
    # Chỉ chọn các sản phẩm có điểm trend_score > 0 để tối ưu dung lượng lưu trữ
    trending_df = trending_df.filter(col("trend_score") > 0)
    
    logger.info("4.3. & 4.4. Trend score calculation completed")

    # 4.5 Tạo Final Output matching schema
    final_output = trending_df.select(
        "product_id",
        "trend_score",
        "view_growth",
        "order_growth",
        "trend_window",
        "trend_date"
    )

    return final_output


# 5 SAVE TO CASSANDRA
def save_to_cassandra(df):
    logger.info("Saving to Cassandra...")

    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("overwrite") \
        .options(
            table=CASSANDRA_CONF["table"],
            keyspace=CASSANDRA_CONF["keyspace"]
        ) \
        .save()

    logger.info("5. Saved to Cassandra successfully")


# 6 RUN PIPELINE
def run_pipeline():
    logger.info("=" * 50)
    logger.info("START TRENDING PRODUCTS PIPELINE")
    logger.info("=" * 50)

    start_time = datetime.now()
    spark = None

    try:
        # 6.1 CREATE SPARK
        spark = create_spark()
        spark.sparkContext.setLogLevel("ERROR")

        # 6.2 LOAD EVENTS
        logger.info("Loading events data...")
        try:
            events = spark.read \
                .option("header", True) \
                .option("inferSchema", True) \
                .csv(INPUT_PATH + "events.csv")
            logger.info(f"Events count: {events.count()}")
        except Exception as e:
            logger.warning(f"Could not load events.csv. Error: {str(e)}. Creating empty events dataframe.")
            # Tạo dataframe rỗng nếu thiếu file để job không chết ngay lập tức
            from pyspark.sql.types import StructType, StructField, StringType, TimestampType
            schema = StructType([
                StructField("event_id", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("event_timestamp", TimestampType(), True)
            ])
            events = spark.createDataFrame([], schema)

        # 6.3 LOAD ORDERS
        logger.info("Loading orders data...")
        try:
            orders = spark.read \
                .option("header", True) \
                .option("inferSchema", True) \
                .csv(INPUT_PATH + "orders.csv")
            logger.info(f"Orders count: {orders.count()}")
        except:
            logger.warning("Could not load orders.csv")
            raise

        # 6.4 LOAD ORDER ITEMS
        logger.info("Loading order_items data...")
        try:
            order_items = spark.read \
                .option("header", True) \
                .option("inferSchema", True) \
                .csv(INPUT_PATH + "order_items.csv")
            logger.info(f"Order items count: {order_items.count()}")
        except:
            logger.warning("Could not load order_items.csv")
            raise

        # 6.5 LOAD PRODUCTS
        logger.info("Loading products data...")
        try:
            products = spark.read \
                .option("header", True) \
                .option("inferSchema", True) \
                .csv(INPUT_PATH + "products.csv")
            logger.info(f"Products count: {products.count()}")
        except:
            logger.warning("Could not load products.csv")
            # products is not strictly needed for the calculation if we only need product_id
            pass

        # 6.6 BUILD TRENDING PRODUCTS
        final_output = build_trending_products(
            events,
            orders,
            order_items,
            products if 'products' in locals() else None
        )

        logger.info(f"Final rows: {final_output.count()}")

        # 6.7 SAVE TO CASSANDRA
        save_to_cassandra(final_output)

        # 6.8 SUCCESS
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
