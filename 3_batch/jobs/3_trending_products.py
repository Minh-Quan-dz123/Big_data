# TRENDING PRODUCTS JOB
# INPUT (MinIO): s3a://datalake/processed/
<<<<<<< HEAD
#       - products.csv
#       - orders.csv
#       - order_items.csv
# OUTPUT (Cassandra): ecommerce.trending_products

# 1 IMPORT thư viện
# 1.1 SparkSession
from pyspark.sql import SparkSession

# 1.2 Spark SQL functions
from pyspark.sql.functions import (
    col,
    count,
    sum,
    mean,
    max,
    min,
    date_format,
    to_timestamp,
    datediff,
    when,
    lit,
    round,
    current_timestamp,
    window as spark_window
)

# 1.3 Window function
from pyspark.sql.window import Window

# 1.4 Python libraries
from datetime import datetime, timedelta
=======
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

>>>>>>> 84d55bd1b8b6f274070f3cbb42babc5450551705
import logging
import sys

# 2 CONFIG
<<<<<<< HEAD

# 2.1 INPUT PATH (MinIO)
INPUT_PATH = "s3a://datalake/processed/"

# 2.2 MINIO CONFIG
=======
INPUT_PATH = "s3a://datalake/processed/"

>>>>>>> 84d55bd1b8b6f274070f3cbb42babc5450551705
MINIO_CONF = {
    "endpoint": "minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
}

<<<<<<< HEAD
# 2.3 CASSANDRA CONFIG
=======
>>>>>>> 84d55bd1b8b6f274070f3cbb42babc5450551705
CASSANDRA_CONF = {
    "host": "cassandra",
    "keyspace": "ecommerce",
    "table": "trending_products"
}

<<<<<<< HEAD
# 2.4 LOGGING CONFIG
=======
>>>>>>> 84d55bd1b8b6f274070f3cbb42babc5450551705
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
<<<<<<< HEAD

=======
>>>>>>> 84d55bd1b8b6f274070f3cbb42babc5450551705
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
<<<<<<< HEAD
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
=======
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
>>>>>>> 84d55bd1b8b6f274070f3cbb42babc5450551705
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ) \
<<<<<<< HEAD
        .config("spark.cassandra.connection.host",CASSANDRA_CONF["host"]) \
        .getOrCreate()


# 4 CALCULATE TRENDING METRICS
# tính view growth và order growth trong các khoảng thời gian (7d, 30d)
def calculate_trending_metrics(products, orders, order_items):

    logger.info("4. Calculating trending metrics...")

    # 4.1 Convert order_date to timestamp
    orders = orders.withColumn("order_date", to_timestamp(col("order_date")))
    logger.info("4.1 Order date converted to timestamp")

    # 4.2 Join orders + order_items + products
    order_product_data = orders.select(
        "order_id",
        "order_date"
    ).join(
        order_items.select("order_id", "product_id"),
        on="order_id",
        how="inner"
    ).join(
        products.select("product_id"),
        on="product_id",
        how="inner"
    )

    logger.info("4.2 Orders, items, and products joined")

    # 4.3 Calculate metrics for 7-day window
    # tính order count, view (có thể dùng order count như proxy)
    current_date = orders.agg(max("order_date")).collect()[0][0]
    seven_days_ago = current_date - timedelta(days=7)
    thirty_days_ago = current_date - timedelta(days=30)

    # Metrics for last 7 days
    orders_7d = order_product_data.filter(
        col("order_date") >= seven_days_ago
    ).groupBy("product_id").agg(
        count("order_id").alias("order_count_7d")
    )

    # Metrics for last 30 days
    orders_30d = order_product_data.filter(
        col("order_date") >= thirty_days_ago
    ).groupBy("product_id").agg(
        count("order_id").alias("order_count_30d")
    )

    # Metrics for previous 7 days (7-14 days ago)
    prev_7d_start = seven_days_ago - timedelta(days=7)
    orders_prev_7d = order_product_data.filter(
        (col("order_date") >= prev_7d_start) &
        (col("order_date") < seven_days_ago)
    ).groupBy("product_id").agg(
        count("order_id").alias("order_count_prev_7d")
    )

    # Metrics for previous 30 days (30-60 days ago)
    prev_30d_start = thirty_days_ago - timedelta(days=30)
    orders_prev_30d = order_product_data.filter(
        (col("order_date") >= prev_30d_start) &
        (col("order_date") < thirty_days_ago)
    ).groupBy("product_id").agg(
        count("order_id").alias("order_count_prev_30d")
    )

    logger.info("4.3 Time window metrics calculated")

    # 4.4 Join all metrics
    trending_7d = orders_7d.join(orders_prev_7d, on="product_id", how="left").fillna(0)
    trending_30d = orders_30d.join(orders_prev_30d, on="product_id", how="left").fillna(0)

    logger.info("4.4 Metrics joined")

    # 4.5 Calculate growth rates
    # order_growth = (current - previous) / previous
    # nếu previous = 0 thì growth = current * 2 (double)
    trending_7d = trending_7d.withColumn(
        "order_growth",
        when(
            col("order_count_prev_7d") == 0,
            col("order_count_7d") * 2
        ).otherwise(
            round(
                (col("order_count_7d") - col("order_count_prev_7d")) / col("order_count_prev_7d"),
                2
            )
        )
    ).withColumn(
        "view_growth",
        col("order_growth") * 0.95  # view growth slightly less than order growth
    ).withColumn(
        "trend_window",
        lit("7d")
    )

    trending_30d = trending_30d.withColumn(
        "order_growth",
        when(
            col("order_count_prev_30d") == 0,
            col("order_count_30d") * 2
        ).otherwise(
            round(
                (col("order_count_30d") - col("order_count_prev_30d")) / col("order_count_prev_30d"),
                2
            )
        )
    ).withColumn(
        "view_growth",
        col("order_growth") * 0.95
    ).withColumn(
        "trend_window",
        lit("30d")
    )

    logger.info("4.5 Growth rates calculated")

    # 4.6 Union both windows
    all_trending = trending_7d.union(
        trending_30d
    ).select(
        "product_id",
        "order_growth",
        "view_growth",
        "trend_window"
    )

    logger.info("4.6 Trending data merged")

    return all_trending


# 5 CALCULATE TREND SCORE
# combine view_growth + order_growth thành 1 điểm
def calculate_trend_score(trending_df):

    logger.info("5. Calculating trend score...")

    # 5.1 Normalize growth rates to 0-100 scale
    # trend_score = (order_growth * 0.6 + view_growth * 0.4) * 50 + 50
    # capped at 100
    trend_score = (
        col("order_growth") * 0.6 + col("view_growth") * 0.4
    ) * 50 + 50

    trending_with_score = trending_df.withColumn(
        "trend_score",
        when(
            trend_score > 100,
            lit(100)
        ).otherwise(
            when(
                trend_score < 0,
                lit(0)
            ).otherwise(
                round(trend_score, 2)
            )
        )
    )

    logger.info("5.1 Trend score calculated")

    return trending_with_score


# 6 BUILD FINAL OUTPUT
def build_trending_products(products, orders, order_items):

    logger.info("6. Building trending products output...")

    # 6.1 Calculate metrics
    trending_metrics = calculate_trending_metrics(products, orders, order_items)

    # 6.2 Calculate score
    final_output = calculate_trend_score(trending_metrics)

    # 6.3 Add trend date
    final_output = final_output.withColumn(
        "trend_date",
        current_timestamp()
    )

    # 6.4 Reorder columns
    final_output = final_output.select(
=======
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
>>>>>>> 84d55bd1b8b6f274070f3cbb42babc5450551705
        "product_id",
        "trend_score",
        "view_growth",
        "order_growth",
        "trend_window",
        "trend_date"
    )

<<<<<<< HEAD
    logger.info("6.4 Final output completed")

    return final_output


# 7 SAVE TO CASSANDRA
def save_to_cassandra(df):

=======
    return final_output


# 5 SAVE TO CASSANDRA
def save_to_cassandra(df):
>>>>>>> 84d55bd1b8b6f274070f3cbb42babc5450551705
    logger.info("Saving to Cassandra...")

    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("overwrite") \
        .options(
            table=CASSANDRA_CONF["table"],
            keyspace=CASSANDRA_CONF["keyspace"]
        ) \
        .save()

<<<<<<< HEAD
    logger.info("7. Saved to Cassandra successfully")


# 8 RUN PIPELINE
def run_pipeline():

=======
    logger.info("5. Saved to Cassandra successfully")


# 6 RUN PIPELINE
def run_pipeline():
>>>>>>> 84d55bd1b8b6f274070f3cbb42babc5450551705
    logger.info("=" * 50)
    logger.info("START TRENDING PRODUCTS PIPELINE")
    logger.info("=" * 50)

    start_time = datetime.now()
<<<<<<< HEAD

    spark = None

    try:

        # 8.1 CREATE SPARK
        spark = create_spark()
        spark.sparkContext.setLogLevel("ERROR")

        # 8.2 LOAD PRODUCTS
        logger.info("Loading products data...")
        products = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(INPUT_PATH + "products.csv")

        logger.info(f"Products count: {products.count()}")

        # 8.3 LOAD ORDERS
        logger.info("Loading orders data...")

        orders = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(INPUT_PATH + "orders.csv")

        logger.info(f"Orders count: {orders.count()}")

        # 8.4 LOAD ORDER ITEMS
        logger.info("Loading order_items data...")

        order_items = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(INPUT_PATH + "order_items.csv")

        logger.info(f"Order items count: {order_items.count()}")

        # 8.5 BUILD TRENDING PRODUCTS
        final_output = build_trending_products(
            products,
            orders,
            order_items
=======
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
>>>>>>> 84d55bd1b8b6f274070f3cbb42babc5450551705
        )

        logger.info(f"Final rows: {final_output.count()}")

<<<<<<< HEAD
        # 8.6 SAVE TO CASSANDRA
        save_to_cassandra(final_output)

        # 8.7 SUCCESS
        duration = (datetime.now() - start_time).total_seconds()

=======
        # 6.7 SAVE TO CASSANDRA
        save_to_cassandra(final_output)

        # 6.8 SUCCESS
        duration = (datetime.now() - start_time).total_seconds()
>>>>>>> 84d55bd1b8b6f274070f3cbb42babc5450551705
        logger.info(f"PIPELINE SUCCESS ({duration}s)")

    except Exception as ex:
        logger.error("PIPELINE FAILED")
        logger.error(str(ex))
        raise

    finally:
        if spark:
            spark.stop()
<<<<<<< HEAD

=======
>>>>>>> 84d55bd1b8b6f274070f3cbb42babc5450551705
        logger.info("=" * 50)


# MAIN
if __name__ == "__main__":
    run_pipeline()
