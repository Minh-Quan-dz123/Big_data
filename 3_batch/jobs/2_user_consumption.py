# USER CONSUMPTION PROFILE JOB
# INPUT (MinIO): s3a://datalake/processed/
#       - users.csv
#       - orders.csv
#       - order_items.csv
#       - products.csv
# OUTPUT (Cassandra): ecommerce.user_consumption_profile

# 1 IMPORT thư viện
# 1.1 SparkSession
from pyspark.sql import SparkSession

# 1.2 Spark SQL functions
from pyspark.sql.functions import (
    col,
    mean,
    sum,
    count,
    row_number,
    to_timestamp,
    date_format,
    collect_list,
    coalesce,
    lit
)

# 1.3 Window function
from pyspark.sql.window import Window # thao tác với nhóm dữ liệu

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
    "table": "user_consumption_profile"
}

# 2.4 LOGGING CONFIG
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("user_consumption_profile.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# 3 CREATE SPARK
def create_spark():

    return SparkSession.builder \
        .appName("UserConsumptionProfileJob") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.cassandra.connection.host", CASSANDRA_CONF["host"]) \
        .getOrCreate()


# 4 BUILD USER CONSUMPTION PROFILE
# đầu vào là các bảng users, orders, order_item và product
# tính toán bảng để cho ra user_consumption_profile
def build_user_consumption_profile(users,orders,order_items,products):

    logger.info("4. Building user consumption profile...")
    
    # 4.1 lấy ra completed_orders
    # trong bảng orders lọc bỏ hết chỉ giữ lại các dòng mà order_status = completed
    completed_orders = orders.filter(col("order_status") == "completed")
    logger.info(f"4.1. Completed orders: {completed_orders.count()}")

    # 4.2 AVG ORDER VALUE
    # trong bảng completed_orders tiếp tục nhóm các hàng theo user_id thành từng nhóm
    # => .agg = tính toán trên từng nhóm 
    # => nghĩa là từng nhóm user_id tính trung bình cột total_amount
    # => sau đó đổi tên cột total_amount thành avg_order_value
    # => kết quả là tạo ra bảng chỉ có user_id và avg_order_value
    avg_order_value = completed_orders.groupBy("user_id").agg(
        mean("total_amount").alias("avg_order_value")
    )
    logger.info("4.2. AVG ORDER VALUE completed")

    # 4.3 MONTHLY SPENDING
    # B1: convert order_date -> timestamp
    completed_orders = completed_orders.withColumn("order_date", to_timestamp(col("order_date")))

    # B2: tạo year_month chỉ lấy năm và tháng
    completed_orders = completed_orders.withColumn("year_month", date_format(col("order_date"), "yyyy-MM"))

    # B3: tổng tiền theo tháng
    # tạo bảng [user_id, year_month, monthly_spending]
    monthly_spending = completed_orders.groupBy("user_id","year_month").agg(
        sum("total_amount").alias("monthly_spending")
    )
    logger.info("4.3. Monthly spending completed")

    # 4.4 LẤY THÁNG GẦN NHẤT
    # định nghĩa latest_window: window partition
    # chia dữ liệu thành từng nhóm user_id (partition)
    # trong từng user, sắp xếp năm tháng giảm dần
    latest_window = Window.partitionBy("user_id").orderBy(col("year_month").desc())

    # từ bảng monthly_speding ta đánh số hàng bằng latest_window trước đó định nghĩa
    # =>sau đó gọi filter để giữ lại những hàng rn = 1 (tháng gần nhất)
    # => sau đó loại cột rn và year_month => còn lại [user_id, monthly_spending] (của tháng gần nhất)
    latest_monthly = monthly_spending.withColumn("rn",row_number().over(latest_window)
    ).filter(col("rn") == 1).drop("rn")

    logger.info("4.4. Latest monthly spending completed")

    # 4.5 USER BEHAVIOR
    # join completed_orders với latest_monthly
    # theo:
    #   user_id
    #   year_month
    # => đảm bảo chỉ lấy order trong tháng gần nhất
    latest_orders = completed_orders.join(
        latest_monthly.select("user_id", "year_month"),
        on=["user_id", "year_month"],
        how="inner"
    )
    logger.info("4.5 Latest orders completed")

    # 4.6 JOIN ORDER_ITEMS = latest_orders + order_items
    behavior_df = latest_orders.join(
        order_items,
        on=["order_id", "user_id"],
        how="inner" # chỉ giữ dòng khớp ở cả 2 bảng
    )
    logger.info("4.6 Join order_items completed")

    # 4.7 JOIN PRODUCTS: latest_orders + order_items + products
    behavior_df = behavior_df.join(
        products,
        on="product_id",
        how="left"
    )
    logger.info("4.7 Join products completed")

    # 4.8 FILL NULL
    behavior_df = behavior_df.withColumn("category", coalesce(col("category"), lit("unknown")))
    behavior_df = behavior_df.withColumn("product_name",coalesce(col("product_name"), lit("unknown")))
    behavior_df = behavior_df.withColumn("price",coalesce(col("price"), lit(0)))

    logger.info("4.8 Fill null completed")


    # 4.9 GOM TOÀN BỘ SẢN PHẨM TRONG THÁNG GẦN NHẤT
    top_behavior = behavior_df.groupBy("user_id"
    ).agg(
        collect_list("product_id").alias("product_ids_in_latest_month"),
        collect_list("product_name").alias("products_in_latest_month"),
        collect_list("category").alias("categories_in_latest_month"),
        collect_list("price").alias("product_prices_in_latest_month")
    )

    logger.info("4.9 User behavior completed")

    # 4.10 FINAL OUTPUT

    final_output = users.select("user_id")

    # join avg_order_value
    final_output = final_output.join(
        avg_order_value,
        on="user_id",
        how="left"
    )

    # join latest_monthly
    final_output = final_output.join(
        latest_monthly.select(
            "user_id",
            "monthly_spending"
        ),
        on="user_id",
        how="left"
    )

    # join top_behavior
    final_output = final_output.join(
        top_behavior,
        on="user_id",
        how="left"
    )

    # 4.11 FILL NULL
    final_output = final_output.fillna({
        "avg_order_value": 0,
        "monthly_spending": 0,
    })

    logger.info("4.11 Final output completed")

    return final_output


# 5 SAVE TO CASSANDRA
def save_to_cassandra(df):

    logger.info("Saving to Cassandra...")

    final_df = df.select(
        "user_id",
        "avg_order_value",
        "monthly_spending",
        "product_ids_in_latest_month",
        "products_in_latest_month",
        "categories_in_latest_month",
        "product_prices_in_latest_month"
    )

    final_df.write \
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
    logger.info("START USER CONSUMPTION PROFILE PIPELINE")
    logger.info("=" * 50)

    start_time = datetime.now()

    spark = None

    try:

        # 6.1 CREATE SPARK
        spark = create_spark()
        spark.sparkContext.setLogLevel("ERROR")

        # 6.2 LOAD USERS
        logger.info("Loading users data...")
        users = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(INPUT_PATH + "users.csv")

        logger.info(f"Users count: {users.count()}")

        # 6.3 LOAD ORDERS
        logger.info("Loading orders data...")

        orders = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(INPUT_PATH + "orders.csv")

        logger.info(f"Orders count: {orders.count()}")

        # 6.4 LOAD ORDER ITEMS
        logger.info("Loading order_items data...")

        order_items = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(INPUT_PATH + "order_items.csv")

        logger.info(
            f"Order items count: {order_items.count()}"
        )

        # 6.5 LOAD PRODUCTS
        logger.info("Loading products data...")

        products = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(INPUT_PATH + "products.csv")

        logger.info(
            f"Products count: {products.count()}"
        )

        # 6.6 BUILD PROFILE
        final_output = build_user_consumption_profile(
            users,
            orders,
            order_items,
            products
        )

        logger.info(
            f"Final rows: {final_output.count()}"
        )

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