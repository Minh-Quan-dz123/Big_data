# 1 IMPORT THƯ VIỆN
# 1.1.
from pyspark.sql import SparkSession
# SparkSession:
# - Là "cổng chính" để làm việc với Spark.
# - Gần giống như object "main" của Spark.
# - Dùng để:
#       + đọc dữ liệu
#       + chạy SQL
#       + tạo DataFrame
#       + cấu hình Spark cluster
#
# pyspark.sql = module xử lý dữ liệu dạng bảng (DataFrame)

# 1.2.
from pyspark.sql.functions import col, when, sum as spark_sum, count as spark_count, avg, datediff, to_date, lit
# pyspark.sql.functions:
# - Chứa hàng trăm hàm xử lý dữ liệu DataFrame.
# Ví dụ:
#   col()           -> lấy cột
#   when()          -> IF ELSE
#   sum()           -> tính tổng
#   count()         -> đếm
#   current_date()  -> ngày hiện tại
#   datediff()      -> tính số ngày chênh lệch


# 1.3.
from pyspark.ml.feature import VectorAssembler
# VectorAssembler:Thuộc thư viện Machine Learning của Spark (MLlib)
# Nhiệm vụ: Gom nhiều cột số thành 1 vector feature.
# Ví dụ:
#   age = 20
#   income = 1000
#   orders = 5
# => gom thành: [20, 1000, 5]
#
# Machine Learning của Spark KHÔNG học trực tiếp từ nhiều cột rời rạc,
# nó cần 1 vector duy nhất.

# 1.4.
from pyspark.ml.feature import StandardScaler
# StandardScaler:Dùng để chuẩn hóa dữ liệu.
# Ví dụ:
#   tuổi:                18 -> 60
#   số đơn hàng:         0 -> 5000
#   cancellation_rate:   0 -> 1
# Nếu không chuẩn hóa: mô hình sẽ bị cột "5000" lấn át cột "0 -> 1"
# StandardScaler sẽ đưa dữ liệu về scale gần nhau.
# Công thức: z = (x - mean) / std
# mean = giá trị trung bình
# std  = độ lệch chuẩn

# 1.5.
from pyspark.ml.clustering import KMeans
# KMeans:Thuật toán Machine Learning dùng để phân cụm.
# Ví dụ:
#   nhóm 1: khách mới
#   nhóm 2: khách VIP
#   nhóm 3: khách nguy cơ rời bỏ
# KMeans KHÔNG biết trước tên nhóm.
# Nó chỉ chia thành các cụm dựa trên độ giống nhau.

# 1.6 các thư viện phụ
from datetime import datetime # - Thư viện thời gian của Python
import logging  # in log
import sys # thao tác với hệ thống Python

from datetime import datetime
def current_time() -> datetime: #2025-11-14 23:18:00
    return datetime(2025, 11, 14, 23, 18, 0)


# 2. CẤU HÌNH 

# 2.1 input path
INPUT_PATH = "s3a://datalake/processed/"

# 2.2 MinIO config (Trỏ về K8s Service)
MINIO_CONF = {
    "endpoint": "http://minio-service:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
}

# 2.3 MongoDB config
MONGODB_CONF = {
    "uri": "mongodb://mongodb-service.default.svc.cluster.local:27017/", # Trỏ về K8s Service nội bộ
    "database": "ecommerce",        # Tên database 
    "collection": "user_segment"              # Tên bảng kết quả
}

# 2.4 logging config (phụ)
logging.basicConfig(
    level=logging.INFO,                               # loại logging
    format="%(asctime)s [%(levelname)s] %(message)s", # format
    handlers=[
        logging.StreamHandler(sys.stdout) # Đẩy thẳng log ra stdout để xem bằng lệnh kubectl logs
    ]
)
logger = logging.getLogger(__name__)


# 3 KHỞI TẠO OBJECT SPARK
# tạo hàm để gọi khởi tạo object
def create_spark():
    # Khởi tạo URI kết nối MongoDB
    #mongo_uri = f"{MONGODB_CONF['uri']}/{MONGODB_CONF['database']}.{MONGODB_CONF['collection']}"
    return SparkSession.builder \
        .appName("UserSegmentationJob") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

# 4 BUIL USER SEGMENT

# 4.1. HÀM BUILD FEATURE ML
# input là 2 bảng users và orders
# output là tạo cột 
# - days_since_signup: số ngày kể từ lúc đăng ký tới giờ
# - total_orders: tổng số đơn
# - total_completed_orders: cột tổng số  đơn hoàn thành completed -> 1, khác -> 0
# - total_bad_orders: cột tổng đơn cancelled/returned->1, khác -> 0

def build_features(users, orders):
    logger.info("Building features....")

    # 1 xử lý cột signup days: convert-> kiểu data
    users = users.withColumn("signup_date", to_date(col("signup_date")))

    # 2 từ đó tạo days_since_signup
    users = users.withColumn(
        "days_since_signup",

        # sử dụng hàm datediff để trừ
        # ngày hiện tại (mô phỏng 2025-11-14) -> tới ngày trong bảng
        datediff(lit(current_time().strftime('%Y-%m-%d')), col("signup_date"))
    )


    # 3 tính cột đơn hàng tệ và hoàn thành
    # dùng groupBy để gom tất cả order theo user
    # sau đó cứ thế tính tổng order, đơn hoàn thành và tệ
    order_status = orders.groupBy("user_id").agg(
        # 3.1 tổng số đơn
        spark_count("order_id").alias("total_orders"),

        # 3.2 tổng các đơn đã hoàn thành: dùng when để giống if else
        spark_sum(
            when(col("order_status") == "completed", 1).otherwise(0) # giống if else
        ).alias("total_completed_orders"),

        # 3.3 tổng các đơn total_bad_orders
        spark_sum(
            when(col("order_status").isin(["cancelled", "returned"]),1).otherwise(0) # giống if else
        ).alias("total_bad_orders")
    )

    # 3.4. tính tỉ lệ đơn xấu
    order_status = order_status.withColumn(
        "cancellation_rate",
        when(col("total_orders")>0, col("total_bad_orders")/col("total_orders")).otherwise(0)
    )

    # 3.5 completion_rate
    order_status = order_status.withColumn(
        "completion_rate",
        when(col("total_orders")>0, col("total_completed_orders")/col("total_orders")).otherwise(0)
    )

    # 3.6 join users + orders -> chỉ lấy cột cần thiết
    data_feature = users.select("user_id", "days_since_signup").join(
        order_status,   on = "user_id",   how = "left" # left join theo user_id
    ).fillna(0) # thay NULL = 0

    # logger.info("Feature engineering completed")
    return data_feature # (user_id, days_since_sign, total_orders, total_.., cancellation rate)

# 4.2 KMEANS PROCESS
# hàm gom nhóm sau khi có data_feature
def run_kmeans(data_feature):
    logger.info("Starting KMeans clustering...")

    # 1. vector hóa
    # khai báo các cột input, output của vector
    assembler = VectorAssembler(
        inputCols=["days_since_signup", "total_orders", "completion_rate","cancellation_rate"],
        outputCol="features"
    )

    # 2. transform()
    # tạo vector
    feature_df = assembler.transform(data_feature)

    # 3. chuẩn hóa (tạo object và cấu hình scaler)
    scaler = StandardScaler(
        inputCol="features",
        outputCol="scaled_features",
        withStd=True, # chia cho std
        withMean=True # trừ mean
    )
    # học mean/std từ dataset
    scaler_model = scaler.fit(feature_df)
    
    # 4. transform()
    scaled_df = scaler_model.transform(feature_df)

    # 5 KMeans
    # khai báo object và config
    kmeans = KMeans(
        featuresCol="scaled_features", # vector input
        predictionCol= "cluster", #  cột out cluster
        k=4,                    # số cụm
        seed=50,            # random seed
    )

    # 6 train model
    model = kmeans.fit(scaled_df)

    # predict cluster
    predicted = model.transform(scaled_df)

    # 7 gán nhãn
    #D ùng giá trị trung bình thực tế
    logger.info("Calculating real cluster profiles for mapping...")

    # 7.1. Gom nhóm dữ liệu đã phân cụm để lấy giá trị thực tế unscaled đưa vào phân loại nhãn
    cluster_profiles = predicted.groupBy("cluster").agg(
        avg("total_orders").alias("avg_orders"),
        avg("completion_rate").alias("avg_completion"),
        avg("cancellation_rate").alias("avg_cancellation")
    ).collect()

    # 7.2 tạo dict lưu thông tin từng cluser
    cluster_info = {}
    for row in cluster_profiles:
        cluster_info[row['cluster']] = {
            "total_orders": row['avg_orders'],
            "completion_rate": row['avg_completion'],
            "cancellation_rate": row['avg_cancellation']
        }

    logger.info(f"Real Centroids profile (Unscaled): {cluster_info}")
    
    # chia cluster mua nhiều / mua ít
    sorted_by_orders = sorted(cluster_info.items(), key=lambda x: x[1]["total_orders"])
    low_order_clusters = [sorted_by_orders[0][0], sorted_by_orders[1][0]]
    high_order_clusters = [sorted_by_orders[2][0],sorted_by_orders[3][0]]

    # chia theo mua nhiều + hoàn thành cao + hủy thấp 
    # = Frequent Shoppers
    high_good = max(
        high_order_clusters,
        key=lambda c: cluster_info[c]["completion_rate"] - cluster_info[c]["cancellation_rate"]
    )

    # mua nhiều + completion thấp + hủy cao
    # => Risky Frequent Buyers
    high_bad = min(
        high_order_clusters,
        key=lambda c: cluster_info[c]["completion_rate"]- cluster_info[c]["cancellation_rate"]
    )

    # mua ít + completion khá + hủy thấp
    # => Low Frequency
    low_good = max(
        low_order_clusters,
        key=lambda c:cluster_info[c]["completion_rate"]- cluster_info[c]["cancellation_rate"]
    )

    # mua ít + completion thấp + hủy cao
    # => Bad Customer
    low_bad = min(
        low_order_clusters,
        key=lambda c:cluster_info[c]["completion_rate"] - cluster_info[c]["cancellation_rate"]
    )

    # 7.3 map

    predicted = predicted.withColumn(
        "segment_name",
        when(col("cluster") == high_good, "Frequent Shoppers")
        .when(col("cluster") == high_bad, "Risky Frequent Buyers")
        .when(col("cluster") == low_good, "Low Frequency")
        .otherwise("Bad Customer")
    )

    # 8 log
    logger.info("KMeans completed")

    return predicted

# 5.  SAVE TO MONGODB
def save_to_mongodb(df):
    logger.info("Saving to MongoDB...")

    final_df = df.select(
        "user_id",
        "days_since_signup",
        "total_orders",
        "total_completed_orders",
        "total_bad_orders",
        "completion_rate",
        "cancellation_rate",
        "cluster",
        "segment_name"
    )

    # ghi xuống MongoDB
    final_df.write \
        .format("mongodb") \
        .mode("append") \
        .option("spark.mongodb.write.connection.uri", MONGODB_CONF["uri"]) \
        .option("spark.mongodb.write.database", MONGODB_CONF["database"]) \
        .option("spark.mongodb.write.collection", MONGODB_CONF["collection"]) \
        .option("spark.mongodb.write.operationType", "update") \
        .option("spark.mongodb.write.idFieldList", "user_id") \
        .save()
    logger.info("Saved to MongoDB successfully")

# 6 RUN PROCESS FULL
def run_pipeline():
    logger.info("="*50)
    logger.info("START USER SEGMENTATION PIPELINE")
    logger.info("=" * 50)

    # bắt đầu thời gian
    start_time = datetime.now()

    # khởi tạo spark
    spark = None
    try:
        # 1 tạo spark
        spark = create_spark()
        spark.sparkContext.setLogLevel("ERROR")

        # 2 spark đọc csv từ minio
        logger.info("Loading users data from MinIO...")
        users = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(INPUT_PATH + "users.csv")

        logger.info("Loading orders data from MinIO...")
        orders = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(INPUT_PATH + "orders.csv")
        
        # 3 feature engineering
        data_feature = build_features(users, orders)

        # 4 KMeans process
        predicted = run_kmeans(data_feature)

        # 5 Ghi dữ liệu xuống tầng Serving Layer (MongoDB)
        save_to_mongodb(predicted)

        # print
        duration = (datetime.now() - start_time).total_seconds()

        logger.info(f"PIPELINE SUCCESS ({duration}s)")

    except Exception as ex:
        logger.error("PIPELINE FAILED")
        logger.error(str(ex)) # convert-> string
        raise #ném lỗi tiếp ra ngoài
    
    finally:
        if spark:   # nếu spark tồn tại
            # tắt spark
            spark.stop()
            logger.info("=" * 50)


if __name__ == "__main__":
    # chạy pipeline
    run_pipeline()




