import os           # làm việc với file system
import sys          # dùng để in log ra terminal
import logging      # ghi log để theo dõi chương trình chạy
from datetime import datetime

from pyspark.sql import SparkSession                # cổng chính để dùng spark
from pyspark.sql.functions import col, lower, trim  # gọi cột, chuyển chữ thường, xóa khoảng trắng

# 1 ================= CONFIG =================
#DATASET_DIR = "1_dataset/raw_data"

# path đến data chưa làm sạch
INPUT_PATH = "s3a://datalake/raw/"
# path ra 
OUTPUT_PATH = "s3a://datalake/processed/"

CSV_FILES = ["orders.csv", "users.csv", "products.csv", "reviews.csv", "order_items.csv"]

# 1.2. Cấu hình đường dẫn để đọc ghi dữ liệu trong MinIO storage
MINIO_CONF = {
    "endpoint": "minio:9000",  # địa chỉ minio trong kind ở dòng 11 trong file minio-config.yaml
    "access_key": "minioadmin", # ở dòng 43->46 trong file minio-config.yaml
    "secret_key": "minioadmin",
}                                                                   #├── orders.csv
                                                                    #├── users.csv
                                                                    #├── products.csv

#2 ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,             # log từ mức INFO trở lên
    format="%(asctime)s [%(levelname)s] %(message)s",   # mỗi log có dạng 2026-04-27 14:30:01 [INFO] Spark job started
    handlers=[                                          # Output log có 2 nơi nhận log 1 là terminal, 2 là File tpark_pipeline.log
        logging.FileHandler("spark_pipeline.log"),      # -> tạo file
        logging.StreamHandler(sys.stdout)               # -> in ra terminal
    ]
)
logger = logging.getLogger(__name__)

#3 ================= SPARK SESSION =================
# hàm tạo SparSession
def get_spark():
    # -> appName = đặt tên job Spark
    # -> cài thư viện cần để Spark đọc S3 (MinIO)
    # -> sau đó spark.hadoop.fs.s3a.endpoint để trỏ tới MinIO
    # -> sau đó spark.hadoop.fs.s3a.access.key và spark.hadoop.fs.s3a.secret.key để xác thực
    # -> sau đó spark.hadoop.fs.s3a.connection.ssl.enabled để tắt HTTPS (SSL) khi Spark ->MinIO
    # -> sau đó spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem để khai báo khai báo filesystem S3A 
    # để Spark dùng S3A driver để làm việc vớ S3/MinIO
    # -> spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
    # để chỉ định cho Spark sử dụng SimpleAWSCredentialsProvider, 
    # tức là lấy trực tiếp access key và secret key từ config để xác thực khi kết nối tới MinIO/S3 thông qua S3A filesystem.
    return SparkSession.builder \
        .appName("CSV_to_MinIO") \
        .config("spark.jars",
                "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
                "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .getOrCreate()

# 4 ================= CLEAN =================
def clean_df(df):
    # 4.1 Lấy danh sách các cột có kiểu dữ liệu string
    string_cols = [c for c, t in df.dtypes if t == 'string']

    # 4.2 Duyệt từng cột string để chuẩn hóa dữ liệu
    for c in string_cols:
        df = df.withColumn(
            c,
            lower(          # chuyển về chữ thường (lowercase)
                trim(       # xóa khoảng trắng đầu và cuối chuỗi
                    col(c)  # lấy dữ liệu của cột c
                )
            )
        )

    # 4.3 Thay tất cả giá trị NULL trong DataFrame bằng chuỗi rỗng ""
    df = df.fillna("")

    # 4.4 Xóa các dòng mà tất cả các cột đều rỗng / NULL
    # how="all" nghĩa là chỉ xóa khi toàn bộ cột đều null
    df = df.na.drop(how="all")

    return df

# 5 ================= MAIN =================
def run_pipeline():
    # 5.1. logger start
    logger.info("="*50)
    logger.info("START SPARK PIPELINE CSV → MINIO")
    logger.info("="*50)

    # 5.3 tạo spark
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

 
    # 5.5 khai báo list chứa file
    success_files = []
    failed_files = []

    # 5.6 for từng file và ghi lại thời gian bắt đầu xử lý file để tính duration
    for file_name in CSV_FILES:
        start_time = datetime.now()
        #file_path = os.path.join(DATASET_DIR, file_name)

        logger.info(f"--- Processing: {file_name} ---")

        try:
            # 5.6.1. Read
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(INPUT_PATH + file_name)

            # 5.6.2. Clean
            df_clean = clean_df(df)

            # 5.6.3. Upload (ghi lên MinIO)
            #target_path = f"s3a://{MINIO_CONF['bucket']}/raw/{file_name}"

            df_clean.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(OUTPUT_PATH + file_name.replace(".csv", ""))

            duration = (datetime.now() - start_time).total_seconds()

            logger.info(f"[SUCCESS] {file_name} ({duration}s)")
            success_files.append(file_name)

        # 5.6.4 (Xử lý lỗi)
        except Exception as e:
            logger.error(f"[FAILED] {file_name}")
            logger.error(str(e))
            failed_files.append(file_name)

    # ================= SUMMARY =================
    logger.info("="*50)
    logger.info("SUMMARY")
    logger.info(f"Tổng file: {len(CSV_FILES)}")
    logger.info(f"Thành công: {len(success_files)}")
    logger.info(f"Thất bại: {len(failed_files)}")

    if failed_files:
        logger.warning(f"File lỗi: {failed_files}")

    logger.info("="*50)

    spark.stop()

# ================= RUN =================
if __name__ == "__main__":
    run_pipeline()