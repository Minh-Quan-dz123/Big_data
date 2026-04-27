import os
import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim

# ================= CONFIG =================
#DATASET_DIR = "1_dataset/raw_data"
DATASET_DIR = "/opt/spark/data"  # Đường dẫn trong container Spark

MINIO_CONF = {
    "endpoint": "minio-service:9000",  # minio-service:9000 chạy K8s
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "bucket": "datalake"
}

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("spark_pipeline.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ================= SPARK SESSION =================
def get_spark():
    return SparkSession.builder \
        .appName("CSV_to_MinIO") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

# ================= CLEAN =================
def clean_df(df):
    # Lấy danh sách các cột có kiểu dữ liệu string
    string_cols = [c for c, t in df.dtypes if t == 'string']

    # Duyệt từng cột string để chuẩn hóa dữ liệu
    for c in string_cols:
        df = df.withColumn(
            c,
            lower(          # chuyển về chữ thường (lowercase)
                trim(       # xóa khoảng trắng đầu và cuối chuỗi
                    col(c)  # lấy dữ liệu của cột c
                )
            )
        )

    # Thay tất cả giá trị NULL trong DataFrame bằng chuỗi rỗng ""
    df = df.fillna("")

    # Xóa các dòng mà tất cả các cột đều rỗng / NULL
    # how="all" nghĩa là chỉ xóa khi toàn bộ cột đều null
    df = df.na.drop(how="all")

    return df

# ================= MAIN =================
def run_pipeline():
    logger.info("="*50)
    logger.info("START SPARK PIPELINE CSV → MINIO")
    logger.info("="*50)

    if not os.path.exists(DATASET_DIR):
        logger.error(f"Không tìm thấy thư mục: {DATASET_DIR}")
        return

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    csv_files = [f for f in os.listdir(DATASET_DIR) if f.endswith(".csv")]

    if not csv_files:
        logger.warning("Không có file CSV nào")
        return

    success_files = []
    failed_files = []

    for file_name in csv_files:
        start_time = datetime.now()
        file_path = os.path.join(DATASET_DIR, file_name)

        logger.info(f"--- Processing: {file_name} ---")

        try:
            # 1. Read
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(f"file://{os.path.join(DATASET_DIR, file_name)}")

            # 2. Clean
            df_clean = clean_df(df)

            # 3. Upload (ghi lên MinIO)
            target_path = f"s3a://{MINIO_CONF['bucket']}/raw/{file_name}"

            df_clean.write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(target_path)

            duration = (datetime.now() - start_time).total_seconds()

            logger.info(f"[SUCCESS] {file_name} ({duration}s)")
            success_files.append(file_name)

        except Exception as e:
            logger.error(f"[FAILED] {file_name}")
            logger.error(str(e))
            failed_files.append(file_name)

    # ================= SUMMARY =================
    logger.info("="*50)
    logger.info("SUMMARY")
    logger.info(f"Tổng file: {len(csv_files)}")
    logger.info(f"Thành công: {len(success_files)}")
    logger.info(f"Thất bại: {len(failed_files)}")

    if failed_files:
        logger.warning(f"File lỗi: {failed_files}")

    logger.info("="*50)

    spark.stop()

# ================= RUN =================
if __name__ == "__main__":
    run_pipeline()