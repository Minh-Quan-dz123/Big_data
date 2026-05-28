# Script đọc file CSV từ local, clean data cơ bản và đẩy lên MinIO
# Cần cài: pip install minio pandas

import os
import io
import logging
from datetime import datetime

import pandas as pd
from minio import Minio
from minio.error import S3Error

# Setup logging: vừa in ra màn hình terminal vừa ghi log ra file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("upload_log.txt", mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# --- Cấu hình MinIO (Hỗ trợ cả môi trường có và không có biến môi trường) ---
# Nếu chạy trực tiếp không cấu hình, sẽ tự lấy giá trị mặc định bên phải
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_SECURE     = os.getenv("MINIO_SECURE", "False").lower() == "true"

BUCKET_NAME = "cleaned-data"
CSV_FOLDER  = "./1_dataset"        # Thư mục gốc chứa data


def connect_minio() -> Minio:
    """Tạo connection tới MinIO client"""
    client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    log.info(f"Đã init MinIO client tại: {MINIO_ENDPOINT}")
    return client


def ensure_bucket(client: Minio, bucket: str) -> None:
    """Check xem bucket có chưa, chưa có thì tạo mới"""
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        log.info(f"Tạo thành công bucket mới: '{bucket}'")
    else:
        log.info(f"Bucket '{bucket}' đã có sẵn.")


def clean_dataframe(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """
    Clean data cơ bản trước khi đẩy lên:
    - Loại bỏ các dòng trống hoàn toàn
    - Chuẩn hóa tên cột: lowercase, thay khoảng trắng bằng underscore
    - Trim text thừa ở các cột dạng chuỗi
    """
    original_rows = len(df)

    # 1. Xóa dòng rỗng
    df.dropna(how="all", inplace=True)

    # 2. Chuẩn hóa tên cột
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^\w]", "_", regex=True)
    )

    # 3. Clean text trong các ô dữ liệu
    text_cols = df.select_dtypes(include="object").columns
    df[text_cols] = df[text_cols].fillna("").apply(lambda col: col.str.strip())

    dropped = original_rows - len(df)
    if dropped > 0:
        log.info(f"  [{filename}] Đã loại bỏ {dropped} dòng rỗng.")

    log.info(f"  [{filename}] Data sau clean: {len(df)} dòng, {len(df.columns)} cột.")
    return df


def upload_one_file(client: Minio, bucket: str, local_path: str) -> bool:
    """Luồng xử lý 1 file: Đọc -> Clean -> Chuyển thành byte buffer -> Đẩy lên MinIO"""
    filename = os.path.basename(local_path)

    try:
        # Đọc CSV
        df = pd.read_csv(local_path, encoding="utf-8")
        log.info(f"  [{filename}] Load xong {len(df)} dòng từ local.")

        # Clean data
        df = clean_dataframe(df, filename)

        # Chuyển DF thành buffer trong RAM để đẩy thẳng lên MinIO
        buffer = io.BytesIO()
        df.to_csv(buffer, index=False, encoding="utf-8")
        buffer.seek(0)
        size = buffer.getbuffer().nbytes  

        # Upload
        client.put_object(
            bucket_name=bucket,
            object_name=filename,
            data=buffer,
            length=size,
            content_type="text/csv",
        )

        log.info(f"  ✅ Thành công: '{filename}' -> bucket '{bucket}' ({size:,} bytes)")
        return True

    except FileNotFoundError:
        log.error(f"  ❌ Không tìm thấy file: {local_path}")
    except pd.errors.ParserError as e:
        log.error(f"  ❌ Lỗi định dạng CSV ở file '{filename}': {e}")
    except S3Error as e:
        log.error(f"  ❌ Lỗi kết nối MinIO khi đẩy '{filename}': {e}")
    except Exception as e:
        log.error(f"  ❌ Lỗi không xác định ở '{filename}': {e}")
    
    return False


def run_pipeline() -> None:
    """Hàm main điều phối toàn bộ quy trình"""
    log.info("=" * 55)
    log.info("  🚀 START PIPELINE UPLOAD CSV LÊN MINIO")
    log.info(f"  Thời gian: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 55)

    # 1. Connect & Check bucket
    try:
        client = connect_minio()
        ensure_bucket(client, BUCKET_NAME)
    except S3Error as e:
        log.critical(f"Không thể kết nối MinIO: {e}")
        log.critical("Vui lòng đảm bảo đã chạy: kubectl port-forward svc/minio-service 9000:9000 -n bigdata")
        return

    # 2. Quét file trong thư mục
    if not os.path.isdir(CSV_FOLDER):
        log.error(f"Thư mục chứa dataset không tồn tại: '{CSV_FOLDER}'")
        return

    csv_files = [
        os.path.join(root, f)
        for root, _, files in os.walk(CSV_FOLDER)
        for f in files if f.lower().endswith(".csv")
    ]

    if not csv_files:
        log.warning(f"Không tìm thấy file .csv nào trong: '{CSV_FOLDER}'")
        return

    log.info(f"Đã quét được {len(csv_files)} file CSV cần xử lý.")
    log.info("-" * 55)

    # 3. Đẩy từng file
    success, failed = [], []
    for path in csv_files:
        log.info(f"Đang xử lý: {os.path.basename(path)}")
        if upload_one_file(client, BUCKET_NAME, path):
            success.append(path)
        else:
            failed.append(path)
        log.info("-" * 55)

    # 4. In báo cáo tổng kết
    log.info("=" * 55)
    log.info("  📊 BÁO CÁO KẾT QUẢ")
    log.info(f"  Tổng số file : {len(csv_files)}")
    log.info(f"  ✅ Thành công: {len(success)}")
    log.info(f"  ❌ Thất bại  : {len(failed)}")

    if failed:
        log.warning("  Danh sách file lỗi:")
        for f in failed:
            log.warning(f"    - {os.path.basename(f)}")
    log.info("=" * 55)

if __name__ == "__main__":
    run_pipeline()