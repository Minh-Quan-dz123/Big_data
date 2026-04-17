# nhiệm vụ Dataset -> đọc + clean -> lưu vào HDFS

# nhớ cài thêm thư viện confluent-kafka và pyarrow
import os
from configs import config #sử dụng các biến cấu hình
from datetime import datetime
import subprocess # chạy lệnh hệ thống

import pyarrow as pa
import pyarrow.csv as pv
from pyarrow import avro as pavro 

# có thể import thêm các thư viện khác
# lưu ý - chưa có HDFS setup



# -----TODO: 1 hàm clean dữ liệu -----
def clean_table(table):
    """
        - bỏ khoảng trắng: ví dụ " hue" -> "hue"
        - chuyển về lowercase nếu là text: "Ahd" -> "ahd"
        - nếu null thì chuyển về ""
    """
    return

# -----TODO: 2 hàm/các hàm ghi dữ liệu vào HDFS -----
"""
    làm nhiệm vụ ghi dữ liệu đã làm sạch vào path nào đó (chưa cấu hình)
    của HDFS ở định dạng Avro
"""


# ----- BATCH PIPELINE-----
def run_batch_ingestion():
    print("[BATCH START]")
    """
        duyệt file trong folder 1_dataset/raw_data
        tiến hình đọc -> clean -> ghi avro local -> upload HDFS
    """
    return


if __name__ == "__main__":
    run_batch_ingestion()