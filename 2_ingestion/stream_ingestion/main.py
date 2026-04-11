import os
from configs import config
from datetime import datetime
import subprocess
import json

# Kafka
from confluent_kafka import Consumer

# PyArrow
import pyarrow as pa
from pyarrow import avro as pavro


#----- TODO: 1 hàm tạo kafka consumer-----
def create_consumer():
    """
    tạo consumer để đọc dữ liệu từ fake_realtime.py từ topic ABC1
    """
    return 

#----- TODO: 2 hàm làm sạch dữ liệu nhận từ kafka-----
def clean_record(record):
    """
        - bỏ khoảng trắng: ví dụ " hue" -> "hue"
        - chuyển về lowercase nếu là text: "Ahd" -> "ahd"
        - nếu null thì chuyển về ""
        ví dụ
        {
            " Name ": "  AN  ",
            "Age": "20",
            "City ": null
        }

        =>  {
                "name": "an",
                "age": "20",
                "city": ""
            }
    """
    return 


#----- TODO: 3 tạo producer-----
def create_producer():
    return 


# ----- TODO: 4 đẩy dữ liệu topic ABC2----
def run_stream_cleaning():
    print("[STREAM CLEAN START]")
    """
        1. tạo consumer, producer
        2. đọc data từ topic ABC1 
        3. clean
        4. gửi sang topic ABC2
    """
    return

if __name__ == "__main__":
    run_stream_cleaning()