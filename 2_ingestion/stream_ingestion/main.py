import os
from configs import config
from datetime import datetime
import subprocess
import json

# Kafka
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

# PyArrow
import pyarrow as pa
from pyarrow import avro as pavro

#thu vien
#pip install confluent-kafka

#----- TODO: 1 hàm tạo kafka consumer-----
def create_consumer():
    """
    tạo consumer để đọc dữ liệu từ fake_realtime.py từ topic ABC1
    """
    conf={
        "bootstrap.servers": config.KAFKA_BROKER,
        "group.id": "stream-ingestion-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    }
    consumer=Consumer(conf)
    consumer.subscribe([config.EVENT_RAW_TOPIC])
    return consumer

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
    if not isinstance(record, dict):
        return {}
    
    cleaned={}
    
    for key, value in record.items():
        
        #normalize key
        new_key=key.strip().lower()
        
        #handle null value
        if value is None:
            cleaned[new_key]=""
            continue
        
        #string clean
        if(isinstance(value, str)):
            cleaned[new_key]=value.strip().lower()
        else:
            cleaned[new_key]=value
    return cleaned


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