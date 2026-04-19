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
    conf={
        "bootstrap.servers": config.KAFKA_BROKER,
        "acks": "all",
        "retries": 3,
        "linger.ms": 5,
        "compression.type": "snappy"
        
    }
    return Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# ----- TODO: 4 đẩy dữ liệu topic ABC2----
def run_stream_cleaning():
    print("[STREAM CLEAN START]")

    """
    Pipeline xử lý stream:
    1. Tạo Kafka consumer + producer
    2. Đọc dữ liệu từ topic ABC1
    3. Làm sạch dữ liệu (clean)
    4. Gửi dữ liệu đã clean sang topic ABC2
    """

    # 1. Khởi tạo consumer để đọc dữ liệu từ Kafka (ABC1)
    consumer = create_consumer()

    # 2. Khởi tạo producer để gửi dữ liệu sang Kafka (ABC2)
    producer = create_producer()

    try:
        # Loop chạy liên tục để consume stream
        while True:

            # 3. Đọc message từ Kafka
            msg = consumer.poll(1.0)  # chờ 1s để lấy message mới

            # Nếu không có message thì bỏ qua vòng lặp
            if msg is None:
                continue

            # Nếu message có lỗi từ Kafka
            if msg.error():
                # End of partition thì bỏ qua
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue

                # Lỗi khác thì log ra
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                # 4. Decode dữ liệu từ Kafka
                raw = msg.value().decode("utf-8")

                # Parse JSON string -> dict
                data = safe_parse(raw)
                
                if data is None:
                    continue

                print("\n[RAW DATA]", data)

                # 5. CLEAN DATA
                # clean_record: hàm xử lý dữ liệu (lọc null, format, chuẩn hóa,...)
                cleaned = clean_record(data)

                # Thêm timestamp xử lý
                cleaned["processed_at"] = datetime.now().isoformat()

                print("[CLEANED DATA]", cleaned)

                # 6. GỬI SANG TOPIC ABC2
                producer.produce(
                    topic=config.EVENT_CLEANED_TOPIC,  # topic đích
                    value=json.dumps(cleaned).encode("utf-8"),
                    callback=delivery_report  # callback báo gửi thành công/thất bại
                )

                # Flush nhẹ để đẩy message đi ngay
                producer.poll(0)

                # Commit offset để Kafka biết message đã xử lý xong
                consumer.commit(asynchronous=True)

            except json.JSONDecodeError as e:
                # Lỗi dữ liệu không phải JSON hợp lệ
                print(f"JSON decode error: {e}")

            except Exception as e:
                # Lỗi xử lý chung
                print(f"Error processing message: {e}")

    except KeyboardInterrupt:
        print("Stream cleaning stopped by user.")

    finally:
        # Cleanup tài nguyên
        consumer.close()
        producer.flush()

#ext safe_parse_json(json_str):
def safe_parse(raw):
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        print(f"Invalid JSON: {raw}")
        return None

# =========================
# ENTRY POINT
# =========================
if __name__ == "__main__":
    run_stream_cleaning()