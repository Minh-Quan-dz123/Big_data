# import thư viện
import pandas as pd                 # đọc file csv
import json                         # đọc file JSON
import time                         # các hàm thời gian như delay
from kafka import KafkaProducer     # gửi dữ liệu lên kafka
from datetime import datetime       # các hàm thời gian

# 1. Cấu hình các tham số
KAFKA_BROKER = 'localhost:9092'  # Địa chỉ Kafka Broker chạy ở local
TOPIC_NAME = 'user_events'       # Tên topic bạn sẽ gửi dữ liệu vào
# DATA_PATH = './raw_data/ecommerce_dataset/events.csv' 
# # Đường dẫn file dữ liệu
DATA_PATH = 'D:/clone clone/Big_data/1_dataset/raw_data/ecommerce_dataset/events.csv'
DELAY_SECONDS = 2                # "n" giây giả lập mỗi hành động

def json_serializer(data):
    """Hàm để convert dữ liệu sang định dạng JSON và encode thành bytes"""
    return json.dumps(data).encode('utf-8')

def start_simulation():
    # 2. Khởi tạo Kafka Producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=json_serializer
        )
        print(f"--- Kết nối thành công tới Kafka tại {KAFKA_BROKER} ---")
    except Exception as e:
        print(f"--- Lỗi kết nối Kafka: {e} ---")
        return

    # 3. Đọc nguồn dữ liệu (CSV)
    try:
        df = pd.read_csv(DATA_PATH)
        # Chuyển đổi dữ liệu sang list các dictionary để dễ xử lý
        events = df.to_dict(orient='records')
        print(f"--- Đã nạp {len(events)} bản ghi từ file ---")
    except Exception as e:
        print(f"--- Lỗi đọc file: {e} ---")
        return

    # 4. Vòng lặp giả lập realtime
    print(f"--- Bắt đầu giả lập. Tần suất: {DELAY_SECONDS}s/event ---")
    for event in events:
        # Thêm timestamp hiện tại để giả lập thời gian thực chính xác hơn
        event['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Gửi dữ liệu lên Kafka
        producer.send(TOPIC_NAME, value=event)
        
        print(f"Đã gửi: {event['user_id']} - {event['event_type']} cho sản phẩm {event['product_id']}")
        
        # Nghỉ n giây trước khi gửi event tiếp theo
        time.sleep(DELAY_SECONDS)

    print("--- Hoàn thành gửi dữ liệu ---")

if __name__ == "__main__":
    start_simulation()