import requests
import csv
import random
import time
import os
# chương trình giả lập hành vi người dùng
# user_id : U000001 -> U000100
# product được view, cart, purchase: P000001 -> P000100
# có ở local fake_realtime.py (chương trình này) có raw_data/products.csv
# Cấu trúc (product_id,product_name,category,brand,price,rating)
BASE_URL = "http://localhost:30070"

current_dir = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(current_dir, "..", "raw_data", "products.csv")

product_map = {}
def load_products():
    global product_map

    # mở file csv
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f) # đọc thành dictionary theo header

        # mỗi dòng sản phẩm lấy id, name, category
        count = 0
        for row in reader:
            product_id = row["product_id"]

            product_map[product_id] = {
                "product_name": row["product_name"],
                "category": row["category"]
            }
            '''
            Kết quả được
            "P000001": {
                    "product_name": "...",
                    "category": "..."
                }
            '''
            count+=1
            if count >= 100:
                break

    print(f"Loaded {len(product_map)} products")

# 1. HEALTH CHECK
def health_check():
    url = f"{BASE_URL}/"
    try:
        res = requests.get(url, timeout=3)
        # Bắt trường hợp Server chạy nhưng chưa định nghĩa API gốc "/"
        if res.status_code == 404:
            print("HEALTH: [Server hoạt động] nhưng chưa có endpoint gốc '/' (Bỏ qua)")
        else:
            print("HEALTH:", res.json())
    except Exception as e:
        print(f"HEALTH CHECK FAILED (Server FastAPI có thể chưa bật): {e}")


# 2. chương trình event vào api 0.5 giây 1 lần
# 2.1 GENERATE RANDOM USER
def random_user():
    user_id = random.randint(1, 100)
    return f"U{user_id:06d}"

# 2.2 GENERATE RANDOM PRODUCT
def random_product():
    if not product_map:
        return None

    # Tối ưu: Lấy random ngẫu nhiên trực tiếp từ các key đã load thành công từ file CSV
    # Đảm bảo 100% tỷ lệ trúng sản phẩm có thật, không bị hụt như hàm random cố định số trước đó
    pid = random.choice(list(product_map.keys()))
    info = product_map[pid]

    return {
        "product_id": pid,
        "product_name": info["product_name"],
        "category": info["category"]
    }

# 2.3 EVENT TYPE
def random_event_type():
    r = random.random()
    # 70% là view, 20% là cart, 10% là purchase
    if r < 0.7: 
        return "view"
    elif r < 0.9:
        return "cart"
    else:
        return "purchase"

# 3. BUILD EVENT
def build_event():
    product = random_product()
    if not product:
        return None

    return {
        "user_id": random_user(),
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "event_type": random_event_type()
    }


# 4. SEND EVENT
def send_event(event):
    try:
        url = f"{BASE_URL}/api/events"
        res = requests.post(url, json=event, timeout=2)
        print("SENT:", event["user_id"], event["product_id"], event["event_type"], "-> HTTP", res.status_code)
    except Exception as e:
        print(f"ERROR: Không thể gửi tới API (Lỗi kết nối) - {type(e).__name__}")

# 7. STREAM LOOP (0.5s)
def send_event_stream():
    print("Starting realtime simulation (0.5s/event)...")

    while True:
        event = build_event()

        if event:
            send_event(event)

        time.sleep(1)

# RUN DEMO FLOW
if __name__ == "__main__":
    print("=== START TEST ===")

    
    load_products()

    health_check()

    send_event_stream()

    print("=== DONE ===")