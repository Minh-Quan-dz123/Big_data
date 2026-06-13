from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from confluent_kafka import Producer
import redis
import os
import json
import logging
from datetime import datetime, timezone
from fastapi.middleware.cors import CORSMiddleware

# --- CẤU HÌNH LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("ServingAPI")


app = FastAPI(title="Serving Layer - Realtime Recommendation")
origins = [
    "http://localhost:3000",
    "http://localhost:5173",
    # Thêm domain production của bạn vào đây khi deploy
    # "https://my-ecommerce-app.com",
]

app.add_middleware(
    CORSMiddleware,     # <--- ĐÂY LÀ THAM SỐ BẠN ĐANG THIẾU
    allow_origins=origins,
    allow_credentials=True, # (Khuyên dùng) Cho phép gửi cookie/token
    allow_methods=["*"],
    allow_headers=["*"],
)

logger.info("Khởi tạo kết nối Redis, MongoDB và Kafka...")

# 2.1. Redis Client
redis_client = redis.Redis(
    host="127.0.0.1",
    port=6379,
    decode_responses=True
)

# 2.2. MongoDB Client
MONGO_URI = "mongodb://127.0.0.1:27017"
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["ecommerce"]

# 2.3. 2.3. Kafka Producer - Đồng bộ với cấu hình Kafka Service đã khai báo
producer = Producer({
    'bootstrap.servers': '127.0.0.1:9092'
})
def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Kafka Delivery Failed: {err}")
    else:
        logger.info(f"Kafka Delivery Success: Topic {msg.topic()} [Partition {msg.partition()}]")

# 3. Utils
CACHE_TTL = 60

def cache_get(key):
    val = redis_client.get(key)
    if val:
        logger.info(f"[CACHE HIT] Lấy dữ liệu thành công từ Redis cho key: {key}")
        return json.loads(val)
    logger.info(f"[CACHE MISS] Không tìm thấy {key} trong Redis, chuẩn bị chọc xuống DB.")
    return None

def cache_set(key, value, ttl=CACHE_TTL):
    redis_client.setex(key, ttl, json.dumps(value))
    logger.info(f"[CACHE SET] Đã lưu {key} vào Redis với TTL={ttl}s")

# 4. APIs
# 4.1. user thanh tác với product
@app.post("/api/events")
def event(event: dict):
    logger.info(f"[API POST /api/events] Nhận event '{event.get('event_type')}' từ User {event.get('user_id')} cho Product {event.get('product_id')}")
    kafka_event = {
        "user_id": event["user_id"],
        "product_id": event["product_id"],
        "product_name": event["product_name"],
        "event_type": event["event_type"],
        "category": event["category"],
        "event_timestamp": datetime.now(timezone.utc).timestamp() * 1000
    }
    try:
        # Gửi message với Producer của confluent_kafka
        producer.produce(
            "user-activity-events", 
            value=json.dumps(kafka_event).encode('utf-8'),
            callback=delivery_report
        )
        producer.poll(0) # Kích hoạt callback
    except Exception as e:
        logger.error(f"Lỗi khi đẩy event vào Kafka: {str(e)}")
        raise HTTPException(500, str(e))
    
    return {"status": "ok", "message": "event sent to kafka"}

# 4.2. gợi ý thao tác theo thao tác của user_id
@app.get("/api/recommendations_realtime/{user_id}")
def recommendations_realtime(user_id: str):
    logger.info(f"[API GET /api/recommendations_realtime] Yêu cầu dữ liệu cho User {user_id}")
    cache_key = f"recommendation_realtime:{user_id}"
    result_cache = cache_get(cache_key)
    if result_cache:
        return result_cache
    
    # Truy vấn MongoDB 
    # Tìm tất cả document trong collection trending_products_realtime
    logger.info(f"Đang query MongoDB collection 'realtime_user_interest' cho User {user_id}...")
    cursor = db.trending_products_realtime.find({"user_id": user_id}).sort("trend_score", -1).limit(50)
    rows = list(cursor)
    
    if not rows:
        logger.warning(f"MongoDB trả về rỗng. User {user_id} chưa có dữ liệu realtime (Ném lỗi 404)")
        raise HTTPException(404, "No data")

    logger.info(f"Tìm thấy {len(rows)} bản ghi realtime cho User {user_id} trong MongoDB.")

    recommendations = []
    for row in rows:
        recommendations.append({
            "product_id": row["product_id"],
            "product_name": row["product_name"],
            "category": row["category"],
            "score": row.get("trend_score", 0),
            "event_type": row.get("event_type", "Aggregated")
        })
    
    result_final = {"user_id": user_id, "recommendations": recommendations}
    cache_set(cache_key, result_final)
    return result_final

# 4.3 sản phẩm trending
@app.get("/api/trending")
def trending():
    logger.info("[API GET /api/trending] Bắt đầu lấy danh sách Trending")
    cache_key = "trending:latest"
    cached = cache_get(cache_key)
    if cached:
        return cached
    
    pipeline = [
        {"$group": {
            "_id": "$product_id",
            "product_name": {"$first": "$product_name"},
            "category": {"$first": "$category"},
            "trend_score": {"$sum": "$trend_score"},
            "view_count": {"$sum": "$view_count"},
            "cart_count": {"$sum": "$cart_count"},
            "purchase_count": {"$sum": "$purchase_count"}
        }},
        {"$sort": {"trend_score": -1}},
        {"$limit": 50}
    ]

    # Query trực tiếp vào collection dữ liệu
    # Sort theo trend_score giảm dần (-1) và lấy top 100
    logger.info("Đang query MongoDB collection 'trending_products_realtime'...")
    products = list(db.trending_products_realtime.aggregate(pipeline))
    
    
    if not products:
        raise HTTPException(404, "No trending data available")

    # Ép kiểu _id (ObjectId) sang chuỗi (string) để JSON có thể serialize được
    for p in products:
        p["product_id"] = str(p.pop("_id"))

    # Format kết quả trả về
    result = {
        "count": len(products),
        "products": products
    }

    cache_set(cache_key, result, ttl=60)
    return result

# 4.4. API lấy recommendation BATCH cho user_id 
# Server -> MongoDB (collection: user_recommendations_batch)
@app.get("/api/recommendations/{user_id}")
def recommendation(user_id: str):
    logger.info(f"[API GET /api/recommendations] Yêu cầu gợi ý Batch cho User {user_id}")
    # 1. Lấy dữ liệu trong cache
    cache_key = f"recommendation:{user_id}"
    result_cache = cache_get(cache_key)

    if result_cache:
        return result_cache
    
    logger.info(f"Đang query MongoDB collection 'user_recommendations_batch' cho User {user_id}...")
    # 2. Query MongoDB 
    # Lưu ý: Trong MongoDB, bảng batch thường sẽ chứa mảng recommendations hoặc nhiều document
    # Tôi giả định collection này chứa document cho từng user_id
    rows = list(db.user_recommendations_batch.find({"user_id": user_id}))
    
    if not rows:
        logger.warning(f"MongoDB trả về rỗng. User {user_id} chưa chạy Batch Job (Ném lỗi 404)")
        raise HTTPException(404, "No batch data found for this user")

    # Giả sử segment_name được lấy từ record đầu tiên
    logger.info(f"Tìm thấy {len(rows)} gợi ý Batch cho User {user_id} trong MongoDB.")

    segment_name = rows[0].get("segment_name", "Unknown")

    # 3. Chuyển đổi dữ liệu
    recommendation_list = []
    for row in rows:
        recommendation_list.append({
            "product_id": row["product_id"],
            "product_name": row["product_name"],
            "category": row["category"],
            "score": row["recommendation_score"],
            "recommendation_type": row["recommendation_type"]
        })
    
    result_final = {
        "user_id": user_id,
        "segment_name": segment_name,
        "recommendations": recommendation_list
    }

    # 4. Lưu vào redis
    cache_set(cache_key, result_final)
    return result_final