from fastapi import FastAPI, HTTPException
from cassandra.cluster import Cluster
from kafka import KafkaProducer
import redis # sử dụng redis client để kết nối redis server
import os
import json
from datetime import datetime, timezone


# 1 tạo Server FastAPI object
app = FastAPI(title="Serving Layer - Realtime Recommendation")

# 2 config
# 2.1. redis client
redis_client  = redis.Redis(
    host = os.getenv("REDIS_HOST", "redis"), # nếu ko có file env thì nó tự dừng "redis" (docker service name) 
    port = 6379,                             # port mặc định của redis
    decode_responses=True                    # False => trả về byte (b'abc') còn true trả về string ("abc")
)

# 2.2. cassandra client
CASSANDRA_CONF = {
    "host": "cassandra",
    "keyspace": "ecommerce",
    "table_batch": "user_recommendations_batch",
    "table_realtime1": "realtime_user_interest",
    "table_realtime2": "trending_products_realtime"
}
# khởi tạo object cassandra client
cluster = Cluster([CASSANDRA_CONF["host"]])
# gọi hàm connect
session = cluster.connect(CASSANDRA_CONF["keyspace"])

# 2.3. cấu hình kafka client
# tạo object
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)



# 3 các hàm utils
# 3.1 hàm lấy data và chuyển trành json an toàn
def safe_json(data):
    if not data:
        return None
    try:
        return json.loads(data)
    except Exception:
        return None
    
# cache TTL
CACHE_TTL = 60

# 3.2. hàm lấy ra value từ key trong redis
def cache_get(key):
    return safe_json(redis_client.get(key))

# 3.3. hàm set value vào redis (set, setex = set + ttl)
def cache_set(key, value, ttl = CACHE_TTL):
    redis_client.setex(key, ttl, json.dumps(value))
    

# 4 APIs

# 4.1. client -> server: post /api/events để server cập nhật thao tác của user_id
#      server -> kafka
'''
Ví dụ: Request
{
    "user_id": "U1",
    "product_id": "P15",
    "category" : clothing,
    "event_type": "view" // event_type = view | cart | purchase
}
'''
@app.get("/")
def root():

    return {
        "service": "Serving Layer",
        "status": "running"
    }

@app.post("/api/events")
def event(event: dict):
   
    # 1 gom các thông tin cần thiết vào 1 object
    kafka_event = {
        "user_id": event["user_id"],
        "product_id": event["product_id"],
        "event_type": event["event_type"],
        "category": event["category"],
        "timestamp": datetime.now(timezone.utc).timestamp()* 1000
    }

    # 2 gọi hàm send là được
    producer.send("user_activity_events", kafka_event)

    # Lưu ý: khi send thì client ko gửi ngay lập tức xuống broker mà đưa vào buffer trong RAM
    # 1 thread nền sẽ gom nhiều message lại thành batch rồi mới gửi tới kafka broker
    
    return {
        "status": "ok",
        "message": "event sent to kafka"
    }

# 4.2 client -> server: get lấy recommendation cho user_id 
#     server -> cassandra
''' 
ví dụ : GET /api/recommendations/U1
Response
{
    "user_id": "U1",
    "segment_name": Frequent Shoppers | Risky Frequent Buyers | Low Frequency | Bad Customer
    "recommendations": [
        {
            "product_id": "P15",
            "product_name": "Fami",
            "category": "vitamin",
            "score": 98.3,
            "recommendation_type": ABC
        },
        {
            "product_id": "P8",
            "product_name": "iphone 11 pro max",
            "category": "smartphone",
            "score": 95
            "recommendation_type": ABC
        }
    ]
}
'''
@app.get("/api/recommendations/{user_id}")
def recommendation(user_id: str):
    # 1 lấy dữ liệu trong cache
    cache_key = f"recommendation:{user_id}"

    result_cache = cache_get(cache_key)

    # 2 nếu tồn tại thì oke, ko thì set
    if result_cache:
        return result_cache
    
    # 3 ko thì set dữ liệu từ cassandra vào redis server
    # 3.1. lấy từ cassandra
    # khai báo query
    query = """
        SELECT segment_name, product_id, product_name, category, recommendation_score, recommendation_type
        FROM user_recommendations_batch
        WHERE user_id=%s
    """
    # gọi hàm và đợi nó trả về các hàng kết quả
    rows = session.execute(query, [user_id]) # trong query có user_id=%s, đó là nhập tham số

    # 4 gán các row trong rows vào 1 mảng json để gửi lại client
    recommendation = []

    for row in rows:
        recommendation.append({
            "product_id": row.product_id,
            "product_name": row.product_name,
            "category": row.category,
            "score": row.recommendation_score,
            "recommendation_type": row.recommendation_type
        })
    
    result_final = {
        "user_id": user_id,
        "segment_name":rows.one().segment_name,
        "recommendations": recommendation
    }

    # 3.2. Lưu vào redis cái đã
    cache_set(cache_key, result_final)
    return result_final

# 4.3. client -> server: get thông tin trending
#     server -> redis, nếu cache miss -> lấy từ cassandra và cập nhật redis
'''
ví dụ GET /api/trending
Response
{
    "window_end": "10:05", // thời điểm thông tin trending product mới nhất
    "products": [
        {
            "product_id": "P15",
            "product_name": "Nimbus Stay",
            "category": "Toys",
            "trend_score": 250
        },
        {
            "product_id": "P8",
            "product_name": "Orion Head",
            "category": "Beauty",
            "trend_score": 220
        }
    ]
}

trong cassandra: 
CREATE TABLE trending_products_realtime (
    computed_time bigint,
    trend_score double,
    product_id text,
    product_name text,
    category text,
    view_count int,
    cart_count int,
    purchase_count int,

    PRIMARY KEY (
        (computed_time),
        trend_score,
        product_id
    )
)
WITH CLUSTERING ORDER BY (
    trend_score DESC,
    product_id ASC
);

CREATE TABLE trending_metadata (
    key text PRIMARY KEY,
    latest_computed_time bigint
);
'''
@app.get("/api/trending")
def trending():
    # 1 lấy dữ liệu , lấy ra 20 sản phẩm có điểm trend_score cao nhất hiện tại
    cache_key = "trending:latest"
    cached = cache_get(cache_key)
    if cached:
        return cached

    # 2 nếu cache miss thì querry trong cassandra
    # 2.1. lấy dòng đầu tiên trong latest_computed_time
    metadata = session.execute("""
        SELECT latest_computed_time
        FROM trending_metadata
        WHERE key='latest'
    """).one() # tương đương lấy thời gian gần nhất

    if not metadata:
        raise HTTPException(
            status_code=404,
            detail="No trending data"
        )
    
    computed_time = metadata.latest_computed_time

    # 2.2. tiếp tục từ computed_time ta lấy dữ liệu từ bảng chính
    rows = session.execute("""
        SELECT product_id,
               product_name,
               category,
               trend_score,
               view_count,
               cart_count,
               purchase_count
        FROM trending_products_realtime
        WHERE computed_time=%s
        LIMIT 20
    """, [computed_time])
    products = []

    for row in rows:
        products.append({
            "product_id": row.product_id,
            "product_name": row.product_name,
            "category": row.category,
            "trend_score": row.trend_score,
            "view_count": row.view_count,
            "cart_count": row.cart_count,
            "purchase_count": row.purchase_count
        })

    result = {
        "window_end": datetime.fromtimestamp(
            computed_time / 1000,
            tz=timezone.utc
        ).strftime("%H:%M"),
        "products": products
    }

    cache_set(cache_key, result, ttl=60)

    return result

# 4.4. client -> server: get ứng với mỗi product, lấy danh sách user 
# mỗi product có thể bán cho user nào
# user đó thuộc loại {Low Frequency, Risky Frequent Buyers, Frequent Shoppers, Bad Customer}
# họ có quan tâm tới sản phẩm này gần đây ko
#     server -> redis, nếu cache miss -> lấy từ cassandra và cập nhật redis
# 
'''
ví dụ: GET /api/products/P15/potential-customers
Response
```json
{
    "product_id": "P15",
    "customers": [
        {
            "user_id": "U1",
            "interest_score": 98.0,
            "type": "consumption"
        },
        {
            "user_id": "U3",
            "interest_score": 95.0,
            "type": "consumption"
        },
        {
            "user_id": "U5",
            "interest_score": 90.2,
            "type": "similar"
        }
    ]
}
'''

# @app.get("/api/products/{product_id}/potential-customers")
# def customer_for_product(product_id: str):

#     # 1 lấy từ trong redis server
#     cached = cache_get(f"potential-customers:{product_id}")
#     if cached:
#         return cached
    
#     # 2 nếu miss thì lấy trong cassandra
    

#     # sau đó 

#     return 1

# # 4.5. client -> server: get lấy ra lịch sử trend của sản phẩm này
# #     server -> redis, nếu cache miss -> lấy từ cassandra và cập nhật redis

# '''
# Ví dụ: GET /api/trending/P15/history
# Response
# [
#     {
#         "window": "10:00",
#         "score": 12.0
#     },
#     {
#         "window": "10:01",
#         "score": 16.2
#     },
#     {
#         "window": "10:02",
#         "score": 25.3
#     }
# ]
# '''
# @app.get("/api/trending/{product_id}/history")
# def history_trend_of_product(product_id: str):
#     # tạm thời 
#     return 2
