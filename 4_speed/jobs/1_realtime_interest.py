from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, unix_timestamp, when, exp, round, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType

'''
- Input
{
  "user_id": "U123",
  "product_id": "P456",
  "product_name": "ABC",
  "category" : clothing
  "event_type": "view",
  "event_timestamp": 1717240000000, (ms)
}

- Output
  + (user_id, product_id, product_name, category, event_type, event_timestamp, computed_score)
 ví dụ: U123 | P456      | ABC        | clothing   |view     | 1717240000000   | 0.1832
'''

# 0 cấu hình ban đầu 
# 0.1 Cassandra
CASSANDRA_CONF = {
    "host": "cassandra",   
    "keyspace": "ecommerce",
    "table": "realtime_user_interest"
}

# KHỞI TẠO OBJECT SPARK STREAMING
spark = SparkSession.builder \
    .appName("Realtime_User_Interest_Lightweight") \
    .config("spark.cassandra.connection.host", CASSANDRA_CONF["host"]) \
    .getOrCreate()

# chỉ hiện warn, error thôi cho dễ
spark.sparkContext.setLogLevel("WARN")

print("--- Bắt đầu tiến trình Real-time View (Tốc độ cao - Không Join) ---")

# BƯỚC 1: ĐỌC LUỒNG ĐỘNG TỪ KAFKA
# 1.1. khai báo cấu trúc json (INPUT)
kafka_schema = StructType([
    StructField("user_id", StringType(), True),       # chuỗi
    StructField("product_id", StringType(), True),    # chuỗi
    StructField("product_name", StringType(), True),    # chuỗi
    StructField("category", StringType(), True),    # chuỗi
    StructField("event_type", StringType(), True),    # chuỗi
    StructField("event_timestamp", LongType(), True),      # kiểu long
])


# 1.2. Đọc luồng kafka (đây chỉ là DAG)
# format: nguồn dữ liệu là kafka
# option1: địa chỉ kafka
# option2: đăng ký topic là user_activity_events
# load() = tạo DataFrame Streaming
df_kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "my-cluster-kafka-bootstrap.default.svc.cluster.local:9092") \
    .option("subscribe", "user-activity-events") \
    .option("startingOffsets", "latest") \
    .load()
# sau khi load()
''', DataFrame là df_kafka_stream có dạng
root
 |-- key: binary
 |-- value: binary
 |-- topic: string
 |-- partition: int
 |-- offset: long
 |-- timestamp: timestamp
 |-- timestampType: int

 muốn đọc thì phải ép kiểu bằng col("value").cast("string") để thu được value:
 +------------------------------------+
|value                               |
+------------------------------------+
|{"product_id":10,"action":"view"}   |
|{"product_id":15,"action":"buy"}    |
+------------------------------------+
'''

# 1.3. Bóc tách JSON (vẫn chỉ là DAG)
# dùng select(nguồn, schemas/form mong muốn) => thu được dữ liệu theo kiểu schema ở trên khai báo
# sau đó đổi tên thành data
# .select("data.*") để từ data.user_id => user_id thôi
df_stream_parsed = df_kafka_stream \
    .select(from_json(col("value").cast("string"), kafka_schema).alias("data")) \
    .select("data.*") \
    .na.drop()


# BƯỚC 2: ÁP DỤNG THUẬT TOÁN TÍNH ĐIỂM TIME DECAY
# 2.1. Gán trọng số Weight trực tiếp vào luồng gốc
df_weighted = df_stream_parsed.withColumn(
    "weight",
    when(col("event_type") == "purchase", lit(1.0))
    .when(col("event_type") == "cart", lit(0.7))
    .when(col("event_type") == "wishlist", lit(0.4))
    .when(col("event_type") == "view", lit(0.2))
    .otherwise(lit(0.0))
)

# 2.2. Tính Time Decay
# công thức score=score0​⋅e−λt
# score₀: điểm ban đầu
# t: thời gian đã trôi qua
# λ (lambda): hệ số suy giảm.
# e ≈ 2.71828

lambda_val = 0.0001 

df_final_realtime = df_weighted \
    .withColumn("t_now", (unix_timestamp(current_timestamp()) * 1000).cast("long")) \
    .withColumn("time_diff_seconds", (col("t_now") - col("event_timestamp")) / 1000) \
    .withColumn(
        "computed_score",
        round(col("weight") * exp(-lit(lambda_val) * col("time_diff_seconds")), 4).cast("float")
    ) \
    .select(
        col("user_id"),
        col("product_id"),
        col("product_name"),
        col("category"),
        col("event_type"),
        col("event_timestamp"),
        col("computed_score")
    )


# BƯỚC 3: lưu vào cassandra
# 3.1. viết hàm
def write_to_cassandra(batch_df, batch_id):
    print(f"Writing batch: {batch_id}")

    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(
            table=CASSANDRA_CONF["table"],
            keyspace=CASSANDRA_CONF["keyspace"]
        ) \
        .save()
    
# áp dụng
# writeStream là tạo vòng lặp
query = df_final_realtime.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_cassandra) \
    .option("checkpointLocation", "/checkpoint/interest") \
    .start()

query.awaitTermination()