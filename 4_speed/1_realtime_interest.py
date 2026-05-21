from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, unix_timestamp, when, exp, round, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType

# ==========================================
# KHỞI TẠO ĐỘNG CƠ SPARK STREAMING
# ==========================================
spark = SparkSession.builder \
    .appName("Realtime_User_Interest_Lightweight") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("--- Bắt đầu tiến trình Real-time View (Tốc độ cao - Không Join) ---")

# ==========================================
# BƯỚC 1: ĐỌC LUỒNG ĐỘNG TỪ KAFKA
# ==========================================
# Cấu trúc JSON thô từ fake_realtime.py
kafka_schema = StructType([
    StructField("user_id", StringType()),
    StructField("product_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", LongType()),
    StructField("session_id", StringType())
])

df_kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_events") \
    .load()

# Bóc tách JSON
df_stream_parsed = df_kafka_stream \
    .select(from_json(col("value").cast("string"), kafka_schema).alias("data")) \
    .select("data.*")


# ==========================================
# BƯỚC 2: ÁP DỤNG THUẬT TOÁN TÍNH ĐIỂM TIME DECAY
# ==========================================
# 1. Gán trọng số Weight trực tiếp vào luồng gốc
df_weighted = df_stream_parsed.withColumn(
    "Weight",
    when(col("event_type") == "purchase", lit(1.0))
    .when(col("event_type") == "cart", lit(0.7))
    .when(col("event_type") == "wishlist", lit(0.4))
    .when(col("event_type") == "view", lit(0.2))
    .otherwise(lit(0.0))
)

# 2. Tính Time Decay
lambda_val = 0.0001 

df_final_realtime = df_weighted \
    .withColumn("t_now", (unix_timestamp(current_timestamp()) * 1000).cast("long")) \
    .withColumn("time_diff_seconds", (col("t_now") - col("event_time")) / 1000) \
    .withColumn(
        "Computed_score",
        round(col("Weight") * exp(-lit(lambda_val) * col("time_diff_seconds")), 4).cast("float")
    ) \
    .select(
        col("user_id").alias("User_id"),
        col("product_id").alias("Product_id"),
        col("event_type").alias("Interest_type"),
        col("event_time").alias("Event_time"),
        col("session_id").alias("Session_id"),
        col("Computed_score")
    )


# ==========================================
# BƯỚC 3: IN KẾT QUẢ RA TERMINAL
# ==========================================
query = df_final_realtime.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

