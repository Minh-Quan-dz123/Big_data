from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum, when
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
import sys

# 1. CONFIGURATION
KAFKA_BROKER = "my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9094"
INPUT_TOPIC = "user-activity-events"
MONGO_URI = "mongodb://mongodb-service.default.svc.cluster.local:27017/"
DB_NAME = "ecommerce"
COLLECTION_NAME = "trending_products_realtime"


# 3. HÀM GHI VÀO MONGODB (foreachBatch)
def write_to_mongo(batch_df, batch_id):
    print(f"\n{'='*15} BẮT ĐẦU XỬ LÝ BATCH ID: {batch_id} {'='*15}")
    if batch_df.count() > 0:
        try:
            batch_df.write \
                .format("mongodb") \
                .mode("append") \
                .option("spark.mongodb.write.connection.uri", f"{MONGO_URI}{DB_NAME}.{COLLECTION_NAME}") \
                .option("spark.mongodb.write.operationType", "update") \
                .option("spark.mongodb.write.idFieldList", "user_id,product_id") \
                .save()
            print(f"-> [THÀNH CÔNG] Đã Upsert Batch {batch_id} vào MongoDB.")
        except Exception as e:
            print(f"-> [LỖI] {str(e)}")
    
    

# 4. MAIN PIPELINE
def run_realtime_trend_pipeline():
    spark = SparkSession.builder \
        .appName("Realtime_Trending_Products_Job") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Schema dữ liệu đầu vào
    event_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_timestamp", DoubleType(), True),
    ])

    # Đọc stream từ Kafka
    print("\n[INFO] Đang khởi tạo kết nối Kafka nội bộ tại cổng 9094...")
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse và xử lý timestamp
    parsed_stream = kafka_stream \
        .select(from_json(col("value").cast("string"), event_schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_timestamp", (col("event_timestamp") / 1000).cast("timestamp"))

    # LUỒNG DEBUG 1: XUẤT RAW DATA TỪ KAFKA RA CONSOLE
    debug_raw_query = parsed_stream.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    # Logic Window Aggregation (5 phút trượt mỗi 1 phút)
    windowed_aggregates = (
        parsed_stream
        .withWatermark("event_timestamp", "10 minutes") 
        .groupBy(
            window(col("event_timestamp"), "5 minutes", "1 minute"),
            col("user_id"),
            col("product_id"),
            col("product_name"),
            col("category")
        ) 
        .agg(
            sum(when(col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
            sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count")
        )
    )

    # Tính trend_score và chuẩn bị dữ liệu ghi
    trending_realtime = windowed_aggregates \
        .withColumn("trend_score", col("view_count") * 0.2 + col("cart_count") * 0.5 + col("purchase_count") * 1.0) \
        .withColumn("computed_time", col("window.end").cast("long")) \
        .select(
            "user_id",
            "product_id", "product_name", "category",
            "view_count", "cart_count", "purchase_count",
            "trend_score", "computed_time"
        )

    # GHI VÀO MONGODB
    print("[INFO] Bắt đầu luồng ghi MongoDB...")
    query = trending_realtime.writeStream \
        .foreachBatch(write_to_mongo) \
        .outputMode("update") \
        .option("checkpointLocation", "/opt/spark/streaming/checkpoint-trending") \
        .trigger(processingTime="30 seconds") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    run_realtime_trend_pipeline()