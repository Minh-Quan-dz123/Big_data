from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

kafka_servers = "my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9094"
# URI chỉ cần trỏ đến IP và Port
mongo_uri = "mongodb://mongodb-service.default.svc.cluster.local:27017/"

spark = SparkSession.builder.appName("KafkaToMongo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=========================================================")
print("ĐANG LẮNG NGHE KAFKA VÀ CHUẨN BỊ GHI VÀO MONGODB...")
print("=========================================================")

# Đổi thành earliest để Spark vét sạch toàn bộ dữ liệu đã bắn vào Kafka từ trước đến nay
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", "user-activity-events") \
    .option("startingOffsets", "earliest") \
    .load()

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("action", StringType(), True),
    StructField("item_id", StringType(), True),
    StructField("timestamp", LongType(), True)
])

parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Cập nhật hàm ghi Mongo: Thêm tính năng Debug và chuẩn hóa Connector v10
def write_to_mongo(df, epoch_id):
    print(f"\n--- Bắt đầu xử lý Batch: {epoch_id} ---")
    
    # In dữ liệu ra màn hình Terminal để bạn dễ đối chiếu
    df.show(truncate=False)
    
    # Kiểm tra nếu Batch có dữ liệu thì mới tiến hành ghi
    if not df.isEmpty():
        df.write \
            .format("mongodb") \
            .mode("append") \
            .option("spark.mongodb.write.connection.uri", mongo_uri) \
            .option("spark.mongodb.write.database", "ecommerce") \
            .option("spark.mongodb.write.collection", "user_events") \
            .save()
        print(f"-> THÀNH CÔNG: Đã lưu Batch {epoch_id} vào MongoDB!")
    else:
        print("-> Batch rỗng, bỏ qua.")

# Thêm checkpointLocation (Bắt buộc để Spark quản lý trạng thái khi ghi ra DB)
query = parsed_df.writeStream \
    .foreachBatch(write_to_mongo) \
    .option("checkpointLocation", "/opt/spark/streaming/checkpoint") \
    .start()

query.awaitTermination()