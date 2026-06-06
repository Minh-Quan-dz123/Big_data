import os
from datetime import datetime


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, sum, when, lit, desc
from pyspark.sql.types import StructType, StructField, StringType, LongType
'''
- Input
{
  "user_id": "U123",
  "product_id": "P456",
  "product_name": "ABC",
  "category" : clothing
  "event_type": "view | cart | wishlist | purchase",  (tạm thời ko dùng wishlist)
  "event_timestamp": 1717240000000, (ms)
}

- Output
product_id | category | view_count | cart_count | purchase_count | trend_score | computed_time
'''
# 1. CONFIG
KAFKA_BROKER = "my-cluster-kafka-bootstrap:9092"
INPUT_TOPIC = "user_activity_events"

CASSANDRA_CONF = {
    "host": "cassandra",
    "keyspace": "ecommerce",
    "table": "trending_products_realtime"
}


# 2. KHỞI TẠO OBJECT SPARK STREAMING
def get_spark_session():
    return SparkSession.builder \
        .appName("Realtime_User_Interest_Lightweight") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
        .config("spark.cassandra.connection.host", CASSANDRA_CONF["host"]) \
        .getOrCreate()

# 3. Hàm lưu dữ liệu vào cassandra
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
    

# 4. MAIN FLOW
def run_realtime_trend_pipeline():

    # 1 tạo spark object
    spark = get_spark_session()

    
    # 2 tạo kafka schema
    event_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_timestamp", LongType(), True),
    ])
    
    # 3 DAG kafka stream
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Bóc tách JSON  sang DataFrame (vẫn chỉ là DAG)
    # chuyển event_timestamp sang dạng s và chuyển sang dạng time của spark
    parsed_stream = kafka_stream \
        .select(from_json(col("value").cast("string"), event_schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_timestamp", (col("event_timestamp") / 1000).cast("timestamp"))
    '''
    user_id
    product_id
    product_name
    category
    event_type
    event_timestamp # dạng timestamp chuẩn
    
    - Logic, giả sử mỗi spark mỗi 1-5s nó đọc 1 nhóm event từ kafka 1 lượt
    - số lượng event mỗi lần đọc là khác nhau, ví dụ lần này có đọc 5 event
    {"event_id":"E1","user_id":"U1","product_id":"P1","category":"clothing","event_type":"view","event_timestamp":1717240000000}
    {"event_id":"E2","user_id":"U2","product_id":"P1","category":"clothing","event_type":"cart","event_timestamp":1717240100000}
    {"event_id":"E3","user_id":"U3","product_id":"P1","category":"clothing","event_type":"view","event_timestamp":1717240200000}
    {"event_id":"E4","user_id":"U4","product_id":"P1","category":"clothing","event_type":"purchase","event_timestamp":1717240300000}
    {"event_id":"E5","user_id":"U5","product_id":"P2","category":"clothing","event_type":"view","event_timestamp":1717240150000}

    - sau khi parse ta được DataFrame
    event_id | product_id | event_type | event_timestamp
    E1       | P1         | view       | 1717240000000
    E2       | P1         | cart       | 1717240100000
    E3       | P1         | view       | 1717240200000
    E4       | P1         | purchase   | 1717240300000
    E5       | P2         | view       | 1717240150000

    - sau đó chuyển event_timestamp dạng ms -> sang  dạng 2024-06-01 10:00:00
    event_id | product_id | event_type | event_timestamp
    E1       | P1         | view       | 2024-06-01 10:00:00
    E2       | P1         | cart       | 2024-06-01 10:01:40
    E3       | P1         | view       | 2024-06-01 10:03:20
    E4       | P1         | purchase   | 2024-06-01 10:05:00
    E5       | P2         | view       | 2024-06-01 10:02:30

    - sau đó Spark sẽ xem event này thuộc những window nào thì sẽ cập nhật vào các window đó 
    - với 5 event trên spark sẽ cập nhật vào 5 window sau 
    - Bước 5, window aggragetion window(col("event_timestamp"), "5 minutes", "1 minute")
        + Đầu tiên Spark sẽ tạo các window 
            E1, E2, E3, E5 -> window1 (10:00 -> 10:05) 
            E2, E3, E4, E5 -> window2 (10:01 -> 10:06)
            E3, E4, E5     -> window3 (10:02 -> 10:07)
            E3, E4         -> window4 (10:03 -> 10:08)
            E4             -> window5 (10:04 -> 10:09)
        + sau đó nó Group by sau:
            (W1, P1, clothing) // các event_type khác nhau hoặc giống nhau thì kệ
            (W1, P2, clothing)
            (W2, P1, clothing)
            (W2, P2, clothing)
            (W3, P1, clothing)
            (W3, P2, clothing)
            (W4, P1, clothing)
            (W5, P1, clothing)
            và rồi nó sẽ tính với mỗi (W_i, P_j, category) thêm cột view_count, cart_count, purchase_count
        - ví dụ 
        windowed_aggregates = 
            window | product | view | cart | purchase
 {10:00,10:05}     | P1      | 2    | 1    | 0
 {10:01,10:06}     | P1      | 2    | 1    | 1
            ..     | P1      | 2    | 1    | 1
            W4     | P1      | 2    | 1    | 0
            W5     | P1      | 1    | 0    | 0
            W1     | P2      | 1    | 0    | 0
....

    cho ra kết quả 
    P1: view_count: 2, cart_count: 1, purchase_count: 1
    P2: view_count: 1, cart_countL 0, purchase_count: 0

    - Bước 6, tính trend_score
    product	score	computed_time
        P1	1.9	10:06
        P1	1.2	10:07
        P2	0.2	10:06
    
        => ta lấy window mới nhất là 10:07 (10:02->10:07)

    - Bước 7: sort (window + score)
    product	window	score 
        P3	10:07	2.5
        P1	10:07	1.2
        P1	10:06	1.9
    '''
    # 5 window aggragetion
    # mỗi 1 phút, tính lại hành vi của sản phẩm trong 5 phút gần nhất
    # event đến muộn hơn 10 phút thì bỏ qua
    windowed_aggregates = (
        parsed_stream
        .withWatermark("event_timestamp", "10 minutes") 
        .groupBy( # group theo từng sản phẩm
            window(col("event_timestamp"), "5 minutes", "1 minute"), # cú pháp (cột, kích thước window, cửa sổ trượt)
            col("product_id"),
            col("category")
        ) 
        .agg(
            sum(when(col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
            sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count")
        )
    )
        
    # trend score
    trending_realtime = windowed_aggregates \
        .withColumn(
            "trend_score",
            col("view_count") * 0.2 + col("cart_count") * 0.5 + col("purchase_count") * 1.0
        ) \
        .withColumn( # thêm vào để sort theo thời gian
            "window_end",
            col("window.end")
        ) \
        .withColumn(
            "computed_time", col("window.end").cast("long") * 1000
        ) \
        .select(
            "product_id",
            "product_name"
            "category",
            "view_count",
            "cart_count",
            "purchase_count",
            "trend_score",
            "computed_time"
        )
        
    # output stream
    query = trending_realtime.writeStream \
        .foreachBatch(write_to_cassandra) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/checkpoint_trending_products") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    run_realtime_trend_pipeline()
