import os
import sys
from datetime import datetime

# Set up python paths for local testing
jobs_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(jobs_dir, '../..'))
sys.path.append(jobs_dir)
sys.path.append(project_root)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, sum, when, lit
from pyspark.sql.types import StructType, StructField, StringType

from configs import config
from common.utils import current_time

# ==========================================
# 1. CONFIGURATION
# ==========================================
IS_PRODUCTION = False

# Cassandra Config
CASSANDRA_KEYSPACE = "ecommerce"
CASSANDRA_TABLE = "trending_products_realtime"
CASSANDRA_HOST = "cassandra"

# Local Output path
LOCAL_OUTPUT_FILE = "1_dataset/output/trending_products_realtime.csv"

# Static data paths
PRODUCTS_CSV_PATH = "1_dataset/raw_data/products.csv"

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = config.KAFKA_BROKER
INPUT_TOPIC = config.EVENT_CLEANED_TOPIC

# ==========================================
# 2. SPARK SESSION INITIALIZATION
# ==========================================
def get_spark_session():
    builder = SparkSession.builder \
        .appName("TrendingProductsRealtimeStream")
    
    if IS_PRODUCTION:
        builder = builder \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                                           "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
            .config("spark.cassandra.connection.host", CASSANDRA_HOST)
    else:
        builder = builder \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
            
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ==========================================
# 3. FOREACH BATCH WRITER
# ==========================================
def write_batch(df, batch_id):
    print(f"--- Processing Batch: {batch_id} at {datetime.now()} ---")
    
    sorted_df = df.orderBy(col("trend_score").desc())
    sorted_df.show(10, truncate=False)
    
    if IS_PRODUCTION:
        print(f"Writing batch {batch_id} to Cassandra table {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}...")
        sorted_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE) \
            .mode("append") \
            .save()
    else:
        print(f"Saving batch {batch_id} to local CSV at {LOCAL_OUTPUT_FILE}...")
        os.makedirs(os.path.dirname(LOCAL_OUTPUT_FILE), exist_ok=True)
        pandas_df = sorted_df.toPandas()
        pandas_df.to_csv(LOCAL_OUTPUT_FILE, index=False)
        print(f"Local file updated successfully (Total items: {len(pandas_df)})")

# ==========================================
# 4. MAIN FLOW
# ==========================================
def run_realtime_trend_pipeline():
    spark = get_spark_session()
    
    print(f"Loading static products from {PRODUCTS_CSV_PATH}...")
    static_products = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(PRODUCTS_CSV_PATH) \
        .select("product_id", "category")
    
    event_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("processed_at", StringType(), True)
    ])
    
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}, topic: {INPUT_TOPIC}...")
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
    
    parsed_stream = kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), event_schema).alias("data")) \
        .select("data.*")
        
    stream_with_time = parsed_stream \
        .withColumn("event_time", to_timestamp(col("event_timestamp")))
    
    enriched_stream = stream_with_time.join(
        static_products,
        on="product_id",
        how="inner"
    )
    
    windowed_aggregates = enriched_stream \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("product_id"),
            col("category")
        ) \
        .agg(
            sum(when(col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
            sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count")
        )
        
    trending_realtime = windowed_aggregates \
        .withColumn(
            "trend_score",
            col("view_count") * 0.2 + col("cart_count") * 0.5 + col("purchase_count") * 1.0
        ) \
        .withColumn(
            "computed_time",
            col("window.end").cast("long") * 1000
        ) \
        .select(
            col("product_id"),
            col("trend_score"),
            col("view_count"),
            col("cart_count"),
            col("purchase_count"),
            col("category"),
            col("computed_time")
        )
        
    query = trending_realtime.writeStream \
        .foreachBatch(write_batch) \
        .outputMode("update") \
        .option("checkpointLocation", "4_speed/checkpoint/trending_realtime") \
        .start()
        
    query.awaitTermination()

if __name__ == "__main__":
    run_realtime_trend_pipeline()
