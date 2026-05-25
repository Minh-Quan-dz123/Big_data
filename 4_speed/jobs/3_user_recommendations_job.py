import os
import sys
from datetime import datetime

# Set up python paths for local testing
jobs_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(jobs_dir, '../..'))
sys.path.append(jobs_dir)
sys.path.append(project_root)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, sum, when, lit,
    row_number, broadcast, collect_set, concat_ws, greatest, max as smax
)
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window as W

from configs import config
from common.utils import current_time

# ==========================================
# 1. CONFIGURATION
# ==========================================
IS_PRODUCTION = False

# Cassandra Config
CASSANDRA_KEYSPACE = "ecommerce"
CASSANDRA_TABLE = "user_recommendations_realtime"
CASSANDRA_HOST = "cassandra"

# Local Output path
LOCAL_OUTPUT_FILE = "1_dataset/output/user_recommendations_realtime.csv"

# Static data paths
PRODUCTS_CSV_PATH = "1_dataset/raw_data/products.csv"
COMPLEMENTARY_CSV_PATH = "1_dataset/output/product_complementary.csv"
TRENDING_CSV_PATH = "1_dataset/output/trending_products.csv"

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = config.KAFKA_BROKER
INPUT_TOPIC = config.EVENT_CLEANED_TOPIC

# Recommendation tuning
TOP_N_PER_USER = 10
CONTENT_NEIGHBORS_PER_PRODUCT = 10
TRENDING_FALLBACK_SIZE = 20

# Event weights (how much each user action contributes to the seed signal)
EVENT_WEIGHTS = {"view": 0.2, "wishlist": 0.4, "cart": 0.5, "purchase": 1.0}

# Channel weights (precision of each recommendation source)
CHANNEL_WEIGHT = {"collaborative": 1.0, "content_based": 0.7, "trending": 0.3}


# ==========================================
# 2. SPARK SESSION INITIALIZATION
# ==========================================
def get_spark_session():
    builder = SparkSession.builder \
        .appName("UserRecommendationsRealtimeStream")

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
# 3. STATIC LOOKUP TABLES
# ==========================================
def load_static_lookups(spark):
    """
    Load và broadcast 3 bảng tĩnh:
      - products: dùng cho exclusion + thông tin category
      - complementary: collaborative candidates (Product_id_1 -> Product_id_2)
      - content_neighbors: với mỗi product giữ top-N sản phẩm cùng category (rating desc)
      - trending: fallback top-N theo trend_score
    """
    print(f"Loading products from {PRODUCTS_CSV_PATH}...")
    products = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(PRODUCTS_CSV_PATH) \
        .select("product_id", "category", "brand", "rating")

    # ---------- Collaborative lookup ----------
    print(f"Loading complementary from {COMPLEMENTARY_CSV_PATH}...")
    complementary = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(COMPLEMENTARY_CSV_PATH) \
        .select(
            col("Product_id_1").alias("seed_product_id"),
            col("Product_id_2").alias("candidate_product_id"),
            col("Complementary_score").cast("double").alias("affinity_score")
        )

    # ---------- Content neighbors ----------
    # Với mỗi product, lấy top-N sản phẩm cùng category (khác chính nó), ưu tiên rating cao.
    print("Building content neighbors lookup...")
    p1 = products.alias("p1")
    p2 = products.alias("p2")
    pairs = p1.join(p2, col("p1.category") == col("p2.category")) \
        .where(col("p1.product_id") != col("p2.product_id")) \
        .select(
            col("p1.product_id").alias("seed_product_id"),
            col("p2.product_id").alias("candidate_product_id"),
            col("p2.rating").alias("cand_rating"),
            when(col("p1.brand") == col("p2.brand"), lit(0.15)).otherwise(lit(0.0)).alias("brand_boost")
        )
    ranked = pairs.withColumn(
        "rn",
        row_number().over(
            W.partitionBy("seed_product_id").orderBy(col("cand_rating").desc())
        )
    ).where(col("rn") <= CONTENT_NEIGHBORS_PER_PRODUCT)

    content_neighbors = ranked.select(
        "seed_product_id",
        "candidate_product_id",
        # affinity 0..1: rating/5 + brand_boost (capped at 1.0)
        when((col("cand_rating") / lit(5.0)) + col("brand_boost") > 1.0, lit(1.0))
            .otherwise((col("cand_rating") / lit(5.0)) + col("brand_boost"))
            .alias("affinity_score")
    )

    # ---------- Trending fallback ----------
    print(f"Loading trending from {TRENDING_CSV_PATH}...")
    trending = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(TRENDING_CSV_PATH) \
        .select(
            col("product_id").alias("candidate_product_id"),
            col("trend_score").cast("double").alias("trend_score")
        )
    # Normalize trend_score to 0..1 and keep top-N
    max_trend = trending.agg(smax("trend_score")).collect()[0][0] or 1.0
    trending_top = trending.withColumn(
        "affinity_score", col("trend_score") / lit(float(max_trend))
    ).orderBy(col("trend_score").desc()).limit(TRENDING_FALLBACK_SIZE) \
     .select("candidate_product_id", "affinity_score")

    return {
        "products": broadcast(products),
        "complementary": broadcast(complementary),
        "content_neighbors": broadcast(content_neighbors),
        "trending": broadcast(trending_top),
    }


# ==========================================
# 4. FOREACH BATCH WRITER
# ==========================================
def write_batch(df, batch_id):
    print(f"--- Processing Batch: {batch_id} at {datetime.now()} ---")

    sorted_df = df.orderBy(col("user_id"), col("recommendation_score").desc())
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
        print(f"Local file updated successfully (Total recs: {len(pandas_df)})")


# ==========================================
# 5. RECOMMENDATION LOGIC (PER BATCH)
# ==========================================
def build_recommendations(user_signals, lookups):
    """
    user_signals: DataFrame[user_id, seed_product_id, signal_weight, computed_time]
                  (đã group theo window và lấy max signal cho mỗi (user, seed))
    lookups: dict các bảng broadcast
    Returns: DataFrame[user_id, product_id, recommendation_score, recommendation_type,
                       computed_time, context_info]
    """
    # ---------- Collaborative ----------
    collab = user_signals.join(
        lookups["complementary"],
        on="seed_product_id",
        how="inner"
    ).select(
        col("user_id"),
        col("candidate_product_id").alias("product_id"),
        (col("signal_weight") * col("affinity_score") * lit(CHANNEL_WEIGHT["collaborative"]))
            .alias("recommendation_score"),
        lit("collaborative").alias("recommendation_type"),
        col("seed_product_id"),
        col("computed_time")
    )

    # ---------- Content-based ----------
    content = user_signals.join(
        lookups["content_neighbors"],
        on="seed_product_id",
        how="inner"
    ).select(
        col("user_id"),
        col("candidate_product_id").alias("product_id"),
        (col("signal_weight") * col("affinity_score") * lit(CHANNEL_WEIGHT["content_based"]))
            .alias("recommendation_score"),
        lit("content_based").alias("recommendation_type"),
        col("seed_product_id"),
        col("computed_time")
    )

    # ---------- Trending fallback ----------
    # Mỗi user trong batch nhận thêm danh sách trending tổng quát (score thấp hơn) để
    # bảo đảm luôn có gợi ý kể cả khi 2 nhánh trên không trả về gì.
    users_in_batch = user_signals.select("user_id", "computed_time").distinct()
    trending_cross = users_in_batch.crossJoin(lookups["trending"]).select(
        col("user_id"),
        col("candidate_product_id").alias("product_id"),
        (col("affinity_score") * lit(CHANNEL_WEIGHT["trending"])).alias("recommendation_score"),
        lit("trending").alias("recommendation_type"),
        lit(None).cast("string").alias("seed_product_id"),
        col("computed_time")
    )

    all_candidates = collab.unionByName(content).unionByName(trending_cross)
    return all_candidates


# ==========================================
# 6. MAIN FLOW
# ==========================================
def run_realtime_recommendation_pipeline():
    spark = get_spark_session()

    lookups = load_static_lookups(spark)

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
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("event_timestamp")))

    # Gắn signal_weight theo event_type
    weighted = parsed_stream.withColumn(
        "signal_weight",
        when(col("event_type") == "purchase", lit(EVENT_WEIGHTS["purchase"]))
        .when(col("event_type") == "cart", lit(EVENT_WEIGHTS["cart"]))
        .when(col("event_type") == "wishlist", lit(EVENT_WEIGHTS["wishlist"]))
        .when(col("event_type") == "view", lit(EVENT_WEIGHTS["view"]))
        .otherwise(lit(0.0))
    ).where(col("signal_weight") > 0)

    # Gom theo (user_id, seed product) trong sliding window 5 phút, slide 1 phút.
    # Lấy max signal trong window — đại diện cho hành vi mạnh nhất user dành cho item.
    user_signals_windowed = weighted \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("user_id"),
            col("product_id").alias("seed_product_id")
        ) \
        .agg(
            smax("signal_weight").alias("signal_weight"),
            # purchased trong window này -> dùng để loại trừ candidate
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchased_cnt")
        ) \
        .withColumn("computed_time", col("window.end").cast("long") * 1000) \
        .select("user_id", "seed_product_id", "signal_weight", "purchased_cnt", "computed_time")

    # foreachBatch xử lý phần còn lại (join, dedupe, top-N) vì update mode + multiple joins
    # với streaming-streaming sẽ phức tạp; mỗi micro-batch là 1 batch DataFrame thông thường.
    def process_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            print(f"--- Batch {batch_id}: empty, skipping ---")
            return

        # Tách seed signals và set sản phẩm đã purchase per user
        seed_signals = batch_df.select(
            "user_id", "seed_product_id", "signal_weight", "computed_time"
        )
        purchased = batch_df.where(col("purchased_cnt") > 0) \
            .select("user_id", col("seed_product_id").alias("excluded_product_id")) \
            .distinct()

        # Sinh candidates từ 3 nhánh
        candidates = build_recommendations(seed_signals, lookups)

        # Loại sản phẩm user đã purchase trong window
        cleaned = candidates.join(
            purchased,
            (candidates.user_id == purchased.user_id) &
            (candidates.product_id == purchased.excluded_product_id),
            how="left_anti"
        )

        # Một (user, product) có thể xuất hiện ở nhiều nhánh -> cộng dồn điểm,
        # gắn type = 'hybrid' nếu nhiều hơn 1 nguồn, ngược lại giữ nguyên nguồn.
        agg = cleaned.groupBy("user_id", "product_id", "computed_time").agg(
            sum("recommendation_score").alias("recommendation_score"),
            collect_set("recommendation_type").alias("types_set"),
            collect_set("seed_product_id").alias("seeds_set")
        )

        typed = agg.withColumn(
            "recommendation_type",
            when(col("types_set").getItem(1).isNotNull(), lit("hybrid"))
            .otherwise(col("types_set").getItem(0))
        ).withColumn(
            "context_info",
            when(col("seeds_set").getItem(0).isNotNull(),
                 concat_ws("|", lit("based_on:"), concat_ws(",", col("seeds_set"))))
            .otherwise(lit("trending_fallback"))
        )

        # Top-N per user
        topn = typed.withColumn(
            "rn",
            row_number().over(
                W.partitionBy("user_id").orderBy(col("recommendation_score").desc())
            )
        ).where(col("rn") <= TOP_N_PER_USER) \
         .select(
            "user_id",
            "product_id",
            col("recommendation_score").cast("double").alias("recommendation_score"),
            "recommendation_type",
            "computed_time",
            "context_info"
         )

        write_batch(topn, batch_id)

    query = user_signals_windowed.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .option("checkpointLocation", "4_speed/checkpoint/user_recommendations_realtime") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    run_realtime_recommendation_pipeline()
