# đây là chương trình tạo ra bảng gợi ý sản phẩm cho khách hàng
# # INPUT (MinIO): s3a://datalake/processed/
#       - order_items.csv
#       - orders.csv
#       - products.csv
#
# INPUT (MongoDB):
#       - ecommerce.user_segments
#       - ecommerce.user_consumption_profile
#       - ecommerce.trending_products
#       - ecommerce.product_similarity
#       - ecommerce.product_complementary
#
# OUTPUT (MongoDB):


# 1 IMPORT LIBRARIES
# 1.1 spark
from pyspark.sql import SparkSession
# lấy class SparkSession làm entry point để làm việc với Spark SQL, DataFrame

#1.2 các hàm 
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, current_timestamp, explode, when, max as spark_max, row_number, coalesce, explode_outer, collect_list, array, size
from datetime import datetime
import logging
import sys


# 2. CONFIG
INPUT_PATH = "s3a://datalake/processed/"

MINIO_CONF = {
    "endpoint": "http://minio-service:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
}

MONGODB_CONF = {
    "uri": "mongodb://mongodb-service.default.svc.cluster.local:27017/",
    "database": "ecommerce",
    "collection": "user_recommendations_batch"
}

# 3 khai báo các object
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


# 4 hàm tạo spark session
def create_spark():
    #mongo_uri = f"{MONGODB_CONF['uri']}/{MONGODB_CONF['database']}.{MONGODB_CONF['collection']}"
    spark = SparkSession.builder \
        .appName("UserRecommendationBatchJob") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    return spark


# 5 hàm load data từ input (CHUYỂN ĐỔI TOÀN BỘ SANG ĐỌC TỪ MONGODB)
class AttrDict(dict):
    # Class phụ trợ giúp truy cập phần tử dict dạng object.attribute giống mã nguồn cũ 
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

def load_data(spark):
    # 5.1 spark lấy data từ minIO
    logger.info("Loading products from MinIO...")
    products = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(INPUT_PATH + "products.csv")

    # 5.2 spark lấy dữ liệu ở mongobd
    logger.info("Loading datasets from MongoDB...")

    # Tạo function đọc nhanh từ Mongo để đỡ lặp code gõ cấu hình options
    def read_from_mongo(collection_name):
        return spark.read \
            .format("mongodb") \
            .option("spark.mongodb.read.connection.uri", MONGODB_CONF["uri"]) \
            .option("database", MONGODB_CONF["database"]) \
            .option("collection", collection_name) \
            .load()
    user_consumption_profile = read_from_mongo("user_consumption_profile")
    trending_products = read_from_mongo("trending_products")
    product_similarity = read_from_mongo("product_similarity")
    product_complementary = read_from_mongo("product_complementary")
    user_segment = read_from_mongo("user_segment")

    logger.info("All datasets loaded successfully from MongoDB")

    # Trả về đối tượng bọc custom để giữ nguyên cú pháp gọi data.tableName bên dưới
    return AttrDict({
        "user_consumption_profile": user_consumption_profile,
        "trending_products": trending_products,
        "product_similarity": product_similarity,
        "product_complementary": product_complementary,
        "user_segment": user_segment,
        "products": products
    })


# 6 hàm build bảng 6
# ý tưởng tính điểm gợi ý
'''
chỉ duyệt trong user_consumption
tại product i (consumption) => tìm similar product [a,b,c] có "similarity_score" [0->1]
tiếp theo là tìm product_complementary có Complementary_score (ví dụ 3.56) (0->vô cùng, nhưng thường là 1->5)
và kiểm tra trending_product có  trend_score (ví dụ 63.2)

=> score = 0.6*type[product]*score[type] + [chuẩn hóa: trend_score]*0.4
type[product] = consumption = 1
type[product] = similar product = 0.8
type[product] = product complementary = 0.7 


output
| user_id | id người dùng (ví dụ: U001922) |
| segment_name| Phân khúc khách hàng ở bảng
| product_id | id sản phẩm được gợi ý (ví dụ: P001233) |
| recommendation_score | điểm gợi ý (ví dụ: 0.87, 0.95) |
| recommendation_type | loại gợi ý (consumption, similar, Complementary, trend) |
| computed_date | ngày tính toán gợi ý  (ví dụ: 2026-04-09T08:04:50)
'''
def build_user_recommendation(data):

    # 6.1 trong user_consumption_profile có dạng 
    '''
    user_id	| avg_order_value	| monthly_spending  | product_ids_in_latest_month | products_in_latest_month | categories_in_latest_month | product_prices_in_latest_month
    1	     450000	                1200000	        [101,205,203]                   ["iPhone 15","AirPods"]	        ["Phone","Accessory"]	    [25000000,4500000]
    2	     320000	                650000	        [122,225,2215]                  ["Bàn phím","Chuột"]	        ["Computer","Computer"]	    [500000,150000]
    '''
    # 6.2 chỉ lấy user_id, product_ids_in_lastest_month as product_id
    logger.info("6. Processing recommendation logic...")
    # user_consumption = data.user_consumption_profile.select("user_id", explode("product_ids_in_latest_month").alias("product_id"))
    # '''
    # user_id | product_id
    # --------+-----------
    # 1       | 10
    # 1       | 20
    # 1       | 30
    # 2       | 15
    # 2       | 18
    # '''
    # # sau đó dùng các hàm trong spark SQL vì Spark sẽ có thể xử lý song song thay vì for
    # #6.3 join với product_similarity để được
    # '''
    # product_id | similar_product_id | similarity_score
    # -----------+--------------------+-----------------
    # 10         | 50                 | 0.9
    # 10         | 60                 | 0.7
    # 20         | 50                 | 0.8
    # '''
    # # Cần cache lại vì dataframe nền này được tái sử dụng để join nhiều nhánh phía sau
    # user_consumption.cache()
    # # 6.4 tương tự ta join với complementary, với trend product
    # # 6.4.1 join với sản phẩm tương tự similarity
    # # đã chuẩn hóa số điểm
    # user_similarity = (
    #     user_consumption.alias("u")
    #     .join(data.product_similarity.alias("s"), 
    #         col("u.product_id") == col("s.product_id_1"),
    #         "inner"
    #         )
    #     .select(col("u.user_id"), 
    #             col("s.product_id_2").alias("product_id"), 
    #             col("s.similarity_score").alias("score"))
    #     .withColumn("type", lit("similar"))
    # )


    # # 6.4.2 join với sản phẩm đi kèm
    # # (0 → +∞ (thực tế 1–5)] chuẩn hóa bằng cách chia max
    # # tìm max_score trước
    # max_comp = data.product_complementary.agg(spark_max("complementary_score")).first()[0]
    # # thêm tí an toàn
    # if max_comp == 0:
    #     max_comp = 1

    # user_complementary = (
    #     user_consumption.alias("a")
    #     .join(data.product_complementary.alias("b"),
    #           col("a.product_id") == col("b.product_id_1"),
    #           "inner")
    #     .select(col("a.user_id"),
    #             col("b.product_id_2").alias("product_id"),
    #             (coalesce(col("b.complementary_score"), lit(0.0))/lit(max_comp)).alias("score")) # chuẩn hóa
    #     .withColumn("type", lit("complementary"))
    # )

    # # 6.4.3 thêm cột type cho consumption
    # user_consumption = user_consumption.withColumn("score", lit(1.0)).withColumn("type", lit("consumption"))

    # # 6.5 lúc này tạo user_id | product_id | score | type
    # # product_id có thể trùng nhưng type thì khác
    # # tiếp theo ta sẽ tính điểm join với trend product
    
    # # 6.5.1 tạo user_id | product_id | score | type   bằng union
    # user_candidate = user_consumption.unionByName(user_similarity).unionByName(user_complementary)

    # # 6.5.2 tạo bảng join với trend product
    # # [0 → 100]
    # user_candidate_final = (
    #     user_candidate.alias("a")
    #     .join(data.trending_products.alias("b"),
    #         col("a.product_id") == col("b.product_id"),
    #         "left")
    #     .select(col("a.user_id"), col("a.product_id"), col("a.score"), col("a.type"), 
    #     (coalesce(col("b.trend_score"), lit(0))/100).alias("trend_score"))
    # )

    # 1. LẤY DANH SÁCH TOP 10 TRENDING ĐỂ DỰ PHÒNG
    top_trending = data.trending_products.orderBy(col("trend_score").desc()).limit(10).collect()
    top_trending_ids = [row["product_id"] for row in top_trending]
    
    # Cách đúng để tạo cột List trong PySpark
    trend_list_col = array([lit(pid) for pid in top_trending_ids])

    # 2. XỬ LÝ USER CONSUMPTION (Giữ lại tất cả User)
    user_base = data.user_consumption_profile.withColumn(
        "safe_product_ids",
        when(
            (col("product_ids_in_latest_month").isNull()) | (size(col("product_ids_in_latest_month")) == 0), 
            trend_list_col # Nếu chưa mua gì, bơm luôn 10 sản phẩm trending vào lịch sử giả
        ).otherwise(col("product_ids_in_latest_month"))
    )

    user_consumption = user_base.select(
        "user_id", 
        explode("safe_product_ids").alias("product_id")
    )
    
    # Nhớ cache lại vì dataframe này dùng nhiều lần
    user_consumption.cache()

    # 3. CHẠY LẠI CÁC LUỒNG JOIN (Đã mở comment code cũ của bạn)
    # 3.1 Sản phẩm tương tự
    user_similarity = (
        user_consumption.alias("u")
        .join(data.product_similarity.alias("s"), 
            col("u.product_id") == col("s.product_id_1"),
            "inner"
            )
        .select(col("u.user_id"), 
                col("s.product_id_2").alias("product_id"), 
                col("s.similarity_score").alias("score"))
        .withColumn("type", lit("similar"))
    )

    # 3.2 Sản phẩm đi kèm
    max_comp = data.product_complementary.agg(spark_max("complementary_score")).first()[0]
    if max_comp is None or max_comp == 0:
        max_comp = 1

    user_complementary = (
        user_consumption.alias("a")
        .join(data.product_complementary.alias("b"),
              col("a.product_id") == col("b.product_id_1"),
              "inner")
        .select(col("a.user_id"),
                col("b.product_id_2").alias("product_id"),
                (coalesce(col("b.complementary_score"), lit(0.0))/lit(max_comp)).alias("score"))
        .withColumn("type", lit("complementary"))
    )

    # Thêm type cho base consumption
    user_consumption = user_consumption.withColumn("score", lit(1.0)).withColumn("type", lit("consumption"))

    # 4. GỘP CÁC TẬP GỢI Ý & JOIN TRENDING
    user_candidate = user_consumption.unionByName(user_similarity).unionByName(user_complementary)

    user_candidate_final = (
        user_candidate.alias("a")
        .join(data.trending_products.alias("b"), col("a.product_id") == col("b.product_id"), "left")
        .select(
            col("a.user_id"), col("a.product_id"), col("a.score"), col("a.type"), 
            (coalesce(col("b.trend_score"), lit(0))/100).alias("trend_score")
        )
    )

    # 6.5.3 tính điểm recommendation ct = 0.5* độ ưu tiên[type] * score[product] + 0.5*trend_score
    # nếu trùng product_id thì giữ lại cái có điểm cao hơn
    type_priority = {
        "consumption": 1.0,
        "similar": 0.8,
        "complementary": 0.7
    }

    # logic: tạo thêm 2 cột priority và score_final cột điểm cuối cùng là được
    user_scored = user_candidate_final.withColumn(
        "priority",
        when(col("type") == "consumption", lit(type_priority["consumption"]))
        .when(col("type") == "similar", lit(type_priority["similar"]))
        .when(col("type") == "complementary", lit(type_priority["complementary"]))
        .otherwise(lit(0.3)) 
    ).withColumn(
        "final_score",
        0.5*col("priority")*col("score")+0.5*col("trend_score")
    )
    # đến đây ta có user_id | product_id | score | type | priority | final_score

    # 6.5.4. dùng window để bỏ các product_id bị trùng và chỉ giữ lại cái có final_score cao
    # giữ lại user_id | product_id | type | final_score
    window_s = Window.partitionBy("user_id", "product_id") \
                    .orderBy(col("final_score").desc())
    user_recommendation = user_scored.withColumn(
        "stt",
        row_number().over(window_s)
    ).filter(col("stt") == 1).drop("stt") # xóa cột phụ, chỉ giữ lại cột 1 trong mỗi patition

    result_0 = user_recommendation.select(col("user_id"), col("product_id"), col("type"), col("final_score"))

    # 6.5.5. Cuối cùng join để lấy bảng user_segment
    result = (
        result_0.alias("r")
        .join(
            data.user_segment.alias("u"),
            col("r.user_id") == col("u.user_id"),
            "left"
        )
        .join(
            data.products.alias("p"),
            col("r.product_id") == col("p.product_id"),
            "left"
        )
        .select(
            col("r.user_id"),
            col("u.segment_name"),
            col("r.product_id"),
            col("p.product_name"),
            col("p.category"),
            col("r.final_score").alias("recommendation_score"),
            col("r.type").alias("recommendation_type"),
            current_timestamp().alias("computed_date")
        )
    )
    # Xóa bộ nhớ đệm sau khi hoàn tất chuỗi tính toán
    user_consumption.unpersist()
    return result
    


# 7 lưu vào mongodb
def save_to_mongodb(df):
    logger.info("Saving recommendations results to MongoDB...")

    # Đồng bộ hóa schema chọn cột cuối cùng trước khi ghi xuống Serving Layer
    final_df = df.select(
        "user_id",
        "segment_name",
        "product_id",
        "product_name",
        "category",
        "recommendation_score",
        "recommendation_type",
        "computed_date"
    )

    # Ghi đè (overwrite) tập kết quả gợi ý tổng hợp của 6 Job xuống bảng đích của Mongo
    final_df.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("spark.mongodb.write.connection.uri", MONGODB_CONF["uri"]) \
        .option("database", MONGODB_CONF["database"]) \
        .option("collection", MONGODB_CONF["collection"]) \
        .save()

    logger.info("Saved to MongoDB successfully")


def run_pipeline():
    # 1 gọi loger
    logger.info("=" * 60)
    logger.info("START USER RECOMMENDATION BATCH JOB")
    logger.info("=" * 60)

    # 2 tạo spark
    spark = None
    start_time = datetime.now()

    
    try:
        spark = create_spark()
        spark.sparkContext.setLogLevel("ERROR")
    # 3 lấy data
        data = load_data(spark)

    # 4 build bảng
        res_df = build_user_recommendation(data)

    # 5 save to mongodb
        save_to_mongodb(res_df)

    # 6 in log thời gian hoàn thành
        duration = (datetime.now() - start_time).total_seconds()

        logger.info(f"PIPELINE SUCCESS in {duration}s")

    # 7 dừng spark
    except Exception as e:
        logger.error("PIPELINE FAILED")
        logger.error(str(e), exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
        logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()