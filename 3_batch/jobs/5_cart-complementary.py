from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, size, lit, unix_timestamp, current_timestamp, when, round
from pyspark.ml.fpm import FPGrowth

# ==========================================
# KHỞI TẠO ĐỘNG CƠ SPARK
# ==========================================
spark = SparkSession.builder \
    .appName("Batch_Cart_Complementary") \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.3.0") \
    .getOrCreate()

# Tắt bớt log INFO cho đỡ rác màn hình terminal
spark.sparkContext.setLogLevel("WARN")

print("--- Bắt đầu tiến trình phân tích Mối quan hệ trong Giỏ hàng (Cart) ---")

# ==========================================
# BƯỚC 1: ĐỌC DỮ LIỆU TỪ BẢNG EVENTS (HÀNH VI)
# ==========================================
events_path = "./1_dataset/raw_data/events.csv"
products_path = "./1_dataset/raw_data/products.csv"

df_events = spark.read.csv(events_path, header=True, inferSchema=True)
df_products = spark.read.csv(products_path, header=True, inferSchema=True)

# ==========================================
# BƯỚC 2: CHUẨN BỊ "GIỎ HÀNG TẠM THỜI"
# ==========================================
# 1. Chỉ lọc ra những hành động 'cart' (thêm vào giỏ)
df_carts = df_events.filter(col("event_type") == "cart")

# 2. Gom nhóm theo user_id thay vì order_id
baskets_df = df_carts.groupBy("user_id").agg(collect_set("product_id").alias("Items"))

# 3. Lọc lấy những người dùng có từ 2 sản phẩm trong giỏ trở lên
baskets_df = baskets_df.filter(size(col("Items")) > 1)

# Lấy tổng số user hợp lệ để tính toán phía sau
total_baskets = baskets_df.count()
print(f"--- Đã tạo tập dữ liệu Cart thành công. Tổng số User hợp lệ: {total_baskets} ---")

# ==========================================
# BƯỚC 3: CHẠY THUẬT TOÁN FP-GROWTH
# ==========================================
# Do dữ liệu click/cart thường nhiễu hơn, ép ngưỡng support/confidence nhỏ xuống để thấy kết quả
fpGrowth = FPGrowth(itemsCol="Items", minSupport=0.0001, minConfidence=0.01)
model = fpGrowth.fit(baskets_df)

# Trích xuất luật kết hợp
rules = model.associationRules

# Chỉ lấy những luật 1 đổi 1 (A -> B)
rules_1_1 = rules.filter("size(antecedent) == 1 AND size(consequent) == 1")

# Tách chuỗi
rules_parsed = rules_1_1.select(
    col("antecedent").getItem(0).alias("Product_id_1"),
    col("consequent").getItem(0).alias("Product_id_2"),
    col("support"),
    col("confidence"),
    col("lift")
)

# ==========================================
# BƯỚC 4: ENRICH DATA & MAP SCHEMA CHUẨN
# ==========================================
# Lấy danh mục sản phẩm từ bảng products
prod_category = df_products.select("product_id", "category")

# Join lấy danh mục sản phẩm 1
enriched_df = rules_parsed.join(
    prod_category.withColumnRenamed("product_id", "Product_id_1").withColumnRenamed("category", "cat_1"),
    on="Product_id_1", how="left"
)

# Join lấy danh mục sản phẩm 2
enriched_df = enriched_df.join(
    prod_category.withColumnRenamed("product_id", "Product_id_2").withColumnRenamed("category", "cat_2"),
    on="Product_id_2", how="left"
)

# Nặn lại Data thành chuẩn Schema
final_df = enriched_df.select(
    col("Product_id_1"),
    col("Product_id_2"),
    
    # Đổi cờ thành 'co_cart' để phân biệt rõ với 'co_purchase' của đơn hàng đã chốt
    lit("co_cart").alias("Relationship_type"), 
    
    # Số lần cùng xuất hiện trong giỏ = Tỷ lệ * Tổng số user
    (col("support") * lit(total_baskets)).cast("long").alias("Co_purchase_count"), 
    
    col("confidence").cast("float").alias("Confidence"),
    
    # Nếu danh mục 1 khác danh mục 2 => True (Bán chéo ngành)
    when(col("cat_1") != col("cat_2"), lit(True)).otherwise(lit(False)).alias("Category_cross_sell"),
    
    # Tính điểm xếp hạng (Vẫn dùng công thức 70% Confidence + 30% Lift_scaled)
    round((col("confidence") * 0.7) + ((col("lift") / 10) * 0.3), 2).cast("float").alias("Complementary_score"),
    
    # Thời gian chạy logic
    (unix_timestamp(current_timestamp()) * 1000).cast("long").alias("Computed_date")
)

print("--- Kết quả tính toán mẫu từ Hành vi Giỏ hàng: ---")
final_df.show(5, truncate=False)
# ==========================================
# BƯỚC 5: XUẤT RA FILE AVRO
# ==========================================
# Đổi tên file đích thành cart_complementary để không ghi đè lên file cũ
output_path = "./3_batch/3_3_batch_view/cart_complementary.avro"
final_df.write.format("avro").mode("overwrite").save(output_path)

print(f"--- Đã lưu dữ liệu Giỏ hàng thành công dưới định dạng Avro tại: {output_path} ---")