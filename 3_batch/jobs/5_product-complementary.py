from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, size, lit, unix_timestamp, current_timestamp, when, round
from pyspark.ml.fpm import FPGrowth

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Batch_Product_Complementary") \
    .getOrCreate()

# Tắt bớt log INFO cho đỡ rác màn hình terminal
spark.sparkContext.setLogLevel("WARN")

print("--- Bắt đầu tiến trình tính toán Sản phẩm mua kèm (FP-Growth) ---")

# ==========================================
# BƯỚC 1: ĐỌC DỮ LIỆU TỪ RAW DATA
# ==========================================
# Sử dụng dấu * để tự động lấy thư mục ecommerce_... của Huy
order_items_path = "./1_dataset/raw_data/*/order_items.csv"
products_path = "./1_dataset/raw_data/*/products.csv"

df_order_items = spark.read.csv(order_items_path, header=True, inferSchema=True)
df_products = spark.read.csv(products_path, header=True, inferSchema=True)

# ==========================================
# BƯỚC 2: CHUẨN BỊ "GIỎ HÀNG" CHO FP-GROWTH
# ==========================================
# Nhóm các product_id theo từng order_id
baskets_df = df_order_items.groupBy("order_id").agg(collect_set("product_id").alias("Items"))

# Tối ưu hóa: Chỉ lấy những đơn hàng có mua từ 2 sản phẩm trở lên
baskets_df = baskets_df.filter(size(col("Items")) > 1)

# Lấy tổng số đơn hàng hợp lệ để tính Co_purchase_count phía sau
total_baskets = baskets_df.count()
print(f"--- Đã tạo giỏ hàng thành công. Tổng số đơn hàng hợp lệ: {total_baskets} ---")

# ==========================================
# BƯỚC 3: CHẠY THUẬT TOÁN FP-GROWTH
# ==========================================
# Lưu ý: minSupport và minConfidence có thể chỉnh nhỏ lại nếu data mẫu quá ít để ra kết quả
fpGrowth = FPGrowth(itemsCol="Items", minSupport=0.005, minConfidence=0.05)
model = fpGrowth.fit(baskets_df)

# Lấy ra các bộ quy tắc (Association Rules)
rules = model.associationRules

# Chỉ lấy những luật 1 đổi 1 (A -> B)
rules_1_1 = rules.filter("size(antecedent) == 1 AND size(consequent) == 1")

# Tách array thành chuỗi String
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

# Join để lấy danh mục của sản phẩm 1
enriched_df = rules_parsed.join(
    prod_category.withColumnRenamed("product_id", "Product_id_1").withColumnRenamed("category", "cat_1"),
    on="Product_id_1", how="left"
)

# Join để lấy danh mục của sản phẩm 2
enriched_df = enriched_df.join(
    prod_category.withColumnRenamed("product_id", "Product_id_2").withColumnRenamed("category", "cat_2"),
    on="Product_id_2", how="left"
)

# Áp dụng công thức và tạo Schema đích
final_df = enriched_df.select(
    col("Product_id_1"),
    col("Product_id_2"),
    lit("co_purchase").alias("Relationship_type"),
    # Co_purchase_count = Tỷ lệ xuất hiện chung * Tổng số đơn
    (col("support") * lit(total_baskets)).cast("long").alias("Co_purchase_count"),
    
    col("confidence").cast("float").alias("Confidence"),
    
    # Nếu danh mục 1 khác danh mục 2 => True (Bán chéo)
    when(col("cat_1") != col("cat_2"), lit(True)).otherwise(lit(False)).alias("Category_cross_sell"),
    
    # Điểm Complementary Score (Dùng 70% confidence + 30% lift scale xuống làm ví dụ)
    # Hàm round để làm tròn 2 chữ số thập phân
    round((col("confidence") * 0.7) + ((col("lift") / 10) * 0.3), 2).cast("float").alias("Complementary_score"),
    
    # Thời gian chạy logic
    (unix_timestamp(current_timestamp()) * 1000).cast("long").alias("Computed_date")
)

print("--- Kết quả tính toán mẫu: ---")
# thay số để hiện ra kết quả
final_df.show(5, truncate=False) 
total_rules = final_df.count()
print(f"--- Tổng số cặp sản phẩm tìm được: {total_rules} cặp ---")

# ==========================================
# BƯỚC 5: XUẤT RA FILE AVRO
# ==========================================
# Xuất vào thư mục 3_3_batch_view (hoặc Data_lake tùy ý Huy)
output_path = "./3_batch/3_3_batch_view/product_complementary.avro"
final_df.write.format("avro").mode("overwrite").save(output_path)

print(f"--- Đã lưu dữ liệu thành công dưới định dạng Avro tại: {output_path} ---")