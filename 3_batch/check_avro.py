from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Check_And_Export_Avro") \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.3.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1. Đọc file Avro của giỏ hàng (Cart) lên
print("--- Đang đọc dữ liệu từ file Avro... ---")
df_cart = spark.read.format("avro").load("./3_batch/3_3_batch_view/cart_complementary.avro")

# Đếm tổng số dòng
print(f"Tổng số cặp sản phẩm: {df_cart.count()}")

# 2. XUẤT RA FILE CSV ĐỂ XEM BÊN NGOÀI
# coalesce(1) giúp gộp tất cả data vào đúng 1 file csv duy nhất cho dễ mở
export_path = "./3_batch/3_3_batch_view/cart_export_csv"
df_cart.coalesce(1).write.csv(export_path, header=True, mode="overwrite")

print(f"--- Đã xuất ra file CSV thành công tại: {export_path} ---")