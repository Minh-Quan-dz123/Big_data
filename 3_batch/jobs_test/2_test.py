# INPUT:
#   thư mục local chứa:
#       - users.csv
#       - orders.csv
#       - order_items.csv
#       - products.csv
# OUTPUT:
#   file local:
#       user_consumption_profile.csv

# =========================================================
# 1 IMPORT
# =========================================================

import pandas as pd # thư viện xử lý dữ liệu dạng bảng
from datetime import datetime # xử lý ngày giờ
import os # thao tác file/path
import sys # thao tác hệ thống

# =========================================================
# 2 LOAD DATA TỪ LOCAL
# =========================================================
print("Program started!", flush=True)


# =========================================================
# 2.1 KHAI BÁO PATH
# =========================================================

users_path = "D:/Project_BigData/Big_data/1_dataset/raw_data/users.csv"
orders_path = "D:/Project_BigData/Big_data/1_dataset/raw_data/orders.csv"
order_items_path = "D:/Project_BigData/Big_data/1_dataset/raw_data/order_items.csv"
products_path = "D:/Project_BigData/Big_data/1_dataset/raw_data/products.csv"

# =========================================================
# 2.2 KIỂM TRA FILE TỒN TẠI
# =========================================================

if (
    os.path.exists(users_path)
    and os.path.exists(orders_path)
    and os.path.exists(order_items_path)
    and os.path.exists(products_path)
):
    print("All files exist, loading...", flush=True)

    # LOAD USERS
    print("\nLoading users.csv...", flush=True)
    users = pd.read_csv(users_path)
    print("Users loaded successfully:")
    print(users.head())
    print()

    # LOAD ORDERS
    print("Loading orders.csv...", flush=True)
    orders = pd.read_csv(orders_path)
    print("Orders loaded successfully:")
    print(orders.head())
    print()

    # LOAD ORDER ITEMS
    print("Loading order_items.csv...", flush=True)
    order_items = pd.read_csv(order_items_path)
    print("Order items loaded successfully:")
    print(order_items.head())
    print()

    # LOAD PRODUCTS
    print("Loading products.csv...", flush=True)
    products = pd.read_csv(products_path)
    print("Products loaded successfully:")
    print(products.head())
    print()

else:

    print("One or more files do not exist!")
    sys.exit("Cannot proceed without input data")


# =========================================================
# 3 DATA PROCESSING
# =========================================================

# Ý tưởng:
# Tạo bảng: user_consumption_profile
# gồm:
#   avg_order_value = giá trị trung bình 1 đơn completed
#   monthly_spending = tổng tiền mỗi h
#   user_behavior = category mua nhiều nhất
# =========================================================

# 3.1 CHỈ LẤY ĐƠN COMPLETED trong orders
# Vì:
# - cancelled
# - returned
# không phản ánh hành vi mua thực sự
completed_orders = orders[orders["order_status"] == "completed"].copy()

print("\nCompleted orders:")
print(completed_orders.head())


# =========================================================
# 3.2 AVG ORDER VALUE
# avg_order_value = trung bình tổng tiền mỗi order
# bước: 1. tính total từng order
#       2. group theo user rồi lấy mean
# =========================================================

# tổng tiền từng order
avg_order_value = completed_orders.groupby("user_id")["total_amount"].mean().reset_index()
avg_order_value.rename(columns={"total_amount": "avg_order_value"}, inplace=True)

print("\n================ AVG ORDER VALUE ================\n")
print(avg_order_value.head())


# =========================================================
# 3.3 MONTHLY SPENDING
# =========================================================
# monthly_spending = tổng tiền mua theo tháng
# ví dụ:
# 2026-01 -> 2 triệu
# 2026-02 -> 5 triệu
# =========================================================

# convert order_date -> datetime
completed_orders["order_date"] = pd.to_datetime(completed_orders["order_date"])

# tạo year_month
completed_orders["year_month"] = (completed_orders["order_date"].dt.strftime("%Y-%m"))

# group theo user + month
monthly_spending = completed_orders.groupby(
    ["user_id", "year_month"]
)["total_amount"].sum().reset_index()

monthly_spending.rename(
    columns={"total_amount": "monthly_spending"},
    inplace=True
)

print("\n================ MONTHLY SPENDING ================\n")
print(monthly_spending.head())

# =========================================================
# 3.4 LẤY THÁNG GẦN NHẤT
# Vì: 1 user có nhiều tháng
# nên: chỉ lấy tháng mới nhất

latest_monthly = monthly_spending.sort_values(["user_id", "year_month"],ascending=[True, False])
latest_monthly = latest_monthly.groupby("user_id").first().reset_index()

print("\n================ LATEST MONTHLY ================\n")
print(latest_monthly.head())

# =========================================================
# 3.5 USER BEHAVIOR
# - Lấy toàn bộ sản phẩm user mua trong THÁNG GẦN NHẤT
# - Có: product_name, category, price

# B1: GHÉP completed_orders với latest_monthly để lọc đúng tháng gần nhất
latest_orders = completed_orders.merge(
    latest_monthly[["user_id", "year_month"]],
    on="user_id",
    how="inner"
)
# CHỈ LẤY ORDER TRONG THÁNG GẦN NHẤT
latest_orders = latest_orders[
    latest_orders["year_month_x"] == latest_orders["year_month_y"]
].copy()

# đổi tên cho rõ ràng (TRÁNH NHẦM user_id / month)
latest_orders = latest_orders.rename(columns={"year_month_x": "year_month"})


behavior_df = latest_orders.merge(order_items, on="order_id",how="inner")
behavior_df = behavior_df.merge(products,on="product_id",how="left")
behavior_df = behavior_df.rename(columns={"user_id_x": "user_id"})
print("\n================ BEHAVIOR DATA ================\n")
print(behavior_df.head())

# ---------------------------------------------------------
# BƯỚC 3: GOM TOÀN BỘ SẢN PHẨM USER ĐÃ MUA TRONG THÁNG
# ---------------------------------------------------------

top_behavior = behavior_df.groupby("user_id").agg({
    "product_name": lambda x: list(x),
    "category": lambda x: list(x),
    "price": lambda x: list(x)
}).reset_index()

# ---------------------------------------------------------
# BƯỚC 4: ĐỔI TÊN CỘT OUTPUT
# ---------------------------------------------------------

top_behavior = top_behavior.rename(columns={
    "product_name": "products_in_latest_month",
    "category": "categories_in_latest_month",
    "price": "product_prices_in_latest_month"
})


print("\n================ TOP USER CATEGORY ================\n")
print(top_behavior.head())

# =========================================================
# 4 FINAL OUTPUT
# =========================================================

# chỉ lấy user_id
final_output = users[["user_id"]]

# 4.1 JOIN avg_order_value
final_output = final_output.merge(
    avg_order_value,
    on="user_id",
    how="left"
)

# 4.2 JOIN latest_monthly
final_output = final_output.merge(
    latest_monthly[[
        "user_id",
        "monthly_spending"
    ]],
    on="user_id",
    how="left"
)



# =========================================================
# 4.3 JOIN top_behavior
# =========================================================
final_output = final_output.merge(
    top_behavior,
    on="user_id",
    how="left"
)



# =========================================================
# 4.4 FILL NULL
# =========================================================

final_output["avg_order_value"] = (
    final_output["avg_order_value"]
    .fillna(0)
)

final_output["monthly_spending"] = (
    final_output["monthly_spending"]
    .fillna(0)
)

final_output["products_in_latest_month"] = (
    final_output["products_in_latest_month"]
    .fillna("[]")
)

final_output["categories_in_latest_month"] = (
    final_output["categories_in_latest_month"]
    .fillna("[]")
)

final_output["product_prices_in_latest_month"] = (
    final_output["product_prices_in_latest_month"]
    .fillna("[]")
)

# =========================================================
# 5 OUTPUT FILE
# =========================================================

output_path = (
    "D:/Project_BigData/Big_data/"
    "1_dataset/output/user_consumption_profile.csv"
)

# tạo folder nếu chưa có
os.makedirs(
    os.path.dirname(output_path),
    exist_ok=True
)

# ghi csv
final_output.to_csv(
    output_path,
    index=False
)


print("\n================ FINAL RESULT ================\n")
print(
    final_output.head(100).to_string(index=False)
)
print("\nDONE ->", output_path, flush=True)