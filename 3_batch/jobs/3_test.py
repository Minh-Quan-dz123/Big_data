# 3_test.py : Pandas (chạy local test job 3)
# Input : file CSV raw local
# Output: file CSV local trending_products.csv

# 1. IMPORT
import pandas as pd
from datetime import datetime, timezone
import os
import sys

# =========================================================
print("Program started!", flush=True)

# =========================================================
# 2. KHAI BÁO PATH
# =========================================================

events_path = "1_dataset/raw_data/events.csv"
orders_path = "1_dataset/raw_data/orders.csv"
order_items_path = "1_dataset/raw_data/order_items.csv"
products_path = "1_dataset/raw_data/products.csv"

# =========================================================
# 3. KIỂM TRA & LOAD DATA
# =========================================================

# Khởi tạo DataFrames
events = pd.DataFrame()
orders = pd.DataFrame()
order_items = pd.DataFrame()

if os.path.exists(events_path):
    print("Loading events.csv...")
    events = pd.read_csv(events_path, parse_dates=["event_timestamp"])
else:
    print("WARNING: events.csv not found. Creating empty dataframe.")
    events = pd.DataFrame(columns=["event_id", "user_id", "product_id", "event_type", "event_timestamp"])

if os.path.exists(orders_path) and os.path.exists(order_items_path):
    print("Loading orders.csv & order_items.csv...")
    orders = pd.read_csv(orders_path, parse_dates=["order_date" if "order_date" in pd.read_csv(orders_path, nrows=0).columns else "order_data"])
    # Đổi tên cột nếu file bị sai chính tả thành order_data
    if "order_data" in orders.columns:
        orders.rename(columns={"order_data": "order_date"}, inplace=True)
        
    order_items = pd.read_csv(order_items_path)
else:
    print("ERROR: orders.csv or order_items.csv not found!")
    sys.exit("Cannot proceed without orders data")

# Xác định 'today' linh hoạt theo dữ liệu (do data có thể cũ)
max_order_date = pd.to_datetime(orders["order_date"]).max()
if not events.empty:
    max_event_date = pd.to_datetime(events["event_timestamp"]).max()
    today_ts = max(max_order_date, max_event_date)
else:
    today_ts = max_order_date
    
print(f"Using reference date: {today_ts}")

# =========================================================
# 4. TÍNH TOÁN (Tương đương Spark DataFrame)
# =========================================================
print("\nProcessing view stats...", flush=True)

# 4.1 VIEW STATS
view_stats = pd.DataFrame(columns=["product_id", "current_views", "previous_views", "view_growth"])

if not events.empty:
    views = events[events["event_type"] == "view"].copy()
    if not views.empty:
        views["days_ago"] = (today_ts - views["event_timestamp"]).dt.days

        # Phân chia window
        views["is_current"] = (views["days_ago"] >= 0) & (views["days_ago"] <= 7)
        views["is_previous"] = (views["days_ago"] > 7) & (views["days_ago"] <= 14)

        current_views_df = views[views["is_current"]].groupby("product_id").size().reset_index(name="current_views")
        previous_views_df = views[views["is_previous"]].groupby("product_id").size().reset_index(name="previous_views")

        # Gom lại
        view_stats = pd.merge(current_views_df, previous_views_df, on="product_id", how="outer").fillna(0)
        view_stats["view_growth"] = (view_stats["current_views"] - view_stats["previous_views"]) / (view_stats["previous_views"] + 1)


print("Processing order stats...", flush=True)

# Phân chia window
orders["order_date_ts"] = pd.to_datetime(orders["order_date"])
orders["days_ago"] = (today_ts - orders["order_date_ts"]).dt.days

# Join orders and order_items
order_details = pd.merge(orders, order_items, on="order_id", how="inner")

# Phân chia window
order_details["is_current"] = (order_details["days_ago"] >= 0) & (order_details["days_ago"] <= 7)
order_details["is_previous"] = (order_details["days_ago"] > 7) & (order_details["days_ago"] <= 14)

# Tính tổng số lượng (quantity)
current_orders_df = order_details[order_details["is_current"]].groupby("product_id")["quantity"].sum().reset_index(name="current_orders")
previous_orders_df = order_details[order_details["is_previous"]].groupby("product_id")["quantity"].sum().reset_index(name="previous_orders")

# Gom order
order_stats = pd.merge(current_orders_df, previous_orders_df, on="product_id", how="outer").fillna(0)
order_stats["order_growth"] = (order_stats["current_orders"] - order_stats["previous_orders"]) / (order_stats["previous_orders"] + 1)


print("Calculating trend score...", flush=True)

# 4.3 KẾT HỢP VÀ TÍNH TREND SCORE
# Join view_stats và order_stats
if not view_stats.empty and not order_stats.empty:
    trending_df = pd.merge(view_stats, order_stats, on="product_id", how="outer").fillna(0)
elif not view_stats.empty:
    trending_df = view_stats.copy()
    trending_df["current_orders"] = 0
    trending_df["previous_orders"] = 0
    trending_df["order_growth"] = 0.0
elif not order_stats.empty:
    trending_df = order_stats.copy()
    trending_df["current_views"] = 0
    trending_df["previous_views"] = 0
    trending_df["view_growth"] = 0.0
else:
    print("No data available to calculate trending products.")
    sys.exit()

# Điền giá trị null
trending_df.fillna(0, inplace=True)

# Công thức trend_score
trending_df["trend_score"] = (
    (trending_df["current_views"] * 0.2 + trending_df["current_orders"] * 0.8) +
    (trending_df["view_growth"] * 5.0 + trending_df["order_growth"] * 15.0)
)

# Thêm các field tĩnh
trending_df["trend_window"] = "7d"
trending_df["trend_date"] = int(datetime.now().timestamp() * 1000)

# Lọc các sản phẩm có điểm xu hướng lớn hơn 0 và sắp xếp
trending_df = trending_df[trending_df["trend_score"] > 0]
trending_df = trending_df.sort_values(by="trend_score", ascending=False).reset_index(drop=True)

# =========================================================
# 5. OUTPUT
# =========================================================

final_output = trending_df[[
    "product_id",
    "trend_score",
    "view_growth",
    "order_growth",
    "trend_window",
    "trend_date"
]]

output_path = "1_dataset/output/trending_products.csv"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

final_output.to_csv(output_path, index=False)

print("\n=== TRENDING PRODUCTS SAMPLE ===", flush=True)
print(final_output.head(20).to_string(index=False))

print(f"\nDONE -> {output_path} (Total records: {len(final_output)})", flush=True)
