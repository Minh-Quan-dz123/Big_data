# user_segment_job_pandas.py : Pandas (chạy local)
# Input : thư mục local chứa file CSV đã processed
# Output: file CSV local  user_segments.csv

# 1 import
import pandas as pd # là thư viện Python dùng để xử lý, phân tích và làm sạch dữ liệu dạng bảng
from datetime import datetime, timezone
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import os
import sys
# Add root path to PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.utils import current_time


# 2 load data từ local
# 2.1. kiểm tra path
print("Program started!", flush=True)

users_path = "D:/Project_BigData/Big_data/1_dataset/raw_data/users.csv"
ordes_path = "D:/Project_BigData/Big_data/1_dataset/raw_data/orders.csv"
if os.path.exists(users_path):
    # nếu tồn tại thì load
    print("File exists, loading...")

    # load users.csv
    print("Loading file users.csv...")
    users = pd.read_csv(users_path, parse_dates=["signup_date"]) 
    # parse_dates=["signup_date"] = Tự động biến cột signup_date thành 
    # kiểu ngày tháng (datetime), không phải chuỗi (string) nữa”
    print("Users data loaded successfully:")
    print(users.head())
    print()


    # load orders.csv
    print("Loading file orders.csv...")
    orders = pd.read_csv(ordes_path) 
    # parse_dates=["signup_date"] = Tự động biến cột signup_date thành 
    # kiểu ngày tháng (datetime), không phải chuỗi (string) nữa”
    print("orders data loaded successfully:")
    print(orders.head())
    print()

    
else: # nếu file ko tồn tại
    print("File does not exist!")
    
    # dừng
    sys.exit("Cannot proceed without users data")
    


today = current_time().date()

# 3 tính
# ý tưởng dùng KMeans để chia phân khúc khách hàng
# - các yếu tố ảnh hưởng: 
#   + dùng thường xuyên ko -> số ngày đã dùng tk
#   + tổng số đơn hàng, tổng số đơn hàng đã hoàn thành, tổng số đơn cancelled, return
# - phân khúc khách hàng: New User, Frequent Buyer, Discount Hunter, Low Purchase 
# vector[số ngày đã dùng tài khoản, tổng số đơn, đơn hoàn thành, đơn xịt]
# - dùng KMeans chia thành 4 nhóm vector (mỗi nhóm đều có centroid) sau đó gán nhãn từng nhóm

# 3.1 tính các chỉ số của vector
# 3.1.1. số ngày đã dùng tài khoản
users["signup_date"] = pd.to_datetime(users["signup_date"]).dt.date # chuyển sang dạng date
users["days_since_signup"] = (today - users["signup_date"]).apply(lambda x: x.days) # tính ra ngày

# 3.1.2. tính ra tổng số đơn, đơn hoàn thành, đơn hủy/trả
order_status = orders.groupby("user_id").agg(
    total_orders = ("order_id", "count"),
    completed_orders = ("order_status", lambda s: (s == "completed").sum()),
    bad_orders = ("order_status", lambda s: s.isin(["cancelled", "returned"]).sum()),
).reset_index()
# tổng đơn = order_status["total_orders"],...

# tính tỉ lệ hủy đơn
order_status["cancellation_rate"] = order_status["bad_orders"] / order_status["total_orders"]
order_status["cancellation_rate"] = order_status["cancellation_rate"].fillna(0)

# 3.1.3 gộp user + order bằng join
data_feature = users[["user_id", "days_since_signup"]].merge(order_status, on = "user_id", how = "left").fillna(0)

# 3.2 KMeans

# 3.2.1 lấu ra các trường dữ liệu cần
features = data_feature[[
    "days_since_signup",
    "total_orders",
    "completed_orders",
    "cancellation_rate"
]]

# 3.2.2 chuẩn hóa dữ liệu cho features
scaler = StandardScaler() 
X_scaled = scaler.fit_transform(features)

# 3.2.3 tạo KMeans
kmeans = KMeans(n_clusters=4, random_state=50, n_init=10)
data_feature["cluster"] = kmeans.fit_predict(X_scaled)

# 3.2.4 gán nhãn

#    segment_map = {
#        0: "Frequent Shoppers",         # mua nhiều + hủy ít
#        1: "Risky Frequent Buyers",     # mua nhiều + hủy nhiều
#        2: "Low Frequency ",            # mua ít + hủy ít
#        3: "Bad Customer"               # mua ít + hủy nhiều
#    }
#    data_feature["segment_name"] = data_feature["cluster"].map(segment_map)

# 
centroids = pd.DataFrame(
    scaler.inverse_transform(kmeans.cluster_centers_),
    columns=features.columns
)

centroids["cluster"] = centroids.index

# tạo score business
centroids["score"] = (
    centroids["total_orders"] * 0.5 +
    centroids["completed_orders"] * 0.3 -
    centroids["cancellation_rate"] * 100
)

# sort theo chất lượng khách hàng
centroids = centroids.sort_values("score").reset_index(drop=True)

labels = [
    "Bad Customers",
    "Low Frequency Users",
    "Risky Frequent Buyers",
    "Frequent Shoppers"
]

cluster_to_label = {
    int(row["cluster"]): labels[i]
    for i, row in centroids.iterrows()
}

data_feature["segment_name"] = data_feature["cluster"].map(cluster_to_label)

# =========================
# 6. OUTPUT
# =========================
output_path = "D:/Project_BigData/Big_data/1_dataset/output/user_segments.csv"

os.makedirs(os.path.dirname(output_path), exist_ok=True)

data_feature.to_csv(output_path, index=False)

print("\nCENTROIDS:")
print(centroids)


# =========================
# 6. OUTPUT FILE
# =========================
output_path = "D:/Project_BigData/Big_data/1_dataset/output/user_segments.csv"

os.makedirs(os.path.dirname(output_path), exist_ok=True)

# thêm segment_id rõ ràng
data_feature["segment_id"] = data_feature["cluster"]

# chỉ giữ cột cần output
final_output = data_feature[[
    "user_id",
    "segment_id",
    "segment_name",
    "total_orders",
    "completed_orders",
    "bad_orders",
    "cancellation_rate",
    "days_since_signup"
]]

final_output.to_csv(output_path, index=False)

# =========================
# 7. PRINT RESULT
# =========================
print("\n=== USER SEGMENTS SAMPLE ===", flush=True)

print(
    final_output.head(100).to_string(index=False)
)

print("\nDONE ->", output_path, flush=True)