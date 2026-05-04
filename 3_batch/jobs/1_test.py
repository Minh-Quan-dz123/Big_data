# user_segment_job_pandas.py : Pandas (chạy local)
# Input : thư mục local chứa file CSV đã processed
# Output: file CSV local  user_segments.csv

# 1 import
import pandas as pd # là thư viện Python dùng để xử lý, phân tích và làm sạch dữ liệu dạng bảng
from datetime import datetime, timezone
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# 2 cấu hình - load data
orders = pd.read_csv("D:/Project_BigData/Big_data/processed/orders.csv")
users = pd.read_csv("D:/Project_BigData/Big_data/processed/user.csv", parse_dates = ["sigup_date"])

today = datetime.now(timezone.utc).date() #timezone.utc lấy ngày ko lấy giờ

# 3 tính
