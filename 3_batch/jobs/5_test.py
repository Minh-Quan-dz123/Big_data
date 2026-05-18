"""
Job: Tính sản phẩm đi cùng nhau 
- Sử dụng FP-Growth (Frequent Pattern Growth) là một thuật toán dùng để:
"""

import os
import glob
import time
import pandas as pd

from mlxtend.frequent_patterns import fpgrowth, association_rules

# 1. input, output test ở local

intput_path = "D:/Project_BigData/Big_data/1_dataset/raw_data/"
output_path = "D:/Project_BigData/Big_data/1_dataset/output/"

# Tạo folder output nếu chưa tồn tại
os.makedirs(output_path, exist_ok=True)

print("====================================================")
print("BẮT ĐẦU CHẠY FP-GROWTH LOCAL (KHÔNG SPARK)")
print("====================================================")

start_time = time.time()

# 2. Đọc dữ liệu từ local

# Đọc trực tiếp 1 file
df_order_items = pd.read_csv(intput_path + "/order_items.csv", usecols=["order_id", "product_id"])
df_products = pd.read_csv(intput_path + "/products.csv", usecols=["product_id", "category"])

print(f"Loaded order_items: {df_order_items.shape}")
print(f"Loaded products: {df_products.shape}")

# 3. TẠO GIỎ HÀNG

# 3.1. tạo giỏ hàng = Gom theo đơn hàng ví dụ order_id 1 thì có 1 list product_id
# ví dụ order_id: [product1_id, product2_id,...]
baskets = df_order_items.groupby("order_id")["product_id"].apply(list)

# 3.2. Lọc chỉ lấy giỏ hàng có >= 2 sản phẩm 
baskets = baskets[baskets.apply(lambda x: len(x) > 1)]

print(f"Tổng số giỏ hàng hợp lệ: {len(baskets)}")

# 4. CHUYỂN SANG ONE-HOT ENCODING

# FP-Growth trong mlxtend cần dạng:
# transaction matrix (True/False hoặc 1/0)

# 4.1. Lấy toàn bộ product_id duy nhất
# ví dụ  (theo chiều dọc) A B C A D -> thành theo chiều ngang all_items = [A, B, C, D]
all_items = list(set(df_order_items["product_id"]))

# 4.2 chuyển [A, B] thành
# A	 B	C  D
# 1	 1	0  0
encoded_df = pd.DataFrame([
    {item: (item in transaction) for item in all_items}
    for transaction in baskets
])

# 5. CHẠY FP-GROWTH

# min_support  (0.0002) <=> tính tỉ lệ {A/tổng số order}
freq_itemsets = fpgrowth(encoded_df, min_support=0.0001, use_colnames=True)

# Sinh association rules <=> tính confidence = P(A + B)/P(A)
rules = association_rules(freq_itemsets, metric="confidence", min_threshold=0.005)

# 6. LỌC RULE 1-1 (A -> B)
# 6.1. nghĩa là chỉ lấy A->B, ko lấy A,B -> C
rules_1_1 = rules[
    (rules["antecedents"].apply(lambda x: len(x) == 1)) &
    (rules["consequents"].apply(lambda x: len(x) == 1))
].copy()

# 6.2. Tách set → giá trị đơn <=> lấy sản phẩm thật sử ra khởi set
rules_1_1["Product_id_1"] = rules_1_1["antecedents"].apply(lambda x: list(x)[0])
rules_1_1["Product_id_2"] = rules_1_1["consequents"].apply(lambda x: list(x)[0])

# 7. ENRICH CATEGORY (JOIN products.csv) 
# 7.1. lấy bảng category
prod_category = df_products[["product_id", "category"]]

rules_1_1 = rules_1_1.merge(
    prod_category.rename(columns={"product_id": "Product_id_1", "category": "cat_1"}),
    on="Product_id_1",
    how="left"
)

rules_1_1 = rules_1_1.merge(
    prod_category.rename(columns={"product_id": "Product_id_2", "category": "cat_2"}),
    on="Product_id_2",
    how="left"
)

# 8. TÍNH TOÁN LOGIC

# 8.1. tổng số giỏ hàng
total_baskets = len(baskets)

# 8.2. gắn nhãn
rules_1_1["Relationship_type"] = "co_purchase"

# Co_purchase_count = support * total baskets 
# ví dụ support = 0.02 , total = 1000 => có 20 giỏ hàng
rules_1_1["Co_purchase_count"] = (rules_1_1["support"] * total_baskets).astype(int)

rules_1_1["Confidence"] = rules_1_1["confidence"].astype(float)

# cross-sell nếu khác category
rules_1_1["Category_cross_sell"] = rules_1_1["cat_1"] != rules_1_1["cat_2"]

# Complementary score = đo mức liên quan thật sự giữa A và B
# điểm số score=0.7×confidence + 0.3×10lift​
rules_1_1["Complementary_score"] = ((rules_1_1["confidence"] * 0.7) +((rules_1_1["lift"] / 10) * 0.3)).round(2)

# timestamp giống unix_timestamp(current_timestamp()) * 1000
rules_1_1["Computed_date"] = int(time.time() * 1000)

# 9. CHỌN OUTPUT COLUMNS 
final_df = rules_1_1[[
    "Product_id_1",
    "Product_id_2",
    "Relationship_type",
    "Co_purchase_count",
    "Confidence",
    "Category_cross_sell",
    "Complementary_score",
    "Computed_date"
]]

# 10. XUẤT FILE CSV

output_file = os.path.join(output_path, "product_complementary.csv")

final_df.to_csv(output_file, index=False, encoding="utf-8")

# 11. LOG KẾT QUẢ

print("\n================= KẾT QUẢ =================")
print(final_df.head())

print(f"\nTổng số cặp sản phẩm: {len(final_df)}")
print(f"Output saved at: {output_file}")

print("\nThời gian chạy:", round(time.time() - start_time, 2), "seconds")
print("============================================")