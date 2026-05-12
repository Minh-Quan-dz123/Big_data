## 2.3.3. Batch View

Thành phần này chứa các bảng dữ liệu lịch sử của khách hàng, là kết quả của quá trình phân tích thói quen mua hàng, phân loại khách hàng, mối quan tâm gần đây, sản phẩm đang xu hướng, sản phẩm tương tự. Các bảng được thiết kế gồm:

---

### 1. Bảng user_segments – cho biết mức độ mua sắm của khách hàng:

| field | mô tả |
|------|--------|
| user_id | id của người dùng (ví dụ: U001, U002) |
| segment_id | id của phân khúc khách hàng (ví dụ: S01, S03) |
| segment_name | tên phân khúc mà người dùng thuộc về (ví dụ: Frequent Buyer, Low Purchase, Discount Hunter, New User) |
| assigned_date | ngày hệ thống gán người dùng vào phân khúc  (ví dụ: 2026-04-09T08:04:50 -> chuyển thành 1775721890000) |

---

### 2. Bảng user_consumption_profile – bản mô tả hành vi mua sắm / thói quen tiêu dùng:

| field | mô tả |
|------|--------|
| user_id | id của người dùng (ví dụ: U001, U002) |
| avg_order_value | giá trị trung bình của mỗi đơn hàng (ví dụ: 350000) |
| monthly_spending | tổng số tiền trung bình chi tiêu mỗi tháng (ví dụ: 1200000) |
| user_behavior | danh mục sản phẩm thường mua hoặc quan tâm nhiều nhất (ví dụ: Electronics, Clothing) |

---

### 3. trending_products – danh sách sản phẩm đang nổi bật theo xu hướng:

| field | mô tả |
|------|--------|
| product_id | id sản phẩm (ví dụ: P001, P002) |
| trend_score | điểm xu hướng tổng hợp, thể hiện mức “hot” (ví dụ: 95.2) |
| view_growth | mức tăng trưởng lượt xem (ví dụ: 3.8) |
| order_growth | mức tăng trưởng số lượng đơn hàng (ví dụ: 4.1) |
| trend_window | cửa sổ thời gian tính xu hướng (ví dụ: 7d, 30d) |
| trend_date | ngày hệ thống ghi nhận & cập nhật xu hướng  (ví dụ: 2026-04-09T08:04:50 -> chuyển thành 1775721890000) |

---

### 4. Product_Similarity – danh sách sản phẩm tương tự có thể thay thế cho nhau:

| field | mô tả |
|------|--------|
| product_id_1 | id sản phẩm gốc (ví dụ: P001) |
| product_id_2 | id sản phẩm tương đồng (ví dụ: P045, P078, P120) |
| similarity_score | điểm độ tương đồng (ví dụ: 0.93, 0.87, 0.79) |
| similarity_type | phương pháp/loại tương đồng: |
|  | - content based  : dựa trên tên, mô tả, brand, category, giá |
|  | - collaborative  : dựa vào hành vi mua cùng/quan tâm cùng |
|  | - hybrid         : kết hợp cả hai |
| category_match | cùng danh mục hay không (true/false) |
| computed_date | ngày tính toán tương đồng  (ví dụ: 2026-04-09T08:04:50 -> chuyển thành 1775721890000) |

---

### 5. Product_complementary – sản phẩm đi kèm với nhau:

| field | mô tả |
|------|--------|
| product_id_1 | id sản phẩm gốc (ví dụ: P000001) |
| product_id_2 | id sản phẩm bổ trợ / mua kèm |
| relationship_type | loại quan hệ: |
|  | - co_purchase   : mua cùng nhau |
|  | - cross_sell    : bán chéo |
|  | - hybrid        : kết hợp cả hai |
| co_purchase_count | số lần mua cùng nhau (ví dụ: 45, 128) |
| confidence | độ tin cậy = Co_purchase_count / order(A) (ví dụ: 0.41, 0.67) |
| category_cross_sell | có khác danh mục hay không (TRUE / FALSE) |
| complementary_score | điểm đánh giá mức độ bổ trợ (ví dụ: 0.82, 0.91) |
| computed_date | ngày tính toán  (ví dụ: 2026-04-09T08:04:50 -> chuyển thành 1775721890000) |

---

### 6. user_recommendations_batch – kết luận các sản phẩm gợi ý cho người dùng:

| field | mô tả |
|------|--------|
| user_id | id người dùng (ví dụ: U001922) |
| product_id | id sản phẩm được gợi ý (ví dụ: P001233) |
| recommendation_score | điểm gợi ý (ví dụ: 0.87, 0.95) |
| recommendation_type | loại gợi ý (content_based, collaborative, hybrid) |
| reason_tag | lý do hoặc nhãn giải thích (ví dụ: bought_together, similar_category) |
| computed_date | ngày tính toán gợi ý  (ví dụ: 2026-04-09T08:04:50 -> chuyển thành 1775721890000) |