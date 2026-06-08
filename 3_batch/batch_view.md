## 2.3.3. Batch View

Thành phần này chứa các bảng dữ liệu lịch sử của khách hàng, là kết quả của quá trình phân tích thói quen mua hàng, phân loại khách hàng, mối quan tâm gần đây, sản phẩm đang xu hướng, sản phẩm tương tự. Các bảng được thiết kế gồm:

---

### 1. Bảng user_segments – cho biết mức độ mua sắm của khách hàng:

| field | mô tả |
|------|--------|
| user_id | id của người dùng (ví dụ: 101, 102) |
| days_since_signup | số ngày từ khi user đăng ký đến hiện tại |
| total_orders | tổng số đơn hàng của user |
| total_completed_orders | số đơn hàng có trạng thái completed |
| total_bad_orders | số đơn hàng bị cancelled hoặc returned |
| completion_rate | tỷ lệ đơn hàng hoàn thành = completed / total_orders |
| cancellation_rate | tỷ lệ đơn hàng xấu = (cancelled + returned) / total_orders |
| cluster | id cụm do KMeans gán (ví dụ: 0, 1, 2, 3) |
| segment_name | tên phân khúc khách hàng (Frequent Shoppers, Risky Frequent Buyers, Low Frequency, Bad Customer) |
---

### 2. Bảng user_consumption_profile – bản mô tả hành vi mua sắm / thói quen tiêu dùng:

| field | mô tả |
|------|--------|
| user_id | id của người dùng (ví dụ: U001, U002) |
| avg_order_value | giá trị đơn hàng trung bình của user (chỉ tính order completed) |
| monthly_spending | tổng chi tiêu của user trong tháng gần nhất |
|product_ids_in_latest_month|
| products_in_latest_month | danh sách tên sản phẩm user đã mua trong tháng gần nhất |
| categories_in_latest_month | danh sách danh mục sản phẩm user đã mua trong tháng gần nhất |
| product_prices_in_latest_month | danh sách giá các sản phẩm user đã mua trong tháng gần nhất |
---

### 3. trending_products – danh sách sản phẩm đang nổi bật theo xu hướng:

| field | mô tả |
|------|--------|
| product_id | id của sản phẩm |
| trend_score | điểm “hot” của sản phẩm (kết hợp view + order + growth) |
| view_growth | mức tăng trưởng lượt xem (7 ngày gần nhất so với 7 ngày trước đó) |
| order_growth | mức tăng trưởng số lượng bán (7 ngày gần nhất so với 7 ngày trước đó) |
| trend_window | cửa sổ thời gian tính trend (ví dụ: "7d") |
| trend_date | thời điểm tính trend (timestamp dạng milliseconds) |
---

### 4. Product_Similarity – danh sách sản phẩm tương tự có thể thay thế cho nhau:

| field | mô tả |
|------|--------|
| product_id_1 | id sản phẩm thứ nhất trong cặp so sánh |
| product_id_2 | id sản phẩm thứ hai trong cặp so sánh |
| similarity_score | điểm tương đồng giữa 2 sản phẩm (0 → 1+) |
| similarity_type | loại similarity (content_based / collaborative / hybrid) |
| category_match | true nếu 2 sản phẩm cùng category, false nếu khác |
| computed_date | thời điểm hệ thống tính similarity |

---

### 5. Product_complementary – sản phẩm đi kèm với nhau:

| field | mô tả |
|------|--------|
| Product_id_1 | sản phẩm A (antecedent trong luật FP-Growth) |
| Product_id_2 | sản phẩm B (consequent trong luật FP-Growth) |
| Relationship_type | loại quan hệ (luôn là "co_purchase") |
| Co_purchase_count | số lần 2 sản phẩm được mua cùng nhau |
| Confidence | xác suất mua B khi đã mua A |
| Category_cross_sell | true nếu 2 sản phẩm khác category (cross-sell) |
| Complementary_score | điểm gợi ý sản phẩm bổ sung (0 → 1+) |
| Computed_date | thời điểm chạy pipeline (timestamp ms) |
---

### 6. user_recommendations_batch – kết luận các sản phẩm gợi ý cho người dùng:

| field | mô tả |
|------|--------|
| user_id | id người dùng (ví dụ: U001922) |
| product_id | id sản phẩm được gợi ý (ví dụ: P001233) |
| final_score | điểm gợi ý cuối cùng sau khi kết hợp độ liên quan sản phẩm và độ hot xu hướng (ví dụ: 0.87, 0.95) |
| type | loại nguồn gợi ý gồm: consumption, similar, complementary |