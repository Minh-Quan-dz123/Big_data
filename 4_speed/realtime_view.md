## 2.3.4. Realtime View

Thành phần này gồm các bảng chứa dữ liệu mới nhất gần realtime, bao gồm:

---

### 1. user_recent_interest – bảng kết luận hành vi với 1 product hiện tại của user:

| field | mô tả |
|------|--------|
| user_id | của người dùng (ví dụ: U001922) |
| product_id | của sản phẩm được gợi ý (ví dụ: P001233) |
| interest_type | loại hành vi thể hiện sự quan tâm (ví dụ: view, cart, purchase, wishlist) |
| event_time | thời điểm người dùng thực hiện hành vi  (ví dụ: 2026-04-09T08:04:50 -> chuyển thành 1775721890000) |
| item_price | giá 1 sản phẩm (ví dụ: 12.000) |
| session_id (tùy chọn) | phiên tương tác của người dùng (ví dụ: S001122) |
| computed_score (tùy chọn) | điểm đánh giá mức độ quan tâm theo thuật toán realtime (ví dụ: 0.87) |

---

### 2. trending_products_realtime – bảng sản phẩm theo xu hướng hiện tại (realtime):

| field | mô tả |
|------|--------|
| product_id | ID sản phẩm (ví dụ: P001233) |
| trend_score | điểm xu hướng realtime tổng hợp, tính bằng trọng số các hành vi (ví dụ: 0.85) |
| view_count | số lượt xem sản phẩm trong cửa sổ realtime gần nhất (ví dụ: 45) |
| cart_count | số lượt thêm vào giỏ hàng trong cửa sổ realtime gần nhất (ví dụ: 12) |
| purchase_count | số lượt mua sản phẩm trong cửa sổ realtime gần nhất (ví dụ: 3) |
| category | danh mục sản phẩm phục vụ lọc theo ngành hàng (ví dụ: clothing, electronics) |
| computed_time | thời điểm tính toán/cập nhật điểm trending realtime (mili-giây, ví dụ: 1763137080000) |

---

### 3. user_recommendations_realtime – gợi ý sản phẩm realtime cho user:

| field | mô tả |
|------|--------|
| user_id | ID người dùng (ví dụ: U000001) |
| product_id | ID sản phẩm được gợi ý (ví dụ: P045) |
| recommendation_score | điểm gợi ý (ví dụ: 0.87) |
| recommendation_type | loại gợi ý (ví dụ: content_based, collaborative, hybrid) |
| computed_time | thời điểm tính/generate gợi ý  (ví dụ: 2026-04-09T08:04:50 -> chuyển thành 1775721890000) |
| context_info (tùy chọn) | thông tin ngữ cảnh như thiết bị, khu vực, phiên hiện tại, dùng để cá nhân hóa gợi ý hơn |
