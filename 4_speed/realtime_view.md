## 2.3.4. Realtime View

Thành phần này gồm các bảng chứa dữ liệu mới nhất gần realtime, bao gồm:


### 1. trending_products_realtime – bảng sản phẩm theo xu hướng hiện tại (realtime):

| field | mô tả |
|------|--------|
| user_id | của người dùng (ví dụ: U001922) |
| product_id | ID sản phẩm (ví dụ: P001233) |
| trend_score | điểm xu hướng realtime tổng hợp, tính bằng trọng số các hành vi (ví dụ: 0.85) |
| view_count | số lượt xem sản phẩm trong cửa sổ realtime gần nhất (ví dụ: 45) |
| cart_count | số lượt thêm vào giỏ hàng trong cửa sổ realtime gần nhất (ví dụ: 12) |
| purchase_count | số lượt mua sản phẩm trong cửa sổ realtime gần nhất (ví dụ: 3) |
| category | danh mục sản phẩm phục vụ lọc theo ngành hàng (ví dụ: clothing, electronics) |
| computed_time | thời điểm tính toán/cập nhật điểm trending realtime (mili-giây, ví dụ: 1763137080000) |

---

