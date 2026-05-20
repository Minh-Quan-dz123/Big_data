# Hướng Dẫn Chạy Luồng Realtime (Speed Layer)

Tài liệu này hướng dẫn cách khởi động, vận hành và kiểm tra kết quả tính toán Realtime (Streaming) cho dự án.

---

## 1. Yêu Cầu Hệ Thống (Local)
* **Python** >= 3.9 (đã cài các thư viện trong `requirements.txt`)
* **Java** (để chạy Spark session local)
* **Docker / Docker Compose** (để chạy Zookeeper và Kafka)

---

## 2. Các Bước Khởi Chạy Toàn Bộ Luồng

### Bước 1: Khởi động cơ sở hạ tầng (Kafka & Zookeeper)
Chạy lệnh sau tại thư mục gốc của dự án để khởi động container Zookeeper và Kafka:
```bash
docker-compose up -d
```
*Kiểm tra trạng thái các container bằng lệnh `docker ps` để đảm bảo cả zookeeper và kafka đều ở trạng thái `Up`.*

### Bước 2: Chạy Stream Ingestion (Bộ lọc và làm sạch dòng)
Chạy tiến trình lắng nghe sự kiện thô, làm sạch và chuẩn bị dữ liệu:
```bash
python 2_ingestion/stream_ingestion/main.py
```
*Tiến trình này sẽ liên tục consume từ topic `ABC1` (dữ liệu thô), chuẩn hóa cấu trúc và đẩy sang topic `ABC2` (dữ liệu sạch).*

### Bước 3: Khởi chạy Spark Streaming Job
Chạy ứng dụng tính toán điểm xu hướng realtime:
```bash
python 4_speed/jobs/2_trending_products_job.py
```
*Spark sẽ tự động tải các gói phụ thuộc (Kafka connector) và tiến hành gom nhóm sự kiện theo Sliding Window 5 phút để tính điểm. Kết quả sẽ được ghi liên tục xuống file:*
* `1_dataset/output/trending_products_realtime.csv`

### Bước 4: Chạy bộ giả lập sinh dữ liệu thực tế (Producer Simulation)
Mở một terminal mới và chạy file giả lập bắn sự kiện:
```bash
python 1_dataset/fake_realtime.py
```
*Bộ sinh dữ liệu sẽ bắt đầu giả lập hành vi người dùng (view, cart, purchase) theo thời gian mô phỏng tăng dần từ ngày `2025-11-14 23:18:00`.*

---

## 3. Kiểm Tra Kết Quả
Quan sát các terminal và kiểm tra file đầu ra:
* Màn hình log của **Spark Streaming** sẽ in bảng mẫu 10 sản phẩm có điểm xu hướng cao nhất ở mỗi micro-batch.
* File kết quả [trending_products_realtime.csv](file:///d:/project_big_data/Big_data/1_dataset/output/trending_products_realtime.csv) sẽ được cập nhật liên tục với điểm số `trend_score` biến động theo thời gian thực.
