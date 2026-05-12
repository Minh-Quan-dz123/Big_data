# GUIDE RUN BIG DATA PIPELINE 

Mục tiêu:
- Chạy Spark job: CSV → Clean → MinIO
- Chạy Spark job xử lý dữ liệu CSV → MinIO

---

# 1. KIẾN TRÚC HỆ THỐNG (HIỂU NHANH)

Hệ thống gồm 4 phần chính:

## 1.1. MinIO (Object Storage)
- Vai trò: lưu dữ liệu như AWS S3
- Chứa:
  - datalake/raw/ (CSV gốc)
  - datalake/processed/ (data sau khi Spark xử lý)

## 1.2. Spark (Processing Engine)
- Vai trò: đọc dữ liệu từ MinIO
- Làm sạch dữ liệu (ETL)
- Ghi lại dữ liệu đã xử lý

## 1.3. Kubernetes (kind cluster)
- Vai trò: chạy các container (Spark, MinIO)
- Giống như một “mini cloud” trên máy local

## 1.4. Docker

- Sử dụng image có sẵn của Spark từ Docker Hub (`apache/spark:3.4.1`) làm nền (base image)
- Đóng gói thêm:
  - Code PySpark (`main.py`)
  - Thư viện Hadoop AWS để Spark kết nối MinIO (S3A)
    - `hadoop-aws-3.3.4.jar`
    - `aws-java-sdk-bundle-1.12.262.jar`

- Tạo ra một Docker image tùy chỉnh (ví dụ: `spark-job1-ingestion-batch`)
- Image này được Kubernetes sử dụng để chạy Spark job (thông qua spark-submit)
  
### ⚠️ Lưu ý quan trọng
- Do sử dụng image có sẵn từ Docker Hub (`apache/spark:3.4.1`), nên **lần đầu build Docker cần có Internet để tải base image về máy**
- Sau khi đã pull image về thành công, các lần build sau **không cần Internet nữa (do Docker cache local)**

---

# 2. CẤU TRÚC PROJECT

```
Big_data/
├── 1_dataset/
│   └── raw_data/
│       ├── orders.csv
│       ├── users.csv
│       ├── products.csv
│       ├── reviews.csv
│       └── order_items.csv
│
├── 2_ingestion/
│   └── batch_ingestion/
│       ├── Dockerfile
│       ├── main.py
│       └── jars/
│           ├── hadoop-aws-3.3.4.jar
│           ├── aws-java-sdk-bundle-1.12.262.jar
│
├── k8s/
│   ├── minio.yaml
│   ├── spark.yaml
│   └── spark-job1-ingestion-batch.yaml
│
└── GUIDE_RUN_PIPELINE.md
```

---

# 3. YÊU CẦU CÀI ĐẶT (PREREQUISITES)

## 3.1 Cài Docker
## 3.2 Cài kubectl
## 3.3 Cài kind (Kubernetes in Docker)
---
# 🚀 4. TẠO CLUSTER

```bash
kind create cluster --config k8s/kind-config.yaml
kubectl get nodes
```

Kiểm tra:
```bash
kubectl get nodes
```

---

# 5. DEPLOY MINIO

```bash
kubectl apply -f k8s/minio.yaml
```

Kiểm tra:
```bash
kubectl get pods
kubectl get svc
```

---

# 6. UPLOAD DATA VÀO MINIO

## 6.1 Truy cập MinIO UI
- Port thường: 9001

Nếu port-forward:
```bash
kubectl port-forward svc/minio 9001:9001
```

Mở trình duyệt:
```
http://localhost:9001
```

Login:
- user: minioadmin
- pass: minioadmin

## 6.2 Tạo bucket:
```
datalake
```

## 6.3 Upload file (đưa dữ liệu từ local lên MinIO)

MinIO không tự động đọc file từ thư mục local. Người dùng phải tự upload thủ công thông qua giao diện web.

### Cách thực hiện:

1. Mở trình duyệt: http://localhost:9001
2. Đăng nhập:
- Username: minioadmin  
- Password: minioadmin  
3. Vào bucket:
4. Mở thư mục: raw/
5. Nhấn nút: Upload -> Chọn: Upload File
6. Trên máy tính, đi tới thư mục project và chọn các file cần thiết
---

# 6. BUILD DOCKER IMAGE SPARK

```bash
cd 2_ingestion/batch_ingestion

docker build -t spark-job1-ingestion-batch . 
```
## ⚠️ Lưu ý quan trọng

- `spark-job1-ingestion-batch` là **tên Docker image**
- Tên này **PHẢI trùng với field `image:` trong file Kubernetes YAML**

👉 Vì Kubernetes sẽ dùng đúng tên này để:
- tìm image đã build
- và chạy Spark job trong cluster

---

# 8. LOAD IMAGE VÀO KIND CLUSTER

```bash
kind load docker-image spark-job1-ingestion-batch
```

---

# 9. DEPLOY SPARK JOB

```bash
kubectl apply -f k8s/spark-job1-ingestion-batch.yaml
```

---

# 10. KIỂM TRA SPARK JOB

```bash
kubectl get pods
```

Xem log:
```bash
kubectl logs <spark-pod-name>
```

---

# 11. KIỂM TRA KẾT QUẢ TRÊN MINIO

Vào:
```
http://localhost:9001
```

Kiểm tra bucket:
```
datalake/processed/
```

Nếu thấy folder file CSV → OK

---

# 12. DEBUG NHANH (NẾU LỖI)

## Xem logs Spark
```bash
kubectl logs -f <pod-name>
```

## Xem pod lỗi
```bash
kubectl describe pod <pod-name>
```

## Restart job
```bash
kubectl delete -f k8s/spark-job1-ingestion-batch.yaml
kubectl apply -f k8s/spark-job1-ingestion-batch.yaml
```

---

# 13. LUỒNG HOẠT ĐỘNG (TÓM TẮT)

1. Upload CSV → MinIO (raw)
2. Kubernetes chạy Spark job
3. Spark đọc dữ liệu từ MinIO
4. Clean data bằng PySpark
5. Ghi lại vào MinIO (processed)

---

# 14. KẾT LUẬN

Nếu chạy đúng, bạn sẽ thấy:

- Spark job chạy trong Kubernetes
- Log ko lỗi
- MinIO có dữ liệu trong processed/