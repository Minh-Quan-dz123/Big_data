# BIG DATA BATCH PIPELINE (MINIO → SPARK → CASSANDRA + AIRFLOW)

---

# 1. TỔNG QUAN KIẾN TRÚC

Hệ thống gồm 4 thành phần chính:

## 🟦 1. MinIO (Data Lake)
- Lưu trữ dữ liệu thô (CSV, JSON)
- Giao thức: S3 (s3a://)
- Bao gồm:
  - datalake/raw/
  - datalake/processed/

## 🟨 2. Spark Cluster
- Xử lý dữ liệu batch
- Gồm:
  - 1 Spark Master
  - N Spark Worker
- Thực thi các job do Airflow trigger

## 🟩 3. Cassandra (Batch View Storage)
- Lưu kết quả sau xử lý (batch view)
- Phục vụ truy vấn nhanh

## 🟥 4. Airflow (Orchestration)
- Điều phối pipeline
- Trigger các Spark job theo thứ tự

---

# 2. PIPELINE DỮ LIỆU

Luồng tổng thể:

```text id="p3k8xa"
MinIO (raw data)
    ↓
Spark Jobs (ETL + Feature Engineering)
    ↓
Cassandra (Batch View)
```
- 👉 Airflow KHÔNG nằm trong luồng dữ liệu
- 👉 Airflow chỉ đóng vai trò điều phối
---

# 3. LUỒNG CHẠY THỰC TẾ

```text
Airflow DAG được trigger
    ↓
job_1 (user_segments)
    ↓
job_2 (user_consumption)
    ↓
job_3 (trending_products)
    ↓
job_4 (product_similarity)
    ↓
job_5 (product_complementary)
    ↓
job_6 (user_recommendations)
    ↓
Ghi dữ liệu vào Cassandra
```
---

# 4. CẤU TRÚC THƯ MỤC

3_batch/
├── dags/                     # airflow DAG
│   └── batch_pipeline.py
│
├── jobs/                     # code Spark
│   ├── common/               # dùng chung
│   │   ├── spark_session.py
│   │   ├── minio_reader.py
│   │   ├── cassandra_writer.py
│   │   └── utils.py
│   │
│   ├── user_segments/
│   ├── user_consumption/
│   ├── trending_products/
│   ├── product_similarity/
│   ├── product_complementary/
│   └── user_recommendations/
│
├── docker/
│   ├── Dockerfile.spark
│   └── Dockerfile.airflow
│
├── ../k8s/  (ở cùng cấp 3_batch/)
│   ├── minio-config.yaml
│   ├── spark-config.yaml
│   ├── cassandra-config.yaml 
│   └── airflow.yaml ← Nhân vật chính
│
└── README.md

---

# 5. NGUYÊN TẮC THIẾT KẾ

- 1 bảng Cassandra = 1 Spark Job
- Không chạy song song (máy cá nhân)
- Dùng Airflow để điều phối
- Code dùng chung đặt trong common/

---

# 6. YÊU CẦU MÔI TRƯỜNG (CÀI TRƯỚC)

## 6.1 Công cụ bắt buộc

Cài các tool sau:

1. Docker
2. kubectl
3. kind (Kubernetes in Docker)

---

## 6.2 Kiểm tra cài đặt

docker --version
kubectl version --client
kind version

---

## 6.3 Python (optional)

Dùng để test local:

- Python >= 3.9
- pip

---

# 7. THƯ VIỆN / DEPENDENCY

## 7.1 Spark

Đã có trong Docker image:
- pyspark

## 7.2 JAR cần thiết

- hadoop-aws-3.3.4.jar
- aws-java-sdk-bundle-1.12.262.jar
- spark-cassandra-connector

(đặt trong thư mục jars/)

---

## 7.3 Airflow

Được cài trong Docker image:

- apache-airflow

---

# 8. TRIỂN KHAI HỆ THỐNG

## Bước 1: Tạo cluster

kind create cluster

---

## Bước 2: Deploy hạ tầng

kubectl apply -f k8s/minio-config.yaml
kubectl apply -f k8s/spark-config.yaml
kubectl apply -f k8s/cassandra-config.yaml

## Bước 3: Build Docker image

cd 3_batch/

# Spark image
docker build -t spark-batch -f dockerfile.spark .

# Airflow image
docker build -t airflow-batch -f dockerfile.airflow .

---

## Bước 4: Load image vào kind

kind load docker-image spark-batch
kind load docker-image airflow-batch

---

## Bước 5: Deploy Airflow

kubectl apply -f k8s/airflow.yaml

---

## Bước 6: Truy cập Airflow UI

http://localhost:30081

---

# 9. CHẠY PIPELINE

- vào Airflow UI
- tìm DAG: batch_pipeline
- click "Trigger DAG"

---

# 10. LUỒNG XỬ LÝ CHI TIẾT

```text
Airflow
   ↓
spark-submit (gửi job tới Spark Master)
   ↓
Spark Master
   ↓
Spark Worker (thực thi)
   ↓
Đọc dữ liệu từ MinIO
   ↓
Xử lý (ETL + Feature)
   ↓
Ghi vào Cassandra
```

---

# 11. DEBUG

Xem log:

kubectl get pods
kubectl logs <pod-name>
kubectl describe pod <pod-name>

---

# 12. LỖI THƯỜNG GẶP

- Không đọc được MinIO → sai endpoint
- Không ghi được Cassandra → sai host
- Spark fail → thiếu jar
- Airflow không thấy DAG → sai path /opt/airflow/dags
