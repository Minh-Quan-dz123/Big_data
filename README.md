# 🚀 Deploy K8s Big Data Recommendation System

## 📌 I. Tổng quan

Đây là repository triển khai **hệ thống Big Data** dùng để:

* Thu thập thông tin, hành vi người dùng trong môi trường thương mại điện tử
* Phân tích, xử lý dữ liệu
* Phân chia phân khúc khách hàng và gợi ý sản phẩm
* Ứng dụng trong hệ thống thương mại điện tử 

---

### 🧠 Kiến trúc và vai trò các thành phần trong hệ thống

Hệ thống được xây dựng theo kiến trúc **Lambda Architecture**, kết hợp cả xử lý batch và streaming nhằm đảm bảo vừa phân tích dữ liệu lịch sử vừa xử lý realtime.
gồm 4 phần:

* 🔹 Data Ingestion
* 🔹 Batch Layer
* 🔹 Speed Layer
* 🔹 Serving Layer


---

### 📡 Kafka – Data Ingestion Layer
Apache Kafka đóng vai trò là trung tâm thu thập và phân phối dữ liệu sự kiện trong toàn hệ thống.

- Thu thập hành vi người dùng: click, view, purchase, rating,...
- Phân phối dữ liệu đến:
  - Spark Streaming (xử lý realtime)
  - Spark Batch (xử lý định kỳ)

Kafka giúp hệ thống tách biệt producer và consumer, tăng khả năng mở rộng và chịu tải.

---

### ⚡ Spark – Processing Layer
Apache Spark là engine xử lý dữ liệu chính của hệ thống, gồm hai nhánh:

#### Streaming (Speed Layer)
- Xử lý dữ liệu realtime từ Kafka
- Tạo kết quả:
  - sản phẩm trending
  - hành vi người dùng realtime

#### Batch Layer
- Xử lý dữ liệu lịch sử từ MinIO
- Tính toán:
  - Phân loại khách hàng
  - Phân tích thói quen tiêu dùng
  - Tính toán sản phẩm trending
  - Tìm sản phẩm tương tự, sản phẩm đi kèm
  - feature phục vụ recommendation

---

### 🗄️ Cassandra – Serving Storage Layer
Apache Cassandra đóng vai trò là lớp lưu trữ phục vụ truy vấn nhanh (serving layer).

- Lưu dữ liệu đã xử lý từ Spark (batch + streaming)

Cassandra được chọn vì:
- khả năng ghi dữ liệu lớn (high write throughput)
- truy vấn nhanh theo key (user_id, product_id)
- phù hợp hệ thống phân tán

---

### ⚡ Redis – Cache Layer
Redis được sử dụng làm tầng cache để tăng tốc độ truy vấn:

- Cache recommendation gần nhất
- Cache trending products
- Giảm tải Cassandra
- Tăng tốc phản hồi API

---

### 🌐 FastAPI – Serving Layer
FastAPI đóng vai trò lớp API trung gian giữa hệ thống và client.

- Nhận request từ dashboard/frontend
- Query Redis → Cassandra
- Trả về:
  - danh sách gợi ý sản phẩm
  - thông tin user
  - dữ liệu analytics realtime

---

### 📊 Streamlit – Visualization Layer
Streamlit được dùng để xây dựng dashboard hiển thị dữ liệu:

- Theo dõi hành vi người dùng realtime
- Hiển thị sản phẩm trending
- Hiển thị kết quả recommendation

---

### 🗃️ MinIO – Data Lake
MinIO đóng vai trò data lake trong hệ thống.

- Lưu trữ dữ liệu raw (CSV, dataset gốc)
- Lưu dữ liệu xử lý từ batch jobs
- Cung cấp nguồn dữ liệu cho Spark Batch

---

### ☸️ Kubernetes (Kind) – Infrastructure Layer
Kubernetes (Kind) được sử dụng để triển khai và quản lý toàn bộ hệ thống.

- Orchestrate các service (Kafka, Spark, Cassandra, Redis,...)
- Hỗ trợ scale hệ thống
- Quản lý deployment đồng bộ

---

    
## 🔄 Luồng xử lý dữ liệu

* User → Serving app → Kafka 
* Kafka → Spark Streaming → Realtime Processing
* Storage → Spark Batch → Analytics

```text
User Activity
      │
      ▼
   Kafka
      │
 ┌────┴────┐
 ▼         ▼
Batch    Streaming
Layer     Layer
 ▼         ▼
Spark     Spark
 ▼         ▼
 Cassandra
      │
    Redis
      │
   FastAPI
      │
 Streamlit
 ```

---

## 📁 Cấu trúc thư mục
### Tổng quan
```
Big_data/
│
├── 1_dataset/          # Raw datasets + data generator
├── 2_ingestion/        # Batch & Stream ingestion
├── 3_batch/            # Airflow + Spark batch jobs
├── 4_speed/            # Spark streaming jobs
├── 5_serving/          # FastAPI serving layer
├── 6_dashboard/        # Streamlit dashboards
├── 7_docker/           # Docker images
├── 10_docs/            # details, diagrams
├── k8s/                # Kubernetes manifests
└── README.md
```
---

# ⚙️ II. Chuẩn bị môi trường

## Cài đặt công cụ

### 1. Docker
```bash
docker --version
```

### 2. Kind tool and Kubernetes
```bash
choco install kind -y
choco install kubernetes-cli -y

kind version
kubectl version --client
```

### 3. Python
```bash
python --version
```

---

# ☸️ III. Triển khai dự án
## Notes
>Đứng ở Big_data/ thực hiện tất cả các lệnh trong thư mục cho các thao tác bên dưới
---
## ☸️ Step 1: Create Kubernetes Cluster

### 1.1. Tạo Kubernetes cluster bằng Kind:

```bash
kind create cluster --name bigdata-cluster --config k8s/kind-config.yaml
```

### 1.2. Kiểm tra cluster:

```bash
kubectl get nodes
```

### 1.3. Kết quả mong đợi:

- 1 control-plane
- 3 workers
---

## 🐳 Step 2: Build Docker Images

### 2.1. Spark Image

```bash
docker build -f 7_docker/dockerfile.spark -t minhquan-spark-image:latest .
```
### 2.2. Airflow Image

```bash
docker build -f 3_batch/dockerfile.airflow -t minhquan-airflow-batch:latest .
```
### 2.3. Serving API

```bash
docker build -f 5_serving/dockerfile -t serving-api:1.0 .
```

---

### 2.4. Load Images into Kind

#### 2.4.1. Spark Image
```bash
kind load docker-image minhquan-spark-image:latest --name bigdata-cluster
```

#### 2.4.2. Airflow Image
```bash
kind load docker-image minhquan-airflow-batch:latest --name bigdata-cluster
```

#### 2.4.3. Serving AP
```bash
kind load docker-image serving-api:1.0 --name bigdata-cluster
```

## 📦 Step 3: Deploy Infrastructure

### 3.1. Kafka

#### 3.1.1. Cài đặt Strimzi Operator:

```bash
kubectl create -f https://strimzi.io/install/latest?namespace=default
```

#### 3.1.2. Đợi Operator khởi động:
```bash
kubectl get pods
```

#### 3.1.3. Deploy Kafka Cluster:
```bash
kubectl apply -f k8s/kafka-config.yaml
```

---

### 3.2. MinIO
#### 3.2.1. Deploy MinIO
```bash
kubectl apply -f k8s/minio-config.yaml
```

#### 3.2.2. Kiểm tra trạng thái
```bash
kubectl get pods
```

#### 3.2.3. Đẩy các files csv vào MinIO 
- Mở MinIO Console:
```text
http://localhost:30091
```

- Thông tin đăng nhập:
Username: minioadmin
Password: minioadmin

#### B1 Tạo Bucket:
- Sau khi đăng nhập -> Chọn Create Bucket -> Tạo bucket: "datalake"
#### B2 Upload dữ liệu đầu vào
- Tạo thư mục: "processed" bên trong bucket datalake.
- Sau đó upload các file trong: 1_dataset/raw_data/ (đang mở ở local)
```
gồm:
   users.csv
   products.csv
   orders.csv
   order_items.csv
   reviews.csv
   events.csv
```

#### Kết quả trên MinIO:
```
datalake/
└── processed/
    ├── users.csv
    ├── products.csv
    ├── orders.csv
    ├── order_items.csv
    ├── reviews.csv
    └── events.csv
```

### 3.3. Cassandra
#### 3.3.1. Deploy Cassandra
```bash
kubectl apply -f k8s/cassandra-config.yaml
```

#### 3.3.2. Kiểm tra trạng thái
```bash
kubectl get pods
```

#### 3.3.3. Tạo bảng
B1: COPY file từ LOCAL vào POD
```bash
kubectl cp k8s/cassandra/init_cassandra.cql cassandra-0:/tmp/init_cassandra.cql
```

B2: chạy file trong Cassandra

```bash
kubectl exec -it cassandra-0 -- cqlsh -f /tmp/init_cassandra.cql
```

Hoặc có thể gộp B1 + B2 thành 1 lệnh
```bash
kubectl exec -i cassandra-0 -- cqlsh < k8s/cassandra/init_cassandra.cql
```
---
### 3.4. Redis
#### 3.4.1. Deploy Redis
```bash
kubectl apply -f k8s/redis.yaml
```

#### 3.4.2. Kiểm tra trạng thái
```bash
kubectl get pods
```
---
### 3.5. Spark Cluster
#### 3.5.1. Deploy Redis
```bash
kubectl apply -f k8s/spark-config.yaml
```

```text
http://localhost:30080
```

#### 3.5.2. Kiểm tra trạng thái
```bash
kubectl get pods
```
---
## ⚡ Step 4: Deploy Processing Layer

### 4.1. Airflow
#### 4.1.1. Airflow
```bash
kubectl apply -f k8s/airflow.yaml
```
Airflow UI:
```text
http://localhost:30081
```

#### 4.1.1. Kiểm tra trạng thái
```bash
kubectl get pods
```

---
### 4.2.Realtime Streaming Jobs

```bash
kubectl apply -f k8s/realtime_speed.yaml
```

Kiểm tra:

```bash
kubectl get pods
```

Phải xuất hiện các pod:

```text
realtime-interest
trending-products
```

---

## 🚀 Step 5: Deploy Serving Layer

```bash
kubectl apply -f k8s/serving.yaml
```

API Endpoint:
```text
http://localhost:30070
```

Swagger Documentation:
```text
http://localhost:30070/docs
```

---

## 📊 Step 6: Run Dashboard
>Di chuyển vào 6_dashboard

### Customer Dashboard

```bash
cd 6_dashboard/my-app

npm install (cài đặt thư viện)

npm run dev
```
Sau khi chạy thành công, mở trình duyệt: 
```bash
http://localhost:5173
```

---


## 🧪 Step 7: Generate Realtime Events
```bash
cd ../fake_realtime

python fake_realtime.py
```

Script sẽ:

- Sinh hành vi người dùng giả lập
- Gửi dữ liệu vào serving app
- Kích hoạt pipeline realtime

---

# Others

## 📈 Monitoring

### Pods

```bash
kubectl get pods
```

### Services

```bash
kubectl get svc
```

### Logs

```bash
kubectl logs <pod-name>
```

Ví dụ:

```bash
kubectl logs deployment/realtime-interest
```

---
## 🌐 Exposed Services

| Service | URL |
|----------|----------|
| Spark UI | http://localhost:30080 |
| Airflow UI | http://localhost:30081 |
| MinIO API | http://localhost:30090 |
| MinIO Console | http://localhost:30091 |
| Kafka | localhost:30092 |
| Serving API | http://localhost:30070 |
| Swagger | http://localhost:30070/docs |

---

## 🧠 Technologies

- Kubernetes (Kind)
- Apache Kafka (Strimzi)
- Apache Spark
- Spark Structured Streaming
- Apache Airflow
- Cassandra
- Redis
- MinIO
- FastAPI
- Streamlit
- Docker

---

## 👨‍💻 Authors

Big Data Recommendation System Project