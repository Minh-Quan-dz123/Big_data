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

### 🗄️ MongoDB – Serving Storage Layer
MongoDB đóng vai trò là lớp lưu trữ phục vụ truy vấn nhanh (serving layer).

- Lưu dữ liệu đã xử lý từ Spark (batch + streaming)

MongoDB được chọn vì:
- khả năng ghi dữ liệu lớn 
- phù hợp hệ thống phân tán

---

### ⚡ Redis – Cache Layer
Redis được sử dụng làm tầng cache để tăng tốc độ truy vấn:

- Cache recommendation gần nhất
- Cache trending products
- Giảm tải MongoDB 
- Tăng tốc phản hồi API

---

### 🌐 FastAPI – Serving Layer
FastAPI đóng vai trò lớp API trung gian giữa hệ thống và client.

- Nhận request từ dashboard/frontend
- Query Redis → MongoDB 
- Trả về:
  - danh sách gợi ý sản phẩm
  - thông tin user
  - dữ liệu analytics realtime

---

### 📊 Dashboard
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

- Orchestrate các service (Kafka, Spark, MongoDB, Redis,...)
- Hỗ trợ scale hệ thống
- Quản lý deployment đồng bộ

---

    
## 🔄 Luồng xử lý dữ liệu

* User → Serving app → Kafka 
* Kafka → Spark Streaming → Realtime Processing
* Storage → Spark Batch → Analytics

```text
    User Activity (Realtime Event)
               │
               ▼
         Serving App (FastAPI) ───► [Redis Cache Layer]
               │
               ▼
         Data Ingestion (Apache Kafka)
               │
         ┌─────┴────────────────┐
         ▼                      ▼
   [MinIO Data Lake]     [Speed Layer]
         │                      │
         ▼                      ▼
    [Batch Layer]        Spark Streaming
    (Spark Batch)               │
         │                      │
         └───────►  MongoDB ◄───┘
                (Serving Layer)
                        ▲
                        │
                [React Dashboard]
```

---

## 📁 Cấu trúc thư mục
### Tổng quan
```
Big_data/
│
├── 1_dataset/          # Tập dữ liệu thô (Raw datasets) 
├── 2_ingestion/        # Lấy dữ liệu và làm sạch dữ liệu từ Kaggle server
├── 3_batch/            # Mã nguồn chuỗi 6 Spark Batch Jobs tuần tự
├── 4_speed/            # Mã nguồn Spark Structured Streaming 
├── 5_serving/          # Mã nguồn API Serving Layer (FastAPI)
├── 6_dashboard/        
│   ├── my-app/         # Giao diện quản trị Frontend (React + Tailwind CSS)
│   └── fake_realtime/  # Script Python giả lập hành vi người dùng realtime
├── 10_docs/            # Tài liệu thiết kế chi tiết, sơ đồ kiến trúc hệ thống
└── k8s/                # Toàn bộ Kubernetes Manifests (Cấu hình cụm Kind, Kafka, Mongo, MinIO, Redis)
```
---

# ⚙️ II. Chuẩn bị môi trường

## 📋 Requirements
Trước khi cài đặt: 
- Máy khi bật lên lượng RAM còn dư sau hiện tại ít nhất phải 4GB để đảm bảo hệ thống hoạt động ổn định
- Có internet

## 🔧 Cài đặt công cụ
### 1. Cài Docker Desktop để có WSL2 backend
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

---

# ☸️ III. Triển khai dự án
## Notes
>⚠️ Lưu ý: Toàn bộ các câu lệnh bên dưới bắt buộc phải được thực thi tại thư mục gốc của dự án: D:\Project_BigData\Big_data>.
---
## 🐳 Step 0: Open Docker Desktop
B1. wsl -l -v để kiểm tra WSL
```bash
wsl -l -v
```

B2a. ví dụ Docker tắt rồi thì sang bước 3
```bash
PS D:\Project_BigData\Big_data> wsl -l -v
  NAME              STATE           VERSION
* Ubuntu-24.04      Running         2
  docker-desktop    Stopped         2
PS D:\Project_BigData\Big_data>
```

B2b. nếu Docker chưa tắt thì tắt rồi sang bước 3
```bash
wsl --terminate docker-desktop
hoặc
wsl --shutdown (ko nên nhưng tạm)
```

B3. Sau đó mở Dockertop để Docker Engine running
```bash
wsl -l -v
thì thấy 
docker-desktop    Running
```



## ☸️ Step 1: Create Kubernetes Cluster

### 1.1. Khởi tạo cụm K8s thông qua cấu hình Kind:

```bash
kind create cluster --name bigdata-cluster --config k8s/kind-config.yaml
```
Kết quả mong chờ
```bash
Creating cluster "bigdata-cluster" ...
...
✓ Installing StorageClass 💾
✓ Joining worker nodes 🚜
Set kubectl context to "kind-bigdata-cluster"
You can now use your cluster with:

kubectl cluster-info --context kind-bigdata-cluster

Not sure what to do next? 😅  Check out https://kind.sigs.k8s.io/docs/user/quick-start/
```
### 1.2. Kiểm tra cluster:

```bash
kubectl get nodes
```

### 1.3. Kết quả mong đợi:

- 1 control-plane
```bash
PS D:\Project_BigData\Big_data> kubectl get nodes
NAME                            STATUS   ROLES           AGE     VERSION
bigdata-cluster-control-plane   Ready    control-plane   
```
---

## 📁 Step 2: Tạo sẵn các thư mục nhận dữ liệu trên Kind Node
```bash
docker exec -it bigdata-cluster-control-plane mkdir -p /src/3_batch/jobs
docker exec -it bigdata-cluster-control-plane mkdir -p /src/4_speed
docker exec -it bigdata-cluster-control-plane mkdir -p /data/spark_cache
```

## ⚙️ SỬA LỖI PHÂN QUYỀN: Cho phép User 1001, 777 của Spark ghi vào Ivy Cache
```bash
docker exec -it bigdata-cluster-control-plane chmod -R 777 /src/4_speed
docker exec -it bigdata-cluster-control-plane chown -R 1001:1001 /data/spark_cache
docker exec -it bigdata-cluster-control-plane chmod -R 777 /data/spark_cache
```


## 📦 Step 3: Deploy Infrastructure
>⚠️ Lưu ý: Đợi tạo pod running 1/1 thì chạy lệnh tiếp
### 3.1. MongoDB
```bash
kubectl apply -f k8s/mongodb.yaml

# kiểm qua quá trình tạo pod đã 1/1 RUNNING chưa (lần đầu mất tầm 1-3')
kubectl get pods

# Đợi pod chạy xong
kubectl exec -it deployment/mongodb -- mongosh mongodb://localhost:27017/ecommerce

# vào môi trường data rồi cấu hình ttl cho table user_events là 600s (10 phút)
db.trending_products_realtime.createIndex({ "timestamp": 1 }, { expireAfterSeconds: 600 })
```
các lệnh để theo dõi table sau khi vào giao diện database
```bash
# xem tables
show collections  

# xem chi tiết thống kê 1 collection
db.trending_products_realtime.stats()
# vuốt xuống dưới cùng thấy size, count = 0 thì oke

# Xem 5 bản ghi mới nhất (Sắp xếp thời gian giảm dần desc):
db.trending_products_realtime.find().sort({ timestamp: -1 }).limit(5)
# hiện tại ra rỗng thôi

# xem tổng lượng document
db.trending_products_realtime.countDocuments() 

# lấy 5 bản ghi đầu tiên
db.trending_products_realtime.find().limit(5)

# lấy 5 bản ghi đầu tiên ở dạng json
db.trending_products_realtime.find().limit(5).pretty()

# xóa cụ thể 1 bảng (collection)
db.trending_products_realtime.drop()
```
### 3.2. Minio
```bash
# exit để thoát MongoDB
exit

# Tiếp theo viết lệnh
kubectl apply -f k8s/minio-config.yaml
# ở dưới nữa sẽ có phần hướng dẫn upload files lên minio
```


### 3.3. Kafka
```bash
kubectl create namespace kafka
kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka

# đợi 1 lúc để kubectl cài strimzi xong
kubectl apply -f k8s/kafka-config2.yaml -n kafka
kubectl get pods -n kafka # để kiểm tra
```
### 3.4. redis
```bash
kubectl apply -f k8s/redis-k8s.yaml
```

Kiểm tra trạng thái
```bash
kubectl get pods

# Kiểm tra trạng thái cụm Cluster Kafka
kubectl get pods -n kafka
```

Ví dụ kết quả
```bash
kubectl get pods
NAME                                          READY   STATUS    RESTARTS      AGE
...
minio-0                                       1/1     Running   0             3m49s
...
```
### Check liên tục kubectl get pods, kubectl get pods -n kafka đợi (2-4') các pod chạy hết 1/1 RUNNING rồi tiếp tục

## 📄 Step 4: Đẩy các files csv vào MinIO 
- Mở MinIO Console:
```text
http://localhost:9001
```

- Thông tin đăng nhập:
Username: minioadmin
Password: minioadmin

### 🪣 B1 Tạo Bucket:
- Sau khi đăng nhập 
- ở menu bên trái có Bucket, Chọn Bucket 
- Tạo bucket: trên giao diện có chữ "Create Bucket", bấm vào 
- Hoặc khi vào bạn thấy luôn chữ Buckets" và "To Get Started, Create a Bucket"
- Nó hiện giao diện Create Bucket, sau đó điền Bucket Name*  là "datalake" -> Click 
  
### 📁 B2 Upload dữ liệu đầu vào
- Ở menu bên trái bấm vào Object Browser
- Vào giao diện Object Browser rồi nó hiện danh sách bucket bên dưới, bấm vào datalake
- Hoặc nếu nó hiện luôn giao diện Object Browser thì trực tiếp xuống bước dưới
- Tiếp theo cần tạo datalake/processed: bên trái có chữ Create new path, bấm vào và điền "processed"
- Rồi giao diện Object Browser của datalake này sẽ thấy chữ Upload thì bấm vào Upload Files => Sau đó tìm lấy thư mục của project hiện tại có tên 1_dataset/raw_data và upload tất cả files csv
- => Kết quả giao diện hiện lên các files đó 

```
gồm:
   users.csv
   products.csv
   orders.csv
   order_items.csv
   reviews.csv
   events.csv
```

### Kết quả trên MinIO:
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

## ⚡Step 5: Đồng bộ mã nguồn và Kích hoạt Spark Pipeline
### 5.1. Đồng bộ mã nguồn từ Windows vào K8s Node:
- Các Batch Job sau tên có tên và thứ tự chạy như dưới
  - 1_user_segment_job.py
  - 2_user_consumption.py
  - 3_trending_products.py
  - 4_product_similarity.py
  - 5_product_complementary.py
  - 6_user_recommendations.py

- còn Streaming Job thì có: streaming_job.py

Ta đẩy code vào không gian cluster

```bash
docker cp 3_batch/jobs/. bigdata-cluster-control-plane:/src/3_batch/jobs/
docker cp 4_speed/streaming-job.py bigdata-cluster-control-plane:/src/4_speed/
```

### 5.2. Vận hành luồng Batch tuần tự (Kích hoạt bằng tay):

>Ở đây, kịch bản là Batch Jobs chạy xong thì sẽ chạy Streaming Jobs (để nhẹ máy, ko chạy nhiều job cùng lúc)
Ta chạy tuần tự 
#### Trước tiên mở 1 Terminal/PowerShell Khác (có path là ./Big_data/) để kiểm tra các collections trong MongoDB

Bước 1 chui vào database cụ thể là ecommerce
```bash
kubectl exec -it deployment/mongodb -- mongosh mongodb://localhost:27017/ecommerce

# liệt kê các collections hiện tại (chắc chắn ra rỗng)
show collections
```

Bước 2: Ta sẽ chạy Batch Job sau đó xem mongodb có thay đổi gì .
Ở terminal cũ, trước tiên cứ xóa lịch sử chạy của Job cũ để K8s không báo trùng tên
```bash
kubectl delete job spark-batch-manual-pipeline --ignore-not-found
```

Bước 3: Kích hoạt chuỗi 6 Job chạy tuần tự ngay lập tức
```bash
kubectl apply -f k8s/spark-batch-job.yaml

Bước 4: Kiểm tra Pob Spark chạy xong chưa
```bash
kubectl get pods
# Với Spark Pull Image khá to trên DockerHub nên cần mạng ổn định, đợi 1-2' rồi xuống lệnh dưới
```

- Xem theo thời gian thực, xem log của Spark khi chạy các Jobs, tầm 3-7'
```bash
kubectl logs -l job-name=spark-batch-manual-pipeline -f
```

- Xem nội dung print (Log) bên trong Job 
```bash
kubectl logs -l job-name=spark-batch-manual-pipeline --tail=500
```

> ⚠️ Lưu ý, vì Pod của Spark Batch Job sẽ bị Kubernetes xóa sạch dữ liệu sau 100 giây sau khi Job chạy xong nên lúc chạy các lệnh xem log thì nó sẽ báo lỗi Error from server (NotFound)

Bước 5, mở Terminal của MongoDB chạy các lệnh xem spark đã tạo được các Collections chưa
```bash
# Xem danh sách collections 
show collections

# Xem Kích thước đơn vị MB của database này
db.stats({ scale: 1024 * 1024 })

# xem thông tin tổng quan
db.user_recommendations_batch.stats()     

# đọc số lượng documents
db.user_recommendations_batch.countDocuments() 

# đọc 5 documents gần đây nhất
db.user_recommendations_batch.find().sort({ timestamp: 1 }).limit(5) 

# Lấy 10 user nhóm "Frequent Shoppers",...
db.user_recommendations_batch.find({ segment_name: "Frequent Shoppers" }).limit(10)
db.user_recommendations_batch.find({ segment_name: "Risky Frequent Buyers" }).limit(10)
db.user_recommendations_batch.find({ segment_name: "Low Frequency" }).limit(10)
db.user_recommendations_batch.find({ segment_name: "Bad Customer" }).limit(10)
```


Bước 6: Nếu máy yếu thì đợi sau khi 6 Job chạy xong thì pod này sẽ bị xóa, ta chạy Streaming Job tiếp, nếu máy khỏe thì chạy luôn Streaming Job

Vào lại terminal chạy Spark Batch Job
```bash
kubectl apply -f k8s/streaming-job.yaml

# đợi nó chạy xong pod thì viết lệnh để theo dõi log (Kiểm tra bằng "kubectl get pods", nhìn pod có tên kiểu spark-streaming-pipeline-..)
kubectl logs -l app=spark-streaming -f
```

> ⚠️ Lưu ý: 2 lệnh sau đây ko nên dùng
```bash
# LỆNH CẬP NHẬT LẠI POD NẾU COPY SỬA ko thì bỏ qua 
kubectl delete pod -l app=spark-streaming
```

```bash
# Nếu sửa code của streaming job khi đang chạy, dùng lệnh này để nạp lại code mới lập tức
kubectl rollout restart deployment spark-streaming-pipeline
```

## 📊 Step 6: QUY TRÌNH KHỞI ĐỘNG CÁC ỨNG DỤNG TRÊN HOST WINDOWS

### 🗄️ 6.1. Khởi động FastAPI Server ở local
Mở 1 Terminal thứ 3 (trước có termial của Spark Job và MongoDB rồi), cd vào Big_data/5_serving
```bash
pip install -r requirements.txt  # (Chỉ chạy lần đầu)
uvicorn app:app --host 127.0.0.1 --port 30070 --reload
```

### 🌐 6.2. Khởi động Web Dashboard (React + Tailwind)

Mở 1 Terminal thứ 4, cd vào Big_data/6_dashboard/my-app

```bash
npm install # (cài đặt thư viện Chỉ cần làm một lần duy nhất đầu tiên)

npm run dev
```
Sau khi chạy thành công, mở trình duyệt: 
```bash
http://localhost:5173
```

### 🖥️ 6.3. Khởi động Chương trình Giả lập 
Mở 1 Terminal thứ 5, cd vào Big_data/6_dashboard/fake_realtime
```bash
# sau đó chạy
pip install -r requirements.txt # chỉ chạy lần đầu để tải thư viện
python fake_realtime.py
```
---


## 🧪 7 Tổng hợp Testing
### 7.1. Check Kafka nhận data
Mở Terminal số 6, không quan trọng path đang đứng để xem dữ liệu từ client gửi vào kafka broker
```bash
kubectl exec -it my-cluster-kafka-pool-0 -n kafka -- bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic user-activity-events
```

### 7.2. Check Kafka nhận data
Cuối cùng vào Web để xem và tương tác thôi


# ⚠️ Lưu ý Quan Trọng
Dọn dẹp hệ thống để xóa sạch cụm kind khi không dùng nữa
```bash
kind delete cluster --name bigdata-cluster
```

## Others
### Kiểm tra tổng quan hạ tầng K8s

```bash
kubectl get pods # Giám sát trạng thái Pods
```

### Services
```bash
kubectl get svc # Giám sát trạng thái Mạng/Cổng dịch vụ
```

### Logs
```bash
kubectl logs <pod-name>
```
### delete deployment Name
```bash
# ví dụ
kubectl delete deployment spark-streaming-pipeline
```

### Tắt Docker
```bash
docker desktop stop
```
---
# 🚨 8 Khi Kubectl ma tắt và mở lại và lỗi kết nối với host

```bash
# khởi động cluster
docker start bigdata-cluster-control-plane

# kiểm tra có kind chưa
docker ps

# viết lệnh chui vào kubectl
docker exec -it bigdata-cluster-control-plane bash
# sau đó dùng các lệnh khác bình thường
```

## 🌐 Danh sách phân phối cổng dịch vụ công khai (Exposed Services)

| Service | URL |
|----------|----------|
| MongoDB API | localhost:27017 |
| MinIO Console | http://localhost:9001 |
| Kafka API | localhost:9092 |
| Redis API | localhost:6379 |
| Serving API | http://localhost:30070 |
| React Dashboard | http://localhost:5173 |

---

## 🧠 Technologies

- Kubernetes (Kind)
- Apache Kafka (KRaft mode)
- Spark, Spark Structured Streaming
- MongoDB
- Redis
- MinIO
- FastAPI
- React, Tailwind
- Docker

---
