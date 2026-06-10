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

### 3. Python
```bash
python --version
```

---

# ☸️ III. Triển khai dự án
## Notes
>Đứng ở Big_data/ thực hiện tất cả các lệnh trong thư mục cho các thao tác bên dưới
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

B2b. nếu Docker chưa tắt thì tắt
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

### 1.1. Tạo Kubernetes cluster bằng Kind:

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
- 3 workers
```
PS D:\Project_BigData\Big_data> kubectl get nodes
NAME                            STATUS   ROLES           AGE     VERSION
bigdata-cluster-control-plane   Ready    control-plane   2m19s   v1.35.0
bigdata-cluster-worker          Ready    <none>          2m4s    v1.35.0
bigdata-cluster-worker2         Ready    <none>          2m4s    v1.35.0
bigdata-cluster-worker3         Ready    <none>          2m4s    v1.35.0
```
---

## 🐳 Step 2: Build Docker Images
> Lưu ý có dâu chấm . ở cuối lệnh
### 2.1. Spark Image

```bash
docker build -f 7_docker/dockerfile.spark -t minhquan-spark-image:latest .
```
Đợi khá lâu tầm 3-8'

```bash
[+] Building 191.7s (11/11) FINISHED        docker:desktop-linux
 => [internal] load build definition from dockerfile.spark  0.1s
 => => transferring dockerfile: 795B                        0.0s
 => [internal] load metadata for docker.io/apache/spark:3.  6.2s
 => [internal] load .dockerignore                           0.0s
 => => transferring context: 2B                             0.0s
 => [1/6] FROM docker.io/apache/spark:3.4.1@sha256:39976  164.2s
 => => resolve docker.io/apache/spark
 ...
  => => exporting manifest list sha256:7cf6d68205aa93764351  0.1s
 => => naming to docker.io/library/minhquan-spark-image:la  0.0s
 => => unpacking to docker.io/library/minhquan-spark-image  1.8s

View build details: docker-desktop://dashboard/build/desktop-linux/desktop-linux/xkt0k7suaqswvv4qrc931hiko
```

### 2.2. Airflow Image

```bash
docker build -f 3_batch/dockerfile.airflow -t minhquan-airflow-batch:latest .
```
Đợi khá lâu tầm 3-8', kết quả na ná khi build spark image


### 2.3. Serving API

```bash
cd 5_serving 
docker build -f dockerfile -t serving-api:1.0 .
```

Đợi tầm 1-3', kết quả na ná khi build spark image
---

### 2.4. Load Images into Kind

#### 2.4.1. Spark Image
```bash
cd ../
kind load docker-image minhquan-spark-image:latest --name bigdata-cluster
```
Đợi tầm 4-8'


#### 2.4.2. Airflow Image
```bash
kind load docker-image minhquan-airflow-batch:latest --name bigdata-cluster
```
Đợi tầm 5-10'


#### 2.4.3. Serving AP
```bash
kind load docker-image serving-api:1.0 --name bigdata-cluster
```
Đợi tầm 30s-2'



## 📦 Step 3: Deploy Infrastructure

### 3.1. Kafka

#### 3.1.1. Cài đặt Strimzi Operator:

```bash
kubectl create -f https://strimzi.io/install/latest?namespace=default
```

kết quả
```bash
...
customresourcedefinition.apiextensions.k8s.io/kafkamirrormaker2s.kafka.strimzi.io created
customresourcedefinition.apiextensions.k8s.io/kafkatopics.kafka.strimzi.io created
serviceaccount/strimzi-cluster-operator created
```

#### 3.1.2. Đợi Operator khởi động:
```bash
kubectl get pods
NAME                                        READY   STATUS    RESTARTS   AGE
strimzi-cluster-operator-57856f5f9b-7pvk4   1/1     Running   0          86s
```

#### 3.1.3. Deploy Kafka Cluster:
```bash
kubectl apply -f k8s/kafka-config.yaml
```

Kết quả
```bash
kafkanodepool.kafka.strimzi.io/kafka-pool unchanged
kafka.kafka.strimzi.io/my-cluster unchanged
kafkatopic.kafka.strimzi.io/user-activity-events created

PS D:\Project_BigData\Big_data> kubectl get svc
NAME                                  TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)                      AGE
kubernetes                            ClusterIP   10.96.0.1      <none>        443/TCP                      10h
my-cluster-kafka-bootstrap            ClusterIP   10.96.75.169   <none>        9091/TCP                     20m
my-cluster-kafka-brokers              ClusterIP   None           <none>        9090/TCP,9091/TCP,8443/TCP   20m
my-cluster-kafka-external-bootstrap   NodePort    10.96.245.25   <none>        9092:30092/TCP               20m
my-cluster-kafka-pool-external-0      NodePort    10.96.145.33   <none>        9092:32580/TCP               20m
PS D:\Project_BigData\Big_data>
```
---

### 3.2. MinIO
#### 3.2.1. Deploy MinIO
```bash
kubectl apply -f k8s/minio-config.yaml
```

Kết quả
```bash
service/minio created
statefulset.apps/minio created
```

#### 3.2.2. Kiểm tra trạng thái
```bash
kubectl get pods
NAME                                          READY   STATUS             RESTARTS      AGE
minio-0                                       0/1     ContainerCreating   0             31s
...
PS D:\Project_BigData\Big_data>
```
Đợi 1-3' nếu đây là lần đầu vì nó cần pull image về nữa 

Kết quả
```bash
kubectl get pods
NAME                                          READY   STATUS    RESTARTS      AGE
minio-0                                       1/1     Running   0             3m49s
minio-1                                       1/1     Running   0             3m23s
minio-2                                       1/1     Running   0             2m53s
minio-3                                       1/1     Running   0             2m12s
```
#### 3.2.3. Đẩy các files csv vào MinIO 
- Mở MinIO Console:
```text
http://localhost:30091
```

- Thông tin đăng nhập:
Username: minioadmin
Password: minioadmin

#### 🪣 B1 Tạo Bucket:
- Sau khi đăng nhập 
- ở menu bên trái có Bucket, Chọn Bucket 
- Tạo bucket: trên giao diện có chữ "Create Bucket", bấm vào 
- Nó hiện giao diện Create Bucket, sau đó điền Bucket Name*  là "datalake" -> Click Create
  
#### 📁 B2 Upload dữ liệu đầu vào
- Kệ nó, ở menu bên trái bấm vào Object Browser
- Vào giao diện Object Browser rồi nó hiện danh sách bucket bên dưới, bấm vào datalake
- Tiếp theo cần tạo datalake/processed: bên trái có chữ Create new path, bấm vào và điền processed
- Rồi vào giao diện Object Browser của datalake này sẽ thấy chữ Upload thì bấm vào Upload Files => Sau đó tìm lấy thư mục của project hiện tại có tên 1_dataset/raw_data và upload tất cả files csv
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
Tiếp tục đợi nó pull image và setup, tầm 3-6'

#### 3.3.2. Kiểm tra trạng thái
```bash
kubectl get pods
cassandra-0                                   1/1     Running   0             4m6s
cassandra-1                                   1/1     Running   0             3m29s
cassandra-2                                   1/1     Running   2 (96s ago)   2m53s
...
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
B3 xử lý lỗi nếu có cảnh báo 
```bash
Warnings :
Your replication factor 3 for keyspace ecommerce is higher than the number of nodes 2 for datacenter DC1
```

Sửa
```bash
kubectl delete pod cassandra-2
```

Kiểm tra xem có đủ 3 dòng có chữ UN không 
```bash
PS D:\Project_BigData\Big_data> kubectl exec -it cassandra-0 -- nodetool status
Datacenter: DC1
===============
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address     Load        Tokens  Owns (effective)  Host ID                               Rack
UN  10.244.2.6  105.37 KiB  16      100.0%            7269d4ba-7636-43fd-8d0c-5fd8def675a2  rack1
UN  10.244.1.8  250.01 KiB  16      100.0%            28bf133a-a2a1-48cf-b330-60bc4ed599b9  rack1
UN  10.244.3.8  100.32 KiB  16      100.0%            b5bbce0a-4fc0-4b58-b151-d8777d7d1110  rack1
```

---
### 3.4. Redis
#### 3.4.1. Deploy Redis
```bash
kubectl apply -f k8s/redis.yaml
```

Tiếp tục đợi nó pull image và setup, tầm 20s-2'

#### 3.4.2. Kiểm tra trạng thái
```bash
kubectl get pods
...
redis-6cf754768b-vsh25                        1/1     Running   0               60s
...
```
---
### 3.5. Spark Cluster
#### 3.5.1. Deploy Spark
```bash
kubectl apply -f k8s/spark2-config.yaml
```

Tiếp tục đợi nó pull image và setup, tầm 20s-2'


#### 3.5.2. Kiểm tra trạng thái
```bash
kubectl get pods
...
spark-master-785dcd6688-b6dh2                 1/1     Running   0               37s
spark-worker-557b7b687-2qkdw                  1/1     Running   0               32s
spark-worker-557b7b687-dh7mb                  1/1     Running   0               32s
spark-worker-557b7b687-gd4bn                  1/1     Running   0     
...
```
---
## ⚡ Step 4: Deploy Processing Layer

### 4.1. Airflow
#### 4.1.1. Airflow
```bash
kubectl apply -f k8s/airflow.yaml
```

#### 4.1.1. Kiểm tra trạng thái
```bash
kubectl get pods
ví dụ kết quả ra
airflow-76c8bf67ff-sxsxg                      1/1     Running   0    
...
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

## 8 Test
### 8.1. Kafka
```bash
kubectl exec -it my-cluster-kafka-pool-0 -- bash (vào giao diện kafka)
=> Kết quả
[kafka@my-cluster-kafka-pool-0 kafka]$

Tiếp theo viết lệnh để làm consumer xem dữ liệu tới
/opt/kafka/bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic user-activity-events \
--from-beginning
```

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