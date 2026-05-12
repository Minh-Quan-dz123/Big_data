# 🚀 Deploy K8s Big Data Recommendation System

## 📌 Tổng quan

Đây là repository triển khai **hệ thống Big Data** dùng để:

* Thu thập dữ liệu người dùng
* Phân tích hành vi
* Gợi ý sản phẩm
* Ứng dụng trong hệ thống thương mại điện tử 

Hệ thống được thiết kế theo **kiến trúc Lambda**, gồm 4 phần:

* 🔹 Data Ingestion
* 🔹 Batch Layer
* 🔹 Speed Layerv
* 🔹 Serving Layer

---
    
## 🔄 Luồng xử lý dữ liệu

* User → Producer → Kafka 
* Kafka → Spark Streaming → Realtime Processing
* Kafka → Storage → Spark Batch → Analytics

---

## 📁 Cấu trúc thư mục

```
.
├── k8s/
│   ├── kafka-config.yaml
│   ├── spark-config.yaml
│   ├── minio-config.yaml
│   └── kind-config.yaml
└── README.md
```

---

# ⚙️ I. Chuẩn bị môi trường

## 1️⃣ Cài đặt công cụ

```bash
choco install kind -y
choco install kubernetes-cli -y
```

## 2️⃣ Kiểm tra

```bash
kind version
kubectl version --client
```

---

# ☸️ II. Tạo Kubernetes Cluster (kind)

## 1️⃣ Tạo file `kind-config.yaml`

👉 *(nội dung trong file `kind-config.yaml`)*

## 2️⃣ Khởi tạo cluster

```bash
kind create cluster --name bigdata-cluster --config kind-config.yaml
```

## 3️⃣ Kiểm tra node

```bash
kubectl get nodes
```

---

# 📦 III. Deploy Kafka (Strimzi)

## 1️⃣ Cài Strimzi Operator

```bash
kubectl create -f https://strimzi.io/install/latest?namespace=default
```

## 2️⃣ Kiểm tra

```bash
kubectl get pods
```

## 3️⃣ Tạo file `kafka-config.yaml`

👉 *(nội dung trong file `kafka-config.yaml`)*

## 4️⃣ Deploy Kafka

```bash
kubectl apply -f kafka-config.yaml
kubectl get pods
```

---

## 🧪 Test Kafka

```bash
# Tạo topic
kubectl exec -it my-cluster-kafka-pool-0 -- \
bin/kafka-topics.sh --create \
--topic test-topic \
--bootstrap-server localhost:9092

# List topic
kubectl exec -it my-cluster-kafka-pool-0 -- \
bin/kafka-topics.sh --list \
--bootstrap-server localhost:9092

# Producer
kubectl exec -it my-cluster-kafka-pool-0 -- \
bin/kafka-console-producer.sh \
--topic test-topic \
--bootstrap-server localhost:9092

# Consumer
kubectl exec -it my-cluster-kafka-pool-0 -- \
bin/kafka-console-consumer.sh \
--topic test-topic \
--from-beginning \
--bootstrap-server localhost:9092
```

---

# ⚡ IV. Deploy Spark Cluster

## 1️⃣ Tạo file `spark-config.yaml`

👉 *(nội dung trong file `spark-config.yaml`)*

## 2️⃣ Deploy

```bash
kubectl apply -f spark-config.yaml
kubectl get pods
```

## 3️⃣ Mở Spark UI

```bash
kubectl port-forward svc/spark-master 8080:8080
```

👉 [http://localhost:8080](http://localhost:8080)

---

# 🪣 V. Deploy MinIO (Thay HDFS)

## 1️⃣ Tạo file `minio-config.yaml`

👉 *(nội dung trong file `minio-config.yaml`)*

## 2️⃣ Deploy

```bash
kubectl apply -f minio-config.yaml
kubectl get pods
```

## 3️⃣ Mở UI

```bash
kubectl port-forward svc/minio 9001:9001
```

👉 [http://localhost:9001](http://localhost:9001)

* User: `minioadmin`
* Password: `minioadmin`

---

# 🗄️ VI. Deploy Cassandra (Serving Layer)

## 1️⃣ Tạo file `cassandra-config.yaml`

👉 *(nội dung trong file `cassandra-config.yaml`)*

## 2️⃣ Deploy Cassandra

```bash
kubectl apply -f cassandra-config.yaml
kubectl get pods
```

## 3️⃣ Kiểm tra Cassandra

```bash
kubectl exec -it cassandra-0 -- cqlsh
```

---

# 📊 VII. Kiểm tra hệ thống

## Xem logs

```bash
kubectl logs <pod-name>
```

## Kiểm tra trạng thái

```bash
kubectl get pods
kubectl get svc
```

---

# 📌 Ghi chú quan trọng

* Có thể giảm replicas khi chạy local
* Kafka chạy chế độ **KRaft**
* MinIO Distributed yêu cầu ≥ 4 pods
* Spark chạy standalone cluster
* Hệ thống chạy trên Docker (kind)

---

# 🧠 Công nghệ sử dụng

* Kubernetes (kind)
* Kafka (Strimzi)
* Apache Spark
* MinIO
* Docker

---

# 🎉 Kết luận

Hệ thống cung cấp:

* Xử lý realtime
* Xử lý batch
* Nền tảng cho hệ thống recommendation
