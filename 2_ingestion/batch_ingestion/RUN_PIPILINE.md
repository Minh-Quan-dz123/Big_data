# RUN PIPELINE: CSV → SPARK → MINIO (K8S + KIND)

---

# PHẦN I — TỔNG QUAN & THIẾT KẾ (1 → 5)

## 1. MỤC TIÊU

Pipeline:

CSV (local)
→ Docker Image (code + data + jar)
→ Kubernetes Job
→ Spark xử lý
→ Ghi dữ liệu → MinIO (S3A)

---

## 2. TƯ DUY HỆ THỐNG

- Docker: đóng gói môi trường
- Kubernetes: chạy container
- Spark: xử lý dữ liệu
- MinIO: lưu dữ liệu

---

## 3. LUỒNG CHẠY

kubectl apply job
→ Kubernetes tạo Pod
→ Container start
→ spark-submit chạy main.py
→ Spark Driver kết nối Master
→ Worker xử lý
→ Ghi dữ liệu → MinIO

---

## 4. LUỒNG DỮ LIỆU

/opt/spark/data/*.csv
→ Spark read (file://)
→ DataFrame
→ Clean
→ Write → s3a://datalake/raw/ (MinIO)

---

## 5. CẤU TRÚC PROJECT
```text
Big_data/
├── 1_dataset/ (dữ liệu cần dùng và lưu vào MinIO, sẽ được làm sạch bởi chương trình ở dưới)
│   └── raw_data/
│       ├── orders.csv
│       ├── users.csv
│
├── 2_ingestion/
│   └── batch_ingestion/
│       ├── Dockerfile (đóng gói dataset, main.py, thư viện)
│       ├── main.py (chương trình chạy - nhân vật chính)
│       └── jars/   (thư viện để spark sử dụng)
│           ├── hadoop-aws-3.3.4.jar
│           ├── aws-java-sdk-bundle-1.12.262.jar
│
├── k8s/
│   ├── minio.yaml
│   ├── spark.yaml
│   └── spark-job1-ingestion-batch.yaml (job cần chạy)
```

---

# PHẦN II — TRIỂN KHAI & VẬN HÀNH

---

## II.1. TRIỂN KHAI & CHẠY HỆ THỐNG (6 → 13)

---

## 6. BUILD DOCKER IMAGE

cd Big_data/2_ingestion/batch_ingestion

docker build -t spark-job1-ingestion-batch .

---

## 7. LOAD IMAGE VÀO KIND

kind load docker-image spark-job1-ingestion-batch

Nếu không làm bước này:
→ lỗi ImagePullBackOff

---

## 8. DEPLOY MINIO (nếu đã deploy trước đó rồi thì thôi)

kubectl apply -f ../../k8s/minio.yaml

---

### Mở UI

kubectl port-forward svc/minio 9001:9001

Truy cập:
http://localhost:9001

Login:
minioadmin / minioadmin

---

### Tạo bucket

datalake

(Phải trùng với code Python)

---

## 9. DEPLOY SPARK (nếu đã deploy trước đó rồi thì thôi)

kubectl apply -f ../../k8s/spark.yaml

---

### Kiểm tra

kubectl get pods

Phải thấy:
spark-master-xxx
spark-worker-xxx

---

## 10. CHẠY JOB

kubectl apply -f ../../k8s/spark-job1-ingestion-batch.yaml

---

## 11. XEM LOG

kubectl logs job/spark-job1-ingestion-batch

---

## 12. KẾT QUẢ

MinIO:

datalake/
└── raw/
    ├── orders.csv/
    ├── users.csv/

---

## 13. CHẠY LẠI JOB

kubectl delete job spark-job1-ingestion-batch
kubectl apply -f ../../k8s/spark-job1-ingestion-batch.yaml

---

## II.2. DEBUG & MỞ RỘNG (14 → 16)

---

## 14. DEBUG

### Job fail

kubectl describe job spark-job1-ingestion-batch

---

### Pod lỗi

kubectl get pods
kubectl logs <pod-name>

---

### Không ghi được MinIO

Check:
- bucket tồn tại chưa
- endpoint: minio:9000

---

### Lỗi S3A

ClassNotFoundException: S3AFileSystem

→ thiếu JAR

---

### Không thấy CSV

Check:
/opt/spark/data

---

## 15. SCALE NHIỀU JOB

Tạo thêm:

spark-job2.yaml
spark-job3.yaml

Mỗi job:
- image riêng hoặc chung
- main.py khác nhau

---

## 16. KẾT LUẬN

Pipeline:

Docker → Kubernetes Job → Spark → MinIO