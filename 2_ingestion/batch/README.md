# Batch Pipeline

Đọc CSV từ `1_dataset/` → clean → ghi Avro → upload HDFS.

```bash
# Cài đặt
pip install -r requirements.txt

# Chạy
cd 2_ingestion/batch
python main.py
```

Cấu hình: chỉnh `config.yaml`.
