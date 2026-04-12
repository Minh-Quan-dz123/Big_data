# Stream Pipeline

Đọc từ Kafka topic `ABC1` → clean → gửi sang topic `ABC2`.

```bash
# Cài đặt
pip install -r requirements.txt

# Chạy (stream mode)
cd 2_ingestion/stream
python main.py

# Chạy (batch mode – đọc 1 lượt rồi thoát)
python main.py batch
```

Cấu hình: chỉnh `config.yaml`.
