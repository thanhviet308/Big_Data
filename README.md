
# Fraud detection (Databricks-first)

Mục tiêu: chạy **toàn bộ** pipeline trên **Databricks** (batch + tuỳ chọn streaming).

## Chạy trên Databricks (khuyến nghị)

### 1) Import repo bằng Databricks Repos

- Databricks → **Repos** → **Add Repo** → trỏ tới Git repo.

### 2) Chạy notebooks theo thứ tự

Các notebook nằm ở [databricks/notebooks/](databricks/notebooks/):

1. [Install deps](databricks/notebooks/00_install_deps.py)
2. [Stage data to DBFS](databricks/notebooks/01_stage_data_to_dbfs.py)
3. [Preprocess + Train + Evaluate](databricks/notebooks/02_preprocess_and_train_eval.py)
4. [Inference demo](databricks/notebooks/03_inference_demo.py)
5. (Optional) [Streaming Kafka](databricks/notebooks/04_streaming_kafka_optional.py)

## Đường dẫn dữ liệu/model (DBFS)

Các script hỗ trợ override bằng biến môi trường (phù hợp cho Databricks Jobs):

- `FRAUD_RAW_DATA_PATH`
- `FRAUD_PROCESSED_DATA_PATH`
- `FRAUD_BLACKLIST_PATH`
- `FRAUD_MODEL_PATH`

Trên Databricks, để pandas/joblib đọc được, nên dùng dạng `/dbfs/...` (ví dụ `/dbfs/FileStore/fraud/raw.csv`).

## Streaming (Kafka) lưu ý

Streaming chỉ chạy được nếu Databricks cluster **kết nối được** Kafka broker.
Bạn cần set:

- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC`

Checkpoint mặc định: `dbfs:/tmp/fraud_detection/checkpoints` (có thể override bằng `SPARK_CHECKPOINT_PATH`).

## Unit tests + CI

- Unit tests: chạy `python -m pytest`
- GitHub Actions workflow: [CI](.github/workflows/ci.yml)

## Docker

Repo có Dockerfile tối thiểu để đóng gói môi trường Python: [Dockerfile](Dockerfile)

Kafka local để test micro-batch có sẵn ở [docker/docker-compose.yml](docker/docker-compose.yml).

## Trực quan hoá (Next.js + SSE)

Phần Next.js + SSE **chưa có trong repo hiện tại** (chưa scaffold dự án frontend/backend stream cho dashboard).
