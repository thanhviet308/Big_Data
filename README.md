
# Fraud detection (Databricks-first)

Mục tiêu: chạy **toàn bộ** pipeline trên **Databricks** (batch + tuỳ chọn streaming).

## Chạy trên Databricks (khuyến nghị)

### 1) Import repo bằng Databricks Repos

- Databricks → **Repos** → **Add Repo** → trỏ tới Git repo.

### 2) Chạy notebooks theo thứ tự

Các notebook nằm ở [databricks/notebooks/](databricks/notebooks/):

1. [Install deps](databricks/notebooks/00_install_deps.py)
2. [Stage data (DBFS-free)](databricks/notebooks/01_stage_data_to_dbfs.py)
3. [Preprocess + Train + Evaluate](databricks/notebooks/02_preprocess_and_train_eval.py)
4. [Inference demo](databricks/notebooks/03_inference_demo.py)
5. (Optional) [Streaming Kafka](databricks/notebooks/04_streaming_kafka_optional.py)

### Dữ liệu đầu vào (quan trọng)

Trong repo này, `data/raw/` và `data/processed/` đang được `.gitignore` (dataset lớn), nên **Databricks Repos sẽ không có file raw CSV**.

Bạn cần upload raw CSV lên một path mà cluster đọc được (khuyến nghị Unity Catalog Volume), rồi chạy notebook 01 với:

- `FRAUD_STORAGE_BASE=/Volumes/<catalog>/<schema>/<volume>/fraud_detection`
- `FRAUD_RAW_SOURCE_PATH=/Volumes/<catalog>/<schema>/<volume>/PS_20174392719_1491204439457_log.csv`

Notebook 01 sẽ copy raw + blacklist vào `${FRAUD_STORAGE_BASE}` để notebook 02/03/04 dùng.

## Đường dẫn dữ liệu/model (Databricks-safe)

Các script hỗ trợ override bằng biến môi trường (phù hợp cho Databricks Jobs):

- `FRAUD_RAW_DATA_PATH`
- `FRAUD_PROCESSED_DATA_PATH`
- `FRAUD_BLACKLIST_PATH`
- `FRAUD_MODEL_PATH`

Trên Databricks, workspace của bạn có thể **chặn** `dbfs:/FileStore` (Public DBFS root).

Vì vậy các notebook trong repo này **không phụ thuộc DBFS** nữa.

- Mặc định, notebooks stage artifacts vào: `/tmp/fraud_detection`
- Bạn có thể đổi location bằng biến môi trường `FRAUD_STORAGE_BASE`

Khuyến nghị cho Jobs/Streaming (bền vững & shared): dùng Unity Catalog Volume, ví dụ:

- `FRAUD_STORAGE_BASE=/Volumes/<catalog>/<schema>/<volume>/fraud_detection`

## Streaming (Kafka) lưu ý

Streaming chỉ chạy được nếu Databricks cluster **kết nối được** Kafka broker.
Bạn cần set:

- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC`

Checkpoint mặc định: `/tmp/fraud_detection/checkpoints` (có thể override bằng `SPARK_CHECKPOINT_PATH`).

Nếu chạy streaming multi-node, checkpoint nên trỏ tới storage shared/durable (ví dụ Unity Catalog Volume).

## Unit tests + CI

- Unit tests: chạy `python -m pytest`
- GitHub Actions workflow: [CI](.github/workflows/ci.yml)

## Docker

Repo có Dockerfile tối thiểu để đóng gói môi trường Python: [Dockerfile](Dockerfile)

Kafka local để test micro-batch có sẵn ở [docker/docker-compose.yml](docker/docker-compose.yml).

## Trực quan hoá (Next.js + SSE)

Phần Next.js + SSE **chưa có trong repo hiện tại** (chưa scaffold dự án frontend/backend stream cho dashboard).
