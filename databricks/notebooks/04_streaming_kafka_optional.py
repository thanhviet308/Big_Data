# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Streaming (Kafka) - Optional
# MAGIC 
# MAGIC Chạy streaming trên Databricks chỉ khả thi khi cluster truy cập được Kafka broker.
# MAGIC 
# MAGIC Bạn cần set 2 biến:
# MAGIC - `KAFKA_BOOTSTRAP_SERVERS`
# MAGIC - `KAFKA_TOPIC`
# MAGIC 
# MAGIC (Nếu cần) set thêm `SPARK_KAFKA_PACKAGE` khi runtime không có sẵn connector.

# COMMAND ----------

import os

# Example (PHẢI thay bằng Kafka thật truy cập được từ Databricks)
# os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "broker:9092"
# os.environ["KAFKA_TOPIC"] = "transactions"

# Optional
os.environ.setdefault("SPARK_CHECKPOINT_PATH", "dbfs:/tmp/fraud_detection/checkpoints")

# Ensure model/blacklist paths on DBFS
os.environ.setdefault("FRAUD_MODEL_PATH", "/dbfs/FileStore/fraud/fraud_model.pkl")
os.environ.setdefault("FRAUD_BLACKLIST_PATH", "/dbfs/FileStore/fraud/blacklist_accounts.csv")

# COMMAND ----------

# Running as a script inside notebook
import runpy

runpy.run_module("src.streaming.spark_streaming_job", run_name="__main__")
