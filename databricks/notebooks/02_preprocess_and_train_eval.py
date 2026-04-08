# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Preprocess + Train + Evaluate
# MAGIC 
# MAGIC Notebook này chạy toàn bộ batch pipeline trên Databricks.

# COMMAND ----------

import os

# Set DBFS paths (pandas/joblib use /dbfs/...; code also accepts dbfs:/...)
os.environ["FRAUD_RAW_DATA_PATH"] = "/dbfs/FileStore/fraud/raw.csv"
os.environ["FRAUD_BLACKLIST_PATH"] = "/dbfs/FileStore/fraud/blacklist_accounts.csv"
os.environ["FRAUD_PROCESSED_DATA_PATH"] = "/dbfs/FileStore/fraud/processed.csv"
os.environ["FRAUD_MODEL_PATH"] = "/dbfs/FileStore/fraud/fraud_model.pkl"

# COMMAND ----------

from src.preprocessing.preprocess_data import load_data, preprocess
from src.utils.paths import ensure_parent_dir

raw_path = os.environ["FRAUD_RAW_DATA_PATH"]
processed_path = os.environ["FRAUD_PROCESSED_DATA_PATH"]

print("RAW:", raw_path)
print("PROCESSED:", processed_path)

df_raw = load_data(raw_path)
df_processed = preprocess(df_raw)
ensure_parent_dir(processed_path)
df_processed.to_csv(processed_path, index=False)

print("✅ Preprocess done. Rows:", len(df_processed))

# COMMAND ----------

from src.model.train_model import main as train_main, MODEL_PATH

print("MODEL_PATH:", MODEL_PATH)
train_main()
