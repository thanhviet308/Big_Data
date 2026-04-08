# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Preprocess + Train + Evaluate
# MAGIC 
# MAGIC Notebook này chạy toàn bộ batch pipeline trên Databricks.

# COMMAND ----------

import os
from pathlib import Path

# Serverless may not allow direct /dbfs filesystem access.
# Pattern used here:
# - read raw from DBFS by copying to local tmp
# - write outputs to local tmp then copy to DBFS

RAW_DBFS = "dbfs:/FileStore/fraud/raw.csv"
BLACKLIST_DBFS = "dbfs:/FileStore/fraud/blacklist_accounts.csv"
PROCESSED_DBFS = "dbfs:/FileStore/fraud/processed.csv"
MODEL_DBFS = "dbfs:/FileStore/fraud/fraud_model.pkl"

LOCAL_DIR = Path("/tmp/fraud")
LOCAL_DIR.mkdir(parents=True, exist_ok=True)

RAW_LOCAL = str(LOCAL_DIR / "raw.csv")
BLACKLIST_LOCAL = str(LOCAL_DIR / "blacklist_accounts.csv")
PROCESSED_LOCAL = str(LOCAL_DIR / "processed.csv")
MODEL_LOCAL = str(LOCAL_DIR / "fraud_model.pkl")

dbutils.fs.mkdirs("dbfs:/FileStore/fraud")
dbutils.fs.cp(RAW_DBFS, f"file:{RAW_LOCAL}", True)
dbutils.fs.cp(BLACKLIST_DBFS, f"file:{BLACKLIST_LOCAL}", True)

os.environ["FRAUD_RAW_DATA_PATH"] = RAW_LOCAL
os.environ["FRAUD_BLACKLIST_PATH"] = BLACKLIST_LOCAL
os.environ["FRAUD_PROCESSED_DATA_PATH"] = PROCESSED_LOCAL
os.environ["FRAUD_MODEL_PATH"] = MODEL_LOCAL

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

# Copy processed to DBFS for later tasks
dbutils.fs.cp(f"file:{PROCESSED_LOCAL}", PROCESSED_DBFS, True)
print("✅ Saved processed to:", PROCESSED_DBFS)

# COMMAND ----------

from src.model.train_model import main as train_main, MODEL_PATH

print("MODEL_PATH:", MODEL_PATH)
train_main()

# Copy model to DBFS for later tasks
dbutils.fs.cp(f"file:{MODEL_LOCAL}", MODEL_DBFS, True)
print("✅ Saved model to:", MODEL_DBFS)
