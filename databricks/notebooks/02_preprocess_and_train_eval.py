# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Preprocess + Train + Evaluate
# MAGIC 
# MAGIC Notebook này chạy toàn bộ batch pipeline trên Databricks.

# COMMAND ----------

import os
from pathlib import Path

# Many workspaces/Serverless disable Public DBFS root (dbfs:/...).
# Use a plain filesystem base instead (local /tmp by default, or a UC Volume).

STORAGE_BASE = Path(os.getenv("FRAUD_STORAGE_BASE", "/tmp/fraud_detection"))
STORAGE_BASE.mkdir(parents=True, exist_ok=True)

RAW_PATH = str(STORAGE_BASE / "raw.csv")
BLACKLIST_PATH = str(STORAGE_BASE / "blacklist_accounts.csv")
PROCESSED_PATH = str(STORAGE_BASE / "processed.csv")
MODEL_PATH = str(STORAGE_BASE / "fraud_model.pkl")

if not Path(RAW_PATH).exists() or not Path(BLACKLIST_PATH).exists():
	raise FileNotFoundError(
		"Missing staged data. Run notebook 01 first, or set FRAUD_STORAGE_BASE to the folder "
		"that contains raw.csv and blacklist_accounts.csv. "
		f"Tried: {STORAGE_BASE}"
	)

os.environ["FRAUD_RAW_DATA_PATH"] = RAW_PATH
os.environ["FRAUD_BLACKLIST_PATH"] = BLACKLIST_PATH
os.environ["FRAUD_PROCESSED_DATA_PATH"] = PROCESSED_PATH
os.environ["FRAUD_MODEL_PATH"] = MODEL_PATH

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
print("✅ Saved processed to:", processed_path)

# COMMAND ----------

from src.model.train_model import main as train_main, MODEL_PATH

print("MODEL_PATH:", MODEL_PATH)
train_main()
print("✅ Saved model to:", MODEL_PATH)
