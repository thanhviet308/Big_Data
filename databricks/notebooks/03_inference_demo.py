# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Inference demo

# COMMAND ----------

import os
import pandas as pd
from pathlib import Path

# Serverless-safe: copy artifacts from DBFS to local tmp, then point env vars there
MODEL_DBFS = "dbfs:/FileStore/fraud/fraud_model.pkl"
BLACKLIST_DBFS = "dbfs:/FileStore/fraud/blacklist_accounts.csv"

LOCAL_DIR = Path("/tmp/fraud")
LOCAL_DIR.mkdir(parents=True, exist_ok=True)

MODEL_LOCAL = str(LOCAL_DIR / "fraud_model.pkl")
BLACKLIST_LOCAL = str(LOCAL_DIR / "blacklist_accounts.csv")

dbutils.fs.cp(MODEL_DBFS, f"file:{MODEL_LOCAL}", True)
dbutils.fs.cp(BLACKLIST_DBFS, f"file:{BLACKLIST_LOCAL}", True)

os.environ["FRAUD_MODEL_PATH"] = MODEL_LOCAL
os.environ["FRAUD_BLACKLIST_PATH"] = BLACKLIST_LOCAL

from src.detection.fraud_detector import detect_fraud

sample = pd.DataFrame(
    [
        {
            "type": "TRANSFER",
            "amount": 500000,
            "nameOrig": "C123",
            "nameDest": "M1979787155",
            "oldbalanceOrg": 700000,
            "newbalanceOrig": 200000,
            "oldbalanceDest": 0,
            "newbalanceDest": 500000,
        },
        {
            "type": "PAYMENT",
            "amount": 5000,
            "nameOrig": "C456",
            "nameDest": "M9999999999",
            "oldbalanceOrg": 10000,
            "newbalanceOrig": 5000,
            "oldbalanceDest": 1000,
            "newbalanceDest": 6000,
        },
    ]
)

result = detect_fraud(sample)
result[["type", "amount", "nameDest", "ml_prediction", "rule_flag", "final_flag"]]
