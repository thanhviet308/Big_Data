# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Inference demo

# COMMAND ----------

import os
import pandas as pd
from pathlib import Path

# DBFS may be disabled in some workspaces/Serverless.
# Read artifacts from a filesystem base (local /tmp by default, or a UC Volume).

STORAGE_BASE = Path(os.getenv("FRAUD_STORAGE_BASE", "/tmp/fraud_detection"))
MODEL_PATH = str(STORAGE_BASE / "fraud_model.pkl")
BLACKLIST_PATH = str(STORAGE_BASE / "blacklist_accounts.csv")

if not Path(MODEL_PATH).exists() or not Path(BLACKLIST_PATH).exists():
    raise FileNotFoundError(
        "Missing artifacts. Run notebook 02 first (train), or set FRAUD_STORAGE_BASE to the folder "
        "that contains fraud_model.pkl and blacklist_accounts.csv. "
        f"Tried: {STORAGE_BASE}"
    )

os.environ["FRAUD_MODEL_PATH"] = MODEL_PATH
os.environ["FRAUD_BLACKLIST_PATH"] = BLACKLIST_PATH

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
