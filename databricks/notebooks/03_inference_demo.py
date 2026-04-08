# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Inference demo

# COMMAND ----------

import os
import pandas as pd

# Must match where model was saved
os.environ.setdefault("FRAUD_MODEL_PATH", "/dbfs/FileStore/fraud/fraud_model.pkl")
os.environ.setdefault("FRAUD_BLACKLIST_PATH", "/dbfs/FileStore/fraud/blacklist_accounts.csv")

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
