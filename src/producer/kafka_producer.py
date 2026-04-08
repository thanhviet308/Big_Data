import json
import os
import time
import pandas as pd
from kafka import KafkaProducer

from src.utils.paths import env_or_repo_path

DATA_PATH = env_or_repo_path(
    "FRAUD_RAW_DATA_PATH",
    "data",
    "raw",
    "PS_20174392719_1491204439457_log.csv",
)
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def main():
    df = pd.read_csv(DATA_PATH)

    # lấy một phần nhỏ để demo trước
    df = df.head(100)

    for _, row in df.iterrows():
        message = {
            "step": int(row["step"]),
            "type": str(row["type"]),
            "amount": float(row["amount"]),
            "nameOrig": str(row["nameOrig"]),
            "oldbalanceOrg": float(row["oldbalanceOrg"]),
            "newbalanceOrig": float(row["newbalanceOrig"]),
            "nameDest": str(row["nameDest"]),
            "oldbalanceDest": float(row["oldbalanceDest"]),
            "newbalanceDest": float(row["newbalanceDest"])
        }

        producer.send(TOPIC, value=message)
        print("Sent:", message)
        time.sleep(0.5)

    producer.flush()
    print("✅ Done sending messages to Kafka")

if __name__ == "__main__":
    main()