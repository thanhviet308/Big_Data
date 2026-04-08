import pandas as pd

from src.model.predict_model import predict
from src.rules.rule_engine import load_blacklist, apply_rules

def detect_fraud(df: pd.DataFrame):
    result = df.copy()

    # ML prediction
    result["ml_prediction"] = predict(result)

    # Rule engine
    blacklist = load_blacklist()
    result = apply_rules(result, blacklist)

    # Final decision: nếu ML hoặc Rule báo fraud thì flag
    result["final_flag"] = (
        (result["ml_prediction"] == 1) |
        (result["rule_flag"] == 1)
    ).astype(int)

    return result

if __name__ == "__main__":
    sample_data = pd.DataFrame([
        {
            "type": "TRANSFER",
            "amount": 500000,
            "nameOrig": "C123",
            "nameDest": "M1979787155",
            "oldbalanceOrg": 700000,
            "newbalanceOrig": 200000,
            "oldbalanceDest": 0,
            "newbalanceDest": 500000
        },
        {
            "type": "PAYMENT",
            "amount": 5000,
            "nameOrig": "C456",
            "nameDest": "M9999999999",
            "oldbalanceOrg": 10000,
            "newbalanceOrig": 5000,
            "oldbalanceDest": 1000,
            "newbalanceDest": 6000
        }
    ])

    result = detect_fraud(sample_data)
    print(result[[
        "type", "amount", "nameDest",
        "ml_prediction", "rule_flag", "final_flag"
    ]])