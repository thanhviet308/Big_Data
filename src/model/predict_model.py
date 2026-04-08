import os
import joblib
import pandas as pd

from src.features.feature_engineering import create_features

from src.utils.paths import env_or_repo_path

MODEL_PATH = env_or_repo_path(
    "FRAUD_MODEL_PATH",
    "src",
    "model",
    "saved_model",
    "fraud_model.pkl",
)

_model = None

FEATURE_COLUMNS = [
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
    "type_CASH_IN",
    "type_CASH_OUT",
    "type_DEBIT",
    "type_PAYMENT",
    "type_TRANSFER",
    "balanceDiffOrig",
    "balanceDiffDest",
    "isLargeAmount"
]

def prepare_model_input(df: pd.DataFrame):
    result = df.copy()

    if "type" in result.columns:
        result["type"] = (
            result["type"]
            .fillna("UNKNOWN")
            .astype(str)
            .str.strip()
            .str.upper()
            .str.replace(r"\s+", "_", regex=True)
        )

    # one-hot cho type giống lúc train
    result = pd.get_dummies(result, columns=["type"])

    # đảm bảo đủ cột
    for col in [
        "type_CASH_IN",
        "type_CASH_OUT",
        "type_DEBIT",
        "type_PAYMENT",
        "type_TRANSFER"
    ]:
        if col not in result.columns:
            result[col] = 0

    result = create_features(result)

    # lấy đúng thứ tự cột model cần
    result = result[FEATURE_COLUMNS]

    return result


def get_model():
    global _model

    if _model is None:
        if not os.path.exists(MODEL_PATH):
            raise FileNotFoundError(
                "Model file not found. Train first (src/model/train_model.py) "
                "or set FRAUD_MODEL_PATH to a valid path (local or /dbfs/... on Databricks). "
                f"Tried: {MODEL_PATH}"
            )
        _model = joblib.load(MODEL_PATH)
    return _model

def predict(df: pd.DataFrame):
    X = prepare_model_input(df)
    preds = get_model().predict(X)
    return preds