import pandas as pd
import numpy as np

from src.utils.paths import env_or_repo_path, ensure_parent_dir


DEFAULT_RAW_DATA_PATH = env_or_repo_path(
    "FRAUD_RAW_DATA_PATH",
    "data",
    "raw",
    "PS_20174392719_1491204439457_log.csv",
)

DEFAULT_PROCESSED_DATA_PATH = env_or_repo_path(
    "FRAUD_PROCESSED_DATA_PATH",
    "data",
    "processed",
    "processed_paysim.csv",
)

def load_data(path):
    df = pd.read_csv(path)
    return df

def preprocess(df):
    required_columns = [
        "type",
        "amount",
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
        "isFraud",
    ]

    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    result = df[required_columns].copy()

    # Normalize categorical text
    result["type"] = (
        result["type"]
        .fillna("UNKNOWN")
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace(r"\s+", "_", regex=True)
    )

    # Coerce numeric columns
    numeric_cols = [
        "amount",
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
    ]
    for col in numeric_cols:
        result[col] = pd.to_numeric(result[col], errors="coerce")

    # Replace inf values then fill missing with median (fallback 0)
    result[numeric_cols] = result[numeric_cols].replace([np.inf, -np.inf], np.nan)
    for col in numeric_cols:
        median = result[col].median(skipna=True)
        if pd.isna(median):
            median = 0.0
        result[col] = result[col].fillna(median)

    # Label cleaning
    result["isFraud"] = pd.to_numeric(result["isFraud"], errors="coerce")
    result = result.dropna(subset=["isFraud"]).copy()
    result["isFraud"] = result["isFraud"].astype(int)

    # Drop exact duplicates to reduce noise
    result = result.drop_duplicates()

    return result

def save_processed(df, path):
    df.to_csv(path, index=False)


if __name__ == "__main__":
    df = load_data(DEFAULT_RAW_DATA_PATH)
    df = preprocess(df)
    ensure_parent_dir(DEFAULT_PROCESSED_DATA_PATH)
    save_processed(df, DEFAULT_PROCESSED_DATA_PATH)

    print("✅ Preprocessing done!")