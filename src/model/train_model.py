import os
import joblib
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

from src.model.predict_model import FEATURE_COLUMNS, prepare_model_input

from src.utils.paths import env_or_repo_path, ensure_parent_dir

DATA_PATH = env_or_repo_path(
    "FRAUD_PROCESSED_DATA_PATH",
    "data",
    "processed",
    "processed_paysim.csv",
)

MODEL_PATH = env_or_repo_path(
    "FRAUD_MODEL_PATH",
    "src",
    "model",
    "saved_model",
    "fraud_model.pkl",
)

def main():
    df = pd.read_csv(DATA_PATH)
    X = prepare_model_input(df)
    y = pd.to_numeric(df["isFraud"], errors="coerce").fillna(0).astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = RandomForestClassifier(
        n_estimators=50,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    # AUC needs probabilities; handle edge cases safely.
    try:
        y_proba = model.predict_proba(X_test)[:, 1]
        auc = roc_auc_score(y_test, y_proba)
    except Exception:
        auc = None

    print("Accuracy:", accuracy_score(y_test, y_pred))
    print("Precision:", precision_score(y_test, y_pred, zero_division=0))
    print("Recall:", recall_score(y_test, y_pred, zero_division=0))
    print("F1-score:", f1_score(y_test, y_pred, zero_division=0))
    if auc is not None:
        print("AUC:", auc)
    print("\nClassification Report:\n")
    print(classification_report(y_test, y_pred, zero_division=0))

    ensure_parent_dir(MODEL_PATH)
    joblib.dump(model, MODEL_PATH)
    print(f"\n✅ Model saved at: {MODEL_PATH}")

if __name__ == "__main__":
    main()