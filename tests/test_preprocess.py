import pandas as pd

from src.preprocessing.preprocess_data import preprocess


def test_preprocess_cleans_and_fills():
    df = pd.DataFrame(
        {
            "type": [" transfer ", None, "cash out"],
            "amount": ["1000", None, 5000],
            "oldbalanceOrg": [100, 200, None],
            "newbalanceOrig": [50, None, 10],
            "oldbalanceDest": [0, 0, 0],
            "newbalanceDest": [10, None, 20],
            "isFraud": [0, 1, 0],
        }
    )

    out = preprocess(df)

    assert len(out) == 3
    assert out["type"].tolist() == ["TRANSFER", "UNKNOWN", "CASH_OUT"]
    assert out[["amount", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest"]].isna().sum().sum() == 0
    assert out["isFraud"].dtype.kind in ("i", "u")
