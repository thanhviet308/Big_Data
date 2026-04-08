import pandas as pd

from src.model.predict_model import prepare_model_input, FEATURE_COLUMNS


def test_prepare_model_input_has_expected_columns():
    df = pd.DataFrame(
        [
            {
                "type": " payment ",
                "amount": 10,
                "oldbalanceOrg": 5,
                "newbalanceOrig": 1,
                "oldbalanceDest": 0,
                "newbalanceDest": 4,
            }
        ]
    )

    X = prepare_model_input(df)
    assert list(X.columns) == FEATURE_COLUMNS
    assert X.shape == (1, len(FEATURE_COLUMNS))
