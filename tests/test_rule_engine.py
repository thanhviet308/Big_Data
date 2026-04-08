import pandas as pd

from src.rules.rule_engine import apply_rules


def test_rule_engine_blacklist_and_risk():
    df = pd.DataFrame(
        [
            {"type": "TRANSFER", "amount": 500000, "nameDest": "BAD"},
            {"type": "PAYMENT", "amount": 1000, "nameDest": "OK"},
        ]
    )

    blacklist = {"BAD"}
    out = apply_rules(df, blacklist)

    assert out.loc[0, "rule_blacklist"] == 1
    assert out.loc[0, "rule_large_amount"] == 1
    assert out.loc[0, "rule_risky_type"] == 1
    assert out.loc[0, "rule_flag"] == 1

    assert out.loc[1, "rule_flag"] == 0
