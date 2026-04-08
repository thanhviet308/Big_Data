import pandas as pd

from src.utils.paths import env_or_repo_path

BLACKLIST_PATH = env_or_repo_path(
    "FRAUD_BLACKLIST_PATH",
    "data",
    "blacklist",
    "blacklist_accounts.csv",
)

def load_blacklist(path=BLACKLIST_PATH):
    df_blacklist = pd.read_csv(path)
    return set(df_blacklist["account"].astype(str).tolist())

def apply_rules(df, blacklist_accounts):
    result = df.copy()

    # Rule 1: tài khoản nhận nằm trong blacklist
    result["rule_blacklist"] = result["nameDest"].astype(str).isin(blacklist_accounts).astype(int)

    # Rule 2: giao dịch lớn
    result["rule_large_amount"] = (result["amount"] > 200000).astype(int)

    # Rule 3: loại giao dịch rủi ro
    result["rule_risky_type"] = result["type"].isin(["TRANSFER", "CASH_OUT"]).astype(int)

    # Rule 4: kết hợp nhiều điều kiện
    result["rule_high_risk"] = (
        (result["rule_large_amount"] == 1) &
        (result["rule_risky_type"] == 1)
    ).astype(int)

    # Flag cuối cùng nếu dính ít nhất 1 rule
    result["rule_flag"] = (
        (result["rule_blacklist"] == 1) |
        (result["rule_high_risk"] == 1)
    ).astype(int)

    return result

if __name__ == "__main__":
    blacklist = load_blacklist()

    sample_data = pd.DataFrame([
        {
            "type": "TRANSFER",
            "amount": 500000,
            "nameOrig": "C123",
            "nameDest": "M1979787155"
        },
        {
            "type": "PAYMENT",
            "amount": 5000,
            "nameOrig": "C456",
            "nameDest": "M9999999999"
        }
    ])

    result = apply_rules(sample_data, blacklist)
    print(result)