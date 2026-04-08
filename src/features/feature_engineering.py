def create_features(df):
    df["balanceDiffOrig"] = df["oldbalanceOrg"] - df["newbalanceOrig"]
    df["balanceDiffDest"] = df["newbalanceDest"] - df["oldbalanceDest"]
    df["isLargeAmount"] = (df["amount"] > 200000).astype(int)
    return df