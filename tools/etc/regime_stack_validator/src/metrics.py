import pandas as pd

def compute_metrics(df, window):
    df = df.copy()

    df["ret"] = df["close"] / df["prev_close"] - 1
    df["adv"] = df["close"] > df["prev_close"]

    adv = df.groupby("date")["adv"].mean()
    median_ret = df.groupby("date")["ret"].median()

    df["rolling_low"] = df.groupby("code")["close"].transform(
        lambda x: x.rolling(window).min()
    )
    df["new_low"] = df["close"] == df["rolling_low"]

    new_low_ratio = df.groupby("date")["new_low"].mean()

    metrics = pd.DataFrame({
        "date": adv.index,
        "adv_ratio": adv.values,
        "median_return": median_ret.values,
        "new_low_ratio": new_low_ratio.values,
    })

    metrics["new_low_persistence"] = (
        metrics["new_low_ratio"] > 0
    ).astype(int).groupby(
        (metrics["new_low_ratio"] == 0).cumsum()
    ).cumsum()

    return metrics
