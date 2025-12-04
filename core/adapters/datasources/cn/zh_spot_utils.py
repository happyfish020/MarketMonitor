
import pandas as pd

COL_MAP = {
    "代码": "symbol",
    "名称": "name",
    "最新价": "price",
    "昨收": "pre_close",
    "涨跌额": "change",
    "涨跌幅": "pct",
    "成交量": "volume",
    "成交额": "amount",
}

def normalize_zh_spot_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df2 = df.copy()
    rename = {c: COL_MAP[c] for c in df2.columns if c in COL_MAP}
    df2 = df2.rename(columns=rename)
    if "pct" in df2.columns:
        try:
            df2["pct"] = df2["pct"].astype(float) / 100.0
        except:
            pass
    return df2
