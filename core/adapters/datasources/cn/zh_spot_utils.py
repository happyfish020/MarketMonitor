
from datetime import date
from typing import Optional

import pandas as pd

from core.utils.logger import log


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


class ZhSpotUtils:
    """
    A 股当日现货行情（zh_spot）占位 Client。
    目前只返回空 DataFrame，确保不报错。
    后续你可以在这里接入 akshare / 东方财富行情。
    """

    def get_today_spot(self, trade_date: Optional[date] = None) -> pd.DataFrame:
        log(f"[ZhSpotUtils] get_today_spot() 暂未实现，返回空 DataFrame。trade_date={trade_date}")
        return pd.DataFrame()