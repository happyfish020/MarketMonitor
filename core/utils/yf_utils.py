# core/utils/yf_utils.py

"""
UnifiedRisk V12
YF 工具层（底层数据访问，不属于 DataSource 层）

所有使用 YF 的 DataSource 应统一从这里调用：
  - fetch_yf_history(symbol, period, interval)
  - fetch_yf_latest(symbol)
"""

from typing import Optional
from datetime import datetime

import pandas as pd

from core.utils.logger import get_logger

LOG = get_logger("Utils.YF")


def _import_yf():
    try:
        import yfinance as yf  # type: ignore
        return yf
    except Exception as e:
        LOG.error("Import yfinance 失败，请安装 pip install yfinance | error=%s", e)
        return None


def fetch_yf_history(
    symbol: str,
    period: str = "6mo",
    interval: str = "1d",
) -> pd.DataFrame:
    """
    从 YF 获取历史数据，返回 DataFrame:
      columns: date, close, volume, pct
    """
    yf = _import_yf()
    if yf is None:
        return pd.DataFrame(columns=["date", "close", "volume", "pct"])

    LOG.info("YF.History: symbol=%s period=%s interval=%s", symbol, period, interval)

    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=period, interval=interval)
    except Exception as e:
        LOG.error("YF.History 获取失败: symbol=%s error=%s", symbol, e)
        return pd.DataFrame(columns=["date", "close", "volume", "pct"])

    if df is None or df.empty:
        LOG.error("YF.History 返回空: symbol=%s", symbol)
        return pd.DataFrame(columns=["date", "close", "volume", "pct"])

    df = df.reset_index()
    # 兼容 DatetimeIndex / 'Date' 列
    if "Date" in df.columns:
        df["date"] = pd.to_datetime(df["Date"])
    elif "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    elif "Datetime" in df.columns:
        df["date"] = pd.to_datetime(df["Datetime"])
    else:
        # 尝试用 index
        df["date"] = pd.to_datetime(df.index)

    df["close"] = df["Close"].astype(float)
    df["volume"] = df.get("Volume", 0.0).fillna(0.0).astype(float)
    df = df[["date", "close", "volume"]].sort_values("date").reset_index(drop=True)

    # 计算涨跌幅（百分比）
    df["pct"] = df["close"].pct_change() * 100.0

    LOG.info("YF.History 成功: symbol=%s rows=%s", symbol, len(df))
    return df


def fetch_yf_latest(symbol: str) -> Optional[dict]:
    """
    获取最新一根 K 线的数据（收盘价 & 涨跌幅）
    返回:
      {"close": float, "pct": float} 或 None
    """
    df = fetch_yf_history(symbol, period="5d", interval="1d")
    if df is None or df.empty:
        LOG.warning("YF.Latest 无数据: symbol=%s", symbol)
        return None

    last = df.iloc[-1]
    close = float(last["close"])
    pct = float(last.get("pct", 0.0))

    LOG.info("YF.Latest: symbol=%s close=%.3f pct=%.2f", symbol, close, pct)
    return {"close": close, "pct": pct}
