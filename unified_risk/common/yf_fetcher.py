
"""
统一的 yfinance 安全数据抓取模块（支持周末/假日自动回退）。
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
import yfinance as yf

from unified_risk.common.logging_utils import log_warning

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:  # pragma: no cover
    from pytz import timezone as ZoneInfo

BJ_TZ = ZoneInfo("Asia/Shanghai")


def get_last_valid_bar(
    symbol: str,
    lookback_days: int = 10,
    interval: str = "1d",
    as_of: Optional[datetime] = None,
) -> Optional[Tuple[pd.Timestamp, float, Optional[float]]]:
    """从 yfinance 返回最近一个有收盘价的交易日。"""
    if as_of is None:
        as_of = datetime.now(BJ_TZ)

    try:
        df = yf.download(
            symbol,
            period=f"{lookback_days}d",
            interval=interval,
            auto_adjust=False,
            progress=False,
        )

        if df is None or df.empty:
            log_warning(f"[YF] {symbol}: empty df")
            return None

        if isinstance(df.columns, pd.MultiIndex):
            try:
                close = df[("Close", symbol)]
            except Exception:
                close = df.xs("Close", axis=1, level=0)
        else:
            close = df.get("Close") or df.get("close")

        if close is None:
            log_warning(f"[YF] {symbol}: no Close column")
            return None

        close = close.dropna()
        if close.empty:
            log_warning(f"[YF] {symbol}: all close NaN")
            return None

        volume = None
        try:
            if isinstance(df.columns, pd.MultiIndex):
                try:
                    volume = df[("Volume", symbol)]
                except Exception:
                    volume = df.xs("Volume", axis=1, level=0)
            else:
                volume = df.get("Volume") or df.get("volume")
            if volume is not None:
                volume = volume.reindex(close.index).fillna(0)
        except Exception:
            volume = None

        valid_idx = close.index[-1]
        last_close = float(close.iloc[-1])
        last_vol = float(volume.iloc[-1]) if volume is not None else None

        return valid_idx, last_close, last_vol

    except Exception as e:
        log_warning(f"[YF] get_last_valid_bar failed for {symbol}: {e}")
        return None

def safe_yf_last_bars(symbol: str,
                      lookback_days: int = 10,
                      interval: str = "1d",
                      min_points: int = 2):
    """
    统一 YF fallback：
    - 使用 5～10 天数据窗口
    - 自动去除 NaN / 假期
    - 自动找到最近两个有效交易日
    - 自动计算涨跌幅
    - 返回结构：
        {
          "last": float,
          "prev": float,
          "changePct": float,
          "bars": DataFrame
        }
    """
    try:
        df = yf.download(
            symbol,
            period=f"{lookback_days}d",
            interval=interval,
            progress=False,
            auto_adjust=False,
        )
    except Exception as e:
        log_warning(f"[YF] {symbol}: download error: {e}")
        return None

    if df is None or df.empty:
        log_warning(f"[YF] {symbol}: no data in {lookback_days}d")
        return None

    df = df.dropna()
    if len(df) < min_points:
        log_warning(f"[YF] {symbol}: not enough bars ({len(df)})")
        return None

    # 最近两个交易日
    last = float(df["Close"].iloc[-1])
    prev = float(df["Close"].iloc[-2])
    pct = (last - prev) / prev * 100 if prev != 0 else 0

    return {
        "last": last,
        "prev": prev,
        "changePct": pct,
        "bars": df,
    }
 
def get_gold_price(as_of: Optional[datetime] = None) -> float:
    """安全获取 GC=F 最近有效交易日收盘价。"""
    bar = get_last_valid_bar("GC=F", lookback_days=15, interval="1d", as_of=as_of)
    if bar is None:
        log_warning("[YF] GC=F no valid bar")
        return 0.0
    _, close, _ = bar
    return close
