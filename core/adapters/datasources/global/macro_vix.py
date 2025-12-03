# core/adapters/datasources/global/macro_vix.py
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional, Dict, Any

from core.utils.logger import log
from core.adapters.cache.symbol_cache import (
    load_symbol_daily,
    save_symbol_daily,
)

VIX_SYMBOL = "^VIX"


def get_vix_daily(trade_date: date) -> Optional[Dict[str, Any]]:
    """
    VIX 日级数据：
    - market="global", kind="macro"
    - 所有 CN / US 的因子都共用这一份缓存
    """
    # 1) 先读缓存
    cached = load_symbol_daily("global", trade_date, VIX_SYMBOL, kind="macro")
    if cached is not None:
        return cached

    # 2) 缓存 miss -> 调 yfinance
    try:
        import yfinance as yf
    except ImportError:
        log("[VIX] yfinance 未安装，无法获取 VIX 数据")
        return None

    try:
        start = trade_date - timedelta(days=15)
        end = trade_date + timedelta(days=1)

        df = yf.download(
            VIX_SYMBOL,
            start=start.isoformat(),
            end=end.isoformat(),
            progress=False,
            auto_adjust=False,
        )
        if df.empty:
            log(f"[VIX] {VIX_SYMBOL} 在 {trade_date} 附近无数据")
            return None

        idx_dates = [idx.date() for idx in df.index]

        row = None
        row_pos = None
        for i, d in enumerate(idx_dates):
            if d == trade_date:
                row = df.iloc[i : i + 1]
                row_pos = i
                break

        if row is None:
            fallback_idx = None
            for i, d in enumerate(idx_dates):
                if d < trade_date:
                    if fallback_idx is None or d > idx_dates[fallback_idx]:
                        fallback_idx = i
            if fallback_idx is not None:
                row = df.iloc[fallback_idx : fallback_idx + 1]
                row_pos = fallback_idx
                log(
                    f"[VIX] 未找到 {trade_date} 精确数据，使用最近交易日 {idx_dates[fallback_idx]} 兜底"
                )
            else:
                row = df.iloc[0:1]
                row_pos = 0
                log(
                    f"[VIX] 所有记录都在 {trade_date} 之后，使用最早记录 {idx_dates[0]} 兜底"
                )

        close = float(row["Close"].values[0])

        if row_pos == 0:
            prev_close = close
        else:
            prev_row = df.iloc[row_pos - 1 : row_pos]
            prev_close = float(prev_row["Close"].values[0])

        pct_change = 0.0
        if prev_close != 0:
            pct_change = (close - prev_close) / prev_close * 100.0

        data = {
            "symbol": VIX_SYMBOL,
            "date": trade_date.isoformat(),
            "close": close,
            "prev_close": prev_close,
            "pct_change": pct_change,
        }

        # 写入 global 日级缓存
        save_symbol_daily("global", trade_date, VIX_SYMBOL, kind="macro", data=data)
        return data

    except Exception as e:
        log(f"[VIX] 获取 VIX 在 {trade_date} 数据失败: {e}")
        return None
