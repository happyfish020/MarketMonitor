# core/adapters/datasources/cn/yf_client_cn.py
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional, Dict, Any

from core.utils.logger import log
from core.adapters.cache.symbol_cache import (
    load_symbol_daily,
    save_symbol_daily,
)


def get_etf_daily(symbol: str, trade_date: date) -> Optional[Dict[str, Any]]:
    """
    CN ETF 日级数据：
    - 优先使用 symbol 级缓存（market="cn", kind="etf"）
    - 缓存 miss 时才调用 yfinance
    """
    # 1) 尝试读取缓存
    cached = load_symbol_daily("cn", trade_date, symbol, kind="etf")
    if cached is not None:
        return cached

    # 2) 缓存 miss -> 调用 yfinance 获取
    try:
        import yfinance as yf
    except ImportError:
        log(f"[YF] yfinance 未安装，无法获取 {symbol} 数据")
        return None

    try:
        start = trade_date - timedelta(days=15)
        end = trade_date + timedelta(days=1)

        df = yf.download(
            symbol,
            start=start.isoformat(),
            end=end.isoformat(),
            progress=False,
            auto_adjust=False,
        )
        if df.empty:
            log(f"[YF] {symbol} 在 {trade_date} 附近无数据")
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
                    f"[YF] {symbol} 未找到 {trade_date} 精确数据，"
                    f"使用最近交易日 {idx_dates[fallback_idx]} 兜底"
                )
            else:
                row = df.iloc[0:1]
                row_pos = 0
                log(
                    f"[YF] {symbol} 所有记录都在 {trade_date} 之后，"
                    f"使用最早记录 {idx_dates[0]} 兜底"
                )

        close = float(row["Close"].values[0])
        volume = float(row["Volume"].values[0])

        if row_pos == 0:
            prev_close = close
        else:
            prev_row = df.iloc[row_pos - 1 : row_pos]
            prev_close = float(prev_row["Close"].values[0])

        pct_change = 0.0
        if prev_close != 0:
            pct_change = (close - prev_close) / prev_close * 100.0

        data = {
            "symbol": symbol,
            "date": trade_date.isoformat(),
            "close": close,
            "prev_close": prev_close,
            "volume": volume,
            "pct_change": pct_change,
        }

        # 3) 写入 symbol 缓存
        save_symbol_daily("cn", trade_date, symbol, kind="etf", data=data)
        return data

    except Exception as e:
        log(f"[YF] 获取 {symbol} 在 {trade_date} 数据失败: {e}")
        return None
