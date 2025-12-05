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


from datetime import date, timedelta
from typing import Optional, Dict, Any

import time
import yfinance as yf

from core.utils.logger import log
from core.adapters.cache.symbol_cache import (
    load_symbol_daily,
    save_symbol_daily,
)

# 已有：get_etf_daily(...) 保持不变
# =====================================
# 在 get_etf_daily 之后新增这个函数
# =====================================

def get_macro_daily(symbol: str, trade_date: date) -> Optional[Dict[str, Any]]:
    """
    Global / Macro 日级数据统一入口（给 global_lead、VIX 等用）：
    - 使用 symbol_cache (market="global", kind="macro")
    - 带 3 次重试 + sleep(10) + timeout=20
    - 返回结构：
        {
            "symbol": symbol,
            "date": "YYYY-MM-DD",
            "close": float,
            "prev_close": float,
            "pct_change": float,
        }
    """
    # 1) 先读 symbol 缓存
    cached = load_symbol_daily("global", trade_date, symbol, kind="macro")
    if cached is not None:
        return cached

    # 2) 缓存 miss → 使用 yfinance 下载（带 retry）
    RETRY = 3
    WAIT = 10

    for i in range(1, RETRY + 1):
        try:
            start = trade_date - timedelta(days=15)
            end = trade_date + timedelta(days=1)

            df = yf.download(
                symbol,
                start=start.isoformat(),
                end=end.isoformat(),
                progress=False,
                auto_adjust=False,
                timeout=20,  # read_timeout = 20
            )

            if df is None or df.empty:
                log(f"[YF] {symbol} 在 {trade_date} 无数据 (empty df)")
                return None

            idx_dates = [idx.date() for idx in df.index]

            # 精确匹配 T
            row = None
            row_pos = None
            for pos, d in enumerate(idx_dates):
                if d == trade_date:
                    row = df.iloc[pos:pos+1]
                    row_pos = pos
                    break

            # fallback：最近 < T 的交易日
            if row is None:
                fallback_pos = None
                for pos, d in enumerate(idx_dates):
                    if d < trade_date:
                        if fallback_pos is None or d > idx_dates[fallback_pos]:
                            fallback_pos = pos

                if fallback_pos is not None:
                    row = df.iloc[fallback_pos:fallback_pos+1]
                    row_pos = fallback_pos
                    log(f"[YF] {symbol} 使用最近交易日 {idx_dates[fallback_pos]} 兜底")
                else:
                    # 全部 > T，就用第一条
                    row = df.iloc[0:1]
                    row_pos = 0
                    log(f"[YF] {symbol} 使用最早记录 {idx_dates[0]} 兜底")

            close = float(row["Close"].values[0])

            if row_pos == 0:
                prev_close = close
            else:
                prev_row = df.iloc[row_pos - 1:row_pos]
                prev_close = float(prev_row["Close"].values[0])

            pct_change = 0.0 if prev_close == 0 else (close - prev_close) / prev_close

            data = {
                "symbol": symbol,
                "date": trade_date.isoformat(),
                "close": close,
                "prev_close": prev_close,
                "pct_change": pct_change,
            }

            # 写入 symbol 缓存（global / macro）
            save_symbol_daily("global", trade_date, symbol, kind="macro", data=data)
            return data

        except Exception as e:
            log(f"[YF] 获取 {symbol} 在 {trade_date} 数据失败 (try {i}/{RETRY}): {e}")
            if i < RETRY:
                time.sleep(WAIT)
            else:
                log(f"[YF] 获取 {symbol} 在 {trade_date} 数据最终失败（已重试 {RETRY} 次）")
                return None



# =====================================
# 新增：中国指数日级别数据 get_index_daily
# 与 get_etf_daily 完全相同逻辑，仅 kind="index"
# =====================================

def get_index_daily(symbol: str, trade_date: date) -> Optional[Dict[str, Any]]:
    """
    CN 指数日级数据（与 get_etf_daily 完全一致的缓存 + fallback + pct 逻辑）
    - market="cn", kind="index"
    - 优先读缓存
    - 缓存 miss 则调用 yfinance
    """
    # === 1) 读取缓存（统一路径规范） ===
    cached = load_symbol_daily("cn", trade_date, symbol, kind="index")
    if cached is not None:
        return cached

    # === 2) 调用 yfinance ===
    try:
        import yfinance as yf
    except ImportError:
        log(f"[YF] yfinance 未安装，无法获取指数 {symbol} 数据")
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

        if df is None or df.empty:
            log(f"[YF] 指数 {symbol} 在 {trade_date} 附近无数据（empty）")
            return None

        idx_dates = [idx.date() for idx in df.index]

        # === 3) 寻找 trade_date 精确行 ===
        row = None
        row_pos = None
        for i, d in enumerate(idx_dates):
            if d == trade_date:
                row = df.iloc[i:i+1]
                row_pos = i
                break

        # === fallback：用最近 T-1、T-2、… ===
        if row is None:
            fallback = None
            for i, d in enumerate(idx_dates):
                if d < trade_date:
                    if fallback is None or d > idx_dates[fallback]:
                        fallback = i
            if fallback is not None:
                log(f"[YF] 指数 {symbol} 未找到 {trade_date}，使用最近交易日 {idx_dates[fallback]}")
                row = df.iloc[fallback:fallback+1]
                row_pos = fallback
            else:
                # 如果所有数据都在 T 之后，则用第一条记录兜底
                log(f"[YF] 指数 {symbol} 所有记录都在 {trade_date} 之后，使用最早 {idx_dates[0]}")
                row = df.iloc[0:1]
                row_pos = 0

        close = float(row["Close"].values[0])

        # === 4) prev_close ===
        if row_pos == 0:
            prev_close = close
        else:
            prev_close = float(df.iloc[row_pos - 1:row_pos]["Close"].values[0])

        pct_change = 0.0 if prev_close == 0 else (close - prev_close) / prev_close * 100.0

        data = {
            "symbol": symbol,
            "date": trade_date.isoformat(),
            "close": close,
            "prev_close": prev_close,
            "volume": float(row["Volume"].values[0]) if "Volume" in row else 0.0,
            "pct_change": pct_change,
        }

        # === 5) 写入缓存 ===
        save_symbol_daily("cn", trade_date, symbol, kind="index", data=data)
        return data

    except Exception as e:
        log(f"[YF] 获取指数 {symbol} 在 {trade_date} 数据失败: {e}")
        return None