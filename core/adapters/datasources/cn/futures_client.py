# -*- coding: utf-8 -*-
"""
FuturesClient (UnifiedRisk V11.7 FINAL)
---------------------------------------
股指期货主力合约数据源（IF/IH/IM） + 基差（basis）计算。

符合规范：
1) 不写 datasource 级 JSON
2) 单标行情写入 symbolcache (kind="futures")
3) 不使用 akshare → 改用 yfinance (稳定 + 可重试)
4) fetch() 返回 dict → 由 fetcher 写入 snapshot
5) 日志统一 log("[Futures] ...")
"""

from __future__ import annotations

from datetime import date as Date, timedelta
from typing import Dict, Any, Optional

import time
import yfinance as yf

from core.utils.logger import log
from core.adapters.cache.symbol_cache import (
    load_symbol_daily,
    save_symbol_daily,
)

# --------------------------------------------------------
# 配置：期货符号 → yfinance 主力连续合约
# --------------------------------------------------------
FUTURES_SYMBOL_MAP = {
    "if": "IF00.CFE",   # 沪深300主力连续
    "ih": "IH00.CFE",   # 上证50主力连续
    "im": "IM00.CFE",   # 中证500主力连续
}

# --------------------------------------------------------
# 对应指数（使用 index_series 已覆盖）
# --------------------------------------------------------
INDEX_YF_MAP = {
    "if": "000300.SS",   # 沪深300
    "ih": "000016.SS",   # 上证50
    "im": "000905.SS",   # 中证500
}


class FuturesClient:
    """股指期货行情数据源（期货 + 指数 + 基差）"""

    def _fetch_yf_last(
        self, yf_symbol: str, trade_date: Date
    ) -> Optional[Dict[str, Any]]:
        """
        通用逻辑：获取最后一条行情（含 retry + symbolcache）
        返回：{close, prev_close, pct_change}
        """
        # 1. 先检查 symbolcache
        cached = load_symbol_daily("cn", trade_date, yf_symbol, kind="futures")
        if cached is not None:
            return cached

        RETRY = 3
        WAIT = 10

        for i in range(1, RETRY + 1):
            try:
                start = trade_date - timedelta(days=20)
                end = trade_date + timedelta(days=1)

                df = yf.download(
                    yf_symbol,
                    start=start.isoformat(),
                    end=end.isoformat(),
                    progress=False,
                    auto_adjust=False,
                    timeout=20,
                )

                if df is None or df.empty:
                    log(f"[Futures] {yf_symbol} 无内容 (empty df)")
                    return None

                idx_dates = [idx.date() for idx in df.index]

                row = None
                pos = None
                for j, d in enumerate(idx_dates):
                    if d == trade_date:
                        row = df.iloc[j:j+1]
                        pos = j
                        break

                if row is None:
                    # fallback：最近 < T 的交易日
                    fallback_pos = None
                    for j, d in enumerate(idx_dates):
                        if d < trade_date:
                            if fallback_pos is None or d > idx_dates[fallback_pos]:
                                fallback_pos = j

                    if fallback_pos is not None:
                        row = df.iloc[fallback_pos:fallback_pos+1]
                        pos = fallback_pos
                        log(f"[Futures] {yf_symbol} 使用 fallback {idx_dates[fallback_pos]}")
                    else:
                        # fallback：只剩第一条
                        row = df.iloc[0:1]
                        pos = 0
                        log(f"[Futures] {yf_symbol} 使用最早记录 {idx_dates[0]}")

                close = float(row["Close"].values[0])
                if pos == 0:
                    prev_close = close
                else:
                    prev_close = float(df.iloc[pos - 1]["Close"])

                pct_change = (
                    0.0 if prev_close == 0 else (close - prev_close) / prev_close
                )

                data = {
                    "symbol": yf_symbol,
                    "date": trade_date.isoformat(),
                    "close": close,
                    "prev_close": prev_close,
                    "pct_change": pct_change,
                }

                save_symbol_daily("cn", trade_date, yf_symbol, kind="futures", data=data)
                return data

            except Exception as e:
                log(f"[Futures] 获取 {yf_symbol} 失败 try {i}/3: {e}")
                time.sleep(WAIT)

        log(f"[Futures] {yf_symbol} 获取失败（已重试 3 次）")
        return None

    # --------------------------------------------------------
    # 主函数：返回 { if: {...}, ih: {...}, im: {...} }
    # --------------------------------------------------------
    def fetch(self, trade_date: Date) -> Dict[str, Any]:
        log(f"[Futures] Fetch start → {trade_date}")

        result: Dict[str, Any] = {}

        for key, fut_symbol in FUTURES_SYMBOL_MAP.items():
            idx_symbol = INDEX_YF_MAP.get(key)
            if not idx_symbol:
                continue

            # 期货 T 日行情
            fut = self._fetch_yf_last(fut_symbol, trade_date)
            # 指数 T 日行情（用 yf_client_cn）
            # 统一复用 get_macro_daily，而非单独获取
            # → 避免重复逻辑，保证格式一致
            idx = self._fetch_yf_last(idx_symbol, trade_date)

            if not fut or not idx:
                result[key] = None
                continue

            basis_pct = (
                (fut["close"] / idx["close"] - 1.0) * 100.0
                if idx["close"] != 0 else None
            )

            result[key] = {
                "future_symbol": fut_symbol,
                "index_symbol": idx_symbol,
                "future_close": fut["close"],
                "index_close": idx["close"],
                "future_pct": fut["pct_change"],
                "index_pct": idx["pct_change"],
                "basis_pct": basis_pct,
            }

            log(
                f"[Futures] OK {key}: fut={fut['close']} "
                f"idx={idx['close']} basis={basis_pct:.3f}"
            )

        log(f"[Futures] Done → keys={list(result.keys())}")
        return result
