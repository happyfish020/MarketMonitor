# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import time
from typing import List, Dict, Any
from datetime import datetime

from core.adapters.datasources.base import BaseDataSource
from core.adapters.cache.file_cache import load_json, save_json
from core.adapters.cache.symbol_cache import _normalize_symbol
from core.utils.datasource_config import DataSourceConfig
from core.utils.logger import get_logger
from core.utils.yf_utils import fetch_yf_history   # ⭐ 关键：北代使用的工具函数（你现有的）

LOG = get_logger("DS.ETFSeries")

CACHE_TTL = 60 * 10   # 10 minutes


class ETFSeriesSource(BaseDataSource):
    """
    UnifiedRisk V12 - ETF 日级序列数据源
    * 使用 fetch_yf_history（与 north_nps 完全一致的工具）
    * 自动 cache + history
    * 所有 ETF 数据序列统一管理（成交量、收盘价）
    """

    def __init__(self):
        super().__init__("ETFSeriesSource")

        self.config = DataSourceConfig(market="cn", ds_name="etf")
        self.config.ensure_dirs()

        self.cache_root = self.config.cache_root
        self.history_root = self.config.history_root

    # -------------------------------------------------------
    # 文件路径
    # -------------------------------------------------------

    def _cache_path(self, symbol: str):
        name = _normalize_symbol(symbol)
        return os.path.join(self.cache_root, f"{name}_today.json")

    def _history_path(self, symbol: str):
        name = _normalize_symbol(symbol)
        return os.path.join(self.history_root, f"{name}.json")

    # -------------------------------------------------------
    # Cache I/O
    # -------------------------------------------------------

    def _load_cache(self, symbol: str) -> Dict[str, Any]:
        path = self._cache_path(symbol)
        data = load_json(path)
        if not data:
            return {}

        ts = data.get("ts", 0)
        if time.time() - ts > CACHE_TTL:
            LOG.info("[ETFSeries] cache expired → symbol=%s", symbol)
            return {}

        LOG.info("[ETFSeries] 使用 cache: %s", path)
        return data

    def _save_cache(self, symbol: str, latest: Dict[str, Any], series: List[Dict[str, Any]]):
        path = self._cache_path(symbol)
        save_json(path, {
            "ts": time.time(),
            "symbol": symbol,
            "latest": latest,
            "series": series
        })
        LOG.info("[ETFSeries] 写入 cache: %s", path)

    # -------------------------------------------------------
    # History I/O
    # -------------------------------------------------------

    def _load_history(self, symbol: str) -> List[Dict[str, Any]]:
        return load_json(self._history_path(symbol)) or []

    def _save_history(self, symbol: str, series: List[Dict[str, Any]]):
        save_json(self._history_path(symbol), series)
        LOG.info("[ETFSeries] 保存 history: %s rows=%d",
                 self._history_path(symbol), len(series))

    # -------------------------------------------------------
    # 主方法：获取 ETF 序列（含 cache + history）
    # -------------------------------------------------------

    def get_series(self, symbol: str, refresh: bool = False) -> List[Dict[str, Any]]:
        LOG.info("[ETFSeries] 请求 ETF=%s refresh=%s", symbol, refresh)

        # 1) 缓存优先
        if not refresh:
            cache = self._load_cache(symbol)
            if cache:
                return cache.get("series", [])

        # 2) 获取最新一段历史（北代的 fetch_yf_history）
        df = fetch_yf_history(symbol, period="6mo", interval="1d")
        if df is None or df.empty:
            LOG.error("[ETFSeries] YF 获取失败 → 使用历史: %s", symbol)
            return self._load_history(symbol)

        # 转成 series 格式
        latest_series = [
            {
                "date": r["date"].strftime("%Y-%m-%d"),
                "close": float(r["close"]),
                "volume": float(r["volume"]),
            }
            for idx, r in df.iterrows()
        ]

        # 3) 合并历史
        history = self._load_history(symbol)

        existed = {x["date"] for x in history}
        for row in latest_series:
            if row["date"] not in existed:
                history.append(row)

        history = sorted(history, key=lambda x: x["date"])
        history = history[-400:]   # 最多 400 条

        # 4) 最新一行
        latest = history[-1] if history else {}

        # 5) 写入 history + cache
        self._save_history(symbol, history)
        self._save_cache(symbol, latest, history)

        return history
