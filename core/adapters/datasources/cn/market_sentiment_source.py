# core/adapters/datasources/cn/market_sentiment_source.py
# -*- coding: utf-8 -*-
# UnifiedRisk V12 - MarketSentimentDataSource (基于 zh_spot 的松耦合实现)

from __future__ import annotations
import os
import time
from typing import Dict, Any, List

import pandas as pd

from core.adapters.datasources.base import BaseDataSource
from core.adapters.cache.file_cache import load_json, save_json
from core.utils.datasource_config import DataSourceConfig
from core.utils.logger import get_logger
from core.utils.spot_store import get_spot_daily

LOG = get_logger("DS.Sentiment")


class MarketSentimentDataSource(BaseDataSource):

    def __init__(self, trade_date:str):
        super().__init__("MarketSentimentDataSource")

        self.config = DataSourceConfig(market="cn", ds_name="sentiment")
        self.config.ensure_dirs()

        self.cache_path = os.path.join(self.config.cache_root, "sentiment_today.json")
        self.hist_path = os.path.join(self.config.history_root, "sentiment_series.json")

        LOG.info(f"[DS.Margin] Init: cache={self.config.cache_root}, history={self.config.history_root}")
        self.trade_date = trade_date
        

        LOG.info(
            f"[DS.Sentiment] Init OK: cache={self.config.cache_root}, "
            f"history={self.config.history_root}"
        )
        LOG.info("Init: Trade_date%s", self.trade_date)

    # ---------- cache ----------
    def _load_cache(self):
        data = load_json(self.cache_path)
        if not data:
            return None
        if time.time() - data.get("ts", 0) > 600:
            return None
        LOG.info("[DS.Sentiment] Hit cache")
        return data.get("data")

    def _save_cache(self, block: Dict[str, Any]):
        save_json(self.cache_path, {"ts": time.time(), "data": block})
        LOG.info("[DS.Sentiment] Save cache")

    # ---------- history ----------
    def _load_history(self) -> List[Dict[str, Any]]:
        hist = load_json(self.hist_path)
        return hist if isinstance(hist, list) else []

    def _save_history(self, series: List[Dict[str, Any]]):
        series = sorted(series, key=lambda x: x["date"])[-400:]
        save_json(self.hist_path, series)
        LOG.info(f"[DS.Sentiment] Save history rows={len(series)}")

    # ---------- main ----------
    def get_sentiment_block(self, refresh_mode: str) -> Dict[str, Any]:
        """
        对外主接口：
            - 内部使用 SpotStore 获取全行情 df
            - 统计 adv/dec/flat/涨停/跌停 等宽度结构
            - 维护 history（仅结构指标）
            - 情绪评分留给 MarketSentimentFactor
        """

        # 1) cache
        if refresh_mode !="snatpshot" and refresh_mode !="full":
            cached = self._load_cache()
            if isinstance(cached, dict):
                return cached

        # 2) spot df
        df: pd.DataFrame = get_spot_daily(self.trade_date, refresh_mode)

        # pct 列名兼容
        if "涨跌幅" in df.columns:
            pct_col = "涨跌幅"
        elif "pct_chg" in df.columns:
            pct_col = "pct_chg"
        else:
            raise ValueError("[DS.Sentiment] spot df 缺少 涨跌幅/pct_chg 字段")

        adv = int((df[pct_col] > 0).sum())
        dec = int((df[pct_col] < 0).sum())
        flat = int((df[pct_col] == 0).sum())
        limit_up = int((df[pct_col] >= 9.8).sum())
        limit_down = int((df[pct_col] <= -9.8).sum())
        total = len(df)
        adv_ratio = float(adv / total) if total > 0 else 0.0

        today = {
            "date": self.trade_date,
            "adv": adv,
            "dec": dec,
            "flat": flat,
            "limit_up": limit_up,
            "limit_down": limit_down,
            "adv_ratio": adv_ratio,
        }

        # 3) history
        hist = self._load_history()
        existed = {x["date"] for x in hist}
        if self.trade_date not in existed:
            hist.append(today)

        hist = sorted(hist, key=lambda x: x["date"])
        self._save_history(hist)

        block = {
            "adv": adv,
            "dec": dec,
            "flat": flat,
            "limit_up": limit_up,
            "limit_down": limit_down,
            "adv_ratio": adv_ratio,
            "series": hist,
        }

        self._save_cache(block)
        LOG.info(
            f"[DS.Sentiment] {self.trade_date} adv={adv} dec={dec} "
            f"up={limit_up} down={limit_down} adv_ratio={adv_ratio:.2f}"
        )

        return block
