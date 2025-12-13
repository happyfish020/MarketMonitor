# core/adapters/datasources/cn/market_sentiment_source.py
# -*- coding: utf-8 -*-

import os
import json
from typing import Dict, Any

import pandas as pd

from core.utils.logger import get_logger
from core.datasources.datasource_base import (
    DataSourceConfig,
    BaseDataSource,
)
from core.utils.ds_refresh import apply_refresh_cleanup
from core.utils.spot_store import get_spot_daily

LOG = get_logger("DS.Sentiment")


class MarketSentimentDataSource(BaseDataSource):
    """
    V12 市场情绪（宽度）数据源：
    - 依赖 SpotStore 全行情（zh_spot）
    - 统计当日市场宽度
    - 只输出当日 block（不做 history）
    """

    def __init__(self, config: DataSourceConfig):
        super().__init__(name="DS.Sentiment")

        self.config = config
        self.cache_root = config.cache_root
        self.history_root = config.history_root

        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        LOG.info(
            "[DS.Sentiment] Init: market=%s ds=%s cache_root=%s history_root=%s",
            config.market,
            config.ds_name,
            self.cache_root,
            self.history_root,
        )

    # ------------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        """
        V12 统一入口
        """
        cache_file = os.path.join(self.cache_root, f"sentiment_{trade_date}.json")

        # refresh 控制
        _ = apply_refresh_cleanup(
            refresh_mode=refresh_mode,
            cache_path=cache_file,
            history_path=None,
            spot_path=None,
        )

        # cache 命中
        if refresh_mode == "none" and os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                LOG.error("[DS.Sentiment] load cache error: %s", e)

        # 1) 获取全行情
        try:
            df: pd.DataFrame = get_spot_daily(trade_date, refresh_mode=refresh_mode)
        except Exception as e:
            LOG.error("[DS.Sentiment] get_spot_daily error: %s", e)
            return self._neutral_block(trade_date)

        if df is None or df.empty:
            LOG.error("[DS.Sentiment] Spot DF empty")
            return self._neutral_block(trade_date)

        if "涨跌幅" not in df.columns:
            LOG.error("[DS.Sentiment] '涨跌幅' column missing")
            return self._neutral_block(trade_date)

        # 2) 统计宽度
        adv = int((df["涨跌幅"] > 0).sum())
        dec = int((df["涨跌幅"] < 0).sum())
        flat = int((df["涨跌幅"] == 0).sum())

        limit_up = int((df["涨跌幅"] >= 9.9).sum())
        limit_down = int((df["涨跌幅"] <= -9.9).sum())

        total = adv + dec + flat
        adv_ratio = round(adv / total, 4) if total > 0 else 0.0

        block: Dict[str, Any] = {
            "trade_date": trade_date,
            "adv": adv,
            "dec": dec,
            "flat": flat,
            "limit_up": limit_up,
            "limit_down": limit_down,
            "adv_ratio": adv_ratio,
        }

        # 写 cache
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
        except Exception as e:
            LOG.error("[DS.Sentiment] save cache error: %s", e)

        return block

    # ------------------------------------------------------------
    def _neutral_block(self, trade_date: str) -> Dict[str, Any]:
        return {
            "trade_date": trade_date,
            "adv": 0,
            "dec": 0,
            "flat": 0,
            "limit_up": 0,
            "limit_down": 0,
            "adv_ratio": 0.0,
        }
