# -*- coding: utf-8 -*-
"""
UnifiedRisk v4.3.8 - AShareDailyEngine
-------------------------------------
桥接：
    DataFetcher  → 获取所有 raw 数据
    RiskScorer   → 计算所有因子 & 总分

engine.run() 返回：
{
    "meta": {...},
    "raw": {...},
    "score": {...}
}
"""

import logging
from datetime import datetime

from .data_fetcher import DataFetcher
from .risk_scorer import RiskScorer

LOG = logging.getLogger(__name__)


class AShareDailyEngine:
    """
    A 股日级别整体调度器：
        fetcher → scorer → 汇总输出
    """

    def __init__(self):
        self.fetcher = DataFetcher()

    # ------------------------------------------------------------
    # 主入口
    # ------------------------------------------------------------
    def run(self, date=None):
        """
        返回结构：
        {
            "meta": {...},
            "raw": {...},
            "score": {...}
        }
        """
        LOG.info("AShareDailyEngine v4.3.8 running ...")

        # 1) 获取所有原始数据
        raw = self.fetcher.fetch_daily_snapshot(date)

        # 2) 计算所有因子
        scorer = RiskScorer(raw)
        score = scorer.score_all()

        # 3) 元信息
        meta = {
            "version": "UnifiedRisk_v4.3.8",
            "run_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "date": raw.get("meta", {}).get("date"),
            "bj_time": raw.get("meta", {}).get("bj_time"),
        }

        return {
            "meta": meta,
            "raw": raw,
            "score": score,
        }

