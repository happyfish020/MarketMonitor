# core/adapters/block_builders/cn/trend_facts_blkbd.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
from typing import Any, Dict, List

from core.adapters.block_builder.block_builder_base import FactBlockBuilderBase
from core.utils.logger import get_logger
from core.utils.config_loader import load_paths

LOG = get_logger("BLKBD.TrendFacts")

 
class TrendFactsBlockBuilder(FactBlockBuilderBase):
    """
    TrendFactsBlockBuilder（冻结）

    职责：
    - 从 history 读取逐日原子事实
    - 生成“趋势事实（facts）”
    - 不做判断、不生成信号、不影响 Gate
    """
    def __init__(self):
        super().__init__(name="Trend_facts")
         
        _paths = load_paths()
        #self.base_dir = _paths.get("cn_history_dir", "data/cn/history")
        
    def _build_turnover_trend(self, snapshot: dict) -> dict | None:
        """
        从 snapshot['turnover_raw']['windows'] 中读取时间序列
        提取 total_turnover，计算成交额趋势指标
        后续计算逻辑与原代码完全一致
        """
        windows = snapshot.get('turnover_raw', {}).get('windows', [])
        assert windows, "snapsot - turnover_trend window is empty!"
        if not windows:
            return None

        records = [
            {
                "trade_date": item["trade_date"],
                "total": item.get("total_turnover")  # 键名改为 total_turnover
            }
            for item in windows
            if item.get("total_turnover") is not None
        ]

        if len(records) < 10:
            LOG.error("Turnover history records < 10")
            raise Exception("Turnover history records < 10")
            return None

        # 按日期排序（升序）
        records.sort(key=lambda x: x["trade_date"])

        totals: list[float] = [r["total"] for r in records]

        last = totals[-1]
        avg_5d = sum(totals[-5:]) / 5
        avg_10d = sum(totals[-10:]) / 10

        slope_5d = (totals[-1] - totals[-6]) / 5
        slope_10d = (totals[-1] - totals[-11]) / 10

        return {
            "last_value": round(last, 2),
            "avg_5d": round(avg_5d, 2),
            "avg_10d": round(avg_10d, 2),
            "ratio_vs_5d": round(last / avg_5d, 4) if avg_5d > 0 else None,
            "ratio_vs_10d": round(last / avg_10d, 4) if avg_10d > 0 else None,
            "slope_5d": round(slope_5d, 2),
            "slope_10d": round(slope_10d, 2),
        }
        
    def _build_turnover_trend_no(self, snapshot: dict) -> dict | None:
        """
        从 snapshot['turnover_raw']['windows'] 中读取时间序列
        提取 total_turnover，计算成交额趋势指标
        后续计算逻辑与原代码完全一致
        """
        windows = snapshot.get('turnover_raw', {}).get('windows', [])
        if not windows:
            return None

        records = [
            {
                "trade_date": item["trade_date"],
                "total": item.get("total_turnover")  # 键名改为 total_turnover
            }
            for item in windows
            if item.get("total_turnover") is not None
        ]

        if len(records) < 10:
            return None

        # 按日期排序（升序）
        records.sort(key=lambda x: x["trade_date"])

        totals: list[float] = [r["total"] for r in records]

        last = totals[-1]
        avg_5d = sum(totals[-5:]) / 5
        avg_10d = sum(totals[-10:]) / 10

        slope_5d = (totals[-1] - totals[-6]) / 5
        slope_10d = (totals[-1] - totals[-11]) / 10

        return {
            "last_value": round(last, 2),
            "avg_5d": round(avg_5d, 2),
            "avg_10d": round(avg_10d, 2),
            "ratio_vs_5d": round(last / avg_5d, 4) if avg_5d > 0 else None,
            "ratio_vs_10d": round(last / avg_10d, 4) if avg_10d > 0 else None,
            "slope_5d": round(slope_5d, 2),
            "slope_10d": round(slope_10d, 2),
        }
            
    # ------------------------------------------------------------
    def build_block(
        self,
        snapshot: Dict[str, Any],
        refresh_mode: str = "none",
    ) -> Dict[str, Any]:
        """
        当前仅实现：Turnover 趋势事实
        """
        facts: Dict[str, Any] = {}

        turnover_trend = self._build_turnover_trend(snapshot)
        assert turnover_trend, "turnover_trend is empty!"
        if turnover_trend:
            facts["turnover"] = turnover_trend
        #
        # Todo other  trend like market_sentiment....breadth trend
        #
        return facts

    # ------------------------------------------------------------
    # Turnover trend
    # ------------------------------------------------------------
    def _build_turnover_trend_no(self) -> Dict[str, Any] | None:
        """
        从 history/turnover/*.json 读取数据，计算均值类趋势事实
        """
         
        history_root = self.base_dir

        if not history_root:
            LOG.warning("[TrendFacts] history_dir not configured")
            return None

        turnover_dir = os.path.join(history_root, "turnover")
        if not os.path.isdir(turnover_dir):
            LOG.warning("[TrendFacts] turnover history dir missing: %s", turnover_dir)
            return None

        records = self._load_turnover_history(turnover_dir)
        if len(records) < 10:
            LOG.warning("[TrendFacts] insufficient turnover history: %d", len(records))
            return None

        # 按日期排序（升序）
        records.sort(key=lambda x: x["trade_date"])

        totals: List[float] = [r["total"] for r in records if r.get("total") is not None]
        if len(totals) < 10:
            return None

        last = totals[-1]
        avg_5d = sum(totals[-5:]) / 5
        avg_10d = sum(totals[-10:]) / 10

        slope_5d = (totals[-1] - totals[-6]) / 5
        slope_10d = (totals[-1] - totals[-11]) / 10
        
        return {
            "last_value": round(last, 2),
            "avg_5d": round(avg_5d, 2),
            "avg_10d": round(avg_10d, 2),
            "ratio_vs_5d": round(last / avg_5d, 4) if avg_5d > 0 else None,
            "ratio_vs_10d": round(last / avg_10d, 4) if avg_10d > 0 else None,
            "slope_5d": round(slope_5d, 2),
            "slope_10d": round(slope_10d, 2),

        }

    # ------------------------------------------------------------
    def _load_turnover_history(self, turnover_dir: str) -> List[Dict[str, Any]]:
        """
        读取 history 中的 turnover 单日原子事实
        """
        records: List[Dict[str, Any]] = []

        for fname in os.listdir(turnover_dir):
            
            if not fname.endswith(".json"):
                continue

            fpath = os.path.join(turnover_dir, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if "trade_date" in data and "total" in data:
                    records.append(data)
                    print(fname)
                else:
                    print(f"{fname} is incorrect json history file.")
            except Exception as e:
                LOG.error("[TrendFacts] failed to load %s: %s", fname, e)
                raise Exception("[TrendFacts] failed to load %s: %s", fname, e)
        
        return records
