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
        
    def _build_amount_trend_1(self, snapshot: dict) -> dict | None:
        """
        从 snapshot['amount_raw']['windows'] 中读取时间序列
        提取 total_amount，计算成交额趋势指标
        后续计算逻辑与原代码完全一致
        """
        windows = snapshot.get('amount_raw', {}).get('windows', [])
        assert windows, "snapsot - amount_trend window is empty!"
        if not windows:
            return None

        records = [
            {
                "trade_date": item["trade_date"],
                "total": item.get("total_amount")  # 键名改为 total_amount
            }
            for item in windows
            if item.get("total_amount") is not None
        ]

        if len(records) < 10:
            LOG.error("Amount history records < 10")
            raise Exception("Amount history records < 10")
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
        当前仅实现：Amount 趋势事实
        """
        facts: Dict[str, Any] = {}

        amount_trend = self._build_amount_trend(snapshot)
        assert amount_trend, "amount_trend is empty!"
        if amount_trend:
            facts["amount"] = amount_trend
        #
        # Todo other  trend like market_sentiment....breadth trend
        #
        return facts

    def _build_amount_trend(self, snapshot: dict) -> dict | None:
        """
        从 snapshot['amount_raw']['windows'] 中读取时间序列
        提取 total_amount，计算成交额趋势指标
    
        冻结原则：
        - 这里只生成【事实 + 时间序列】
        - 不做判断、不生成信号
        - 不裁剪窗口长度（保留 30d 供下游使用）
        """
        windows = snapshot.get("amount_raw", {}).get("window", [])
        if not windows:
            LOG.error("snapshot.amount_raw.windows is empty")
            return None
    
        # 过滤无效值，规范字段
        records = [
            {
                "trade_date": item["trade_date"],
                "total": item.get("total_amount"),
            }
            for item in windows
            if item.get("total_amount") is not None
        ]
    
        if len(records) < 10:
            LOG.error("Amount history records < 10")
            return None
    
        # 按日期升序排列（最旧 → 最新）
        records.sort(key=lambda x: x["trade_date"])
    
        totals: List[float] = [r["total"] for r in records]
    
        # --- 核心统计（与你现有逻辑保持一致） ---
        last = totals[-1]
        avg_5d = sum(totals[-5:]) / 5
        avg_10d = sum(totals[-10:]) / 10
    
        slope_5d = (totals[-1] - totals[-6]) / 5
        slope_10d = (totals[-1] - totals[-11]) / 10
    
        return {
            # 单点事实
            "last_value": round(last, 2),
    
            # 均值
            "avg_5d": round(avg_5d, 2),
            "avg_10d": round(avg_10d, 2),
    
            # 比例
            "ratio_vs_5d": round(last / avg_5d, 4) if avg_5d > 0 else None,
            "ratio_vs_10d": round(last / avg_10d, 4) if avg_10d > 0 else None,
    
            # 斜率
            "slope_5d": round(slope_5d, 2),
            "slope_10d": round(slope_10d, 2),
    
            # ★ 新增：完整时间序列（冻结）
            # 供 FRF / 其他 Observation 做窗口化分析
            "window": records,
        }
     