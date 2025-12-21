# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 · TrendFactsBlockBuilder（冻结）

职责：
- 统一构建“趋势派生事实”（Trend Facts）
- 支撑 Trend-in-Force / Failure-Rate Factor / DRS
- 不判断、不裁决、不预测

趋势对象（冻结）：
1. index          —— 指数趋势
2. capital_flow  —— 北向/北代资金趋势
3. sector        —— 主导行业趋势（占位）

设计铁律：
- 不拉 Provider
- 不在 DS / Factor 中计算趋势
- 所有趋势只在 FactBlockBuilder 生成
"""

from __future__ import annotations
from typing import Dict, Any, List, Optional

from core.block_builders.block_builder_base import FactBlockBuilderBase
from core.utils.logger import get_logger

LOG = get_logger("BB.TrendFacts")


class TrendFactsBlockBuilder(FactBlockBuilderBase):
    """
    趋势派生事实构建器（冻结）
    """

    block_key = "trend_facts_raw"

    # ===============================
    # Public API
    # ===============================
    def build_block(
        self,
        snapshot: Dict[str, Any],
        refresh_mode: str = "auto",
    ) -> Dict[str, Any]:
        warnings: List[str] = []

        index_block = self._build_index_trend(snapshot, warnings)
        capital_flow_block = self._build_capital_flow_trend(snapshot, warnings)
        sector_block = self._build_sector_trend(snapshot, warnings)

        return {
            "index": index_block,
            "capital_flow": capital_flow_block,
            "sector": sector_block,
            "_meta": {
                "warnings": warnings,
            },
        }

    # ===============================
    # Index Trend（指数趋势）
    # ===============================
    def _build_index_trend(
        self,
        snapshot: Dict[str, Any],
        warnings: List[str],
    ) -> Dict[str, Any]:
        price_raw = snapshot.get("index_price_raw")
        turnover_raw = snapshot.get("turnover_raw")
        price_series = snapshot.get("index_price_series")
        turnover_series = snapshot.get("turnover_series")

        close = price_raw.get("close") if price_raw else None
        ma20 = self._calc_avg(self._last_n(price_series, "close", 20))
        above_ma20 = (
            close is not None and ma20 is not None and close >= ma20
        )

        turnover_ratio_5d = self._calc_ratio(
            turnover_raw,
            turnover_series,
            key="turnover",
            window=5,
        )

        if close is None:
            warnings.append("index_price_missing")

        return {
            "close": close,
            "ma20": ma20,
            "above_ma20": above_ma20,
            "turnover_ratio_5d": turnover_ratio_5d,
        }

    # ===============================
    # Capital Flow Trend（资金趋势）
    # ===============================
    def _build_capital_flow_trend(
        self,
        snapshot: Dict[str, Any],
        warnings: List[str],
    ) -> Dict[str, Any]:
        north_raw = snapshot.get("northbound_fund_raw")
        north_series = snapshot.get("northbound_fund_series")

        net_today = north_raw.get("net") if north_raw else None
        avg_5d = self._calc_avg(self._last_n(north_series, "net", 5))

        ratio = (
            net_today / avg_5d
            if net_today is not None and avg_5d not in (None, 0)
            else None
        )

        direction = None
        if net_today is not None:
            if net_today > 0:
                direction = "inflow"
            elif net_today < 0:
                direction = "outflow"
            else:
                direction = "flat"

        if north_raw is None:
            warnings.append("northbound_data_missing")

        return {
            "northbound": {
                "net_today": net_today,
                "avg_5d": avg_5d,
                "ratio_to_avg_5d": ratio,
                "direction": direction,
            }
        }

    # ===============================
    # Sector Trend（主导行业，占位）
    # ===============================
    def _build_sector_trend(
        self,
        snapshot: Dict[str, Any],
        warnings: List[str],
    ) -> Dict[str, Any]:
        # 当前阶段仅做结构占位，避免后续返工
        return {
            "leading_sector": {
                "name": None,
                "fund_flow_ratio_5d": None,
                "price_above_ma20": None,
            }
        }

    # ===============================
    # Helpers
    # ===============================
    @staticmethod
    def _last_n(
        series: Optional[List[Dict[str, Any]]],
        key: str,
        n: int,
    ) -> List[float]:
        if not series:
            return []
        values = [item.get(key) for item in series if item.get(key) is not None]
        return values[-n:]

    @staticmethod
    def _calc_avg(values: List[float]) -> Optional[float]:
        if not values:
            return None
        return sum(values) / len(values)

    def _calc_ratio(
        self,
        today_raw: Optional[Dict[str, Any]],
        series: Optional[List[Dict[str, Any]]],
        *,
        key: str,
        window: int,
    ) -> Optional[float]:
        if not today_raw or not series:
            return None
        today = today_raw.get(key)
        avg = self._calc_avg(self._last_n(series, key, window))
        if today is None or avg in (None, 0):
            return None
        return today / avg
