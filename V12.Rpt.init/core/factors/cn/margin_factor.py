# -*- coding: utf-8 -*-

from __future__ import annotations
from typing import Dict, Any
import json
from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult
from core.utils.logger import get_logger

LOG = get_logger("Factor.Margin")


class MarginFactor(FactorBase):
    """
    两融杠杆因子（UnifiedRisk V12）
    """

    WEIGHTS = {
        "trend": 0.35,
        "accel": 0.25,
        "rz_ratio": 0.20,
        "rz_buy": 0.20,
    }

    def __init__(self):
        super().__init__(name="margin_raw")

    # ------------------------------------------------------------------
    # level 映射（V12 强约束）
    # ------------------------------------------------------------------
    def _map_zone_to_level(self, zone: str | None) -> str:
        if not zone:
            return "NEUTRAL"

        z = str(zone)
        if z in ("低", "偏低", "安全", "LOW"):
            return "LOW"
        if z in ("高", "偏高", "风险", "HIGH"):
            return "HIGH"
        return "NEUTRAL"

    # ------------------------------------------------------------------
    # 子评分函数
    # ------------------------------------------------------------------
    def _score_trend(self, val: float | None) -> float:
        if val is None:
            return 50.0
        if val >= 200:
            return 100.0
        if val <= -200:
            return 0.0
        return 50.0 + (val / 200.0) * 50.0

    def _score_accel(self, val: float | None) -> float:
        if val is None:
            return 50.0
        if val >= 80:
            return 100.0
        if val <= -80:
            return 0.0
        return 50.0 + (val / 80.0) * 50.0

    def _score_rz_ratio(self, ratio: float | None) -> float:
        if ratio is None:
            return 50.0
        if ratio <= 5:
            return 80.0
        if ratio >= 15:
            return 40.0
        return 80.0 - (ratio - 5) * 4.0

    def _score_rz_buy(self, rz_buy: float | None) -> float:
        if rz_buy is None:
            return 50.0
        if rz_buy >= 500:
            return 100.0
        if rz_buy <= -200:
            return 0.0
        return 50.0 + (rz_buy / 500.0) * 50.0

    # ------------------------------------------------------------------
    # 主计算函数（V12 标准）
    # ------------------------------------------------------------------
    def compute(self, snapshot: Dict[str, Any]) -> FactorResult:
        data = snapshot.get("margin_raw") or {}

        if not data:
            return self.build_result(
                score=50.0,
                level="NEUTRAL",
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "reason": "margin_raw data missing",
                },
            )

        trend = data.get("trend_10d")
        accel = data.get("acc_3d")
        rz_ratio = data.get("rz_ratio")
        rz_buy = data.get("rz_buy")
        zone_raw = data.get("risk_zone")

        trend_score = self._score_trend(trend)
        accel_score = self._score_accel(accel)
        ratio_score = self._score_rz_ratio(rz_ratio)
        buy_score = self._score_rz_buy(rz_buy)

        score = (
            trend_score * self.WEIGHTS["trend"]
            + accel_score * self.WEIGHTS["accel"]
            + ratio_score * self.WEIGHTS["rz_ratio"]
            + buy_score * self.WEIGHTS["rz_buy"]
        )
        score = round(max(0.0, min(100.0, score)), 2)

        level = self._map_zone_to_level(zone_raw)

        LOG.info(
            "[MarginFactor] score=%.2f level=%s zone=%s",
            score,
            level,
            zone_raw,
        )

        return self.build_result(
            score=score,
            level=level,
            details={
                "trend_10d": trend,
                "acc_3d": accel,
                "rz_ratio": rz_ratio,
                "rz_buy": rz_buy,
                "risk_zone_raw": zone_raw,
                "data_status": "OK",
                "_raw_data": json.dumps(data)[:160] + "...",
            },
        )
