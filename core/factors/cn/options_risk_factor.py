# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - Options Risk Factor (E Block)

职责：
    根据 snapshot 中的 options_risk_raw 原始数据计算风险评分和等级。

关键点（V12 Frozen Contract）：
    - FactorResult.level 必须是：LOW / NEUTRAL / HIGH
    - 传统颜色语义（RED/ORANGE/YELLOW/MISSING）仅作为 human semantic，
      必须写入 details["semantic_level"]，不得塞进 FactorResult.level

口径说明：
    - semantic_level（RED/ORANGE/YELLOW）表示“方向/趋势的语义组合”，便于报告解释
    - risk level（LOW/NEUTRAL/HIGH）表示“风险强度”，用于 Gate/Pred/治理层的统一判定
"""

from __future__ import annotations

from typing import Dict, Any, Optional

from core.factors.factor_base import FactorBase, FactorResult
from core.utils.logger import get_logger

LOG = get_logger("Factor.OptionsRisk")


class OptionsRiskFactor(FactorBase):
    """
    Options Risk Factor

    根据期权风险原始数据评估风险：
      - weighted_change：按成交量加权的涨跌额均值（正值表示总体上涨，负值表示总体下跌）。
      - trend_10d：近 10 日 weighted_change 的变化。
      - acc_3d：近 3 日 weighted_change 的变化。
      - change_ratio：weighted_change 相对加权收盘价的比值。

    输出：
      - score: 0~100
      - level: LOW/NEUTRAL/HIGH（Frozen）
      - details.semantic_level: RED/ORANGE/YELLOW/MISSING（解释用）
    """

    WEIGHTS = {
        "change": 0.4,
        "trend": 0.3,
        "accel": 0.2,
        "ratio": 0.1,
    }

    def __init__(self) -> None:
        super().__init__(name="options_risk")

    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        data = input_block.get("options_risk_raw") or {}

        # data 缺失：不抛异常，不输出 0；用 NEUTRAL + semantic=MISSING 告知
        if not data:
            return self.build_result(
                score=50.0,
                level="NEUTRAL",
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "reason": "options_risk_raw missing/empty",
                    "semantic_level": "MISSING",
                },
            )

        weighted_change = self._safe_float(data.get("weighted_change"))
        trend_10d = self._safe_float(data.get("trend_10d"))
        acc_3d = self._safe_float(data.get("acc_3d"))
        ratio = self._safe_float(data.get("change_ratio"))

        change_score = self._score_change(weighted_change)
        trend_score = self._score_trend(trend_10d)
        accel_score = self._score_accel(acc_3d)
        ratio_score = self._score_ratio(ratio)

        score = (
            change_score * self.WEIGHTS["change"]
            + trend_score * self.WEIGHTS["trend"]
            + accel_score * self.WEIGHTS["accel"]
            + ratio_score * self.WEIGHTS["ratio"]
        )
        score = round(max(0.0, min(100.0, score)), 2)

        semantic = self._map_semantic_level(weighted_change, trend_10d)
        level = self._map_risk_level(score=score, semantic_level=semantic)

        LOG.info(
            "[OptionsRiskFactor] score=%.2f level=%s semantic=%s change=%.3f trend=%.3f accel=%.3f ratio=%.6f",
            score,
            level,
            semantic,
            weighted_change if weighted_change is not None else 0.0,
            trend_10d if trend_10d is not None else 0.0,
            acc_3d if acc_3d is not None else 0.0,
            ratio if ratio is not None else 0.0,
        )

        return self.build_result(
            score=score,
            level=level,
            details={
                "weighted_change": weighted_change,
                "trend_10d": trend_10d,
                "acc_3d": acc_3d,
                "change_ratio": ratio,
                "data_status": "OK",
                "semantic_level": semantic,
                "_raw_data": str(data)[:160] + "..." if isinstance(data, dict) else str(data),
            },
        )

    @staticmethod
    def _safe_float(v: Any) -> Optional[float]:
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return float(v)
        try:
            if isinstance(v, str) and v.strip() != "":
                return float(v)
        except Exception:
            return None
        return None

    def _score_change(self, val: Optional[float]) -> float:
        if val is None:
            return 50.0
        a = abs(val)
        if a >= 2.0:
            return 95.0
        if a >= 1.0:
            return 80.0
        if a >= 0.5:
            return 65.0
        return 50.0

    def _score_trend(self, val: Optional[float]) -> float:
        if val is None:
            return 50.0
        a = abs(val)
        if a >= 2.0:
            return 90.0
        if a >= 1.0:
            return 75.0
        if a >= 0.5:
            return 60.0
        return 50.0

    def _score_accel(self, val: Optional[float]) -> float:
        if val is None:
            return 50.0
        a = abs(val)
        if a >= 1.5:
            return 85.0
        if a >= 0.8:
            return 70.0
        if a >= 0.4:
            return 60.0
        return 50.0

    def _score_ratio(self, val: Optional[float]) -> float:
        if val is None:
            return 50.0
        a = abs(val)
        if a >= 0.02:
            return 90.0
        if a >= 0.01:
            return 75.0
        if a >= 0.005:
            return 60.0
        return 50.0

    @staticmethod
    def _map_semantic_level(weighted_change: Optional[float], trend_10d: Optional[float]) -> str:
        if weighted_change is None or trend_10d is None:
            return "YELLOW"
        if weighted_change < 0.0 and trend_10d < 0.0:
            return "RED"
        if weighted_change > 0.0 and trend_10d > 0.0:
            return "ORANGE"
        return "YELLOW"

    @staticmethod
    def _map_risk_level(*, score: float, semantic_level: str) -> str:
        s = float(score)
        if (semantic_level or "").upper() == "RED":
            return "HIGH"
        if s <= 40.0:
            return "HIGH"
        if s >= 60.0:
            return "LOW"
        return "NEUTRAL"
