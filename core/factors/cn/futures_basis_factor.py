# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - Futures Basis Factor (D Block)

职责：
    根据 snapshot 中的 futures_basis_raw 原始数据计算风险评分和等级。
    - 核心指标包括 avg_basis（加权基差均值）、trend_10d、acc_3d 和 basis_ratio。
    - 通过比较基差和趋势的符号确定风险等级：
        • avg_basis < 0 且 trend_10d < 0 → 高风险（RED），持续贴水扩大，市场悲观。
        • avg_basis > 0 且 trend_10d > 0 → 中等风险（ORANGE），持续升水扩大，市场过热。
        • 其它情况 → 中性（YELLOW），方向不一致或趋势不明显。

约束：
    - 不访问任何 DataSource/DB/API，只使用提供的 input_block 中的 futures_basis_raw。
    - 评分逻辑可根据需求调整，但接口不变。
"""

from __future__ import annotations

from typing import Dict, Any, Optional

from core.factors.factor_base import FactorBase, FactorResult
from core.utils.logger import get_logger

LOG = get_logger("Factor.FuturesBasis")


class FuturesBasisFactor(FactorBase):
    """
    Futures Basis Factor

    根据期指基差原始数据评估风险：
      - avg_basis：加权基差均值（期货 - 指数），正值为升水，负值为贴水。
      - trend_10d：近 10 日基差变化。
      - acc_3d：近 3 日基差变化。
      - basis_ratio：基差相对指数价格的比值。

    使用这些指标生成 0~100 的风险评分，并映射为等级（GREEN/YELLOW/ORANGE/RED）。
    """

    # 权重定义（总和为 1.0）。可根据实际市场校准。
    WEIGHTS = {
        "basis": 0.4,
        "trend": 0.3,
        "accel": 0.2,
        "ratio": 0.1,
    }

    def __init__(self) -> None:
        super().__init__(name="futures_basis")

    # ------------------------------------------------------------------
    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        """Compute futures basis risk.

        IMPORTANT:
        FactorResult.level is frozen to LOW / NEUTRAL / HIGH in V12.
        Reporter may still want RED/ORANGE/YELLOW semantics; we keep it in
        details["semantic_level"] for backward readability.
        """

        data = input_block.get("futures_basis_raw") or {}
        if not data:
            # Do NOT raise; return neutral placeholder so engine can continue.
            return self.build_result(
                score=50.0,
                level="NEUTRAL",
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "reason": "futures_basis_raw missing/empty",
                    "semantic_level": "MISSING",
                },
            )

        avg_basis = self._safe_float(data.get("avg_basis"))
        trend_10d = self._safe_float(data.get("trend_10d"))
        acc_3d = self._safe_float(data.get("acc_3d"))
        ratio = self._safe_float(data.get("basis_ratio"))

        # 子评分
        basis_score = self._score_basis(avg_basis)
        trend_score = self._score_trend(trend_10d)
        accel_score = self._score_accel(acc_3d)
        ratio_score = self._score_ratio(ratio)

        score = (
            basis_score * self.WEIGHTS["basis"]
            + trend_score * self.WEIGHTS["trend"]
            + accel_score * self.WEIGHTS["accel"]
            + ratio_score * self.WEIGHTS["ratio"]
        )
        score = round(max(0.0, min(100.0, score)), 2)

        semantic = self._map_semantic_level(avg_basis, trend_10d)
        level = self._map_risk_level(score, semantic)

        LOG.info(
            "[FuturesBasisFactor] score=%.2f level=%s basis=%.3f trend=%.3f accel=%.3f ratio=%.6f",
            score,
            level,
            avg_basis if avg_basis is not None else 0.0,
            trend_10d if trend_10d is not None else 0.0,
            acc_3d if acc_3d is not None else 0.0,
            ratio if ratio is not None else 0.0,
        )

        return self.build_result(
            score=score,
            level=level,
            details={
                "avg_basis": avg_basis,
                "trend_10d": trend_10d,
                "acc_3d": acc_3d,
                "basis_ratio": ratio,
                "data_status": "OK",
                "semantic_level": semantic,
                "_raw_data": str(data)[:160] + "..." if isinstance(data, dict) else str(data),
            },
        )

    # ------------------------------------------------------------------
    def _safe_float(self, v: Any) -> Optional[float]:
        try:
            if v is None or isinstance(v, bool):
                return None
            return float(v)
        except Exception:
            return None

    # ------------------------------------------------------------------
    def _score_basis(self, val: float | None) -> float:
        """
        基差得分：绝对值越大表示市场分歧越大，风险越高。
        阈值约束：假设 ±10 点左右为极值范围。
        """
        if val is None:
            return 50.0
        v = abs(float(val))
        # ≥ 10 点则风险高（0 分）；0 点风险低（100 分）
        if v >= 10.0:
            return 0.0
        # 线性映射 [0,10] -> [100,0]
        return max(0.0, min(100.0, 100.0 - (v / 10.0) * 100.0))

    def _score_trend(self, val: float | None) -> float:
        if val is None:
            return 50.0
        v = abs(float(val))
        # ≥ 5 点变化被视为强趋势（0 分）；0 变化（100 分）
        if v >= 5.0:
            return 0.0
        return max(0.0, min(100.0, 100.0 - (v / 5.0) * 100.0))

    def _score_accel(self, val: float | None) -> float:
        if val is None:
            return 50.0
        v = abs(float(val))
        # ≥ 3 点变化被视为加速度强（0 分）；0 加速度（100 分）
        if v >= 3.0:
            return 0.0
        return max(0.0, min(100.0, 100.0 - (v / 3.0) * 100.0))

    def _score_ratio(self, val: float | None) -> float:
        if val is None:
            return 50.0
        v = abs(float(val))
        # 基差/指数比值绝对值 ≥0.005 (~0.5%) 视为高风险 0 分
        if v >= 0.005:
            return 0.0
        return max(0.0, min(100.0, 100.0 - (v / 0.005) * 100.0))

    def _map_semantic_level(self, basis: float | None, trend: float | None) -> str:
        """Return legacy semantic level for human readability (RED/ORANGE/YELLOW/MISSING)."""
        try:
            b = float(basis) if basis is not None else 0.0
            t = float(trend) if trend is not None else 0.0
        except Exception:
            return "MISSING"
        if b < 0.0 and t < 0.0:
            return "RED"
        if b > 0.0 and t > 0.0:
            return "ORANGE"
        return "YELLOW"

    @staticmethod
    def _map_risk_level(score: float, semantic_level: str) -> str:
        """Map score/semantic to V12 frozen RiskLevel: LOW / NEUTRAL / HIGH.

        Convention in this repo: score=100 means "good/low risk", score=0 means "bad/high risk".
        - HIGH: score <= 40 (risk high)
        - LOW: score >= 60 (risk low)
        - else NEUTRAL

        semantic_level is used as a weak override when data is clearly extreme.
        """
        try:
            s = float(score)
        except Exception:
            return "NEUTRAL"

        # weak override: extreme semantics imply at least NEUTRAL->HIGH
        if (semantic_level or "").upper() in ("RED",):
            return "HIGH"

        if s <= 40.0:
            return "HIGH"
        if s >= 60.0:
            return "LOW"
        return "NEUTRAL"