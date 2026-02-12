# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - ETF Flow Factor (C Block)

职责：
    根据 snapshot 中的 etf_flow_raw 原始数据计算风险评分和等级。
    - 综合考虑 ETF 份额变化代理（total_change_amount）、趋势、加速度与比值
    - 输出 FactorResult（score, level, details）

约束：
    - 不访问任何 DataSource/DB/API，只使用提供的 input_block
    - 评分逻辑可根据需求调整，但接口不变
"""

from __future__ import annotations

from typing import Dict, Any

from core.factors.factor_base import FactorBase, FactorResult
from core.utils.logger import get_logger

LOG = get_logger("Factor.ETFFlow")


class ETFFlowFactor(FactorBase):
    """
    ETF Flow Factor

    根据 etf_flow_raw 的原始指标计算风险评分：
      - total_change_amount: 当日所有 ETF price change 之和，作为净申购/赎回代理
      - trend_10d: 近 10 天累计 price change
      - acc_3d: 近 3 天累计 price change
      - flow_ratio: price change 与成交量之比

    评分结果映射为风险等级：HIGH / NEUTRAL / LOW。
    """

    # 权重定义，可根据实际校准
    WEIGHTS = {
        "flow": 0.4,
        "trend": 0.3,
        "accel": 0.2,
        "ratio": 0.1,
    }

    def __init__(self) -> None:
        super().__init__(name="etf_flow")

    # ------------------------------------------------------------------
    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        data = input_block.get("etf_flow_raw") or {}
        assert data, "etf_flow_raw is empty"
        if not data:
            return self.build_result(
                score=50.0,
                level="NEUTRAL",
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "reason": "etf_flow_raw data missing",
                },
            )

        total_change_amount = data.get("total_change_amount")
        trend_10d = data.get("trend_10d")
        acc_3d = data.get("acc_3d")
        ratio = data.get("flow_ratio")

        # 计算子评分
        flow_score = self._score_flow(total_change_amount)
        trend_score = self._score_trend(trend_10d)
        accel_score = self._score_accel(acc_3d)
        ratio_score = self._score_ratio(ratio)

        score = (
            flow_score * self.WEIGHTS["flow"]
            + trend_score * self.WEIGHTS["trend"]
            + accel_score * self.WEIGHTS["accel"]
            + ratio_score * self.WEIGHTS["ratio"]
        )
        score = round(max(0.0, min(100.0, score)), 2)

        level = self._map_level(total_change_amount, trend_10d)

        LOG.info(
            "[ETFFlowFactor] score=%.2f level=%s flow=%.2f trend=%.2f accel=%.2f ratio=%.4f",
            score,
            level,
            total_change_amount,
            trend_10d,
            acc_3d,
            ratio,
        )

        return self.build_result(
            score=score,
            level=level,
            details={
                "total_change_amount": total_change_amount,
                "trend_10d": trend_10d,
                "acc_3d": acc_3d,
                "flow_ratio": ratio,
                "data_status": "OK",
                "_raw_data": str(data)[:160] + "..." if isinstance(data, dict) else str(data),
            },
        )

    # ------------------------------------------------------------------
    def _score_flow(self, val: float | None) -> float:
        """
        根据当日总变化额映射评分。
        正值越大代表净申购强，负值越大代表净赎回。
        阈值以百万为单位，可按需调整。
        """
        if val is None:
            return 50.0
        v = float(val)
        # 万元为单位，假设 >= 50 万表示强净申购；<= -50 万为强净赎回
        if v >= 5e5:
            return 100.0
        if v <= -5e5:
            return 0.0
        return 50.0 + (v / 5e5) * 50.0

    def _score_trend(self, val: float | None) -> float:
        if val is None:
            return 50.0
        v = float(val)
        if v >= 2e6:
            return 100.0
        if v <= -2e6:
            return 0.0
        return 50.0 + (v / 2e6) * 50.0

    def _score_accel(self, val: float | None) -> float:
        if val is None:
            return 50.0
        v = float(val)
        if v >= 8e5:
            return 100.0
        if v <= -8e5:
            return 0.0
        return 50.0 + (v / 8e5) * 50.0

    def _score_ratio(self, val: float | None) -> float:
        if val is None:
            return 50.0
        v = float(val)
        # 假设比值在 [-0.01, 0.01] 内映射到 [0, 100]
        if v >= 0.01:
            return 100.0
        if v <= -0.01:
            return 0.0
        return 50.0 + (v / 0.01) * 50.0

    def _map_level(self, flow: float | None, trend: float | None) -> str:
        """
        将总变化额和趋势映射为风险等级：
          - 同正为 Low（持续净申购）
          - 同负为 High（持续净赎回）
          - 否则为 Neutral
        """
        try:
            f = float(flow) if flow is not None else 0.0
            t = float(trend) if trend is not None else 0.0
        except Exception:
            return "NEUTRAL"
        if f > 0 and t > 0:
            return "LOW"
        if f < 0 and t < 0:
            return "HIGH"
        return "NEUTRAL"