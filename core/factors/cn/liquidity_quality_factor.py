# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - Liquidity Quality Factor (F Block)

职责：
    根据 snapshot 中的 liquidity_quality_raw 原始数据计算流动性质量风险评分和等级。
    - 核心指标包括 top20_ratio（前 20 名成交额占比）、big_small_ratio（大盘与小盘成交额比）
      和 down_low_ratio（下跌股票中缩量比）。
    - 附带趋势指标（top20_trend_10d、big_small_trend_10d、down_low_trend_10d）用来判断
      指标是否恶化或改善。
    - 通过比较指标和趋势综合评估流动性环境，映射为风险等级：
        • 若成交集中度偏高且仍在上升，则认为流动性迅速收缩，标记为 RED。
        • 若大盘与小盘成交严重失衡（≥2 或 ≤0.5），标记为 ORANGE。
        • 若缩量下跌比例较高（≥0.5），表明下跌时资金并未急于出逃，流动性环境较稳，标记为 GREEN。
        • 其它情况标记为 YELLOW。

约束：
    - 不访问任何 DataSource/DB/API，只使用提供的 input_block 中的 liquidity_quality_raw。
    - 评分逻辑和阈值可根据实际需求调整，但接口不变。
"""

from __future__ import annotations

from typing import Dict, Any, Optional

from core.factors.factor_base import FactorBase, FactorResult
from core.utils.logger import get_logger

LOG = get_logger("Factor.LiquidityQuality")


class LiquidityQualityFactor(FactorBase):
    """
    Liquidity Quality Factor

    依据流动性质量原始数据评估市场流动性环境。主要关注以下指标：
      - top20_ratio：前 20 名股票成交额占全市场的比重。高值意味着成交高度集中，流动性分布差。
      - big_small_ratio：大盘股票成交额与小盘股票成交额之比。极端偏离 1 说明结构失衡。
      - down_low_ratio：下跌股票中成交额低于自身近 20 日均额的比例。高值表示下跌时缩量，
        资金抛压不强，流动性压力较小。
      - 各指标的 10 日趋势用于判断指标是否恶化或改善。

    该因子输出一个 0~100 的风险评分以及等级：GREEN/YELLOW/ORANGE/RED。
    """

    # 权重定义（总和为 1.0）。根据经验可调整。
    WEIGHTS = {
        "top20": 0.5,
        "big_small": 0.3,
        "down_low": 0.2,
    }

    def __init__(self) -> None:
        super().__init__(name="liquidity_quality")

    # ------------------------------------------------------------------
    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        data = input_block.get("liquidity_quality_raw") or {}
        assert data, "liquidity_quality_raw is empty"
        if not data:
            return self.build_result(
                score=50.0,
                level="MISSING",
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "reason": "liquidity_quality_raw data missing",
                },
            )

        # 尝试安全转换为 float
        def _safe(v: Any) -> Optional[float]:
            try:
                if v is None or isinstance(v, bool):
                    return None
                return float(v)
            except Exception:
                return None

        top20_ratio = _safe(data.get("top20_ratio"))
        big_small_ratio = _safe(data.get("big_small_ratio"))
        down_low_ratio = _safe(data.get("down_low_ratio"))
        top20_trend = _safe(data.get("top20_trend_10d"))
        big_small_trend = _safe(data.get("big_small_trend_10d"))
        down_low_trend = _safe(data.get("down_low_trend_10d"))

        # 计算子评分
        top20_score = self._score_top20(top20_ratio)
        big_small_score = self._score_big_small(big_small_ratio)
        down_low_score = self._score_down_low(down_low_ratio)

        score = (
            top20_score * self.WEIGHTS["top20"]
            + big_small_score * self.WEIGHTS["big_small"]
            + down_low_score * self.WEIGHTS["down_low"]
        )
        score = round(max(0.0, min(100.0, score)), 2)

        # 评估等级（颜色级别）
        color_level = self._map_level(top20_ratio, top20_trend, big_small_ratio, down_low_ratio)

        # 映射颜色级别到风险级别（FactorResult.level 仅接受 LOW/NEUTRAL/HIGH）
        def _color_to_risk(lv: str) -> str:
            lv_upper = (lv or "").upper()
            if lv_upper in ("RED", "ORANGE"):
                return "HIGH"
            if lv_upper == "GREEN":
                return "LOW"
            # YELLOW, MISSING 或其他 -> NEUTRAL
            return "NEUTRAL"

        risk_level = _color_to_risk(color_level)

        LOG.info(
            "[LiquidityQualityFactor] score=%.2f level=%s (risk_level=%s) top20=%.4f big_small=%.4f down_low=%.4f",
            score,
            color_level,
            risk_level,
            top20_ratio if top20_ratio is not None else 0.0,
            big_small_ratio if big_small_ratio is not None else 0.0,
            down_low_ratio if down_low_ratio is not None else 0.0,
        )

        return self.build_result(
            score=score,
            level=risk_level,
            details={
                "top20_ratio": top20_ratio,
                "big_small_ratio": big_small_ratio,
                "down_low_ratio": down_low_ratio,
                "top20_trend_10d": top20_trend,
                "big_small_trend_10d": big_small_trend,
                "down_low_trend_10d": down_low_trend,
                "data_status": "OK",
                # Expose color level for report panels
                "color_level": color_level,
                "_raw_data": str(data)[:160] + "..." if isinstance(data, dict) else str(data),
            },
        )

    # ------------------------------------------------------------------
    def _score_top20(self, val: float | None) -> float:
        """
        根据成交集中度映射评分。
        越高的 top20_ratio 表示流动性集中度越高，风险越大。
        假设 0.1 以下为极分散（100 分），0.3 以上为极集中（0 分）。
        """
        if val is None:
            return 50.0
        v = float(val)
        if v >= 0.3:
            return 0.0
        if v <= 0.1:
            return 100.0
        # 线性映射 [0.1, 0.3] -> [100, 0]
        return max(0.0, min(100.0, 100.0 - ((v - 0.1) / 0.2) * 100.0))

    def _score_big_small(self, val: float | None) -> float:
        """
        根据大小盘成交比例映射评分。
        偏离 1.0 越大，说明大盘或小盘占比失衡，风险越大。
        假设偏离 ≤0.2 为极平衡（100 分），偏离 ≥1.0 为极失衡（0 分）。
        """
        if val is None:
            return 50.0
        try:
            ratio = float(val)
        except Exception:
            return 50.0
        deviation = abs(ratio - 1.0)
        if deviation >= 1.0:
            return 0.0
        if deviation <= 0.2:
            return 100.0
        return max(0.0, min(100.0, 100.0 - ((deviation - 0.2) / 0.8) * 100.0))

    def _score_down_low(self, val: float | None) -> float:
        """
        根据缩量下跌比映射评分。
        比例越高表示下跌时缩量越多，杀跌意愿不强，风险越低。
        假设 ≥0.5 为极佳（100 分），≤0.2 为极差（0 分）。
        """
        if val is None:
            return 50.0
        v = float(val)
        if v >= 0.5:
            return 100.0
        if v <= 0.2:
            return 0.0
        return max(0.0, min(100.0, ((v - 0.2) / 0.3) * 100.0))

    def _map_level(self, top20_ratio: float | None, top20_trend: float | None, big_small_ratio: float | None, down_low_ratio: float | None) -> str:
        """
        根据核心指标和趋势映射风险等级。

        - 若 top20_ratio > 0.3 且 top20_trend > 0，则判定为 RED，表示市场成交高度集中且集中度仍在上升。
        - 若 big_small_ratio ≥ 2.0 或 ≤ 0.5，则判定为 ORANGE，表示大小盘成交严重失衡。
        - 若 down_low_ratio ≥ 0.5，则判定为 GREEN，表示下跌过程中缩量明显，杀跌意愿不强。
        - 其它情况判定为 YELLOW。
        若某指标缺失，则自动跳过该条件。
        """
        try:
            if top20_ratio is not None and top20_trend is not None:
                if float(top20_ratio) > 0.3 and float(top20_trend) > 0:
                    return "RED"
            if big_small_ratio is not None:
                br = float(big_small_ratio)
                if br >= 2.0 or br <= 0.5:
                    return "ORANGE"
            if down_low_ratio is not None:
                dl = float(down_low_ratio)
                if dl >= 0.5:
                    return "GREEN"
            return "YELLOW"
        except Exception:
            return "MISSING"