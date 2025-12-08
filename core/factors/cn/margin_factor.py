# core/factors/cn/margin_factor.py
# -*- coding: utf-8 -*-

from __future__ import annotations
from typing import Dict, Any

from core.factors.base import BaseFactor, FactorResult
from core.utils.logger import get_logger

LOG = get_logger("Factor.Margin")


class MarginFactor(BaseFactor):
    """
    两融杠杆因子（V12 专业版）
    支持：
        - 总余额（total）
        - 趋势（10日）
        - 加速度（3日）
        - 融资余额比例 rz_ratio
        - 融资买入力 rz_buy
        - 风险区间（高/中/低）

    输出：score (0-100) + desc + detail
    """

    def __init__(self):
        #super().__init__("margin")
            
        super().__init__()
        self.name = "margin"

    #
    # ------------------ 评分权重体系（可调） ------------------
    #
    WEIGHTS = {
        "trend": 0.35,        # 趋势
        "accel": 0.25,        # 加速度
        "rz_ratio": 0.20,     # 融资比例
        "rz_buy": 0.20,       # 买入力
    }

    #
    # ------------------ normalize functions ------------------
    #
    def _score_trend(self, val: float) -> float:
        """趋势越大越多，越负越空"""
        if val >= 200:
            return 100
        if val <= -200:
            return 0
        return 50 + (val / 200) * 50

    def _score_accel(self, val: float) -> float:
        """加速度（短期）变动更敏感"""
        if val >= 80:
            return 100
        if val <= -80:
            return 0
        return 50 + (val / 80) * 50

    def _score_rz_ratio(self, ratio: float) -> float:
        """
        融资余额占比（%）
        过高 → 杠杆偏危险
        过低 → 风险不大
        """
        if ratio <= 5:
            return 80
        if ratio >= 15:
            return 40
        return 80 - (ratio - 5) * 4

    def _score_rz_buy(self, rz_buy: float) -> float:
        """融资买入力（短期风险放大器）"""
        if rz_buy >= 500:
            return 100
        if rz_buy <= -200:
            return 0
        return 50 + (rz_buy / 500) * 50

    #
    # ------------------ risk zone描述 ------------------
    #
    def _risk_zone_desc(self, zone: str) -> str:
        return {
            "高": "市场总体杠杆偏高（需关注潜在风险）",
            "中": "杠杆水平中性（风险中性）",
            "低": "市场杠杆偏低（风险较小）",
        }.get(zone, "未知")

    #
    # ------------------ 主 compute ------------------
    #
    def compute(self, snapshot: Dict[str, Any]) -> FactorResult:

        data = snapshot.get("margin", {})

        total = data.get("total", 0.0)
        rz = data.get("rz_balance", 0.0)
        rq = data.get("rq_balance", 0.0)
        trend = data.get("trend_10d", 0.0)
        accel = data.get("acc_3d", 0.0)
        rz_ratio = data.get("rz_ratio", 0.0)
        rz_buy = data.get("rz_buy", 0.0)
        zone = data.get("risk_zone", "中")

        # 数据缺失 → 中性
        if total <= 0:
            return FactorResult(
                name="margin",
                score=50,
                desc="两融数据缺失（按中性处理）",
                detail={
                    "rz_balance": rz,
                    "rq_balance": rq,
                    "total": total,
                    "trend_10d": trend,
                    "acc_3d": accel,
                    "risk_zone": zone,
                },
            )

        #
        # ------------------ 各项子评分 ------------------
        #
        trend_score = self._score_trend(trend)
        accel_score = self._score_accel(accel)
        ratio_score = self._score_rz_ratio(rz_ratio)
        buy_score = self._score_rz_buy(rz_buy)

        #
        # ------------------ 综合评分 ------------------
        #
        score = (
            trend_score * self.WEIGHTS["trend"]
            + accel_score * self.WEIGHTS["accel"]
            + ratio_score * self.WEIGHTS["rz_ratio"]
            + buy_score * self.WEIGHTS["rz_buy"]
        )

        score = max(0, min(100, score))

        #
        # ------------------ 描述文本 ------------------
        #
        desc = f"两融杠杆{self._risk_zone_desc(zone)}"

        detail = {
            "rz_balance": rz,
            "rq_balance": rq,
            "total": total,
            "trend_10d": trend,
            "trend_score": trend_score,
            "acc_3d": accel,
            "accel_score": accel_score,
            "rz_ratio": rz_ratio,
            "ratio_score": ratio_score,
            "rz_buy": rz_buy,
            "rz_buy_score": buy_score,
            "risk_zone": zone,
        }

        LOG.info(
            f"[MarginFactor] score={score:.2f} trend={trend} accel={accel} ratio={rz_ratio} rz_buy={rz_buy}"
        )

        fr = FactorResult()
        fr.name = "margin"
        fr.score=round(score, 2)
        fr.desc=desc
        fr.detail = detail  
        return fr 
