# core/factors/cn/turnover_factor.py
# -*- coding: utf-8 -*-

"""
UnifiedRisk V12 - TurnoverFactor（成交额因子）

设计原则：
- 只读取 snapshot["turnover"]，完全松耦合，不访问任何数据源
- 使用 TurnoverSource 预处理好的：
    · total
    · trend_5d / trend_10d
    · percentile
    · zscore
    · zone
- 输出 FactorResult(score, desc, detail)，供统一评分和预测引擎使用
"""

from typing import Dict, Any

from core.factors.base import BaseFactor
from core.models.factor_result import FactorResult
from core.utils.logger import get_logger

LOG = get_logger("Factor.Turnover")


def _safe_float(v, default=None):
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


class TurnoverFactor(BaseFactor):
    """
    成交额因子（流动性因子）：

    score 逻辑（当前版本）：
        - 基础得分：由成交额相对 60 日分位 percentile 决定
            · percentile=0.0 → 20 分
            · percentile=0.5 → 50 分
            · percentile=1.0 → 80 分
        - 趋势修正：结合 5/10 日成交额趋势（trend_5d / trend_10d）
        - 最终分数限制在 [0, 100]
    """

    name = "turnover"

    def compute(self, snapshot: Dict[str, Any]) -> FactorResult:
        data = snapshot.get("turnover") or {}

        sh = _safe_float(data.get("sh"), 0.0)
        sz = _safe_float(data.get("sz"), 0.0)
        bj = _safe_float(data.get("bj"), 0.0)
        total = _safe_float(data.get("total"), sh + sz)

        trend_5d = _safe_float(data.get("trend_5d"), None)
        trend_10d = _safe_float(data.get("trend_10d"), None)
        percentile = _safe_float(data.get("percentile"), None)
        zscore = _safe_float(data.get("zscore"), None)
        zone = (data.get("zone") or "").strip() or "中性"

        LOG.info(
            "TurnoverFactor input: sh=%.2f sz=%.2f bj=%.2f total=%.2f trend5=%s trend10=%s pct=%s z=%s zone=%s",
            sh,
            sz,
            bj,
            total,
            trend_5d,
            trend_10d,
            percentile,
            zscore,
            zone,
        )

        # ---------------- 基础分：成交额相对 60 日分位 ----------------
        if percentile is not None:
            # 分位限制在 [0, 1]
            p = max(0.0, min(1.0, percentile))
            # 20 ~ 80 之间线性插值
            base_score = 20.0 + 60.0 * p
        else:
            # 没有历史分位数据时，退化为旧版区间逻辑
            if total <= 3000:
                base_score = 30.0
            elif total <= 7000:
                base_score = 50.0
            elif total <= 10000:
                base_score = 65.0
            else:
                base_score = 80.0

        # ---------------- 趋势修正：5/10 日成交额趋势 ----------------
        def _norm(v: float, cap: float) -> float:
            if v is None or cap <= 0:
                return 0.0
            if v > cap:
                v = cap
            if v < -cap:
                v = -cap
            return v / cap  # [-1, 1]

        n5 = _norm(trend_5d, 3000.0)    # 5 日内 ±3000 亿 视为完整区间
        n10 = _norm(trend_10d, 5000.0)  # 10 日内 ±5000 亿

        # 放量（trend>0） → 加分，缩量 → 减分
        trend_score = 5.0 * n5 + 5.0 * n10

        score = base_score + trend_score
        score = max(0.0, min(100.0, score))

        # ---------------- 文案：主描述 ----------------
        if score >= 75:
            desc = "沪深北成交显著放大，流动性强，做多资金和博弈资金较为活跃"
        elif score >= 60:
            desc = "沪深北成交偏强，流动性良好，市场参与度较高"
        elif score <= 30:
            desc = "沪深北成交显著缩量，流动性偏弱，需警惕无量下跌或阴跌风险"
        elif score <= 40:
            desc = "沪深北成交偏弱，市场观望情绪较重，资金参与意愿不高"
        else:
            desc = "沪深北成交处于中性区间，整体流动性尚可"

        # ---------------- 文案：细节 ----------------
        # 1) 分位文本
        if percentile is not None:
            p_pct = percentile * 100.0
            if p_pct >= 80:
                p_text = f"{p_pct:.1f}%（历史偏高水平）"
            elif p_pct >= 60:
                p_text = f"{p_pct:.1f}%（略高于均值）"
            elif p_pct <= 20:
                p_text = f"{p_pct:.1f}%（历史偏低水平）"
            elif p_pct <= 40:
                p_text = f"{p_pct:.1f}%（略低于均值）"
            else:
                p_text = f"{p_pct:.1f}%（接近历史中值）"
        else:
            p_text = "暂无分位数据（仅基于单日总量判断）"

        # 2) 趋势文本
        def _trend_text(v: float, label: str) -> str:
            if v is None:
                return f"{label}：数据缺失"
            if v > 1500:
                return f"{label}：+{v:.0f} 亿（明显放量）"
            if v > 500:
                return f"{label}：+{v:.0f} 亿（温和放量）"
            if v > 100:
                return f"{label}：+{v:.0f} 亿（轻微放量）"
            if v < -1500:
                return f"{label}：{v:.0f} 亿（明显缩量）"
            if v < -500:
                return f"{label}：{v:.0f} 亿（温和缩量）"
            if v < -100:
                return f"{label}：{v:.0f} 亿（轻微缩量）"
            return f"{label}：{v:.0f} 亿（相对平稳）"

        trend5_text = _trend_text(trend_5d, "5 日成交额趋势")
        trend10_text = _trend_text(trend_10d, "10 日成交额趋势")

        # 3) 异动指数文本（z-score）
        if zscore is not None:
            z = zscore
            if z > 2.5:
                z_text = f"{z:.2f}（极端放量，需警惕情绪性交易）"
            elif z > 1.5:
                z_text = f"{z:.2f}（明显放量，有资金集中博弈迹象）"
            elif z < -2.5:
                z_text = f"{z:.2f}（极端缩量，交易极度清淡）"
            elif z < -1.5:
                z_text = f"{z:.2f}（明显缩量，资金大幅收缩）"
            else:
                z_text = f"{z:.2f}（无显著异动）"
        else:
            z_text = "暂无（缺少足够历史样本）"

        detail_lines = [
            f"上证成交额：{sh:.2f} 亿",
            f"深证成交额：{sz:.2f} 亿",
            f"北交成交额：{bj:.2f} 亿",
            f"总成交额：{total:.2f} 亿",
            f"成交额相对近 60 日分位：{p_text}",
            trend5_text,
            trend10_text,
            f"成交额异动指数（zscore）：{z_text}",
            f"流动性区间：{zone}",
        ]

        detail = "\n".join(detail_lines)

        LOG.info("TurnoverFactor output: score=%.2f desc=%s", score, desc)
        fr = FactorResult()
        fr.score=score
        fr.desc=desc
        fr.detail=detail
        return fr
