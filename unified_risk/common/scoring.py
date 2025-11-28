
from typing import Dict

def normalize_score(v: float, lo: float, hi: float) -> float:
    """线性映射到 [-3, 3]。"""
    if hi == lo:
        return 0.0
    r = (v - lo) / (hi - lo)
    r = max(0.0, min(1.0, r))
    return round(-3.0 + 6.0 * r, 2)


def aggregate_factor_scores(d: Dict[str, float]) -> float:
    if not d:
        return 0.0
    return round(sum(d.values()) / len(d), 2)


def classify_level_with_advice(score: float):
    """简单 demo 分级，可后续替换为更精细规则。"""
    if score <= -2:
        level = "较安全"
        desc = "系统性风险较低，偏多头环境。"
        advice = "可适度加仓，关注高景气板块。"
    elif score <= 0:
        level = "中性"
        desc = "多空力量均衡，指数以震荡为主。"
        advice = "控制节奏，精选个股，避免追高。"
    elif score <= 2:
        level = "偏高风险"
        desc = "空头力量增强，短期回调压力加大。"
        advice = "适当降低仓位，减少高波动标的敞口。"
    else:
        level = "高风险"
        desc = "情绪或资金有踩踏风险。"
        advice = "防守为主，必要时减仓或观望。"

    return {
        "risk_level": level,
        "risk_description": desc,
        "risk_advice": advice,
    }
