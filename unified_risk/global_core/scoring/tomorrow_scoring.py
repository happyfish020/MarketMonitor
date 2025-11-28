from __future__ import annotations

from typing import Optional
from .macro_scoring import GlobalMacroSnapshot, GlobalMacroScore


def compute_tomorrow_risk(
    macro_snap: GlobalMacroSnapshot,
    macro_score: GlobalMacroScore,
    sh_change: Optional[float],
    cyb_change: Optional[float],
) -> dict:
    """
    简化版明日风险偏好：
    - 宏观分数为主
    - 今日 A 股是否为“杀跌日”修正
    """

    base = macro_score.total_score  # 0-100
    reasons = [f"宏观环境：{macro_score.risk_level}（{macro_score.description}）"]

    # 今日 A 股是否杀跌
    kill_day = False
    if sh_change is not None and sh_change < -2.0:
        base -= 8
        kill_day = True
        reasons.append(f"上证今日大跌 {sh_change:.2f}%")
    if cyb_change is not None and cyb_change < -3.0:
        base -= 6
        kill_day = True
        reasons.append(f"创业板今日大跌 {cyb_change:.2f}%")

    # 如果今天是普涨，则略微提高明日风险偏好（但也可能透支）
    if sh_change is not None and sh_change > 2.0:
        base += 3
        reasons.append(f"上证今日大涨 {sh_change:.2f}%，短期风险偏好提升但需防次日冲高回落")

    base = max(0.0, min(100.0, base))

    if base >= 70:
        level = "明日偏乐观"
        view = "整体环境偏多，明日 A 股偏向风险偏好提高，可适度进攻。"
        prob = 0.65
    elif base >= 55:
        level = "明日中性偏乐观"
        view = "整体环境中性略偏多，明日 A 股大概率震荡偏强。"
        prob = 0.55
    elif base >= 40:
        level = "明日中性偏谨慎"
        view = "整体环境偏紧，明日 A 股以震荡为主，控制仓位，选择性参与。"
        prob = 0.45
    elif base >= 25:
        level = "明日偏悲观"
        view = "整体环境较弱，明日 A 股存在进一步回调风险，建议防守为主。"
        prob = 0.35
    else:
        level = "明日高风险预警"
        view = "宏观 + 今日杀跌叠加，明日 A 股存在二次杀跌或恐慌盘，强烈建议降低仓位。"
        prob = 0.25

    if kill_day:
        view += "（今日已出现明显杀跌，注意反弹节奏与二次探底风险。）"

    return {
        "score": base,
        "level": level,
        "view": view,
        "probability": prob,
        "reason": "；".join(reasons),
        "today_sh_change": sh_change,
        "today_cyb_change": cyb_change,
    }
