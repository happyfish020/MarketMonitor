
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, Optional

from ..utils.logging_utils import setup_logger

logger = setup_logger(__name__)


@dataclass
class AShareDailyInputs:
    sh_change_pct: Optional[float]
    cyb_change_pct: Optional[float]
    adv_count: Optional[int]
    dec_count: Optional[int]
    liquidity: Dict[str, Any]


@dataclass
class AShareDailyScores:
    total_score: float
    risk_level: str
    emotion_score: float
    emotion_desc: str
    short_term_score: float
    short_term_desc: str
    mid_term_score: float
    mid_term_desc: str


def _score_emotion(sh, cyb, adv, dec, liq: Dict[str, Any]):
    score = 0.0
    parts = []

    if sh is not None:
        if sh > 1.0:
            score += 4
        elif sh > 0.3:
            score += 3
        elif sh > -0.3:
            score += 2
        else:
            score += 1
    if cyb is not None:
        if cyb > 1.5:
            score += 4
        elif cyb > 0.5:
            score += 3
        elif cyb > -0.5:
            score += 2
        else:
            score += 1

    if adv is not None and dec is not None and adv + dec > 0:
        adv_ratio = adv / (adv + dec)
        if adv_ratio > 0.65:
            score += 4
            parts.append(f"上涨家数明显占优（{adv_ratio:.2f}）")
        elif adv_ratio > 0.55:
            score += 3
            parts.append(f"上涨家数略占优（{adv_ratio:.2f}）")
        elif adv_ratio > 0.45:
            score += 2
            parts.append(f"涨跌家数均衡（{adv_ratio:.2f}）")
        else:
            score += 1
            parts.append(f"下跌家数占优（{adv_ratio:.2f}）")
    else:
        parts.append("涨跌家数数据缺失（未计入情绪分）")

    drying = bool(liq.get("liquidity_risk", False) or liq.get("drying", False))
    if drying:
        score -= 1
        parts.append("流动性偏弱")

    score = max(0.0, min(20.0, score))

    if score >= 14:
        desc = "情绪偏热"
    elif score >= 9:
        desc = "情绪中性偏暖"
    elif score >= 5:
        desc = "情绪中性偏冷"
    else:
        desc = "情绪偏冷"

    if parts:
        desc += "（" + "；".join(parts) + "）"
    return score, desc


def _score_short_term(core_etf_5d_change: float):
    v = core_etf_5d_change
    v_clamped = max(-3.0, min(3.0, v))
    score = (v_clamped + 3.0) / 6.0 * 20.0
    if score >= 14:
        desc = "短期趋势偏强"
    elif score >= 9:
        desc = "短期震荡偏强"
    elif score >= 5:
        desc = "短期震荡偏弱"
    else:
        desc = "短期趋势偏弱"
    return round(score, 1), desc


def _score_mid_term(sh_4w_change: float):
    v = sh_4w_change
    if v > 3.0:
        score = 16
        desc = "中期偏多头"
    elif v > 0.0:
        score = 12
        desc = "中期震荡偏多"
    elif v > -3.0:
        score = 8
        desc = "中期区间震荡"
    else:
        score = 4
        desc = "中期偏空头"
    return float(score), desc


def compute_ashare_daily_score(inputs: AShareDailyInputs,
                               core_etf_5d_change: float,
                               sh_4w_change: float) -> AShareDailyScores:
    emo_score, emo_desc = _score_emotion(
        inputs.sh_change_pct,
        inputs.cyb_change_pct,
        inputs.adv_count,
        inputs.dec_count,
        inputs.liquidity,
    )

    st_score, st_desc = _score_short_term(core_etf_5d_change)
    mt_score, mt_desc = _score_mid_term(sh_4w_change)

    total = emo_score + st_score + mt_score

    if total >= 60:
        level = "极低风险 / 积极进攻"
    elif total >= 45:
        level = "偏低风险 / 正常进攻"
    elif total >= 30:
        level = "中性风险 / 中性仓位"
    elif total >= 20:
        level = "偏高风险 / 谨慎"
    else:
        level = "高风险 / 防守为主"

    logger.info(
        "=== A-Share Daily Risk ===\n"
        f"[A股日级] 综合得分 {total:.1f}/100（{level}）\n"
        f"- 情绪：市场情绪得分 {emo_score:.1f} / 20，{emo_desc}。\n"
        f"- 短期：核心ETF近5日平均涨跌 {core_etf_5d_change:.2f}%，短期趋势：{st_desc}（得分 {st_score:.1f}/20）。\n"
        f"- 中期：上证近4周平均涨跌 {sh_4w_change:.2f}%，判定为「{mt_desc}」，中期得分 {mt_score:.1f}/20。"
    )

    return AShareDailyScores(
        total_score=total,
        risk_level=level,
        emotion_score=emo_score,
        emotion_desc=emo_desc,
        short_term_score=st_score,
        short_term_desc=st_desc,
        mid_term_score=mt_score,
        mid_term_desc=mt_desc,
    )
