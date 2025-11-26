"""聚合 A 股日级各个因子，形成 ashare_daily 统一得分。"""
from __future__ import annotations

from typing import Dict, Any, Tuple

from .index_risk import compute_index_risk
from .fund_flow import compute_fund_flow_risk
from .liquidity import compute_liquidity_risk
from .sentiment import compute_sentiment_risk
from .style_rotation import compute_style_rotation_risk
from .valuation import compute_valuation_risk


DEFAULT_WEIGHTS = {
    "index": 0.30,
    "fund_flow": 0.25,
    "liquidity": 0.15,
    "sentiment": 0.15,
    "style_rotation": 0.10,
    "valuation": 0.05,
}


def aggregate_ashare_daily_factors(raw: Dict[str, Any] | None = None,
                                   weights: Dict[str, float] | None = None
                                   ) -> Tuple[float, Dict[str, Any], Dict[str, str]]:
    """统一聚合 A 股日级各因子。

    返回：
    - total_score: 聚合后的总分（-3 ~ +3）
    - factors: 每个大类因子的 detail
    - comments: 每个大类因子的说明文字
    """
    w = dict(DEFAULT_WEIGHTS)
    if weights:
        w.update(weights)

    factors: Dict[str, Any] = {}
    comments: Dict[str, str] = {}

    idx_score, idx_detail, idx_comment = compute_index_risk(raw)
    ff_score, ff_detail, ff_comment = compute_fund_flow_risk(raw)
    liq_score, liq_detail, liq_comment = compute_liquidity_risk(raw)
    sent_score, sent_detail, sent_comment = compute_sentiment_risk(raw)
    style_score, style_detail, style_comment = compute_style_rotation_risk(raw)
    val_score, val_detail, val_comment = compute_valuation_risk(raw)

    factors["index"] = {"score": idx_score, "detail": idx_detail}
    factors["fund_flow"] = {"score": ff_score, "detail": ff_detail}
    factors["liquidity"] = {"score": liq_score, "detail": liq_detail}
    factors["sentiment"] = {"score": sent_score, "detail": sent_detail}
    factors["style_rotation"] = {"score": style_score, "detail": style_detail}
    factors["valuation"] = {"score": val_score, "detail": val_detail}

    comments["index"] = idx_comment
    comments["fund_flow"] = ff_comment
    comments["liquidity"] = liq_comment
    comments["sentiment"] = sent_comment
    comments["style_rotation"] = style_comment
    comments["valuation"] = val_comment

    total_score = (
        idx_score * w["index"]
        + ff_score * w["fund_flow"]
        + liq_score * w["liquidity"]
        + sent_score * w["sentiment"]
        + style_score * w["style_rotation"]
        + val_score * w["valuation"]
    )

    return total_score, factors, comments
