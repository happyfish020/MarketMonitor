"""Aggregate all global daily factors into a single score."""
from typing import Dict, Any, Tuple

from .macro import compute_macro_factor
from .global_equity import compute_global_equity_factor
from .commodity import compute_commodity_factor
from .gold import compute_gold_factor
from .fx_liquidity import compute_fx_liquidity_factor
from .volatility import compute_volatility_factor


DEFAULT_WEIGHTS = {
    "macro": 0.25,
    "global_equity": 0.25,
    "commodity": 0.15,
    "gold": 0.10,
    "fx_liquidity": 0.15,
    "volatility": 0.10,
}


def aggregate_global_daily_factors(raw: Dict[str, Any] | None = None,
                                   weights: Dict[str, float] | None = None
                                   ) -> Tuple[float, Dict[str, Any], Dict[str, str]]:
    """统一聚合外盘日级所有因子。

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

    macro_score, macro_detail, macro_comment = compute_macro_factor(raw)
    equity_score, equity_detail, equity_comment = compute_global_equity_factor(raw)
    comm_score, comm_detail, comm_comment = compute_commodity_factor(raw)
    gold_score, gold_detail, gold_comment = compute_gold_factor(raw)
    fx_score, fx_detail, fx_comment = compute_fx_liquidity_factor(raw)
    vol_score, vol_detail, vol_comment = compute_volatility_factor(raw)

    factors["macro"] = {"score": macro_score, "detail": macro_detail}
    factors["global_equity"] = {"score": equity_score, "detail": equity_detail}
    factors["commodity"] = {"score": comm_score, "detail": comm_detail}
    factors["gold"] = {"score": gold_score, "detail": gold_detail}
    factors["fx_liquidity"] = {"score": fx_score, "detail": fx_detail}
    factors["volatility"] = {"score": vol_score, "detail": vol_detail}

    comments["macro"] = macro_comment
    comments["global_equity"] = equity_comment
    comments["commodity"] = comm_comment
    comments["gold"] = gold_comment
    comments["fx_liquidity"] = fx_comment
    comments["volatility"] = vol_comment

    total_score = (
        macro_score * w["macro"]
        + equity_score * w["global_equity"]
        + comm_score * w["commodity"]
        + gold_score * w["gold"]
        + fx_score * w["fx_liquidity"]
        + vol_score * w["volatility"]
    )

    return total_score, factors, comments
