"""Global equity (stock index) factor scoring.

主要处理：
- 纳指 (^IXIC)
- 标普 SPY
- 粗略的全球股市风险偏好信号
"""
from __future__ import annotations

from typing import Dict, Any, Tuple
from .yf_utils import get_latest_change


def _map_index_pct_to_score(pct: float) -> float:
    """将指数日涨跌幅粗略映射到 [-2, 2] 区间。"""
    if pct >= 2.5:
        return 2.0
    if pct >= 1.0:
        return 1.0
    if pct <= -2.5:
        return -2.0
    if pct <= -1.0:
        return -1.0
    return 0.0


def compute_global_equity_factor(raw: Dict[str, Any] | None = None) -> Tuple[float, Dict[str, float], str]:
    """Compute global equity factor score.

    逻辑：
    - 纳指、标普分别打分，然后做加权平均：
      score = 0.6 * nas_score + 0.4 * spy_score
    """
    nas_pct, _ = get_latest_change("^IXIC")
    spy_pct, _ = get_latest_change("SPY")

    nas_score = _map_index_pct_to_score(nas_pct)
    spy_score = _map_index_pct_to_score(spy_pct)

    score = 0.6 * nas_score + 0.4 * spy_score

    if score > 0.5:
        comment = "全球股市：纳指 / 标普整体上涨，风险偏好偏强。"
    elif score < -0.5:
        comment = "全球股市：纳指 / 标普整体回落，风险偏好承压。"
    else:
        comment = "全球股市：涨跌有限，整体偏中性。"

    detail: Dict[str, float] = {
        "nasdaq_pct": nas_pct,
        "spy_pct": spy_pct,
        "nasdaq_score": nas_score,
        "spy_score": spy_score,
    }
    return score, detail, comment
