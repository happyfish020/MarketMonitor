"""Commodity factor scoring.

主要处理：
- 原油 (CL=F)
- 铜 (HG=F)
作为全球需求 / 通胀 proxy 的粗略信号。
"""
from __future__ import annotations

from typing import Dict, Any, Tuple
from .yf_utils import get_latest_change


def _map_change_to_score(pct: float, positive_is_good: bool = True) -> float:
    """简单将涨跌幅映射到 [-2, 2]。

    positive_is_good:
      - True: 上涨视为利好风险资产
      - False: 上涨视为利空风险资产
    """
    if pct >= 3.0:
        base = 2.0
    elif pct >= 1.5:
        base = 1.0
    elif pct <= -3.0:
        base = -2.0
    elif pct <= -1.5:
        base = -1.0
    else:
        base = 0.0
    return base if positive_is_good else -base


def compute_commodity_factor(raw: Dict[str, Any] | None = None) -> Tuple[float, Dict[str, float], str]:
    """Compute commodity factor score.

    逻辑：
    - 铜上涨 → 视作全球需求改善（正向）
    - 油价大涨 → 通胀压力 / 成本上升，一般视作略偏空
    """
    crude_pct, _ = get_latest_change("CL=F")
    copper_pct, _ = get_latest_change("HG=F")

    copper_score = _map_change_to_score(copper_pct, positive_is_good=True)
    crude_score = _map_change_to_score(crude_pct, positive_is_good=False)

    score = 0.6 * copper_score + 0.4 * crude_score

    if score > 0.5:
        comment = "大宗商品：铜价偏强且油价压力可控，整体利好风险资产。"
    elif score < -0.5:
        comment = "大宗商品：油价走强或铜价走弱，对风险资产略有压制。"
    else:
        comment = "大宗商品：整体波动有限，对风险偏好影响中性。"

    detail: Dict[str, float] = {
        "crude_pct": crude_pct,
        "copper_pct": copper_pct,
        "crude_score": crude_score,
        "copper_score": copper_score,
    }
    return score, detail, comment
