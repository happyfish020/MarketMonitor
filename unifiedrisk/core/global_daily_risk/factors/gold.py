"""Gold factor scoring (黄金六因子的简化占位实现)."""
from __future__ import annotations

from typing import Dict, Any, Tuple
from .yf_utils import get_latest_change


def compute_gold_factor(raw: Dict[str, Any] | None = None) -> Tuple[float, Dict[str, float], str]:
    """Compute gold factor score.

    当前简化逻辑：
    - 黄金大涨通常对应避险情绪升温 → 对风险资产偏空
    - 黄金大跌在多数情况下对应风险偏好改善 → 略偏多
    """
    gold_pct, _ = get_latest_change("GC=F")

    if gold_pct >= 2.0:
        score = -1.5
        comment = "黄金大幅上涨，避险情绪升温，对权益风险偏好不利（简化判断）。"
    elif gold_pct <= -2.0:
        score = 1.0
        comment = "黄金明显回落，避险需求降温，利好风险资产（简化判断）。"
    else:
        score = 0.0
        comment = "黄金波动有限，对整体风险偏好影响中性（简化判断）。"

    detail: Dict[str, float] = {
        "gold_pct": gold_pct,
    }
    return score, detail, comment
