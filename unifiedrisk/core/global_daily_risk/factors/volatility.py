"""Volatility regime factor scoring (VIX)."""
from __future__ import annotations

from typing import Dict, Any, Tuple
from .yf_utils import get_latest_change


def compute_volatility_factor(raw: Dict[str, Any] | None = None) -> Tuple[float, Dict[str, float], str]:
    """Compute volatility-based factor score using VIX level.

    使用 VIX 收盘价的粗略 regime：
    - < 14   → 低波动（略偏多）
    - 14-20  → 正常波动（中性）
    - 20-25  → 偏高波动（略偏空）
    - 25-30  → 高波动（偏空）
    - >= 30  → 恐慌（强烈偏空）
    """
    # 这里用 get_latest_change 仅为拿到 last price（pct 用不到）
    _, last = get_latest_change("^VIX")
    vix = last

    if vix == 0.0:
        score = 0.0
        comment = "波动率：暂无法获取 VIX，默认中性。"
    elif vix < 14.0:
        score = 0.8
        comment = f"波动率：VIX≈{vix:.1f}，处于低波动 regime，利好风险资产。"
    elif vix < 20.0:
        score = 0.0
        comment = f"波动率：VIX≈{vix:.1f}，处于正常波动区间。"
    elif vix < 25.0:
        score = -0.7
        comment = f"波动率：VIX≈{vix:.1f}，波动有所放大，对风险偏好略有压制。"
    elif vix < 30.0:
        score = -1.5
        comment = f"波动率：VIX≈{vix:.1f}，处于高波动区间，对风险资产不利。"
    else:
        score = -3.0
        comment = f"波动率：VIX≈{vix:.1f}，接近或进入恐慌区，需高度警惕系统性风险。"

    detail: Dict[str, float] = {
        "vix": vix,
    }
    return score, detail, comment
