"""FX & global liquidity factor scoring.

主要处理：
- DXY（^DXY）
- CNH（CNH=F 或近似替代）
"""
from __future__ import annotations

from typing import Dict, Any, Tuple
from .yf_utils import get_latest_change


def _map_dxy_to_score(pct: float) -> float:
    """美元指数：上涨通常压制风险资产。"""
    if pct >= 1.0:
        return -2.0
    if pct >= 0.5:
        return -1.0
    if pct <= -1.0:
        return 1.5
    if pct <= -0.5:
        return 0.5
    return 0.0


def _map_cnh_to_score(pct: float) -> float:
    """CNH 这里用一个简化 proxy：

    我们假设 symbol 代表 USD/CNH：
    - 上涨 → 人民币走弱 → 通常对 A 股略偏空
    - 下跌 → 人民币走强 → 偏多
    """
    if pct >= 0.8:
        return -1.5
    if pct >= 0.3:
        return -0.5
    if pct <= -0.8:
        return 1.5
    if pct <= -0.3:
        return 0.5
    return 0.0


def compute_fx_liquidity_factor(raw: Dict[str, Any] | None = None) -> Tuple[float, Dict[str, float], str]:
    """Compute FX & liquidity factor score."""
    dxy_pct, _ = get_latest_change("^DXY")
    # CNH 代码在不同数据源下可能略有差异，这里用 CNH=F 作为近似
    cnh_pct, _ = get_latest_change("CNH=F")

    dxy_score = _map_dxy_to_score(dxy_pct)
    cnh_score = _map_cnh_to_score(cnh_pct)

    score = 0.6 * dxy_score + 0.4 * cnh_score

    if score > 0.5:
        comment = "汇率与流动性：美元偏弱、人民币偏稳，整体利好风险资产。"
    elif score < -0.5:
        comment = "汇率与流动性：美元走强或人民币承压，对 A 股风险偏好不利。"
    else:
        comment = "汇率与流动性：波动有限，对风险偏好影响中性。"

    detail: Dict[str, float] = {
        "dxy_pct": dxy_pct,
        "cnh_pct": cnh_pct,
        "dxy_score": dxy_score,
        "cnh_score": cnh_score,
    }
    return score, detail, comment
