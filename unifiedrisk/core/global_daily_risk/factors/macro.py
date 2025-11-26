"""Global macro factor scoring for GlobalDailyRiskEngine.

当前实现：
- 使用美债 10Y (^TNX) 与 3M (^IRX) 收益率差作为简单宏观 proxy。
- 仅根据利差形态给出粗略风险偏好评分。
"""
from __future__ import annotations

from typing import Dict, Any, Tuple
from .yf_utils import get_latest_change


def compute_macro_factor(raw: Dict[str, Any] | None = None) -> Tuple[float, Dict[str, float], str]:
    """Compute macro factor score.

    逻辑：
    - 利用 ^TNX（10Y）与 ^IRX（短端）最近两日变化，估算收益率曲线形态。
    - 这里不使用绝对值，只做一个简单 regime 判断：
      - 利差深度倒挂 → 风险偏好受压（score 负）
      - 利差恢复 / 变陡 → 略偏多（score 正）
    """
    # 注意：这里我们只能拿到日度涨跌幅，无法直接拿到利差绝对值，
    # 因此实现为一个非常简化的占位版：
    tnx_pct, _ = get_latest_change("^TNX")
    irx_pct, _ = get_latest_change("^IRX")

    # 简单 proxy：若长端收益率涨幅远高于短端，则视为曲线变陡（偏多），反之则偏空
    spread_delta = tnx_pct - irx_pct

    if spread_delta > 3.0:
        score = 1.0
        comment = "宏观：长端利率涨幅显著高于短端，曲线略有变陡，利好风险资产（简化版判断）。"
    elif spread_delta < -3.0:
        score = -1.0
        comment = "宏观：短端利率涨幅高于长端，曲线趋于倒挂，对风险偏好略有压制（简化版判断）。"
    else:
        score = 0.0
        comment = "宏观：长短端利率变化相近，曲线信号中性（占位简化版）。"

    detail: Dict[str, float] = {
        "tnx_pct": tnx_pct,
        "irx_pct": irx_pct,
        "spread_delta": spread_delta,
    }
    return score, detail, comment
