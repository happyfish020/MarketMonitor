"""A 股风格与板块轮动因子实战骨架."""

from __future__ import annotations

from typing import Dict, Any, Tuple


def compute_style_rotation_risk(raw: Dict[str, Any] | None = None) -> Tuple[float, Dict[str, float], str]:
    """计算风格 / 板块轮动相关的风险 / 机会因子。

    预期 raw schema:

    raw["ashare"]["style"] = {
        "large_small": float,        # 大盘 vs 小盘 强弱比（>0 大盘强，<0 小盘强）
        "value_growth": float,       # 价值 vs 成长 强弱比（>0 价值强，<0 成长强）
        "sector_strength": {         # 各行业相对强弱（可选）
            "tech": float,
            "energy": float,
            "finance": float,
            "consumption": float,
            "manufacturing": float,
        }
    }
    """
    if not raw or "ashare" not in raw or "style" not in raw["ashare"]:
        return 0.0, {}, "风格轮动：无数据，默认中性。"

    st = raw["ashare"]["style"]

    large_small = float(st.get("large_small", 0.0))
    value_growth = float(st.get("value_growth", 0.0))
    sector_strength = st.get("sector_strength", {}) or {}

    # === 1) 大小盘结构 ===
    # 偏小盘上涨通常对应风险偏好较强；大盘防御主导则偏谨慎
    if large_small <= -0.02:
        ls_score = 1.0   # 小盘明显强于大盘，风险偏好较高
    elif large_small <= -0.005:
        ls_score = 0.5
    elif large_small >= 0.02:
        ls_score = -0.8  # 大盘明显占优，偏防御
    elif large_small >= 0.005:
        ls_score = -0.3
    else:
        ls_score = 0.0

    # === 2) 价值/成长风格 ===
    # 成长相对占优时，往往情绪更偏进攻；价值占优则偏防御。
    if value_growth <= -0.02:
        vg_score = 0.8   # 成长明显强于价值
    elif value_growth <= -0.005:
        vg_score = 0.3
    elif value_growth >= 0.02:
        vg_score = -0.6  # 价值显著占优
    elif value_growth >= 0.005:
        vg_score = -0.2
    else:
        vg_score = 0.0

    # === 3) 主线行业是否清晰 ===
    # 如果 tech / consumption / manufacturing 等有明显正向强度，认为主线相对清晰。
    tech = float(sector_strength.get("tech", 0.0))
    cons = float(sector_strength.get("consumption", 0.0))
    manu = float(sector_strength.get("manufacturing", 0.0))

    # 简化：若多个核心赛道同时为正，则视为有明确主线。
    positive_sectors = sum(1 for v in [tech, cons, manu] if v > 0.01)

    if positive_sectors >= 2:
        sector_score = 0.7
    elif positive_sectors == 1:
        sector_score = 0.3
    else:
        sector_score = 0.0

    total = ls_score * 0.4 + vg_score * 0.3 + sector_score * 0.3
    total = max(-3.0, min(3.0, total))

    comment = (
        f"风格轮动：大盘相对小盘强弱 {large_small:.3f}，价值相对成长 {value_growth:.3f}，"
        f"核心赛道正向个数 {positive_sectors}。"
    )

    detail: Dict[str, float] = {
        "ls_score": ls_score,
        "vg_score": vg_score,
        "sector_score": sector_score,
        "large_small": large_small,
        "value_growth": value_growth,
        "tech_strength": tech,
        "consumption_strength": cons,
        "manufacturing_strength": manu,
    }
    return total, detail, comment
