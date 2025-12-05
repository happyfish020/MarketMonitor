# -*- coding: utf-8 -*-
"""
EmotionEngine for A-share market (UnifiedRisk V11)
计算七大情绪指标并输出情绪总分与等级
"""

from __future__ import annotations
from typing import Dict, Any


def _level_from_score(score: float) -> str:
    """情绪等级"""
    if score < 20:
        return "Panic"
    elif score < 40:
        return "Risk-Off"
    elif score < 60:
        return "Neutral"
    elif score < 80:
        return "Risk-On"
    return "Euphoria"


def compute_cn_emotion_from_snapshot(snap: Dict[str, Any]) -> Dict[str, Any]:
    """
    snap 需要字段：
        index_pct
        volume_change_pct
        breadth_adv
        breadth_total
        north_net_flow
        main_force_net_flow
        futures_basis_pct
        ivx_change_pct
        limit_up_count
        limit_down_count
    """

    # ========== 1) 指数 ==========
    idx = snap.get("index_pct", 0)
    if idx <= -2.5: idx_s, idx_l = 10, "Panic：急跌"
    elif idx <= -1.0: idx_s, idx_l = 30, "Risk-Off：明显下跌"
    elif idx < 0.8: idx_s, idx_l = 50, "Neutral：震荡"
    elif idx < 1.8: idx_s, idx_l = 70, "Risk-On：上涨"
    else: idx_s, idx_l = 90, "Euphoria：大涨"

    # ========== 2) 成交量 ==========
    vol = snap.get("volume_change_pct", 0)
    if vol >= 30:
        vol_s = 90 if idx > 0 else 10
        vol_l = "放量上涨" if idx > 0 else "放量下跌"
    elif 10 <= vol < 30:
        vol_s = 70 if idx > 0 else 30
        vol_l = "放量上涨" if idx > 0 else "放量下跌"
    elif -10 <= vol <= 10:
        vol_s, vol_l = 50, "中性"
    elif vol < -20:
        vol_s, vol_l = 35, "缩量 → 冷淡"
    else:
        vol_s, vol_l = 45, "偏弱"

    # ========== 3) breadth ==========
    adv = snap.get("breadth_adv", 0)
    total = snap.get("breadth_total", 1)
    adv_ratio = adv / total

    if adv < 700: br_s, br_l = 10, "Panic：全面下跌"
    elif adv < 1400: br_s, br_l = 30, "Risk-Off：下跌占优"
    elif adv < 2200: br_s, br_l = 50, "Neutral"
    elif adv < 3300: br_s, br_l = 70, "Risk-On：上涨占优"
    else: br_s, br_l = 90, "Euphoria：普涨"

    # ========== 4) 北向 ==========
    north = snap.get("north_net_flow", 0)
    if north <= -120: nf_s, nf_l = 10, "Panic：大幅流出"
    elif north <= -30: nf_s, nf_l = 35, "Risk-Off：流出"
    elif -15 <= north <= 15: nf_s, nf_l = 50, "Neutral：观望"
    elif 20 <= north <= 80: nf_s, nf_l = 70, "Risk-On：流入"
    else: nf_s, nf_l = (90, "Euphoria：强流入") if north > 100 else (55, "略偏多")

    # ========== 5) 主力 ==========
    mf = snap.get("main_force_net_flow", 0)
    if mf <= -300: mf_s, mf_l = 15, "Panic：主力出逃"
    elif mf <= -100: mf_s, mf_l = 35, "Risk-Off"
    elif -30 <= mf <= 30: mf_s, mf_l = 50, "Neutral"
    elif 50 <= mf <= 150: mf_s, mf_l = 70, "Risk-On"
    else: mf_s, mf_l = (90, "Euphoria") if mf > 150 else (55, "略偏多")

    # ========== 6) 衍生品 ==========
    basis = snap.get("futures_basis_pct", 0)
    ivx = snap.get("ivx_change_pct", 0)

    # 升贴水
    if basis <= -1.2: b_s = 10
    elif basis < -0.5: b_s = 35
    elif basis < 0.5: b_s = 50
    elif basis < 1.0: b_s = 70
    else: b_s = 90

    # 波动率
    if ivx >= 15: iv_s = 10
    elif ivx >= 5: iv_s = 35
    elif ivx > -5: iv_s = 50
    elif ivx > -10: iv_s = 70
    else: iv_s = 90

    der_s = (b_s + iv_s) // 2
    der_l = f"期指={basis:.2f}%，iVX={ivx:.2f}%"

    # ========== 7) 涨跌停 ==========
    up = snap.get("limit_up_count", 0)
    dn = snap.get("limit_down_count", 0)

    if dn > 80: lim_s, lim_l = 10, "Panic：大面积跌停"
    elif dn > 20: lim_s, lim_l = 35, "Risk-Off"
    elif up > 100: lim_s, lim_l = 90, "Euphoria：涨停潮"
    elif 40 <= up <= 80: lim_s, lim_l = 70, "Risk-On"
    else: lim_s, lim_l = 50, "Neutral"

    # ========== 综合得分 ==========
    score = (
        0.10*idx_s + 0.15*vol_s + 0.20*br_s +
        0.15*nf_s + 0.15*mf_s + 0.15*der_s + 0.10*lim_s
    )

    level = _level_from_score(score)

    return {
        "EmotionScore": round(score, 2),
        "EmotionLevel": level,

        "IndexScore": idx_s,
        "VolumeScore": vol_s,
        "BreadthScore": br_s,
        "NorthboundScore": nf_s,
        "MainForceScore": mf_s,
        "DerivativeScore": der_s,
        "LimitScore": lim_s,

        "IndexLabel": idx_l,
        "VolumeLabel": vol_l,
        "BreadthLabel": br_l,
        "NorthLabel": nf_l,
        "MainForceLabel": mf_l,
        "DerivativeLabel": der_l,
        "LimitLabel": lim_l,

        "raw": snap,
    }
