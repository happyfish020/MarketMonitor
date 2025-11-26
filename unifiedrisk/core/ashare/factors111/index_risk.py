"""A-share index risk factor scoring (实战版).

使用指数涨跌幅 + 成交额 + 三大指数是否共振
生成 [-3, +3] 的专业风险分数。
"""

from __future__ import annotations

from typing import Dict, Any, Tuple


def _score_single_index(pct: float) -> float:
    """根据指数日涨跌获得基础得分 [-2, +2]"""
    if pct >= 2.5:
        return 2.0
    if pct >= 1.5:
        return 1.5
    if pct >= 0.5:
        return 1.0
    if pct <= -2.5:
        return -2.0
    if pct <= -1.5:
        return -1.5
    if pct <= -0.5:
        return -1.0
    return 0.0


def compute_index_risk(raw: Dict[str, Any] | None = None) -> Tuple[float, Dict[str, float], str]:
    """计算指数层面的风险因子（指数强弱 + 成交额 + 三指数共振）。

    输入 raw schema:
    raw["ashare"]["index"] = {
        "sh": {"pct": float, "turnover": float},
        "sz": {"pct": float, "turnover": float},
        "cyb": {"pct": float, "turnover": float}
    }
    """
    if not raw or "ashare" not in raw or "index" not in raw["ashare"]:
        return 0.0, {}, "指数风险：无数据，默认中性。"

    idx = raw["ashare"]["index"]

    sh_pct = float(idx.get("sh", {}).get("pct", 0.0))
    sz_pct = float(idx.get("sz", {}).get("pct", 0.0))
    cyb_pct = float(idx.get("cyb", {}).get("pct", 0.0))

    sh_turn = float(idx.get("sh", {}).get("turnover", 0.0))
    sz_turn = float(idx.get("sz", {}).get("turnover", 0.0))
    cyb_turn = float(idx.get("cyb", {}).get("turnover", 0.0))

    total_turnover = sh_turn + sz_turn + cyb_turn

    # ===== 1) 价格信号 (60%) =====
    sh_score = _score_single_index(sh_pct)
    sz_score = _score_single_index(sz_pct)
    cyb_score = _score_single_index(cyb_pct)

    price_score = (sh_score + sz_score + cyb_score) / 3.0

    # ===== 2) 量能信号 (30%) =====
    # 这里使用一个静态阈值作为示例，你可以在 DataFetcher 中根据历史数据动态调整。
    HIGH_TURNOVER = 9000.0   # 单位：亿
    LOW_TURNOVER  = 7000.0

    if total_turnover >= HIGH_TURNOVER:
        volume_signal = 1.0       # 放量
    elif total_turnover <= LOW_TURNOVER:
        volume_signal = -0.5      # 缩量
    else:
        volume_signal = 0.0       # 正常

    if price_score > 0 and volume_signal > 0:
        liquidity_score = 1.0
    elif price_score < 0 and volume_signal > 0:
        liquidity_score = -1.5
    elif price_score > 0 and volume_signal < 0:
        liquidity_score = 0.5
    elif price_score < 0 and volume_signal < 0:
        liquidity_score = -0.5
    else:
        liquidity_score = 0.0

    # ===== 3) 一致性信号 (10%) =====
    if sh_pct > 0 and sz_pct > 0 and cyb_pct > 0:
        coherence_score = 0.5   # 三指数共涨
    elif sh_pct < 0 and sz_pct < 0 and cyb_pct < 0:
        coherence_score = -0.5  # 三指数共跌
    else:
        coherence_score = 0.0

    # ===== 总分 =====
    total = (
        price_score * 0.6 +
        liquidity_score * 0.3 +
        coherence_score * 0.1
    )

    # 限制在 [-3, +3]
    total = max(-3.0, min(3.0, total))

    comment = (
        f"指数风险：上证{sh_pct:.2f}%，深成{sz_pct:.2f}%，创业板{cyb_pct:.2f}%，"
        f"成交额约 {total_turnover:.0f} 亿。"
    )

    detail: Dict[str, float] = {
        "price_score": price_score,
        "liquidity_score": liquidity_score,
        "coherence_score": coherence_score,
        "sh_pct": sh_pct,
        "sz_pct": sz_pct,
        "cyb_pct": cyb_pct,
        "total_turnover": total_turnover,
    }
    return total, detail, comment
