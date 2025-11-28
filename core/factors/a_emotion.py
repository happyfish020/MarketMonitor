from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class EmotionResult:
    score: float
    level: str
    description: str
    raw: Dict[str, Any]

def compute_a_emotion(snapshot: Dict[str, Any]) -> EmotionResult:
    """日级情绪因子（简化版 MorningView 风格）。

    目前综合了：
      - 涨跌家数（breadth）
      - 指数涨跌（SH / CYB）
      - 流动性枯竭信号
    后续可以继续加入：涨停 / 炸板 / 连板等。
    """
    advdec = snapshot.get("advdec") or {}
    index = snapshot.get("index") or {}
    liq = snapshot.get("liquidity") or {}

    adv = advdec.get("advance", 0) or 0
    dec = advdec.get("decline", 0) or 0
    total = max(adv + dec, 1)
    breadth = (adv - dec) / total  # -1~+1

    sh = index.get("sh_change", 0.0) or 0.0
    cyb = index.get("cyb_change", 0.0) or 0.0

    # 1) breadth 映射到 0~10 分
    breadth_score = max(min((breadth + 1) * 5, 10), 0)

    # 2) 指数涨跌映射到 0~8 分（CYB 权重略高）
    idx_mix = sh * 0.4 + cyb * 0.6  # 单位：%
    idx_mix = max(min(idx_mix, 4.0), -4.0)  # 截断 [-4,4]
    index_score = (idx_mix + 4.0) / 8.0 * 8.0  # 映射到 0~8

    # 3) 流动性枯竭惩罚
    liq_risk = liq.get("liquidity_risk", False)
    liq_penalty = 2.0 if liq_risk else 0.0

    score = max(min(breadth_score + index_score - liq_penalty, 20), 0)

    if score >= 15:
        level = "情绪亢奋（高风险）"
    elif score >= 8:
        level = "情绪偏热"
    elif score >= 4:
        level = "情绪平稳"
    else:
        level = "情绪低迷（防御）"

    desc = (
        f"市场情绪得分 {score:.1f} / 20，{level}，"
        f"上涨 {adv} 家，下跌 {dec} 家，"
        f"上证 {sh:.2f}%，创业板 {cyb:.2f}%，"
        f"流动性枯竭：{'是' if liq_risk else '否'}。"
    )

    raw = {
        "breadth": breadth,
        "adv": adv,
        "dec": dec,
        "sh": sh,
        "cyb": cyb,
        "liq_risk": liq_risk,
        "breadth_score": breadth_score,
        "index_score": index_score,
        "liq_penalty": liq_penalty,
    }
    return EmotionResult(score=score, level=level, description=desc, raw=raw)
