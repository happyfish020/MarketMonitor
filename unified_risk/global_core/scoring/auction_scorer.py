from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, Optional


@dataclass
class AuctionSentiment:
    score: float       # -5 ~ +5
    level: str         # "强势竞价" / "弱势竞价" / "中性"
    desc: str          # 文本解释


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def score_auction_sentiment(raw: Dict[str, Any]) -> AuctionSentiment:
    """
    输入：auction_cache 中记录的一天的 raw snapshot（payload["raw"]）
    输出：竞价情绪分 + 文本说明

    评分逻辑（晨景 MorningView v4 风格）：
      - 上证 / 创业板竞价涨跌
      - adv/dec 市场广度
      - 流动性（510300 ETF）
      - 权重：指数 40%，广度 40%，流动性 20%
    """

    if not raw:
        return AuctionSentiment(
            score=0,
            level="中性",
            desc="暂无竞价数据（时间不在 09:15–09:25 或缓存缺失）"
        )

    index = raw.get("index", {})
    advdec = raw.get("advdec", {})
    liquidity = raw.get("liquidity", {}) or raw.get("liq_510300", {})

    sh = _safe_float(index.get("sh_change_pct"))
    cyb = _safe_float(index.get("cyb_change_pct"))
    adv = advdec.get("adv")
    dec = advdec.get("dec")

    vol_ratio = _safe_float(liquidity.get("volume_ratio"))
    today_vs_min = _safe_float(liquidity.get("today_vs_20d_min"))

    score = 0.0
    desc_list = []

    # ===== 1）指数竞价（上证 / 创业板） =====
    if sh is not None and cyb is not None:
        if sh > 0.3 and cyb > 0.3:
            score += 2.0
            desc_list.append("上证与创业板竞价同步偏强")
        elif sh < -0.3 and cyb < -0.3:
            score -= 2.0
            desc_list.append("上证与创业板竞价同步偏弱")
        else:
            desc_list.append("上证/创业板竞价表现分化")

    # ===== 2）市场广度（adv/dec） =====
    if adv is not None and dec is not None and (adv + dec) > 0:
        ratio = adv / max(1, adv + dec)
        if ratio >= 0.65:
            score += 2.0
            desc_list.append(f"上涨占比 {ratio:.1%}（竞价广度强）")
        elif ratio <= 0.35:
            score -= 2.0
            desc_list.append(f"上涨占比 {ratio:.1%}（竞价广度弱）")
        else:
            desc_list.append(f"上涨占比 {ratio:.1%}（广度中性）")

    # ===== 3）流动性（510300 ETF） =====
    if vol_ratio is not None:
        if vol_ratio > 1.8:
            score += 1.0
            desc_list.append("蓝筹资金偏积极（510300 成交量偏强）")
        elif vol_ratio < 0.6:
            score -= 1.0
            desc_list.append("蓝筹资金偏谨慎（510300 成交量偏弱）")

    # ===== 等级划分 =====
    if score >= 3:
        level = "强势竞价"
    elif score <= -3:
        level = "弱势竞价"
    else:
        level = "中性"

    desc = "；".join(desc_list) if desc_list else "竞价情绪中性"

    return AuctionSentiment(score=score, level=level, desc=desc)
