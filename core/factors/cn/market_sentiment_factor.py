# core/factors/cn/market_sentiment_factor.py

from typing import Dict, Any

from core.factors.base import BaseFactor
from core.models.factor_result import FactorResult
from core.utils.logger import get_logger

LOG = get_logger("Factor.MarketSentiment")


def _safe_float(v, default=0.0):
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


class MarketSentimentFactor(BaseFactor):
    """
    市场宽度 / 情绪因子：
      - 涨跌家数
      - adv/dec 比例
    与 EmotionFactor 区分：
      - 这里更偏「广度、参与度」的定量描述
    """

    def compute(self, snapshot: Dict[str, Any]) -> FactorResult:
        sentiment = snapshot.get("sentiment", {}) or {}
        spot = snapshot.get("spot", {}) or {}

        adv = _safe_float(sentiment.get("adv", spot.get("adv")))
        dec = _safe_float(sentiment.get("dec", spot.get("dec")))
        ratio = _safe_float(sentiment.get("ratio"))

        LOG.info(
            "Compute MarketSentiment: adv=%.0f dec=%.0f ratio=%.2f",
            adv, dec, ratio
        )

        total = adv + dec
        if total <= 0:
            adv_ratio = 0.5
        else:
            adv_ratio = adv / total

        # 把 adv_ratio 映射到 0-100
        score = 50 + (adv_ratio - 0.5) * 100
        score = max(0.0, min(100.0, score))

        if score >= 70:
            desc = "上涨家数明显占优，市场广度偏多"
        elif score <= 30:
            desc = "下跌家数明显占优，市场广度偏空"
        else:
            desc = "市场涨跌家数相对均衡"

        detail_lines = [
            f"上涨家数：{adv:.0f}；下跌家数：{dec:.0f}",
            f"adv_ratio={adv_ratio:.2f}；原始 sentiment.ratio={ratio:.2f}",
        ]
        detail = "\n".join(detail_lines)

        LOG.info("MarketSentimentFactor: score=%.2f desc=%s", score, desc)
        fr = FactorResult()
        fr.score=score 
        fr.desc=desc
        fr.detail=detail
        return fr 
        
