# core/adapters/transformers/cn/unified_emotion_tr.py
# UnifiedRisk V12 - Unified Emotion Transformer
#
# 作用：
# - 将 Turnover + MarketSentiment 整合为“情绪结构块”
# - 不评分，不判断方向
# - 输出供 Factor / Predictor 使用的标准化特征

from __future__ import annotations

from typing import Dict, Any

from core.adapters.transformers.transformer_base import TransformerBase
from core.utils.logger import get_logger

LOG = get_logger("TR.UnifiedEmotion")


class UnifiedEmotionTransformer(TransformerBase):
    """
    UnifiedEmotionTransformer
    -------------------------
    输入：
        snapshot["turnover"]
        snapshot["market_sentiment"]

    输出：
        snapshot["unified_emotion"] = {
            market_internal: {...},
            behavior: {...},
            meta: {...}
        }
    """

    def __init__(self):
        super().__init__(name="UnifiedEmotion")

    # -------------------------------------------------
    def transform(self, snapshot: Dict[str, Any], refresh_mode: str = "none") -> Dict[str, Any]:
        sentiment = snapshot.get("market_sentiment") or {}
        turnover = snapshot.get("turnover") or {}

        if not sentiment and not turnover:
            LOG.warning("[UnifiedEmotion] empty inputs")
            return {}

        market_internal = self._build_market_internal(sentiment)
        behavior = self._build_behavior(turnover)

        out = {
            "market_internal": market_internal,
            "behavior": behavior,
            "meta": {
                "has_sentiment": bool(sentiment),
                "has_turnover": bool(turnover),
            },
        }

        return out

    # -------------------------------------------------
    @staticmethod
    def _build_market_internal(sentiment: Dict[str, Any]) -> Dict[str, Any]:
        """
        市场内部结构（宽度 & 极端）
        """
        adv = int(sentiment.get("adv", 0))
        dec = int(sentiment.get("dec", 0))
        flat = int(sentiment.get("flat", 0))
        total = adv + dec + flat

        adv_ratio = sentiment.get("adv_ratio")
        if adv_ratio is None and total > 0:
            adv_ratio = round(adv / total, 4)

        limit_up = int(sentiment.get("limit_up", 0))
        limit_down = int(sentiment.get("limit_down", 0))

        extreme_ratio = None
        if total > 0:
            extreme_ratio = round((limit_up + limit_down) / total, 4)

        return {
            "adv": adv,
            "dec": dec,
            "flat": flat,
            "adv_ratio": adv_ratio,
            "limit_up": limit_up,
            "limit_down": limit_down,
            "extreme_ratio": extreme_ratio,  # 极端情绪占比
        }

    # -------------------------------------------------
    @staticmethod
    def _build_behavior(turnover: Dict[str, Any]) -> Dict[str, Any]:
        """
        行为因子（成交参与结构）
        """
        sh = float(turnover.get("sh", 0.0))
        sz = float(turnover.get("sz", 0.0))
        bj = float(turnover.get("bj", 0.0))
        total = float(turnover.get("total", 0.0))

        concentration = None
        if total > 0:
            max_part = max(sh, sz, bj)
            concentration = round(max_part / total, 4)

        return {
            "total": total,
            "sh": sh,
            "sz": sz,
            "bj": bj,
            "concentration": concentration,  # 成交是否集中
        }
