# -*- coding: utf-8 -*-
# UnifiedRisk V12 - Unified Emotion BlockBuilder
#
# 作用：
# - 将 market_sentiment_raw 转换为“情绪结构块”
# - 不评分，不判断方向
# - 只输出被 factor 实际使用的字段（Pre-Stable）

from __future__ import annotations

from typing import Dict, Any

from core.adapters.block_builder.block_builder_base import FactBlockBuilderBase
from core.utils.logger import get_logger

LOG = get_logger("TR.UnifiedEmotion")


class UnifiedEmotionBlockBuilder(FactBlockBuilderBase):
    """
    UnifiedEmotionBlockBuilder
    -------------------------
    输入：
        snapshot["market_sentiment_raw"]

    输出：
        snapshot["unified_emotion"] = {
            market_internal: {...},
            meta: {...}
        }
    """

    def __init__(self):
        super().__init__(name="UnifiedEmotion")

    # -------------------------------------------------
    def build_block(
        self,
        snapshot: Dict[str, Any],
        refresh_mode: str = "none"
    ) -> Dict[str, Any]:

        sentiment = snapshot.get("market_sentiment_raw") or {}
        assert sentiment, "market_sentiment_raw is empty."
        if not sentiment:
            LOG.warning("[UnifiedEmotion] market_sentiment_raw is empty")
            return {}

        market_internal = self._build_market_internal(sentiment)

        return {
            "market_internal": market_internal,
            "meta": {
                "has_sentiment": True,
            },
        }

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
            "extreme_ratio": extreme_ratio,
        }
