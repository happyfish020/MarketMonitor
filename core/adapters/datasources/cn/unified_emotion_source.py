# core/adapters/datasources/cn/unified_emotion_source.py
from __future__ import annotations
from typing import Dict, Any

from core.adapters.datasources.cn.emotion_source import EmotionDataSource
from core.adapters.datasources.cn.market_sentiment_source import MarketSentimentDataSource
from core.utils.logger import get_logger

LOG = get_logger("DS.UnifiedEmotion")


class UnifiedEmotionDataSource:
    """
    V12 推荐：组合 EmotionDataSource + MarketSentimentDataSource 的结果，
    提供 snapshot-friendly 的 unified emotion block。
    """

    def __init__(self, trade_date: str):
        self.sentiment_ds = MarketSentimentDataSource(trade_date)
        self.emotion_ds = EmotionDataSource()

    def get_blocks(self, snapshot: Dict[str, Any], refresh_mode: str):
        """
        返回：
            sentiment_block
            emotion_block
        """

        sentiment_block = self.sentiment_ds.get_sentiment_block(refresh_mode)
        snapshot["sentiment"] = sentiment_block   # 原样注入 snapshot

        emotion_block = self.emotion_ds.get_block(snapshot)
        snapshot["emotion"] = emotion_block

        LOG.info("[DS.UnifiedEmotion] emotion=%s", emotion_block)
        LOG.info("[DS.UnifiedEmotion] sentiment=%s", sentiment_block)

        return sentiment_block, emotion_block
