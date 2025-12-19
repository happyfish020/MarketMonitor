# -*- coding: utf-8 -*-
"""
UnifiedRisk V12
UnifiedEmotionFactor (CN A-Share)

职责：
- 基于 snapshot 的标准字段：
    - market_sentiment
    - emotion
- 计算综合情绪评分
- 暴露 _raw_data 用于报告校验
"""

from typing import Dict, Any
import json

from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult
 
from core.utils.logger import get_logger

LOG = get_logger("Factor.UnifiedEmotion")


class UnifiedEmotionFactor(FactorBase):
    """
    综合情绪因子（V12 定稿）
    """

    def __init__(self):
        super().__init__("unified_emotion_raw")

    # ------------------------------------------------------------------
    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        """
        输入：
            input_block["market_sentiment"]
            input_block["emotion"]

        输出：
            FactorResult(name="unified_emotion", ...)
        """

        market_sentiment = input_block.get("market_sentiment_raw") or {}
        emotion = input_block.get("emotion_raw") or {}
        # Todom
        # ---------------- 原始数据透传 ----------------
        raw_data = {
            "market_sentiment": market_sentiment,
            "emotion": emotion,
        }

        # ---------------- 兜底评分 ----------------
        score = 50.0
        level = "NEUTRAL"

        # ----------- 使用市场情绪（adv_ratio）-----------
        adv_ratio = market_sentiment.get("adv_ratio")

        try:
            if adv_ratio is not None:
                adv_ratio = float(adv_ratio)

                if adv_ratio >= 0.65:
                    score = 65.0
                    level = "HIGH"
                elif adv_ratio <= 0.35:
                    score = 35.0
                    level = "LOW"
        except Exception as e:
            LOG.warning(
                "[UnifiedEmotionFactor] invalid adv_ratio=%s (%s)",
                adv_ratio,
                e,
            )

        LOG.info(
            "[UnifiedEmotionFactor] score=%.2f level=%s adv_ratio=%s",
            score,
            level,
            adv_ratio,
        )

        return self.build_result(
            score=score,
            level=level,
            details={
                "data_status": "OK",
                "_raw_data": raw_data,
                "adv_ratio": adv_ratio,
            },
        )
