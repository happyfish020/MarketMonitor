# -*- coding: utf-8 -*-
"""
UnifiedEmotionFactor (V12)
融合两层情绪：
  - MarketSentiment（广度情绪）
  - EmotionEngine（行为情绪）
"""

from __future__ import annotations
from typing import Dict, Any

from core.factors.base import BaseFactor
from core.models.factor_result import FactorResult
from core.factors.cn.market_sentiment_factor import MarketSentimentFactor
from core.factors.cn.emotion_engine import compute_cn_emotion_from_snapshot
from core.utils.logger import get_logger

LOG = get_logger("Factor.UnifiedEmotion")


class UnifiedEmotionFactor(BaseFactor):

    def __init__(self):
        #super().__init__("unified_emotion")
        self.name = "unified_emotion"
        self.ms_factor = MarketSentimentFactor()  # 广度层

    def compute(self, snapshot: Dict[str, Any]) -> FactorResult:

        LOG.info("[UnifiedEmotion] === Start Computation ===")

        # -------------------------
        # 1) 广度层 (MarketSentiment)
        # -------------------------
        fr_ms = self.ms_factor.compute(snapshot)

        ms_score = fr_ms.score
        ms_desc = fr_ms.desc
        ms_detail = fr_ms.detail

        LOG.info("[UnifiedEmotion] MarketSentiment score=%.2f", ms_score)

        # -------------------------
        # 2) 行为层 (EmotionEngine)
        # -------------------------
        emo_raw = compute_cn_emotion_from_snapshot(snapshot)
        emo_score = emo_raw.get("EmotionScore", 50)
        emo_level = emo_raw.get("EmotionLevel", "Neutral")

        LOG.info("[UnifiedEmotion] EmotionEngine score=%.2f level=%s",
                 emo_score, emo_level)

        # -------------------------
        # 3) 综合情绪得分
        # -------------------------
        # 可根据 predict_weights.yaml 调权重
        w_ms = 0.45  # market_sentiment 权重
        w_emo = 0.55  # emotion 行为情绪权重

        final_score = w_ms * ms_score + w_emo * emo_score
        final_score = round(final_score, 2)

        if final_score >= 75:
            final_desc = "情绪强势（市场情绪趋于 Risk-On）"
        elif final_score <= 35:
            final_desc = "情绪偏弱（市场情绪趋于 Risk-Off）"
        else:
            final_desc = "市场情绪中性偏震荡"

        # -------------------------
        # 4) 详细解释
        # -------------------------
        detail_lines = [
            "【一】市场广度层（MarketSentiment）",
            f"- score={ms_score:.2f}",
            f"- {ms_desc}",
            "",
            "【二】行为层（EmotionEngine）",
            f"- score={emo_score:.2f} | level={emo_level}",
            f"- Index={emo_raw['IndexLabel']}",
            f"- Volume={emo_raw['VolumeLabel']}",
            f"- Breadth={emo_raw['BreadthLabel']}",
            f"- Northbound={emo_raw['NorthLabel']}",
            f"- MainForce={emo_raw['MainForceLabel']}",
            f"- Derivative={emo_raw['DerivativeLabel']}",
            f"- LimitUp/Down={emo_raw['LimitLabel']}",
            "",
            "【三】综合解读",
            f"- 综合情绪得分：{final_score:.2f}",
            f"- 结论：{final_desc}",
        ]

        detail = "\n".join(detail_lines)

        # -------------------------
        # 5) 返回 FactorResult（松耦合）
        # -------------------------
        fr = FactorResult()
        fr.score = final_score
        fr.desc = final_desc
        fr.detail = detail

        LOG.info("[UnifiedEmotion] DONE final_score=%.2f", final_score)
        return fr
