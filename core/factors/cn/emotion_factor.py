# -*- coding: utf-8 -*-
"""
EmotionFactor (V12 松耦合)
将 EmotionDataSource block 转换成评分模型
"""

from __future__ import annotations
from typing import Dict, Any

from core.factors.base import BaseFactor
from core.models.factor_result import FactorResult
from core.utils.logger import get_logger

LOG = get_logger("Factor.Emotion")


class EmotionFactor(BaseFactor):
    def __init__(self):
        self.name = "emotion"

    def compute(self, snapshot: Dict[str, Any]) -> FactorResult:
        block = snapshot.get("emotion", {})
        LOG.info("[EmotionFactor] compute block=%s", block)

        score = 50.0

        # -------- 指数贡献 --------
        idx_label = block.get("index_label", "Neutral")
        score += self._score_index(idx_label)

        # -------- 成交量贡献 --------
        vol_label = block.get("volume_label", "Neutral")
        score += self._score_volume(vol_label)

        # -------- 广度贡献 --------
        br_label = block.get("breadth_label", "Neutral")
        score += self._score_breadth(br_label)

        # -------- 北向贡献 --------
        nt_label = block.get("north_label", "Neutral")
        score += self._score_north(nt_label)

        # -------- 主力贡献 --------
        mf_label = block.get("main_force_label", "Neutral")
        score += self._score_main(mf_label)

        score = max(0, min(100, score))

        desc = (
            f"Emotion 综合情绪得分：{score:.2f}；"
            f"Index={idx_label}, Volume={vol_label}, Breadth={br_label}, "
            f"North={nt_label}, Main={mf_label}"
        )

        fr = FactorResult()
        fr.score = score
        fr.desc = desc
        fr.detail = desc
        LOG.info("[EmotionFactor] DONE score=%.2f", score)
        return fr

    # --- 以下为评分细则 ---
    def _score_index(self, lbl):
        return {"Strong Bull": +12, "Bullish": +4,
                "Bearish": -4, "Strong Bear": -12}.get(lbl, 0)

    def _score_volume(self, lbl):
        return {"High Volume": +6, "Normal Volume": +2,
                "Low Volume": -4}.get(lbl, 0)

    def _score_breadth(self, lbl):
        return {"Strong Breadth": +8, "Positive Breadth": +3,
                "Weak Breadth": -6}.get(lbl, 0)

    def _score_north(self, lbl):
        return {"Strong Inflow": +10, "Inflow": +4,
                "Outflow": -4, "Strong Outflow": -10}.get(lbl, 0)

    def _score_main(self, lbl):
        return {"Strong MF In": +6, "MF In": +2,
                "MF Out": -2, "Strong MF Out": -6}.get(lbl, 0)
