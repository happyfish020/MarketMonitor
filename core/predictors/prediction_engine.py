# core/engines/prediction_engine.py
# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - PredictionEngine
-----------------------------------

核心职责：
1. 接受各因子的 FactorResult → 得分 score
2. 根据 weights.yaml 中的权重进行加权
3. 生成最终 UnifiedPrediction 结构体（供 Reporter / Engine 使用）
4. 不读 snapshot、DS、外部文件（松耦合）
5. 日志完全遵守 V12 规范
"""
from dataclasses import dataclass
from typing import Dict, Any

from core.utils.config_loader import load_weights
from core.utils.logger import get_logger

logger = get_logger("PredictionEngine")


# ----------------------------------------------------------------------
# 数据结构：预测结果
# ----------------------------------------------------------------------
@dataclass
class UnifiedPrediction:
    weighted_score: float
    scores: Dict[str, float]
    weights: Dict[str, float]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "weighted_score": self.weighted_score,
            "scores": self.scores,
            "weights": self.weights,
        }


# ----------------------------------------------------------------------
# 预测引擎核心类
# ----------------------------------------------------------------------
class PredictionEngine:
    """
    V12 Prediction Engine
    - 单一入口：predict(factors)
    - factors 是 dict，例如：
        {
            "north_nps": FactorResult,
            "unified_emotion": FactorResult,
           # "turnover": FactorResult,
            "margin": FactorResult,
            "index_tech": FactorResult,
            ...
        }
    """
    def __init__(self):
        cfg = load_weights()
        self.weights: Dict[str, float] = cfg.get("prediction_weights", {})
        logger.info("[PredictionEngine] Loaded weights: %s", self.weights)

    # ------------------------------------------------------------------
    # 计算预测得分
    # ------------------------------------------------------------------
    def predict(self, factors: Dict[str, Any]) -> UnifiedPrediction:
        """
        主入口
        :param factors: dict[str → FactorResult]
        """
        logger.info("[PredictionEngine] Start prediction using factors: %s", list(factors.keys()))

        # --------------------------------------------------------------
        # 1. 读取因子得分（松耦合：只读 factor.score）
        # --------------------------------------------------------------
        def _safe_score(name: str) -> float:
            if name not in factors:
                logger.warning("[PredictionEngine] Factor '%s' missing, use 50 (neutral)", name)
                return 50.0
            try:
                return float(factors[name].score)
            except Exception as e:
                logger.error("[PredictionEngine] Cannot read score of '%s'. error=%s", name, e)
                return 50.0  # neutral fallback

        scores = {
            "unified_emotion": _safe_score("unified_emotion"),
            "north_nps": _safe_score("north_nps"),
            #"turnover": _safe_score("turnover"),
            "margin": _safe_score("margin"),
            "sector_rotation": _safe_score("sector_rotation"),
            "index_tech": _safe_score("index_tech"),
            "index_global": _safe_score("index_global"),
            "global_lead": _safe_score("global_lead"),
        }
        logger.info("[PredictionEngine] Collected factor scores: %s", scores)

        # --------------------------------------------------------------
        # 2. 加权平均
        # --------------------------------------------------------------
        weighted_sum = 0.0
        weight_total = 0.0

        for name, score in scores.items():
            w = float(self.weights.get(name, 0.0))
            if w <= 0:
                # 忽略权重为 0 的因子
                logger.debug("[PredictionEngine] weight[%s]=0, skip", name)
                continue
            weighted_sum += score * w
            weight_total += w
            logger.debug("[PredictionEngine] + %s: score=%.2f weight=%.3f → part=%.2f",
                         name, score, w, score * w)

        # --------------------------------------------------------------
        # 3. 防止除零（极端情况）
        # --------------------------------------------------------------
        if weight_total <= 0:
            logger.warning("[PredictionEngine] Total weight=0, fallback=50")
            final_score = 50.0
        else:
            final_score = weighted_sum / weight_total

        logger.info("[PredictionEngine] Final weighted score = %.2f", final_score)

        # --------------------------------------------------------------
        # 4. 输出预测结构体
        # --------------------------------------------------------------
        return UnifiedPrediction(
            weighted_score=round(final_score, 2),
            scores=scores,
            weights=self.weights,
        )
