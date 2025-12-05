# -*- coding: utf-8 -*-
"""
V11.8 预测引擎（T+1 / T+5）
不再依赖 index_series，完全基于因子 scores 构建。
"""

from __future__ import annotations
from typing import Dict, Any


class PredictorT1T5:
    """
    输入：factors = {name: FactorResult}
    输出：
        {
           "T+1": {...},
           "T+5": {...}
        }
    """

    WEIGHTS_T1 = {
        "north_nps": 0.30,
        "turnover": 0.20,
        "market_sentiment": 0.20,
        "index_global": 0.20,
        "global_lead": 0.10,
    }

    WEIGHTS_T5 = {
        "global_lead": 0.40,
        "index_global": 0.30,
        "emotion": 0.15,
        "market_sentiment": 0.10,
        "north_nps": 0.05,
    }

    def predict(self, factors: Dict[str, Any]) -> Dict[str, Any]:
        """
        统一入口：只传 factor dict，不再需要 index_series！
        engine 调用：
            prediction_raw = predictor.predict(factors)
        """
        score_t1 = self._weighted_score(factors, self.WEIGHTS_T1)
        score_t5 = self._weighted_score(factors, self.WEIGHTS_T5)

        result_t1 = {
            "score": score_t1,
            "direction": self._label(score_t1),
            "explain": self._explain(score_t1, horizon="T+1"),
        }

        result_t5 = {
            "score": score_t5,
            "direction": self._label(score_t5),
            "explain": self._explain(score_t5, horizon="T+5"),
        }

        return {
            "T+1": result_t1,
            "T+5": result_t5,
        }

    # ============================================================
    # 工具函数
    # ============================================================
    def _weighted_score(self, factors, weights: Dict[str, float]):
        total = 0
        for name, w in weights.items():
            if name in factors:
                total += factors[name].score * w
        return round(total, 2)

    def _label(self, score: float) -> str:
        if score >= 65:
            return "上涨"
        elif score >= 55:
            return "偏强震荡"
        elif score > 45:
            return "震荡"
        elif score > 35:
            return "偏弱震荡"
        else:
            return "下跌"

    def _explain(self, score: float, horizon: str) -> str:
        if horizon == "T+1":
            base = "短线（T+1）"
        else:
            base = "中短期（T+5）"

        return f"{base}综合风险评分={score}，方向判定：{self._label(score)}"
