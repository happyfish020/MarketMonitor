# -*- coding: utf-8 -*-
"""
UnifiedRisk v11.7
prediction_engine.py — T+1 / T+5 市场预测引擎（最终合并版）

位置：
    core/predictors/prediction_engine.py

说明：
    此文件完全取代旧的 predict_t1_t5.py
    Engine 使用方式：
        predictor = PredictorT1T5()
        pred = predictor.predict(factors)
        pred_block = predictor.format_report(pred)
        build_daily_report_text(..., prediction=pred_block)
"""

from __future__ import annotations
from typing import Dict, Any


class PredictorT1T5:
    """
    UnifiedRisk 官方预测引擎（未来可扩展为机器学习/概率模型）
    """

    # ----------------------------------------------------------------------
    # T+1：短周期 → 情绪、成交、北向主导
    # ----------------------------------------------------------------------
    T1_WEIGHTS = {
        "market_sentiment": 0.30,
        "turnover":         0.20,
        "north_nps":        0.15,
        "emotion":          0.15,
        "margin":           0.10,
        "global_lead":      0.10,
    }

    # ----------------------------------------------------------------------
    # T+5：中周期 → 杠杆、全球方向、成交主导
    # ----------------------------------------------------------------------
    T5_WEIGHTS = {
        "market_sentiment": 0.15,
        "turnover":         0.20,
        "north_nps":        0.15,
        "emotion":          0.10,
        "margin":           0.20,
        "global_lead":      0.20,
    }

    # ----------------------------------------------------------------------
    # Score → Direction 映射
    # ----------------------------------------------------------------------
    @staticmethod
    def _score_to_direction(score: float) -> str:
        if score >= 65:
            return "偏多（上涨）"
        elif score >= 55:
            return "震荡偏多"
        elif score > 45:
            return "震荡"
        elif score > 35:
            return "震荡偏空"
        else:
            return "偏空（下跌）"

    # ----------------------------------------------------------------------
    # 单周期预测：按权重计算贡献 & 综合分
    # ----------------------------------------------------------------------
    def _compute(self, factors: Dict[str, Any], weights: Dict[str, float]) -> Dict[str, Any]:
        score_sum = 0.0
        detail = {}

        for name, w in weights.items():
            factor = factors.get(name)
            if factor is None:
                factor_score = None
                contrib = 0.0
            else:
                factor_score = float(factor.score)
                contrib = factor_score * w
                score_sum += contrib

            detail[name] = {
                "factor_score": factor_score,
                "weight": w,
                "contribution": contrib,
            }

        final_score = round(score_sum, 2)
        direction = self._score_to_direction(final_score)

        return {
            "score": final_score,
            "direction": direction,
            "details": detail,
        }

    # ----------------------------------------------------------------------
    # 主接口：返回包含 T+1 / T+5 的 dict
    # ----------------------------------------------------------------------
    def predict(self, factor_results: Dict[str, Any]) -> Dict[str, Any]:
        t1 = self._compute(factor_results, self.T1_WEIGHTS)
        t5 = self._compute(factor_results, self.T5_WEIGHTS)
        return {
            "T+1": t1,
            "T+5": t5,
        }

    # ----------------------------------------------------------------------
    # 文本输出（插入日报报告）
    # ----------------------------------------------------------------------
    def format_report(self, pred: Dict[str, Any]) -> str:
        def block(name: str, obj: Dict[str, Any]) -> str:
            s = f"【{name}】方向：{obj['direction']}  |  综合分：{obj['score']}\n"
            s += "  - 因子贡献：\n"
            for fname, info in obj["details"].items():
                fs = info["factor_score"]
                if fs is None:
                    continue
                s += (
                    f"      · {fname}: {fs:.2f} × {info['weight']:.2f}"
                    f" = {info['contribution']:.2f}\n"
                )
            return s

        return block("T+1", pred["T+1"]) + "\n" + block("T+5", pred["T+5"])
