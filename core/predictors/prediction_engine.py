# core/predictors/prediction_engine.py
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

Step-3（制度补强）：
- 当 factor.details['data_status'] != 'OK' 或缺失时：
  * 不允许被当成 NEUTRAL 参与加权（避免误导）
  * 剩余权重必须归一化（可审计）
- 输出 diagnostics：缺失/降级因子清单、使用因子、权重归一证据链
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Mapping, Optional, Tuple, List

from core.utils.config_loader import load_weights
from core.utils.logger import get_logger

logger = get_logger(__name__)

from typing import Dict, Any
from core.factors.factor_result import FactorResult
from core.factors.factor_base import RiskLevel
 
#import logging
 

class PredictionEngine:
    """
    UnifiedRisk V12 – PredictionEngine (Step-3 CLEAN)

    关键冻结：
    - 不再构造“all factors”
    - 不再使用默认 50 分 NEUTRAL 占位
    - 只基于 PolicySlotBinder 实际交付的 factors
    """

    def __init__(self, weights: Dict[str, float] | None = None):
        self.weights = weights or self._load_weights()

    def _load_weights(self) -> Dict[str, float]:
        # 原有加载逻辑，保持不变
        from core.utils.config_loader import load_weights
        cfg = load_weights()

        if "prediction_weights" not in cfg:
            raise ValueError("weights.yaml missing 'prediction_weights' section")

        return cfg["prediction_weights"]

    def predict(self, factors: Dict[str, FactorResult]) -> Dict[str, Any]:
        """
        factors:
          key = 制度槽位名（不带 _raw）
          value = FactorResult
        """

        evidence: Dict[str, Any] = {
            "used": [],
            "used_in_aggregation": [],
            "missing_factors": [],
            "degraded_factors": [],
            "raw_weights": dict(self.weights),
            "normalized_weights": {},
            "raw_weight_total": 0.0,
            "normalized_weight_total": 0.0,
            "policy": "STEP3_ONLY_BOUND_FACTORS",
        }

        weighted_sum = 0.0
        raw_weight_sum = 0.0

        for slot, weight in self.weights.items():
            fr = factors.get(slot)

            if fr is None:
                evidence["missing_factors"].append(slot)
                continue

            data_status = fr.details.get("data_status") if fr.details else "OK"
            if data_status != "OK":
                evidence["degraded_factors"].append(slot)
                continue

            evidence["used"].append(slot)
            evidence["used_in_aggregation"].append(slot)

            weighted_sum += fr.score * weight
            raw_weight_sum += weight

        evidence["raw_weight_total"] = round(raw_weight_sum, 4)

        # 极端情况：无可用因子
        if raw_weight_sum <= 0:
            logger.warning("[PredictionEngine] NO_EFFECTIVE_FACTORS")
            return {
                "final_score": 50.0,
                "risk_level": "NEUTRAL",
                "evidence": {
                    **evidence,
                    "normalized_weight_total": 0.0,
                    "reason": "NO_EFFECTIVE_FACTORS",
                },
            }

        # 权重归一
        normalized_weights = {}
        for slot in evidence["used_in_aggregation"]:
            normalized_weights[slot] = round(
                self.weights[slot] / raw_weight_sum, 6
            )

        evidence["normalized_weights"] = normalized_weights
        evidence["normalized_weight_total"] = 1.0

        final_score = weighted_sum / raw_weight_sum
        risk_level: RiskLevel = self._level_from_score(final_score)

        logger.info(
            "[PredictionEngine] used=%s raw_weight_total=%.4f final_score=%.2f",
            evidence["used_in_aggregation"],
            raw_weight_sum,
            final_score,
        )

        return {
            "final_score": round(final_score, 2),
            "risk_level": risk_level,
            "evidence": evidence,
        }

    def _level_from_score(self, score: float) -> RiskLevel:
        if score >= 66:
            return "LOW"
        if score <= 33:
            return "HIGH"
        return "NEUTRAL"
