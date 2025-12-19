# core/predictors/prediction_engine.py
# -*- coding: utf-8 -*-

"""
UnifiedRisk V12 - PredictionEngine
-----------------------------------
（原注释保持不变，略）
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Mapping, Optional, Tuple, List

from core.utils.config_loader import load_weights
from core.utils.logger import get_logger

logger = get_logger(__name__)

from core.factors.factor_result import FactorResult
from core.factors.factor_base import RiskLevel


class PredictionEngine:
    """
    UnifiedRisk V12 – PredictionEngine (Step-3 CLEAN)
    """

    def __init__(self, weights: Dict[str, float] | None = None):
        self.weights = weights or self._load_weights()

    def _load_weights(self) -> Dict[str, float]:
        cfg = load_weights()
        if "prediction_weights" not in cfg:
            raise ValueError("weights.yaml missing 'prediction_weights' section")
        return cfg["prediction_weights"]

    # ==============================
    # NEW: Action Hint Builder
    # ==============================
    def _build_action_hint(
        self,
        factors: Dict[str, FactorResult],
        risk_level: RiskLevel,
    ) -> Dict[str, Any]:
        """
        Build Gate × Action Matrix hint.
        Pure function:
        - Only reads FactorResult (score / level / details)
        - No snapshot / DS dependency
        - Missing info => safe HOLD
        """

        def _get_level(slot: str) -> Optional[str]:
            fr = factors.get(slot)
            return fr.level if fr else None

        def _get_score(slot: str) -> Optional[float]:
            fr = factors.get(slot)
            return fr.score if fr else None

        # ---- Safely extract signals ----
        participation = _get_level("participation")
        breadth = _get_level("breadth")
        margin_score = _get_score("margin")
        global_macro = _get_level("global_macro")
        index_global = _get_level("index_global")

        # Default: HOLD
        hint = {
            "action": "HOLD",
            "reason": "DEFAULT_HOLD",
            "allowed": {
                "etf_add": False,
                "stock_add": False,
            },
            "limits": {},
            "conditions": [],
            "forbidden": [],
        }

        # ==============================
        # CAUTION Gate Logic
        # ==============================
        if risk_level == "NEUTRAL":
            cond_struct = participation == "Hidden Weakness"
            cond_margin = margin_score is not None and margin_score >= 70
            cond_global = (
                global_macro == "NEUTRAL"
                and index_global == "NEUTRAL"
            )

            if cond_struct and cond_margin and cond_global:
                hint.update({
                    "action": "HOLD",
                    "reason": "Caution + HiddenWeakness + HighMargin + GlobalNeutral",
                    "allowed": {
                        "etf_add": True,
                        "stock_add": False,
                    },
                    "limits": {
                        "etf_add_units_max": 1,
                        "mode": "CONDITIONAL_ONLY",
                    },
                    "conditions": [
                        "仅允许核心ETF回踩支撑后的条件型加仓（≤1单位）",
                        "不追涨",
                    ],
                    "forbidden": [
                        "加仓高弹性个股",
                        "一次性打光现金",
                        "情绪性追涨ETF",
                    ],
                })
                return hint

        # ==============================
        # NORMAL Gate Logic（ETF Ladder）
        # ==============================
        if risk_level == "LOW":
            improving_struct = participation != "Hidden Weakness"
            ok_margin = margin_score is None or margin_score < 70

            if improving_struct and ok_margin:
                hint.update({
                    "action": "ETF_LADDER",
                    "reason": "Normal Gate with improving structure",
                    "allowed": {
                        "etf_add": True,
                        "stock_add": True,
                    },
                    "limits": {
                        "etf_add_units_max": 2,
                        "ladder": "L1->L2 same day, L3 requires T+1",
                    },
                    "conditions": [
                        "L1: 回踩支撑/均价 + 不破低点 → +1单位",
                        "L2: 突破前高 + 指数同步 → 再+1单位",
                        "L3: 连续2日确认 + Breadth改善（T+1）",
                    ],
                    "forbidden": [
                        "直线拉升追涨",
                        "单日ETF加仓>2单位",
                        "结构退化当日加仓",
                    ],
                })
                return hint

        return hint

    # ==============================
    # Original predict()
    # ==============================
    def predict(self, factors: Dict[str, FactorResult]) -> Dict[str, Any]:
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

        if raw_weight_sum <= 0:
            logger.warning("[PredictionEngine] NO_EFFECTIVE_FACTORS")
            return {
                "final_score": 50.0,
                "risk_level": "NEUTRAL",
                "action_hint": {"action": "HOLD", "reason": "NO_EFFECTIVE_FACTORS"},
                "evidence": {
                    **evidence,
                    "normalized_weight_total": 0.0,
                    "reason": "NO_EFFECTIVE_FACTORS",
                },
            }

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
