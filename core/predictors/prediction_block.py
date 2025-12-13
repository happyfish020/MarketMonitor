# core/predictors/prediction_block.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict

from core.models.risk_level import RiskLevel


@dataclass(frozen=True, slots=True)
class PredictionBlock:
    """
    UnifiedRisk V12 - Prediction 输出结构（结构化、可审计、无文本）

    overall_score: 0~100 综合风险分
    overall_level: RiskLevel
    weights_used: 实际参与合成的权重（归一后）
    factor_scores: 各因子 score 快照（便于 report/UI）
    factor_levels: 各因子 level 快照
    diagnostics: 合成过程诊断信息（例如 degraded、missing_factors 等）
    """
    overall_score: float
    overall_level: RiskLevel

    weights_used: Dict[str, float] = field(default_factory=dict)
    factor_scores: Dict[str, float] = field(default_factory=dict)
    factor_levels: Dict[str, RiskLevel] = field(default_factory=dict)

    diagnostics: Dict[str, Any] = field(default_factory=dict)
