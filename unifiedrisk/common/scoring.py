
"""UnifiedRisk common scoring helpers (v4.0)

This module centralizes basic risk level classification logic so that both
A-share engines and global risk engines can share the same mapping rules.

It is intentionally dependency-light: it only relies on the standard library.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

# --- Core API ------------------------------------------------------------

RISK_LEVELS = [
    (-9999.0, "极高风险", "🔴 极高风险：系统性或剧烈波动风险，建议大幅减仓甚至观望。"),
    (-5.0,   "偏高风险", "🟠 偏高风险：短期调整压力较大，建议控制仓位、择机减仓。"),
    (-1.0,   "中性偏空", "🟡 中性偏空：略偏空，但风险可控，注意防守。"),
    (1.0,    "中性",     "⚪ 中性：多空力量基本均衡，可保持正常仓位。"),
    (5.0,    "友好偏多", "🟢 友好偏多：环境偏多，适度加仓或持股为主。"),
    (9999.0, "极度友好", "🔵 极度友好：趋势性机会明显，但仍需控制整体风险。"),
]


def classify_level(score: float) -> str:
    """Return only the textual risk level name for a numeric score.

    This is a very small and stable API that other modules can import:

        from unifiedrisk.common.scoring import classify_level

    If you need more detail than just the label, use :func:`classify_level_detail`.
    """
    label, _ = classify_level_detail(score)
    return label


def classify_level_detail(score: float) -> Tuple[str, str]:
    """Return (label, description) for the given total_score.

    The thresholds are inclusive on the upper bound; they are ordered from
    low score (more risky) to high score (more friendly).
    """
    for threshold, label, desc in RISK_LEVELS:
        if score <= threshold:
            return label, desc
    # Fallback (should never hit because last threshold is +inf-like)
    return "中性", "⚪ 中性：多空力量基本均衡，可保持正常仓位。"


@dataclass
class RiskSummary:
    """Lightweight container for risk scoring results.

    This is optional sugar: engines can choose to use it or simply work with
    dicts. It is kept here because it is generic enough to be shared.
    """
    total_score: float
    level: str
    description: str

    @classmethod
    def from_score(cls, score: float) -> "RiskSummary":
        lvl, desc = classify_level_detail(score)
        return cls(total_score=score, level=lvl, description=desc)

    def to_dict(self) -> Dict[str, object]:
        return {
            "total_score": self.total_score,
            "risk_level": self.level,
            "risk_description": self.description,
        }
