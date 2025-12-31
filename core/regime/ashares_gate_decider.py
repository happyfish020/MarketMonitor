# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 FULL
AShares Gate Decider（Phase-2 · 一致性冻结版）
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List

from core.factors.factor_result import FactorResult

_GATE_ORDER = ("NORMAL", "CAUTION", "PLANB", "FREEZE")
_GATE_RANK = {g: i for i, g in enumerate(_GATE_ORDER)}


def _max_gate(a: str, b: str) -> str:
    return a if _GATE_RANK[a] >= _GATE_RANK[b] else b


@dataclass(frozen=True, slots=True)
class GateDecision:
    level: str
    reasons: List[str]
    evidence: Dict[str, Any]


class ASharesGateDecider:
    """
    Phase-2 Gate 决策器（冻结）

    铁律：
    - 只使用 confirmed daily 结构因子
    - 只允许降级
    - 不读取 Execution / DRS / Prediction
    """

    def decide(
        self,
        slots: Dict[str, Any],
        factors: Dict[str, FactorResult],
    ) -> GateDecision:

        gate = "NORMAL"
        reasons: List[str] = []
        evidence: Dict[str, Any] = {}

        # ------------------------------
        # Phase-3 结构分布风险
        # ------------------------------
        structure = slots.get("structure", {})
        regime = structure.get("regime") if isinstance(structure, dict) else None
        if isinstance(regime, dict):
            dist = regime.get("structure_distribution")
            if isinstance(dist, dict) and dist.get("state") == "DISTRIBUTION_RISK":
                gate = _max_gate(gate, "CAUTION")
                reasons.append("phase3_distribution_risk")
                evidence["regime"] = dist

        # ------------------------------
        # Trend-in-Force（最高优先级）
        # ------------------------------
        trend = slots.get("trend_in_force")
        if isinstance(trend, dict) and trend.get("state") == "broken":
            gate = _max_gate(gate, "CAUTION")
            reasons.append("trend_in_force=broken")
            evidence["trend_in_force"] = trend

        # ------------------------------
        # Breadth
        # ------------------------------
        breadth = factors.get("breadth")
        if breadth and getattr(breadth, "level", None) == "HIGH":
            gate = _max_gate(gate, "CAUTION")
            reasons.append("breadth=HIGH")
            evidence["breadth"] = breadth.level

        # ------------------------------
        # Participation
        # ------------------------------
        participation = factors.get("participation")
        if participation and getattr(participation, "level", None) == "LOW":
            gate = _max_gate(gate, "CAUTION")
            reasons.append("participation=LOW")
            evidence["participation"] = participation.level

        # ------------------------------
        # ETF × Index Sync
        # ------------------------------
        sync = factors.get("etf_index_sync_daily")
        if sync and getattr(sync, "level", None) == "HIGH":
            gate = _max_gate(gate, "CAUTION")
            reasons.append("etf_index_sync_daily=HIGH")
            evidence["etf_index_sync_daily"] = sync.level

        # ------------------------------
        # Northbound Proxy Pressure (price-trend proxy)
        # ------------------------------
        npp = factors.get("north_proxy_pressure")
        if npp:
            lvl = getattr(npp, "level", None)
            # Frozen policy: only HIGH pressure can downgrade Gate
            if lvl == "HIGH":
                gate = _max_gate(gate, "CAUTION")
                reasons.append("north_proxy_pressure=HIGH")
                evidence["north_proxy_pressure"] = getattr(npp, "details", None) or lvl
            else:
                # still record evidence for audit (does not affect Gate)
                evidence.setdefault("north_proxy_pressure", getattr(npp, "details", None) or lvl)

        # ------------------------------
        # Failure Rate
        # ------------------------------
        frf = factors.get("failure_rate")
        if frf and getattr(frf, "level", None) == "HIGH":
            gate = _max_gate(gate, "CAUTION")
            reasons.append("failure_rate=HIGH")
            evidence["failure_rate"] = frf.level

        return GateDecision(
            level=gate,
            reasons=reasons,
            evidence=evidence,
        )
