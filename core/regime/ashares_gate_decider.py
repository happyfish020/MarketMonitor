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
            # Soft-release rule:
            # participation=LOW alone should not hard-lock Gate when structure is repaired.
            trend_ok = False
            fr_ok = False
            br_ok = False

            tif = factors.get("trend_in_force")
            if tif:
                tif_state = str(((getattr(tif, "details", None) or {}).get("state") or "")).lower()
                trend_ok = (tif_state == "in_force")

            frf = factors.get("failure_rate")
            if frf:
                fr_level = str(getattr(frf, "level", "")).upper()
                fr_state = str(((getattr(frf, "details", None) or {}).get("state") or "")).lower()
                fr_ok = (fr_level in ("LOW", "NEUTRAL")) and (fr_state in ("stable", "rising", ""))

            breadth = factors.get("breadth")
            if breadth:
                br_level = str(getattr(breadth, "level", "")).upper()
                br_state = str(((getattr(breadth, "details", None) or {}).get("state") or "")).lower()
                br_ok = (br_level in ("LOW", "NEUTRAL")) and (br_state in ("healthy", "early", ""))

            if trend_ok and fr_ok and br_ok:
                reasons.append("participation=LOW(soft_released)")
                evidence["participation"] = {
                    "level": participation.level,
                    "soft_release": True,
                    "release_if": {
                        "trend_in_force": trend_ok,
                        "failure_rate": fr_ok,
                        "breadth": br_ok,
                    },
                }
            else:
                gate = _max_gate(gate, "CAUTION")
                reasons.append("participation=LOW")
                evidence["participation"] = {
                    "level": participation.level,
                    "soft_release": False,
                    "release_if": {
                        "trend_in_force": trend_ok,
                        "failure_rate": fr_ok,
                        "breadth": br_ok,
                    },
                }

        # ------------------------------
        # ETF × crowding_conce
        # ------------------------------
        sync = factors.get("crowding_concentration")
        if sync and getattr(sync, "level", None) == "HIGH":
            gate = _max_gate(gate, "CAUTION")
            reasons.append("crowding_concentration=HIGH")
            evidence["crowding_concentration"] = sync.level

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
