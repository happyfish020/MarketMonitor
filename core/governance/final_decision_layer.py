#-*- coding: utf-8 -*-
"""UnifiedRisk V12 · FinalDecisionLayer (Frozen)

Purpose
-------
Provide a single, auditable "final ruling" that reconciles outputs from
Gate / DRS / Execution / overlays (DOS / AttackPermit / SectorPermit).

Design invariants (Frozen)
-------------------------
1) Priority: DRS > Gate > overlays (DOS/AP/SP). Execution affects *how* to execute,
   not whether to add risk. No combined strings like "A/D1".
2) When DRS=RED: final_action_code MUST be 'D' (defensive) and overlays are ignored.
3) This layer is read-only evidence for report/persistence; it must not crash
   even if inputs are missing.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from core.utils.logger import get_logger


LOG = get_logger("FinalDecision")


def _norm_upper(v: Any) -> str:
    try:
        return str(v or "").strip().upper()
    except Exception:
        return ""


@dataclass
class FinalDecision:
    actionhint_code: str  # A/N/D
    veto: Optional[str]
    gate: str
    drs: str
    execution_band: str
    notes: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": "FINAL_DECISION_V1",
            "actionhint_code": self.actionhint_code,
            "veto": self.veto,
            "gate": self.gate,
            "drs": self.drs,
            "execution_band": self.execution_band,
            "notes": self.notes,
        }


class FinalDecisionLayer:
    """Compute the final action code with strict veto ordering."""

    def build(
        self,
        *,
        asof: str,
        gate_final: Any,
        drs_signal: Any,
        execution_band: Any,
        dos: Optional[Dict[str, Any]] = None,
        attack_permit: Optional[Dict[str, Any]] = None,
        sector_permit: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        gate = _norm_upper(gate_final) or "UNKNOWN"
        drs = _norm_upper(drs_signal)
        execu = _norm_upper(execution_band) or "NA"

        veto: Optional[str] = None

        # 1) Hard veto
        if drs == "RED":
            veto = "DRS=RED hard veto"
            code = "D"
        else:
            # 2) Gate mapping (frozen)
            if gate in ("FREEZE", "DEFENSIVE"):
                code = "D"
            elif gate == "CAUTION":
                code = "N"
            else:
                code = "A"

        notes: Dict[str, Any] = {
            "asof": asof,
            "dos_level": (dos or {}).get("level") if isinstance(dos, dict) else None,
            "attack_permit": (attack_permit or {}).get("permit") if isinstance(attack_permit, dict) else None,
            "sector_permit": (sector_permit or {}).get("permit") if isinstance(sector_permit, dict) else None,
            "frozen_priority": "DRS > Gate > overlays",
        }

        out = FinalDecision(
            actionhint_code=code,
            veto=veto,
            gate=gate,
            drs=drs,
            execution_band=execu,
            notes=notes,
        ).to_dict()

        try:
            LOG.info(
                "[FinalDecision] asof=%s gate=%s drs=%s exec=%s => code=%s veto=%s",
                asof,
                gate,
                drs,
                execu,
                out.get("actionhint_code"),
                out.get("veto"),
            )
        except Exception:
            pass

        return out
