# -*- coding: utf-8 -*-
"""UnifiedRisk V12 · Market State Service · v1.0

Read-only report helper to unify multiple risk expressions into ONE primary label.

Inputs (slots):
- regime_current_stage_raw: 'S1'..'S5'/'UNKNOWN'
- regime_shift: dict (optional)
- regime_stats: dict (optional)

Outputs:
- market_state dict: {state, stage, severity, shift_type, consecutive_s5_days, reasons, asof_trade_date}
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


class MarketStateService:
    @staticmethod
    def build(*, trade_date: Optional[str], stage_raw: str,
              regime_shift: Optional[Dict[str, Any]] = None,
              regime_stats: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        stage = (stage_raw or "UNKNOWN").upper()

        consec_s5 = None
        if isinstance(regime_stats, dict):
            consec_s5 = regime_stats.get("consecutive_s5_days")

        # base severity by stage
        if stage == "S5":
            severity = "HIGH"
        elif stage == "S4":
            severity = "MED"
        elif stage in ("S2", "S3"):
            severity = "LOW"
        else:
            severity = "LOW"

        # streak upgrade (Rule v1)
        streak_tag = None
        if stage == "S5" and isinstance(consec_s5, int):
            if consec_s5 >= 5:
                severity = "EXTREME"
                streak_tag = "S5_STREAK_5D"
            elif consec_s5 >= 3:
                severity = "VERY_HIGH"
                streak_tag = "S5_STREAK_3D"
            elif consec_s5 >= 1:
                streak_tag = "S5_STREAK_1D"

        shift_type = None
        if isinstance(regime_shift, dict):
            shift_type = regime_shift.get("shift_type")

        # unified state
        if stage == "S5":
            base = "DE_RISK"
        elif stage == "S4":
            base = "CAUTION_REPAIR"
        elif stage == "S1":
            base = "RISK_ON"
        elif stage in ("S2", "S3"):
            base = "NEUTRAL"
        else:
            base = "UNKNOWN"

        if shift_type == "RISK_ESCALATION":
            state = f"{base}_ESCALATION"
        elif shift_type == "RISK_EASING":
            state = f"{base}_EASING"
        else:
            state = base

        reasons: List[str] = []
        if streak_tag:
            reasons.append(streak_tag)
        if shift_type:
            reasons.append(shift_type)

        return {
            "asof_trade_date": trade_date,
            "state": state,
            "stage": stage,
            "severity": severity,
            "shift_type": shift_type,
            "consecutive_s5_days": consec_s5,
            "reasons": reasons,
        }
