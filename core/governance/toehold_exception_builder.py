from __future__ import annotations
from typing import Any, Dict, List

def build_toehold_exception(*, rotation_switch: Dict[str, Any] | None, gate: str | None, drs: str | None, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Build toe-hold exception governance block.

    Purpose:
    - Provide an auditable, ultra-small exception allowing a 1-lot 'toe-hold' position
      ONLY for whitelist 'brake' names (e.g., 久立特材) during defensive regimes.
    - This does NOT relax Gate/DRS; it only provides a tightly bounded manual exception.
    """
    out: Dict[str, Any] = {
        "version": str(cfg.get("version") or "TOEHOLD-EXCEPTION-V1"),
        "enabled": bool(cfg.get("enabled", True)),
        "permit": "NO",
        "max_lots": int(cfg.get("max_lots", 1)),
        "whitelist": cfg.get("whitelist") or [],
        "rules": cfg.get("rules") or {},
        "reasons": [],
    }

    if not out["enabled"]:
        out["reasons"].append({"code": "TOEHOLD_DISABLED", "level": "INFO", "msg": "disabled by config"})
        return out

    mode = None
    if isinstance(rotation_switch, dict):
        mode = rotation_switch.get("mode")

    allow_when = cfg.get("allow_when") or {}
    allow_modes = allow_when.get("rotation_mode") or []
    gate_any = allow_when.get("gate_any_of") or []
    drs_any = allow_when.get("drs_any_of") or []

    # Conditions: only consider toe-hold when rotation is NOT ON
    if mode and allow_modes and mode not in allow_modes:
        out["reasons"].append({"code": "TOEHOLD_NOT_NEEDED", "level": "INFO", "msg": f"rotation_mode={mode}"})
        return out

    if gate_any and (gate not in gate_any):
        out["reasons"].append({"code": "TOEHOLD_GATE_NOT_MATCH", "level": "INFO", "msg": f"Gate={gate}"})
        return out

    if drs_any and (drs not in drs_any):
        out["reasons"].append({"code": "TOEHOLD_DRS_NOT_MATCH", "level": "INFO", "msg": f"DRS={drs}"})
        return out

    # Permit YES: conditions matched; actual execution still limited by whitelist + 1 lot only.
    out["permit"] = "YES"
    out["reasons"].append({"code": "TOEHOLD_ALLOWED", "level": "WARN", "msg": "Defensive toe-hold allowed (whitelist only, 1 lot max, no add/no chase)"})
    return out
