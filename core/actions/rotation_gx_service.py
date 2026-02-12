# -*- coding: utf-8 -*-
"""UnifiedRisk V12 · Rotation GX Service (Frozen)

Implements three practical execution templates (GX) for Sector Rotation:

- GX-ROT-ENTRY-SPLIT-V1
- GX-ROT-EXIT-T1-V1
- GX-ROT-HARDSTOP-V1

Frozen Contract:
- Must be derived ONLY from existing slots (rotation_switch + rotation_snapshot) and basic price/position facts if present.
- Fail-closed: any missing data yields veto/notes; never raise.
- Does NOT modify Gate/Execution/DRS; outputs an auditable action_plan-like dict into slots['rotation_gx'].
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from core.utils.logger import get_logger
from core.actions.rotation_target_selector import RotationTargetSelector

LOG = get_logger("RotationGX")


def _to_iso(x: Any) -> Any:
    if isinstance(x, (datetime, date)):
        return x.isoformat()
    if isinstance(x, Decimal):
        return float(x)
    return x


def _safe_dict(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): _safe_dict(_to_iso(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_safe_dict(_to_iso(v)) for v in obj]
    return _to_iso(obj)


def _pick_rotation_snapshot(slots: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # Preferred normalized slot
    rs = slots.get("rotation_snapshot")
    if isinstance(rs, dict):
        return rs
    # Fallback: raw slot name used by some fetchers
    rs = slots.get("rotation_snapshot_raw")
    if isinstance(rs, dict):
        return rs
    return None


def _pick_rotation_switch(slots: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    r = slots.get("rotation_switch")
    return r if isinstance(r, dict) else None


def _as_bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return x != 0
    if isinstance(x, str):
        return x.strip().lower() in ("1", "true", "yes", "y", "on")
    return False


def _top2_transition_str(summary: Any) -> str:
    try:
        if isinstance(summary, dict):
            top = summary.get("transition_top")
        else:
            top = None
        if not isinstance(top, list):
            return ""
        parts: List[str] = []
        for it in top[:2]:
            if not isinstance(it, dict):
                continue
            tr = it.get("transition")
            cnt = it.get("cnt")
            if tr is None or cnt is None:
                continue
            parts.append(f"{tr}={cnt}")
        return ", ".join(parts)
    except Exception:
        return ""


def _extract_reason_code(summary: Any) -> str:
    if isinstance(summary, dict):
        rc = summary.get("reason_code")
        return str(rc) if rc is not None else ""
    return ""


def _extract_watch(summary: Any) -> Optional[int]:
    if isinstance(summary, dict):
        ss = summary.get("signal_stat")
        if isinstance(ss, dict) and "WATCH" in ss:
            try:
                return int(ss.get("WATCH"))
            except Exception:
                return None
    return None


def _norm_status(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip().upper()


@dataclass(frozen=True)
class RotationGXConfig:
    hardstop_pct: float = 0.08
    hardstop_break_rule: str = "LOW_N"
    hardstop_break_n: int = 5


class RotationGXService:
    """Build rotation execution templates from slots."""

    def __init__(self, cfg: Optional[Dict[str, Any]] = None) -> None:
        cfg = cfg or {}
        self.cfg = RotationGXConfig(
            hardstop_pct=float(cfg.get("hardstop_pct", 0.08)),
            hardstop_break_rule=str(cfg.get("hardstop_break_rule", "LOW_N")),
            hardstop_break_n=int(cfg.get("hardstop_break_n", 5)),
        )
        self.selector = RotationTargetSelector()

    def build(self, *, slots: Dict[str, Any], trade_date: str) -> Dict[str, Any]:
        try:
            return self._build_impl(slots=slots, trade_date=trade_date)
        except Exception as e:
            LOG.warning("RotationGX exception: %s", e)
            return {
                "meta": {"schema": "ROT_GX_V1", "trade_date": trade_date, "status": "ERROR"},
                "entry_split": {"enabled": True, "allowed": False, "reasons": [f"exception:{e}"]},
                "exit_t1": {"enabled": True, "actions": [], "reasons": [f"exception:{e}"]},
                "hardstop": {"enabled": True, "actions": [], "reasons": [f"exception:{e}"]},
            }

    def _build_impl(self, *, slots: Dict[str, Any], trade_date: str) -> Dict[str, Any]:
        rot_sw = _pick_rotation_switch(slots) or {}
        rot_snap = _pick_rotation_snapshot(slots) or {}

        # ---------- Switch gate ----------
        mode = str(rot_sw.get("mode") or rot_sw.get("today_mode") or "UNKNOWN").upper()
        switch_on = mode == "ON"

        # ---------- Snapshot entry ----------
        entry = rot_snap.get("entry") if isinstance(rot_snap.get("entry"), dict) else {}
        entry_allowed = _as_bool(entry.get("allowed")) if entry else False
        entry_rows = entry.get("rows") if isinstance(entry.get("rows"), list) else []
        entry_summary = entry.get("summary") if isinstance(entry.get("summary"), dict) else None

        top1 = None
        if entry_rows:
            # assume already ordered by rank
            top1 = entry_rows[0]

        # ---------- Snapshot exits ----------
        exits = rot_snap.get("exit") if isinstance(rot_snap.get("exit"), dict) else {}
        exit_rows = exits.get("rows") if isinstance(exits.get("rows"), list) else []
        exit_summary = exits.get("summary") if isinstance(exits.get("summary"), dict) else None

        # ---------- Snapshot holdings ----------
        holds = rot_snap.get("holding") if isinstance(rot_snap.get("holding"), dict) else {}
        holding_rows = holds.get("rows") if isinstance(holds.get("rows"), list) else []
        holding_summary = holds.get("summary") if isinstance(holds.get("summary"), dict) else None

        # ---------- GX-ROT-EXIT-T1-V1 ----------
        exit_actions: List[Dict[str, Any]] = []
        for r in exit_rows:
            if not isinstance(r, dict):
                continue
            st = _norm_status(r.get("EXIT_EXEC_STATUS"))
            if st in ("EXIT_ALLOWED", "EXIT_PENDING"):
                exit_actions.append(
                    {
                        "gx": "GX-ROT-EXIT-T1-V1",
                        "sector_id": r.get("SECTOR_ID"),
                        "sector_name": r.get("SECTOR_NAME"),
                        "status": st,
                        "exec_exit_date": r.get("EXEC_EXIT_DATE"),
                        "qty": "ALL",
                        "priority": "HIGH",
                    }
                )

        # ---------- GX-ROT-ENTRY-SPLIT-V1 ----------
        entry_split = {
            "gx": "GX-ROT-ENTRY-SPLIT-V1",
            "enabled": True,
            "allowed": bool(switch_on and entry_allowed and isinstance(top1, dict)),
            "reasons": [],
            "target": None,
            "cap_weight": None,
            "split": {"legs": 2, "schedule": ["T+1", "T+2"], "ratios": [0.5, 0.5]},
            "constraints": {"topk_execute": 1, "rounding": "FLOOR_TO_1LOT"},
        }
        if not switch_on:
            entry_split["reasons"].append(f"veto:switch_mode={mode}")
        if not entry_allowed:
            rc = _extract_reason_code(entry_summary) if entry_summary else "NO_ENTRY"
            entry_split["reasons"].append(f"veto:entry_allowed=NO:{rc}")
        if isinstance(top1, dict) and entry_split["allowed"]:
            sel = self.selector.select(sector_id=top1.get("SECTOR_ID"), sector_name=top1.get("SECTOR_NAME"))
            entry_split["target_selector"] = sel
            entry_split["target_symbol"] = sel.get("symbol")
            if not sel.get("symbol") and any(str(r).startswith("veto:") for r in (sel.get("reasons") or [])):
                entry_split["allowed"] = False
                entry_split["reasons"].extend(sel.get("reasons") or [])
            entry_split["target"] = {
                "sector_id": top1.get("SECTOR_ID"),
                "sector_name": top1.get("SECTOR_NAME"),
                "entry_rank": top1.get("ENTRY_RANK"),
                "energy_pct": top1.get("ENERGY_PCT"),
                "signal_score": top1.get("SIGNAL_SCORE"),
            }
            entry_split["cap_weight"] = top1.get("WEIGHT_SUGGESTED")

        # Exit priority veto for entry (if any exit actions exist)
        if exit_actions:
            entry_split["allowed"] = False
            entry_split["reasons"].append("veto:exit_priority")

        # ---------- GX-ROT-HARDSTOP-V1 ----------
        # Hardstop requires basic position facts. If not present, keep auditable note.
        hardstop_actions: List[Dict[str, Any]] = []
        hardstop_reasons: List[str] = []

        positions = _infer_positions(slots)
        if not positions:
            hardstop_reasons.append("data_missing:positions")
        else:
            hardstop_actions.extend(
                self._eval_hardstop(positions=positions, trade_date=trade_date)
            )

        out = {
            "meta": {"schema": "ROT_GX_V1", "trade_date": trade_date, "status": "OK"},
            "switch": {"mode": mode, "switch_on": switch_on, "raw": _safe_dict(rot_sw)},
            "entry_split": _safe_dict(entry_split),
            "exit_t1": _safe_dict(
                {
                    "gx": "GX-ROT-EXIT-T1-V1",
                    "enabled": True,
                    "actions": exit_actions,
                    "summary_reason": _safe_dict(exit_summary) if exit_summary else None,
                }
            ),
            "hardstop": _safe_dict(
                {
                    "gx": "GX-ROT-HARDSTOP-V1",
                    "enabled": True,
                    "cfg": {"stop_pct": self.cfg.hardstop_pct, "break_rule": self.cfg.hardstop_break_rule, "break_n": self.cfg.hardstop_break_n},
                    "actions": hardstop_actions,
                    "reasons": hardstop_reasons,
                }
            ),
            "holding_reason": _safe_dict(holding_summary) if holding_summary else None,
        }
        return out

    def _eval_hardstop(self, *, positions: List[Dict[str, Any]], trade_date: str) -> List[Dict[str, Any]]:
        acts: List[Dict[str, Any]] = []
        for p in positions:
            try:
                lots = int(p.get("lots") or p.get("lot") or p.get("qty_lots") or 0)
            except Exception:
                lots = 0
            symbol = p.get("symbol") or p.get("code") or p.get("ticker")
            name = p.get("name") or p.get("sec_name") or ""
            # pct stop
            pnl_pct = p.get("pnl_pct") or p.get("return_pct")
            if pnl_pct is not None:
                try:
                    v = float(pnl_pct)
                    if v <= -abs(self.cfg.hardstop_pct):
                        acts.append(
                            {
                                "gx": "GX-ROT-HARDSTOP-V1",
                                "type": "HARDSTOP_PCT",
                                "symbol": symbol,
                                "name": name,
                                "pnl_pct": v,
                                "action": "TRIM_1LOT" if lots >= 2 else "EXIT_ALL",
                                "exec": "ASAP",
                                "note": f"pnl_pct<=-{self.cfg.hardstop_pct:.2f}",
                            }
                        )
                        continue
                except Exception:
                    pass
            # break stop (optional facts)
            # If provided by upstream: break_hit True/False
            if _as_bool(p.get("break_hit")):
                acts.append(
                    {
                        "gx": "GX-ROT-HARDSTOP-V1",
                        "type": "HARDSTOP_BREAK",
                        "symbol": symbol,
                        "name": name,
                        "action": "TRIM_1LOT" if lots >= 2 else "EXIT_ALL",
                        "exec": "ASAP",
                        "note": f"break_rule={self.cfg.hardstop_break_rule} n={self.cfg.hardstop_break_n}",
                    }
                )
        return acts


def _infer_positions(slots: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Best-effort to locate position facts. Must NOT raise."""
    # 1) governance/portfolio slots (if any)
    for key in ("portfolio", "positions", "holdings", "account"):
        v = slots.get(key)
        if isinstance(v, dict):
            for k2 in ("positions", "holdings"):
                arr = v.get(k2)
                if isinstance(arr, list) and arr and isinstance(arr[0], dict):
                    return arr
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return v
    # 2) watchlist slot may carry holdings in some builds
    w = slots.get("watchlist")
    if isinstance(w, dict):
        arr = w.get("holdings") or w.get("positions")
        if isinstance(arr, list) and arr and isinstance(arr[0], dict):
            return arr
    return []
