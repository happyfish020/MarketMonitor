# -*- coding: utf-8 -*-
"""UnifiedRisk V12 · Regime History Service · v1.0

OOP rule:
- Engine files act as orchestrators only (no new defs added there).
- All history loading/injection lives here.

Data source:
- SQLite L2 table: ur_decision_evidence_snapshot (structured des_payload_json)

This service is read-only (report narrative convenience). It does not affect Gate/DRS/Execution.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import json
import sqlite3
import re


def _as_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _dig(d: Any, *path: str) -> Any:
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _detect_stage(trend: str, drs_sig: Optional[str], adv_ratio: Optional[float], amount_ratio: Optional[float]) -> str:
    if not trend or drs_sig is None:
        return "UNKNOWN"
    t = (trend or "").strip().lower()
    s = (drs_sig or "").strip().upper()
    adv = adv_ratio if adv_ratio is not None else 0.0
    amt = amount_ratio if amount_ratio is not None else 0.0

    # Normalize trend states from StructureFacts/Factor outputs.
    if t in ("in_force", "inforce", "intact"):
        t = "in_force"
    elif t in ("weakening", "mixed", "weak"):
        t = "weakening"
    elif t in ("broken",):
        t = "broken"
    else:
        return "UNKNOWN"

    if t == "broken" and s == "RED":
        return "S5"
    if t == "broken":
        return "S4"
    if t == "in_force" and s == "GREEN" and adv >= 0.55 and amt >= 0.9:
        return "S1"
    if t == "in_force" and s in ("GREEN", "YELLOW"):
        return "S2"
    if t == "weakening" and s == "RED":
        return "S4"
    if t == "weakening" and s == "GREEN" and adv >= 0.50 and amt >= 0.85:
        return "S2"
    if t == "weakening":
        return "S3"
    return "UNKNOWN"


_STAGE_NAME = {
    "S1": "进攻期（S1）",
    "S2": "修复期（S2）",
    "S3": "震荡期（S3）",
    "S4": "防守期（S4）",
    "S5": "去风险期（S5）",
    "UNKNOWN": "结构不明期（UNKNOWN）",
}


_STAGE_KEY_RE = re.compile(r"[（(]([A-Z0-9]+)[）)]")


def _stage_key_from_label(stage_label: str) -> str:
    """Convert a human stage label like '去风险期（S5）' to raw key 'S5'.

    This is used to keep report-layer logic consistent with persistence-layer audit,
    avoiding duplicated/branched stage computations.
    """
    if not isinstance(stage_label, str):
        return "UNKNOWN"
    s = stage_label.strip()
    m = _STAGE_KEY_RE.search(s)
    if m:
        return m.group(1).upper()
    su = s.upper()
    if su.startswith("S") and len(su) <= 4:
        return su
    if su == "UNKNOWN":
        return "UNKNOWN"
    return "UNKNOWN"


def _read_audit_note(conn: sqlite3.Connection, trade_date: str, report_kind: str, event: str) -> Optional[Dict[str, Any]]:
    """Read the latest audit note JSON for (trade_date, report_kind, event)."""
    try:
        if conn is None:
            return None
        row = conn.execute(
            """
            SELECT note
            FROM ur_persistence_audit
            WHERE trade_date=? AND report_kind=? AND event=?
            ORDER BY id DESC
            LIMIT 1
            """,
            (trade_date, report_kind, event),
        ).fetchone()
        if not row:
            return None
        raw = row[0] if isinstance(row, (tuple, list)) else row.get("note")
        if not raw:
            return None
        return json.loads(raw)
    except Exception:
        return None


@dataclass(frozen=True)
class RegimeHistoryService:
    @staticmethod
    def load_history(conn: sqlite3.Connection, report_kind: str, n: int = 10) -> List[Dict[str, Any]]:
        if conn is None:
            return []
        rows = conn.execute(
            """
            SELECT trade_date, des_payload_json
            FROM ur_decision_evidence_snapshot
            WHERE report_kind=?
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            (report_kind, int(n)),
        ).fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows or []:
            trade_date = r["trade_date"]
            raw_json = r["des_payload_json"]
            try:
                payload = json.loads(raw_json) if raw_json else {}
            except Exception:
                payload = {}

            drs_sig = _dig(payload, "governance", "drs")
            drs_u = drs_sig.strip().upper() if isinstance(drs_sig, str) else None

            trend = _dig(payload, "structure", "trend_in_force", "state")
            trend_s = trend if isinstance(trend, str) else None

            amount_ratio = _as_float(_dig(payload, "structure", "amount", "evidence", "amount_ratio"))
            adv_ratio = _as_float(_dig(payload, "structure", "crowding_concentration", "evidence", "adv_ratio"))

            st = _detect_stage(trend_s or "", drs_u, adv_ratio, amount_ratio)
            out.append(
                {
                    "trade_date": trade_date,
                    "stage_raw": st,
                    "stage": _STAGE_NAME.get(st, _STAGE_NAME["UNKNOWN"]),
                    "drs": drs_u or "NA",
                    "trend": trend_s or "NA",
                    "amount_ratio": amount_ratio,
                    "adv_ratio": adv_ratio,
                }
            )

        return sorted(out, key=lambda x: x.get("trade_date") or "")

    @staticmethod
    def inject(context, conn: sqlite3.Connection, report_kind: str, n: int = 10) -> None:
        """Inject regime history + shift/stats into context.slots.

        Rules:
        - Always inject `regime_history` (if available).
        - Always inject `regime_stats` and `regime_shift` from history as a fallback (so report never shows missing).
        - If persistence audit has REGIME_SHIFT/REGIME_STATS for the same day, it overwrites fallback as single source of truth.
        """
        try:
            hist = RegimeHistoryService.load_history(conn=conn, report_kind=report_kind, n=n)
            if isinstance(hist, list) and hist:
                context.slots["regime_history"] = hist
                last = hist[-1] if isinstance(hist[-1], dict) else {}
                context.slots["regime_current_stage_raw"] = last.get("stage_raw") or _stage_key_from_label(last.get("stage") or "")
                context.slots["regime_current_stage"] = last.get("stage")
        
            trade_date = getattr(context, "trade_date", None) or (context.slots.get("trade_date") if isinstance(getattr(context, "slots", None), dict) else None)
        
            # ---- Fallback: derive stats/shift from history (report must never show missing) ----
            if isinstance(hist, list) and len(hist) >= 2:
                # normalize last 20
                last20 = [x for x in hist[-20:] if isinstance(x, dict)]
                dist20 = {}
                for x in last20:
                    s = x.get("stage_raw") or "UNKNOWN"
                    dist20[s] = dist20.get(s, 0) + 1
        
                # consecutive S5 days (count from end)
                consec_s5 = 0
                for x in reversed(hist):
                    if isinstance(x, dict) and (x.get("stage_raw") or "UNKNOWN") == "S5":
                        consec_s5 += 1
                    else:
                        break
        
                context.slots["regime_stats"] = {
                    "last_10d": [{"trade_date": x.get("trade_date"), "stage": x.get("stage")} for x in hist[-10:] if isinstance(x, dict)],
                    "consecutive_s5_days": consec_s5,
                    "stage_distribution_20d": dist20,
                    "asof_trade_date": trade_date,
                    "source": "history_fallback",
                }
        
                prev = hist[-2] if isinstance(hist[-2], dict) else {}
                cur = hist[-1] if isinstance(hist[-1], dict) else {}
                prev_stage = prev.get("stage_raw") or "UNKNOWN"
                cur_stage = cur.get("stage_raw") or "UNKNOWN"
        
                order = {"UNKNOWN": 0, "S1": 1, "S2": 2, "S3": 3, "S4": 4, "S5": 5}
                prev_rank = order.get(prev_stage, 0)
                cur_rank = order.get(cur_stage, 0)
        
                if prev_stage != cur_stage:
                    shift_type = "RISK_ESCALATION" if cur_rank > prev_rank else "RISK_EASING"
                    severity = "HIGH" if cur_stage == "S5" else ("MED" if shift_type == "RISK_ESCALATION" else "LOW")
                else:
                    shift_type = "NO_CHANGE"
                    severity = "LOW"
        
                context.slots["regime_shift"] = {
                    "from": prev_stage,
                    "to": cur_stage,
                    "shift_type": shift_type,
                    "severity": severity,
                    "reason": [],
                    "prev": {"trade_date": prev.get("trade_date"), "drs": prev.get("drs"), "trend": prev.get("trend"), "amount_ratio": prev.get("amount_ratio"), "adv_ratio": prev.get("adv_ratio")},
                    "curr": {"trade_date": cur.get("trade_date"), "drs": cur.get("drs"), "trend": cur.get("trend"), "amount_ratio": cur.get("amount_ratio"), "adv_ratio": cur.get("adv_ratio")},
                    "source": "history_fallback",
                }
        
            # ---- Overwrite from persistence audit if exists (single source of truth) ----
            if isinstance(trade_date, str) and trade_date.strip():
                shift = _read_audit_note(conn, trade_date=trade_date, report_kind=report_kind, event="REGIME_SHIFT")
                if isinstance(shift, dict) and shift:
                    shift["source"] = "audit"
                    context.slots["regime_shift"] = shift
        
                stats = _read_audit_note(conn, trade_date=trade_date, report_kind=report_kind, event="REGIME_STATS")
                if isinstance(stats, dict) and stats:
                    stats["source"] = "audit"
                    context.slots["regime_stats"] = stats
        
        except Exception:
            return
