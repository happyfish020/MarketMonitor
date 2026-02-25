#-*- coding: utf-8 -*-
"""
reporting.py (CN_ENTRY_POOL_ROTATION_V1)

CLI-only output formatter.
Hard constraints:
- No side effects
- Stable, readable, non-empty output even when there are no actions/transitions
"""
from __future__ import annotations

from typing import Dict, List, Any


def _fmt_float(x):
    if x is None:
        return "None"
    try:
        return f"{float(x):.4g}"
    except Exception:
        return str(x)


def format_eod_summary(trade_date: str, snaps: Dict[str, dict], transitions: List[dict]) -> str:
    lines: List[str] = []
    lines.append(f"[CN_ENTRY_POOL_ROTATION_V1] EOD {trade_date}")
    lines.append("")
    lines.append("State Snapshot:")
    for sym in sorted(snaps.keys()):
        s = snaps[sym]
        state = s.get("state")
        breakout = _fmt_float(s.get("breakout_level"))
        cooling = s.get("cooldown_days_left")
        ok = s.get("confirm_ok_streak")
        fail = s.get("fail_streak")
        lines.append(f"- {sym} {state} | breakout={breakout} | cooling={cooling} | above_streak={ok} | below_streak={fail}")
    lines.append("")
    lines.append(f"Transitions: {len(transitions)}")
    for t in transitions:
        lines.append(f"- {t.get('from_state')} -> {t.get('to_state')} | {t.get('reason_code')} | {t.get('reason_text')}")
    return "\n".join(lines) + "\n"


def format_t1_summary(trade_date: str, executions: List[dict], transitions: List[dict]) -> str:
    lines: List[str] = []
    lines.append(f"[CN_ENTRY_POOL_ROTATION_V1] T+1 {trade_date}")
    lines.append("")
    lines.append("Execution Plan:")
    if not executions:
        lines.append("- (none) No BUY/SELL actions for this date based on current state.")
    else:
        for e in executions:
            sym = e.get("symbol")
            action = e.get("action")
            lots = e.get("lots")
            note = e.get("note") or ""
            limit_price = e.get("limit_price")
            lp = _fmt_float(limit_price) if limit_price is not None else "MKT"
            lines.append(f"- {sym} {action} {lots} lots @ {lp} | {note}")
    lines.append("")
    lines.append(f"State Transitions: {len(transitions)}")
    if not transitions:
        lines.append("- (none) No state transitions written on T+1.")
    else:
        for t in transitions:
            lines.append(f"- {t.get('from_state')} -> {t.get('to_state')} | {t.get('reason_code')} | {t.get('reason_text')}")
    return "\n".join(lines) + "\n"
