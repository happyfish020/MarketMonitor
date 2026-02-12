# -*- coding: utf-8 -*-
"""rotation.snapshot (ReportBlock)

Frozen Engineering Contract
---------------------------
- Report layer reads ONLY snapshot-derived slots produced upstream (DS -> Fetcher -> Engine).
- Report block MUST NOT touch DB / SP / large tables / views / analytics.
- This block renders a short human-friendly summary for NO_*_TODAY cases.

Remember: 报告层只读 snapshot，任何复杂计算一律禁止。
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from core.reporters.blocks.base_block import ReportBlockBase


def _safe_get(d: Any, *keys: str) -> Optional[Any]:
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _parse_source_json(obj: Any) -> Optional[Dict[str, Any]]:
    """SOURCE_JSON may be dict already, or a JSON string."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, str):
        s = obj.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return None
    return None


def _fmt_signal_stat(stat: Any) -> str:
    if not isinstance(stat, dict) or not stat:
        return ""
    parts = []
    for k, v in stat.items():
        parts.append(f"{k}={v}")
    return "signal:" + ",".join(parts)


def _fmt_transition_top(tt: Any, topn: int = 2) -> str:
    if not isinstance(tt, list) or not tt:
        return ""
    parts = []
    for item in tt[: max(0, int(topn))]:
        if not isinstance(item, dict):
            continue
        tr = item.get("transition")
        cnt = item.get("cnt")
        if tr is None or cnt is None:
            continue
        parts.append(f"{tr}={cnt}")
    if not parts:
        return ""
    return "trans:" + ", ".join(parts)


def _short_summary(label: str, sector_name: str, source_json: Any) -> str:
    """Return a concise one-liner; fallback to raw if parsing fails."""
    data = _parse_source_json(source_json)
    if not data:
        # fallback raw
        return f"- {sector_name}: {source_json}"
    reason = data.get("reason_code") or data.get("reason") or "UNKNOWN"
    stat = _fmt_signal_stat(data.get("signal_stat"))
    trans = _fmt_transition_top(data.get("transition_top"), topn=2)
    segs = [f"- {sector_name}: reason={reason}"]
    if stat:
        segs.append(stat)
    if trans:
        segs.append(trans)
    return "; ".join(segs)


class RotationSnapshotBlock(ReportBlockBase):
    """Sector Rotation Snapshot block (slot-only)."""

    block_key = "rotation.snapshot"
    title = "板块轮换（Sector Rotation · Snapshot）"

    def render(self, context) -> str:
        # slot-only: provided by upstream DS/fetcher/engine
        snap = getattr(context, "slots", {}).get("rotation_snapshot")
        lines = [f"## {self.title}", ""]

        trade_date = getattr(context, "trade_date", None) or _safe_get(snap, "meta", "trade_date")
        run_id = getattr(context, "run_id", None) or _safe_get(snap, "meta", "run_id")

        if trade_date:
            lines.append(f"- Trade Date: **{trade_date}**")
        if run_id:
            lines.append(f"- Rotation Baseline: `{run_id}`")
        lines.append("")

        if not isinstance(snap, dict):
            lines.append("> ⚠️ rotation_snapshot slot missing (upstream DS/fetcher not executed)")
            return "\n".join(lines).rstrip() + "\n"

        # Entry
        entry = snap.get("entry") if isinstance(snap.get("entry"), dict) else {}
        entry_allowed = bool(entry.get("allowed"))
        entry_rows = entry.get("rows") if isinstance(entry.get("rows"), list) else []
        entry_summary = entry.get("summary")

        lines.append(f"- EntryAllowed: **{'YES' if entry_allowed else 'NO'}**")
        if entry_rows:
            # detailed entries should be rendered elsewhere (already handled in your previous version)
            # keep minimal to avoid verbosity
            top1 = entry_rows[0]
            lines.append(f"  - Top1: {top1.get('SECTOR_NAME') or top1.get('sector_name')}")
        else:
            # summary short format
            sector_name = "NO_ENTRY_TODAY"
            src = entry_summary.get("SOURCE_JSON") if isinstance(entry_summary, dict) else None
            lines.append("  " + _short_summary("entry", sector_name, src))
        lines.append("")

        # Holding
        holding = snap.get("holding") if isinstance(snap.get("holding"), dict) else {}
        holding_rows = holding.get("rows") if isinstance(holding.get("rows"), list) else []
        holding_summary = holding.get("summary")

        lines.append("- Holding:")
        if holding_rows:
            for r in holding_rows:
                nm = r.get("SECTOR_NAME") or r.get("sector_name")
                hd = r.get("HOLD_DAYS") or r.get("hold_days")
                mh = r.get("MIN_HOLD_DAYS") or r.get("min_hold_days")
                lines.append(f"  - {nm}: hold={hd}/{mh}")
        else:
            sector_name = "NO_HOLDING_TODAY"
            src = holding_summary.get("SOURCE_JSON") if isinstance(holding_summary, dict) else None
            lines.append("  " + _short_summary("holding", sector_name, src))
        lines.append("")

        # Exit
        ex = snap.get("exit") if isinstance(snap.get("exit"), dict) else {}
        exit_rows = ex.get("rows") if isinstance(ex.get("rows"), list) else []
        exit_summary = ex.get("summary")

        lines.append("- Exit:")
        if exit_rows:
            for r in exit_rows:
                nm = r.get("SECTOR_NAME") or r.get("sector_name")
                st = r.get("EXIT_EXEC_STATUS") or r.get("exit_exec_status")
                ed = r.get("EXEC_EXIT_DATE") or r.get("exec_exit_date")
                lines.append(f"  - {nm}: status={st}, exec={ed}")
        else:
            sector_name = "NO_EXIT_TODAY"
            src = exit_summary.get("SOURCE_JSON") if isinstance(exit_summary, dict) else None
            lines.append("  " + _short_summary("exit", sector_name, src))

        lines.append("")
        lines.append("> 注：本块仅展示 AScraperRunner 已落库的 rotation snapshots（SECOPR.CN_ROTATION_*_SNAP_T），报告层不参与信号计算。")
        return "\n".join(lines).rstrip() + "\n"
