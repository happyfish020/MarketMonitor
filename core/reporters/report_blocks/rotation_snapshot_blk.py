# -*- coding: utf-8 -*-
"""UnifiedRisk V12 · Rotation Snapshot block (Frozen)

- slot-only: context.slots["rotation_snapshot"]
- NO DB/SP access in report layer
- NO_*_TODAY uses short format (reason_code / signal_stat / transition_top Top2)

记住：报告层只读 snapshot，任何复杂计算一律禁止。
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase
from core.utils.logger import get_logger

log = get_logger(__name__)


def _parse(x: Any) -> Optional[Dict[str, Any]]:
    if x is None:
        return None
    if isinstance(x, dict):
        return x
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        try:
            v = json.loads(s)
            return v if isinstance(v, dict) else None
        except Exception:
            return None
    return None


def _fmt_signal(stat: Any) -> str:
    if not isinstance(stat, dict) or not stat:
        return ""
    return "signal:" + ",".join([f"{k}={v}" for k, v in stat.items()])


def _fmt_trans(tt: Any, topn: int = 2) -> str:
    if not isinstance(tt, list) or not tt:
        return ""
    parts: List[str] = []
    for item in tt[:max(0, int(topn))]:
        if not isinstance(item, dict):
            continue
        tr = item.get("transition")
        cnt = item.get("cnt")
        if tr is None or cnt is None:
            continue
        parts.append(f"{tr}={cnt}")
    return ("trans:" + ", ".join(parts)) if parts else ""


def _short(name: str, src: Any) -> str:
    d = _parse(src)
    if not d:
        return f"- {name}: {src}"
    reason = d.get("reason_code") or d.get("reason") or "UNKNOWN"
    segs = [f"- {name}: reason={reason}"]
    s = _fmt_signal(d.get("signal_stat"))
    t = _fmt_trans(d.get("transition_top"), topn=2)
    if s:
        segs.append(s)
    if t:
        segs.append(t)
    return "; ".join(segs)


class RotationSnapshotBlock(ReportBlockRendererBase):
    block_alias = "rotation.snapshot"
    title = "板块轮换（Sector Rotation · Snapshot）"

    def render(self, context: ReportContext, doc_partial: Dict[str, Any]) -> ReportBlock:
        warnings: List[str] = []
        try:
            snap = context.slots.get("rotation_snapshot") if isinstance(context.slots, dict) else None
            if not isinstance(snap, dict) or not snap:
                warnings.append("empty:rotation_snapshot")
                return ReportBlock(
                    block_alias=self.block_alias,
                    title=self.title,
                    warnings=warnings,
                    payload={"content": ["（未生成 rotation_snapshot：该区块仅用于占位）"],
                            "note": "注：本块只读展示 rotation snapshots；缺失不影响其它 block。"},
                )

            meta = snap.get("meta") if isinstance(snap.get("meta"), dict) else {}
            trade_date = meta.get("trade_date") or getattr(context, "trade_date", None)
            run_id = meta.get("run_id") or getattr(context, "run_id", None)

            lines: List[str] = []
            if trade_date:
                lines.append(f"- Trade Date: **{trade_date}**")
            if run_id:
                lines.append(f"- Rotation Baseline: `{run_id}`")
            lines.append("")

            entry = snap.get("entry") if isinstance(snap.get("entry"), dict) else {}
            allowed = bool(entry.get("allowed"))
            rows = entry.get("rows") if isinstance(entry.get("rows"), list) else []
            summary = entry.get("summary")

            lines.append(f"- EntryAllowed: **{'YES' if allowed else 'NO'}**")
            if rows:
                nm = (rows[0].get("SECTOR_NAME") or rows[0].get("sector_name") or "UNKNOWN")
                lines.append(f"  - Top1: {nm} (pool={max(0, len(rows)-1)})")
            else:
                src = summary.get("SOURCE_JSON") if isinstance(summary, dict) else None
                lines.append("  " + _short("NO_ENTRY_TODAY", src))
            lines.append("")

            holding = snap.get("holding") if isinstance(snap.get("holding"), dict) else {}
            h_rows = holding.get("rows") if isinstance(holding.get("rows"), list) else []
            h_sum = holding.get("summary")
            lines.append("- Holding:")
            if h_rows:
                for r in h_rows:
                    nm = r.get("SECTOR_NAME") or r.get("sector_name") or "UNKNOWN"
                    hd = r.get("HOLD_DAYS") or r.get("hold_days")
                    mh = r.get("MIN_HOLD_DAYS") or r.get("min_hold_days")
                    lines.append(f"  - {nm}: hold={hd}/{mh}")
            else:
                src = h_sum.get("SOURCE_JSON") if isinstance(h_sum, dict) else None
                lines.append("  " + _short("NO_HOLDING_TODAY", src))
            lines.append("")

            ex = snap.get("exit") if isinstance(snap.get("exit"), dict) else {}
            e_rows = ex.get("rows") if isinstance(ex.get("rows"), list) else []
            e_sum = ex.get("summary")
            lines.append("- Exit:")
            if e_rows:
                for r in e_rows:
                    nm = r.get("SECTOR_NAME") or r.get("sector_name") or "UNKNOWN"
                    st = r.get("EXIT_EXEC_STATUS") or r.get("exit_exec_status")
                    ed = r.get("EXEC_EXIT_DATE") or r.get("exec_exit_date")
                    lines.append(f"  - {nm}: status={st}, exec={ed}")
            else:
                src = e_sum.get("SOURCE_JSON") if isinstance(e_sum, dict) else None
                lines.append("  " + _short("NO_EXIT_TODAY", src))

            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                warnings=warnings,
                payload={
                    "content": lines,
                    "raw": snap,
                    "note": "注：本块仅展示 AScraperRunner 已落库的 rotation snapshots（SECOPR.CN_ROTATION_*_SNAP_T），报告层不参与信号计算。",
                },
            )

        except Exception as e:
            log.exception("RotationSnapshotBlock.render failed: %s", e)
            warnings.append("exception:rotation_snapshot_render")
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                warnings=warnings,
                payload={"content": ["RotationSnapshot 渲染异常（已捕获）。"],
                        "note": "注：异常已记录日志；本 block 不影响其它 block。"},
            )
