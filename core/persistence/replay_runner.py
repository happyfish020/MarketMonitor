# -*- coding: utf-8 -*-
"""UnifiedRisk V12 · Replay Runner (db → report) + Audit Closure (Frozen)

This module does NOT assume your engine/pipeline structure.
Instead, it accepts:
- store: RunStore adapter
- builder: a callable that can rebuild report_dump from persisted payload
- diff options

Two modes:
- "stored": return persisted report_dump only (fast, exact record)
- "recompute": rebuild report_dump from persisted payload, then diff vs stored

Frozen principles:
- deterministic diff
- explicit warnings for missing stored/recomputed artifacts
- no silent coercions
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Optional, Sequence, Tuple, List

from core.persistence.audit_diff import DiffItem, diff_json
from core.persistence.run_store import ReportDump, RunPayload, RunStore

ReportBuilder = Callable[[RunPayload, Optional[str]], ReportDump]


@dataclass(frozen=True)
class ReplayResult:
    run_id: str
    trade_date: str
    kind: str
    schema_version: str
    engine_version: Optional[str]

    stored_report_dump: Optional[ReportDump]
    recomputed_report_dump: Optional[ReportDump]

    diffs: Tuple[DiffItem, ...]
    warnings: Tuple[str, ...]


def replay_run(
    *,
    store: RunStore,
    run_id: str,
    mode: str = "recompute",
    builder: Optional[ReportBuilder] = None,
    block_specs_path: Optional[str] = None,
    float_atol: float = 1e-8,
    ignore_globs: Optional[Sequence[str]] = None,
) -> ReplayResult:
    if mode not in ("stored", "recompute"):
        raise ValueError(f"invalid mode: {mode} (expected stored|recompute)")

    payload = store.load_run(run_id)
    stored = payload.report_dump

    warnings: List[str] = []
    recomputed: Optional[ReportDump] = None
    diffs: List[DiffItem] = []

    if stored is None:
        warnings.append("missing:stored_report_dump")

    if mode == "recompute":
        if builder is None:
            raise ValueError("builder is required for mode=recompute")
        recomputed = builder(payload, block_specs_path)
        if isinstance(recomputed, dict):
            rm = recomputed.get("report_meta")
            if not isinstance(rm, dict):
                rm = {}
                recomputed["report_meta"] = rm
            rm.setdefault("run_id", payload.run_id)



        if recomputed is None:
            warnings.append("empty:recomputed_report_dump")
        if (stored is not None) and (recomputed is not None):
            diffs = diff_json(stored, recomputed, float_atol=float_atol, ignore_globs=ignore_globs)

    return ReplayResult(
        run_id=payload.run_id,
        trade_date=payload.trade_date,
        kind=payload.kind,
        schema_version=payload.schema_version,
        engine_version=payload.engine_version,
        stored_report_dump=stored,
        recomputed_report_dump=recomputed,
        diffs=tuple(diffs),
        warnings=tuple(warnings),
    )


def dump_json(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
