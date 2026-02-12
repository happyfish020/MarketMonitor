# -*- coding: utf-8 -*-
"""UnifiedRisk V12 · Replay CLI (SQLite)

Run from repo root.

List recent runs:
  python replay_run.py --db data/persistent/unifiedrisk.db --list --limit 20

Replay stored only:
  python replay_run.py --db data/persistent/unifiedrisk.db --run-id <RUN_ID> --mode stored --out out/replay

Re-render (recompute) and diff:
  python replay_run.py --db data/persistent/unifiedrisk.db --run-id <RUN_ID> --mode recompute \
      --block-specs config/report_blocks.yaml --out out/replay

Notes:
- recompute mode re-renders markdown from persisted L2 decision-evidence (des_payload)
  using *current* report blocks/specs. It does NOT recompute factors/gate.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Optional

from core.actions.actionhint_service import ActionHintService
from core.persistence.audit_diff import diffs_to_markdown
from core.persistence.replay_runner import dump_json, replay_run
from core.persistence.run_store import ReportDump, RunPayload, RunStore
from core.persistence.sqlite.sqlite_connection import connect_sqlite
from core.persistence.sqlite.sqlite_run_store import SqliteRunStore
from core.reporters.report_context import ReportContext
from core.reporters.report_engine import ReportEngine
from core.reporters.report_renderer import MarkdownRenderer
from core.reporters.report_blocks.market_overview_blk import MarketOverviewBlock
from core.reporters.report_blocks.structure_facts_blk import StructureFactsBlock
from core.reporters.report_blocks.summary_a_n_d_blk import SummaryANDBlock
from core.reporters.report_blocks.execution_summary_blk import ExecutionSummaryBlock
from core.reporters.report_blocks.execution_quick_reference_blk import ExecutionQuickReferenceBlock
from core.reporters.report_blocks.exit_readiness_blk import ExitReadinessBlock
from core.reporters.report_blocks.context_overnight_blk import ContextOvernightBlock


def get_store(db_path: str) -> RunStore:
    conn = connect_sqlite(db_path)
    conn.row_factory = sqlite3.Row
    return SqliteRunStore(conn)


def v12_rerender_builder(payload: RunPayload, block_specs_path: Optional[str]) -> ReportDump:
    stored = payload.report_dump or {}
    des_payload = stored.get("des_payload")
    if not isinstance(des_payload, dict):
        if isinstance(payload.slots_final, dict):
            des_payload = payload.slots_final
        else:
            raise ValueError("missing des_payload for rerender (need L2 decision evidence snapshot)")

    # Ensure minimal governance fields exist for ReportEngine contracts.
    # Frozen engineering rule: no silent fallbacks; best-effort repair using persisted L1/L2.
    gov = des_payload.get("governance")
    if not isinstance(gov, dict):
        gov = {}
        des_payload["governance"] = gov

    if gov.get("gate") in (None, ""):
        # Prefer L1 gate_decision if available
        gd = payload.gate_decision
        if isinstance(gd, dict):
            g = gd.get("gate") or gd.get("code")
            if isinstance(g, str) and g.strip():
                gov["gate"] = g.strip()

    if gov.get("gate") in (None, ""):
        # Try report_meta fallback (some versions store gate here)
        rm = stored.get("report_meta")
        if isinstance(rm, dict):
            g = rm.get("gate") or rm.get("gate_code")
            if isinstance(g, str) and g.strip():
                gov["gate"] = g.strip()

    if gov.get("gate") in (None, ""):
        raise ValueError(
            "gate missing for rerender: expected des_payload['governance']['gate'] "
            "or payload.gate_decision.* / report_meta.*"
        )

    engine = ReportEngine(
        market="CN",
        actionhint_service=ActionHintService(),
        block_builders={
            "summary": SummaryANDBlock().render,
            "structure.facts": StructureFactsBlock().render,
            "context.overnight": ContextOvernightBlock().render,
            "market.overview": MarketOverviewBlock().render,
            "execution.summary": ExecutionSummaryBlock().render,
            "execution_quick_reference": ExecutionQuickReferenceBlock().render,
            "exit.readiness": ExitReadinessBlock().render,
        },
        block_specs_path=block_specs_path,
    )

    ctx = ReportContext(kind=payload.kind, trade_date=payload.trade_date, slots=des_payload)
    doc = engine.build_report(ctx)
    rendered = MarkdownRenderer().render(doc)

    return {"des_payload": des_payload, "rendered": rendered}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="SQLite db path, e.g. data/persistent/unifiedrisk.db")
    ap.add_argument("--run-id", default=None, help="Run ID to replay")
    ap.add_argument("--mode", default="recompute", choices=["stored", "recompute"], help="Replay mode")
    ap.add_argument("--block-specs", default=None, help="Path to report_blocks.yaml")
    ap.add_argument("--out", default="out/replay", help="Output directory")
    ap.add_argument("--trade-date", default=None, help="Filter for --list")
    ap.add_argument("--kind", default=None, help="Filter for --list")
    ap.add_argument("--limit", type=int, default=20, help="Limit for --list")
    ap.add_argument("--list", action="store_true", help="List runs (uses find_runs)")
    ap.add_argument("--float-atol", type=float, default=1e-8)
    ap.add_argument("--ignore", action="append", default=[], help="Ignore glob paths (repeatable)")
    args = ap.parse_args()

    store = get_store(args.db)

    if args.list or not args.run_id:
        runs = store.find_runs(trade_date=args.trade_date, kind=args.kind, limit=int(args.limit))
        print(json.dumps(runs, ensure_ascii=False, indent=2))
        return

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    res = replay_run(
        store=store,
        run_id=str(args.run_id),
        mode=str(args.mode),
        builder=v12_rerender_builder if args.mode == "recompute" else None,
        block_specs_path=args.block_specs,
        float_atol=float(args.float_atol),
        ignore_globs=args.ignore,
    )

    if res.stored_report_dump is not None:
        dump_json(str(out_dir / "stored.json"), res.stored_report_dump)
    if res.recomputed_report_dump is not None:
        dump_json(str(out_dir / "recomputed.json"), res.recomputed_report_dump)

    md = diffs_to_markdown(res.diffs)
    (out_dir / "diff.md").write_text(md, encoding="utf-8")

    summary = {
        "run_id": res.run_id,
        "trade_date": res.trade_date,
        "kind": res.kind,
        "schema_version": res.schema_version,
        "engine_version": res.engine_version,
        "diff_count": len(res.diffs),
        "warnings": list(res.warnings),
        "out_dir": str(out_dir.resolve()),
    }
    dump_json(str(out_dir / "summary.json"), summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
