# -*- coding: utf-8 -*-
"""UAT: ReportEngine should not crash when a block builder returns None.

Run:
    python core/uat/uat_report_engine_builder_none_test.py
"""
from __future__ import annotations

from core.reporters.report_engine import ReportEngine, BlockSpec
from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock

def _none_builder(context, doc_partial):
    return None

def main() -> None:
    engine = ReportEngine(market="CN", actionhint_service=None, block_builders={"market.overview": _none_builder})
    # Minimal context (only fields accessed by build_report_no)
    ctx = ReportContext(
        trade_date="2099-01-01",
        kind="EOD",
        slots={"governance": {"gate": {"raw_gate": "CAUTION", "final_gate": "CAUTION"}}, "actionhint": {"gate": "CAUTION", "summary": "N"}},
        actionhint={"gate": "CAUTION", "summary": "N"},
    )
    doc = engine.build_report(ctx)
    # It should produce a placeholder for market.overview or warn if spec doesn't include it
    assert doc is not None
    # No exception means pass
    print("PASS: build_report does not crash on None builder return")

if __name__ == "__main__":
    main()
