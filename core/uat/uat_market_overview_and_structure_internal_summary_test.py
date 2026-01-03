# -*- coding: utf-8 -*-
"""UAT-P0: ensure MarketOverview shows adv/dec from market_sentiment,
and StructureFactsBlock does not warn missing_semantics for internal _summary.

Run from project root:
    python -m core.uat.uat_market_overview_and_structure_internal_summary_test
or:
    python core/uat/uat_market_overview_and_structure_internal_summary_test.py
"""

from __future__ import annotations

import os
import sys
from typing import Dict, Any

# allow direct script run
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from core.reporters.report_context import ReportContext
from core.reporters.report_blocks.market_overview_blk import MarketOverviewBlock
from core.reporters.report_blocks.structure_facts_blk import StructureFactsBlock


def test_market_overview_breadth_from_market_sentiment() -> None:
    blk = MarketOverviewBlock()
    slots: Dict[str, Any] = {
        # minimal indices + amount
        "market_overview": {
            "indices": {
                "SH": {"pct": 0.0, "close": 3000.0},
            },
            "amount": {"amount": 10000.0, "delta": -100.0, "unit": "亿元"},
            "north_proxy_pressure": {"pressure_level": "NEUTRAL", "pressure_score": 27.9},
        },
        # THIS is the new expected source for adv/dec counts
        "market_sentiment": {
            "adv": 1234,
            "dec": 3456,
            "flat": 78,
            "limit_up": 12,
            "limit_down": 3,
            "adv_ratio": 0.26,
        },
    }
    ctx = ReportContext(trade_date="2025-12-31", kind="EOD", slots=slots)
    rb = blk.render(ctx, doc_partial={})
    payload = rb.payload or {}
    content = payload.get("content")
    assert isinstance(content, list), payload
    text = "\n".join([str(x) for x in content])
    assert "上涨 1234 家" in text and "下跌 3456 家" in text, text


def test_structure_facts_skip_internal_summary_no_missing_semantics() -> None:
    blk = StructureFactsBlock()
    slots: Dict[str, Any] = {
        "structure": {
            "index_tech": {"state": "strong", "meaning": "ok", "evidence": {"modifier": "x"}},
            "_summary": {"tags": ["breadth_damaged"]},  # internal, tags-only, no state
        }
    }
    ctx = ReportContext(trade_date="2025-12-31", kind="EOD", slots=slots)
    rb = blk.render(ctx, doc_partial={})
    warnings = rb.warnings or []
    assert not any(w == "missing_semantics:_summary" for w in warnings), warnings
    payload = rb.payload or {}
    content = payload.get("content") or []
    text = "\n".join([str(x) for x in content])
    assert "_summary" not in text, text


def main() -> None:
    test_market_overview_breadth_from_market_sentiment()
    test_structure_facts_skip_internal_summary_no_missing_semantics()
    print("PASS: market_overview breadth + structure_facts internal summary handling")


if __name__ == "__main__":
    main()
