# -*- coding: utf-8 -*-
"""UAT: MarketOverviewBlock should never return None and should not raise.

Run:
    python core/uat/uat_market_overview_no_none.py
"""
from __future__ import annotations

from core.reporters.report_blocks.market_overview_blk import MarketOverviewBlock
from core.reporters.report_context import ReportContext

def _ctx(slots: dict) -> ReportContext:
    # Minimal context object fields used by block
    return ReportContext(
        market="CN",
        kind="EOD",
        trade_date="2025-12-31",
        slots=slots,
        meta={},
    )

def main() -> None:
    blk = MarketOverviewBlock()

    # Case 1: empty slot -> placeholder
    out = blk.render(_ctx({"market_overview": {}}), {})
    assert out is not None and out.block_alias == "market.overview"

    # Case 2: wrong shapes should not crash
    slots = {"market_overview": {"amount": None, "breadth": "oops", "indices": 1}}
    out = blk.render(_ctx(slots), {})
    assert out is not None and out.block_alias == "market.overview"

    # Case 3: valid minimal
    slots = {"market_overview": {"amount": {"amount": 20446.69, "unit": "亿元"}, "breadth": {"adv_ratio": 0.39}}}
    out = blk.render(_ctx(slots), {})
    assert out is not None and out.block_alias == "market.overview"

    print("ALL TESTS PASSED")

if __name__ == "__main__":
    main()
