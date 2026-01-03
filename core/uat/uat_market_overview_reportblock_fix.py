# -*- coding: utf-8 -*-
"""
UAT · MarketOverviewBlock (ReportBlock payload contract)

Run: python core/uat/uat_market_overview_reportblock_fix.py
"""
from __future__ import annotations

from core.reporters.report_context import ReportContext
from core.reporters.report_blocks.market_overview_blk import MarketOverviewBlock


def main() -> None:
    blk = MarketOverviewBlock()

    # 1) indices only use symbols.yaml index_core keys; extra keys should not appear
    ctx = ReportContext(
        trade_date="2099-01-01",
        kind="EOD",
        slots={
            "market_overview": {
                "indices": {
                    "sh": {"pct": 0.0, "close": 3000.0},
                    "sz": {"pct": 0.0, "close": 10000.0},
                    "hs300": {"pct": 0.0, "close": 4000.0},
                    "zz500": {"pct": 0.0, "close": 7000.0},
                    "kc50": {"pct": 0.0, "close": 1000.0},
                    # extra keys
                    "kcb50": {"pct": 0.0, "close": 1000.0},
                    "cyb": {"pct": 0.0, "close": 2000.0},
                },
                "amount": {"amount_total": 20000.0, "amount_delta": -1000.0, "unit": "亿元"},
                "breadth": {"adv_ratio": 0.45},
            }
        },
    )
    rb = blk.render(ctx, {})
    assert rb.block_alias == "market.overview"
    assert "KCB50" not in rb.payload
    assert "CYB" not in rb.payload

    # 2) fundflow without delta -> no output line but warning
    ctx2 = ReportContext(
        trade_date="2099-01-01",
        kind="EOD",
        slots={
            "market_overview": {
                "indices": {"hs300": {"pct": 0.0, "close": 4000.0}},
                "amount": {"amount_total": 20000.0, "amount_delta": -1000.0, "unit": "亿元"},
                "breadth": {"adv_ratio": 0.45},
                "fundflow": {"main_net": 12.3, "north_net": -4.5},
            }
        },
    )
    rb2 = blk.render(ctx2, {})
    assert "资金流" not in rb2.payload
    assert "missing:fundflow_delta" in rb2.warnings

    # 3) fundflow window derives delta -> output exists
    ctx3 = ReportContext(
        trade_date="2099-01-01",
        kind="EOD",
        slots={
            "market_overview": {
                "indices": {"hs300": {"pct": 0.0, "close": 4000.0}},
                "amount": {"amount_total": 20000.0, "amount_delta": -1000.0, "unit": "亿元"},
                "breadth": {"adv_ratio": 0.45},
                "fundflow": {
                    "window": [
                        {"main_net": 10.0, "north_net": 5.0},
                        {"main_net": 6.0, "north_net": 7.0},
                    ],
                    "unit": "亿",
                },
            }
        },
    )
    rb3 = blk.render(ctx3, {})
    assert "资金流" in rb3.payload
    assert "较前一日" in rb3.payload

    print("OK")


if __name__ == "__main__":
    main()
