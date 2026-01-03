# -*- coding: utf-8 -*-
"""
UAT · MarketOverviewBlock
- indices list/order driven by config/symbols.yaml index_core
- fundflow only prints when delta exists
"""
from __future__ import annotations

from typing import Any, Dict

from core.reporters.report_context import ReportContext
from core.reporters.report_blocks.market_overview_blk import MarketOverviewBlock


def _mk_ctx(slots: Dict[str, Any]) -> ReportContext:
    # minimal ReportContext: most fields are unused by MarketOverviewBlock
    return ReportContext(
        market="CN",
        kind="EOD",
        trade_date="2099-01-01",
        asof="2099-01-01",
        slots=slots,
    )


def test_indices_use_symbols_yaml_only():
    blk = MarketOverviewBlock()

    slots = {
        "market_overview": {
            "indices": {
                # expected keys likely exist in symbols.yaml index_core
                "sh": {"pct": 0.0, "close": 3000.0},
                "sz": {"pct": 0.0, "close": 10000.0},
                "hs300": {"pct": 0.0, "close": 4000.0},
                "zz500": {"pct": 0.0, "close": 7000.0},
                "kc50": {"pct": 0.0, "close": 1000.0},

                # extra keys (should NOT appear)
                "kcb50": {"pct": 0.0, "close": 1000.0},
                "cyb": {"pct": 0.0, "close": 2000.0},
                "_order": ["kcb50", "cyb", "hs300"],
            },
            "amount": {"amount_total": 20000.0, "amount_delta": -1000.0, "unit": "亿元"},
            "breadth": {"adv_ratio": 0.45},
            "feeling": "test feeling",
        }
    }

    ctx = _mk_ctx(slots)
    rb = blk.render(ctx, doc_partial={})
    s = "\n".join(rb.content)

    # should NOT contain extra keys
    assert "KCB50" not in s
    assert "CYB" not in s

    # should contain at least one core index
    assert "HS300" in s or "SH" in s or "SZ" in s


def test_fundflow_no_delta_not_printed():
    blk = MarketOverviewBlock()

    slots = {
        "market_overview": {
            "indices": {"hs300": {"pct": 0.0, "close": 4000.0}},
            "amount": {"amount_total": 20000.0, "amount_delta": -1000.0, "unit": "亿元"},
            "breadth": {"adv_ratio": 0.45},
            "fundflow": {"main_net": 12.3, "north_net": -4.5},  # no delta
            "feeling": "test feeling",
        }
    }
    ctx = _mk_ctx(slots)
    rb = blk.render(ctx, doc_partial={})
    s = "\n".join(rb.content)

    assert "资金流" not in s
    assert "missing:fundflow_delta" in (rb.warnings or [])


def test_fundflow_window_derives_delta():
    blk = MarketOverviewBlock()

    slots = {
        "market_overview": {
            "indices": {"hs300": {"pct": 0.0, "close": 4000.0}},
            "amount": {"amount_total": 20000.0, "amount_delta": -1000.0, "unit": "亿元"},
            "breadth": {"adv_ratio": 0.45},
            "fundflow": {
                "window": [
                    {"main_net": 10.0, "north_net": 5.0},
                    {"main_net": 6.0, "north_net": 7.0},
                ]
            },
            "feeling": "test feeling",
        }
    }
    ctx = _mk_ctx(slots)
    rb = blk.render(ctx, doc_partial={})
    s = "\n".join(rb.content)

    assert "资金流" in s
    assert "Δ" in s


if __name__ == "__main__":
    # minimal runner
    test_indices_use_symbols_yaml_only()
    test_fundflow_no_delta_not_printed()
    test_fundflow_window_derives_delta()
    print("OK")
