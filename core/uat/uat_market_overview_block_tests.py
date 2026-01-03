# -*- coding: utf-8 -*-
"""UAT: MarketOverviewBlock amount/feeling formatting.

Run:
    python core/uat/uat_market_overview_block_tests.py
"""
from __future__ import annotations

from core.reporters.report_context import ReportContext
from core.reporters.report_blocks.market_overview_blk import MarketOverviewBlock



def _payload_text(payload):
    """Normalize ReportBlock.payload to a searchable string."""
    if isinstance(payload, dict):
        content = payload.get("content")
        if isinstance(content, list):
            return "\n".join([str(x) for x in content])
        return str(payload)
    return str(payload)


def main() -> None:

    blk = MarketOverviewBlock()

    # Case A: nested amount shape (legacy from some engines)
    slots = {
        "market_overview": {
            "indices": {
                "hs300": {"pct_1d": 0.12, "close": 3888.0},
            },
            "amount": {"amount": 20446.69, "delta": 100.0, "unit": "亿元", "top20_amount_ratio": 0.779},
            "breadth": {"adv_ratio": 0.39},
        }
    }
    ctx = ReportContext(trade_date="2025-12-31", kind="EOD", slots=slots)
    rb = blk.render(ctx, doc_partial={})
    assert "missing:amount" not in (rb.warnings or []), rb.warnings
    assert "成交额" in _payload_text(rb.payload), rb.payload

    # Case B: flat amount shape (recommended)
    slots["market_overview"]["amount"] = {"amount": 20446.69, "delta": -50.0, "unit": "亿元", "top20_amount_ratio": 0.779}
    ctx = ReportContext(trade_date="2025-12-31", kind="EOD", slots=slots)
    rb = blk.render(ctx, doc_partial={})
    assert "missing:amount" not in (rb.warnings or []), rb.warnings
    assert "成交额" in _payload_text(rb.payload), rb.payload

    # Feeling: derived from adv_ratio/top20 should not warn missing:feeling
    assert "missing:feeling" not in (rb.warnings or []), rb.warnings



    # Case C: breadth missing in market_overview, fallback to unified_emotion._raw_data.market_sentiment
    slots_c = {
        "market_overview": {
            "indices": {"hs300": {"pct_1d": 0.0, "close": 4000.0}},
            "amount": {"amount_total": 20000.0, "amount_delta": -1000.0, "unit": "亿元"},
        },
        "structure": {
            "north_proxy_pressure": {"state": "pressure_low", "meaning": "", "evidence": {"pressure_level": "NEUTRAL", "pressure_score": 27.9}},
        },
        "factors": {
            "unified_emotion": {
                "score": 50.0,
                "level": "NEUTRAL",
                "details": {
                    "_raw_data": {
                        "market_sentiment": {"adv": 1234, "dec": 2345, "flat": 100, "limit_up": 45, "limit_down": 12},
                    }
                },
            }
        },
    }
    ctx_c = ReportContext(trade_date="2025-12-31", kind="EOD", slots=slots_c)
    rb_c = blk.render(ctx_c, doc_partial={})
    assert "missing:breadth" not in (rb_c.warnings or []), rb_c.warnings
    assert "上涨 1234" in _payload_text(rb_c.payload), rb_c.payload

    print("ALL TESTS PASSED")

if __name__ == "__main__":
    main()
