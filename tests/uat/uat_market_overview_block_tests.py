# -*- coding: utf-8 -*-
import datetime as _dt

from core.reporters.report_context import ReportContext
from core.reporters.report_blocks.market_overview_blk import MarketOverviewBlock


def _mk_ctx(slots):
    return ReportContext(
        trade_date=_dt.date(2026, 1, 21),
        kind="EOD",
        slots=slots,
    )


def test_market_overview_top20_ratio_canonical_and_3d_series():
    slots = {
        "market_overview": {
            "indices": {
                "sh": {"pct": 0.0017, "close": 4108.88},
                "sz": {"pct": 0.0015, "close": 14301.92},
            },
            "amount": {
                "total": 30253.57,
                "delta": 1207.53,
                # upstream may also provide canonical top20_ratio, but here we rely on liquidity_quality factor
            },
            "breadth": {"adv": 2251, "dec": 2814, "flat": 118},
        },
        "factors": {
            "liquidity_quality": {"details": {"top20_ratio": 0.1063}},
            "watchlist_lead": {
                "details": {
                    "lead_panels": {
                        "liquidity_quality": {
                            "evidence": {
                                "series": [
                                    {"trade_date": "2026-01-17", "top20_ratio": 0.1031},
                                    {"trade_date": "2026-01-20", "top20_ratio": 0.1048},
                                    {"trade_date": "2026-01-21", "top20_ratio": 0.1063},
                                ]
                            }
                        }
                    }
                }
            },
        },
    }
    ctx = _mk_ctx(slots)
    blk = MarketOverviewBlock()
    out = blk.render(ctx)
    text = "\n".join(out.payload.get("content", []))
    assert "Top20 成交占比" in text
    assert "3D:" in text
    assert "src=liquidity_quality.details.top20_ratio" in text
    assert not out.warnings


def test_market_overview_proxy_top20pct_warns_and_not_mislabeled():
    slots = {
        "market_overview": {
            "indices": {"sh": {"pct": 0.0017, "close": 4108.88}},
            "amount": {"total": 30253.57, "delta": 1207.53},
        },
        "factors": {
            # canonical missing on purpose
            "crowding_concentration": {"details": {"top20_amount_ratio": 0.779}},
        },
    }
    ctx = _mk_ctx(slots)
    blk = MarketOverviewBlock()
    out = blk.render(ctx)
    text = "\n".join(out.payload.get("content", []))
    assert "拥挤代理(top20pct)" in text
    assert "Top20 成交占比" not in text  # no canonical -> do not print mislabeled
    assert any(w.startswith("suspect:top20_amount_ratio_high_from:") for w in out.warnings)
