# -*- coding: utf-8 -*-
import datetime as _dt

from core.reporters.report_context import ReportContext
from core.reporters.report_blocks.market_overview_blk import MarketOverviewBlock


def test_market_overview_stability_v7_proxy_does_not_override_canonical():
    slots = {
        "market_overview": {
            "indices": {"sh": {"pct": -0.0000, "close": 4741.93}},
            "amount": {"total": 39400.86, "delta": 2899.10, "top20_ratio": 0.115},
        },
        "factors": {
            # proxy present but should not override canonical
            "crowding_concentration": {"details": {"top20_amount_ratio": 0.715}},
        },
    }
    ctx = ReportContext(trade_date=_dt.date(2026, 1, 14), kind="EOD", slots=slots)
    out = MarketOverviewBlock().render(ctx)
    text = "\n".join(out.payload.get("content", []))
    assert "Top20 成交占比" in text
    assert "拥挤代理(top20pct)" not in text
    # canonical accepted, proxy should not generate suspect warning in this case
    assert not any("suspect:top20_amount_ratio_high_from" in w for w in out.warnings)
