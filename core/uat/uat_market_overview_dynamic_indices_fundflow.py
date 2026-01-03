# -*- coding: utf-8 -*-
from __future__ import annotations

from core.reporters.report_blocks.market_overview_blk import MarketOverviewBlock
from core.reporters.report_context import ReportContext


def _ctx(slots: dict) -> ReportContext:
    return ReportContext(
        market="CN",
        kind="EOD",
        trade_date="2025-12-31",
        slots=slots,
        meta={},
    )


def main() -> None:
    blk = MarketOverviewBlock()

    # 1) indices 顺序来自 _order（不 hardcode）
    slots = {
        "market_overview": {
            "indices": {
                "_order": ["zz500", "hs300"],
                "hs300": {"pct": -0.01, "close": 4629.94, "name": "HS300"},
                "zz500": {"pct": 0.02, "close": 7465.57, "name": "ZZ500"},
            },
            "amount": {"amount": 20000.0, "unit": "亿元"},
            "breadth": {"adv_ratio": 0.45},
        }
    }
    out = blk.render(_ctx(slots), {})
    assert out and out.block_alias == "market.overview"
    assert "ZZ500" in out.payload.split("\n")[0]

    # 2) fundflow 只有数值无 delta -> 不输出资金流
    slots = {
        "market_overview": {
            "amount": {"amount": 20000.0, "unit": "亿元"},
            "breadth": {"adv_ratio": 0.45},
            "fundflow": {"main_net": 1.2, "north_net": -0.3},
        }
    }
    out = blk.render(_ctx(slots), {})
    assert "资金流" not in out.payload

    # 3) fundflow 有 delta -> 输出资金流 + “较前一日”
    slots = {
        "market_overview": {
            "amount": {"amount": 20000.0, "unit": "亿元"},
            "breadth": {"adv_ratio": 0.45},
            "fundflow": {
                "main_net": {"value": 1.2, "delta": 0.3},
                "north_net": {"value": -0.3, "delta": -0.1},
                "unit": "亿",
            },
        }
    }
    out = blk.render(_ctx(slots), {})
    assert "资金流" in out.payload and "较前一日" in out.payload

    print("ALL TESTS PASSED")


if __name__ == "__main__":
    main()
