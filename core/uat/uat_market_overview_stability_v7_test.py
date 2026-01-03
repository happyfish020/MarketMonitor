# -*- coding: utf-8 -*-
"""UAT-P0 · MarketOverview & StructureFacts stability regression

目标（冻结工程）：
- MarketOverviewBlock 不得抛异常（尤其是 self._logger 缺失等属性错误）
- 当 factors 提供 indices/amount/market_sentiment 时，不得出现 missing:indices / missing:amount / missing:breadth
- 指数 pct_1d (ratio) 必须正确渲染为百分比（×100）
- StructureFactsBlock 必须跳过 meta keys（例如 _summary），避免 missing_semantics:_summary

运行方式：
    python -m core.uat.uat_market_overview_stability_v7_test

注意：该脚本只做 block 级单元回归，不依赖全链路 engine。
"""

from __future__ import annotations

import json

from core.reporters.report_context import ReportContext
from core.reporters.report_blocks.market_overview_blk import MarketOverviewBlock
from core.reporters.report_blocks.structure_facts_blk import StructureFactsBlock


def _mk_ctx() -> ReportContext:
    # indices raw_data 采用 index_tech.details._raw_data
    indices_raw = {
        "sh": {"close": 3968.84, "pct_1d": 0.0009392212},
        "sz": {"close": 13525.02, "pct_1d": -0.0058108183},
        "hs300": {"close": 4629.94, "pct_1d": -0.0045879510},
        "zz500": {"close": 7465.57, "pct_1d": 0.0008899661},
        "kc50": {"close": 27.80, "pct_1d": 0.0349962770},
        "_meta": {"indices": ["sh", "sz", "hs300", "zz500", "kc50"]},
    }

    amount_raw = {
        "trade_date": "2025-12-31",
        "total_amount": 20448.51,
        "window": [
            {"trade_date": "2025-12-31", "total_amount": 20448.51},
            {"trade_date": "2025-12-30", "total_amount": 21419.09},
        ],
    }

    market_sentiment = {
        "trade_date": "2025-12-31",
        "adv": 2360,
        "dec": 2607,
        "flat": 214,
        "limit_up": 74,
        "limit_down": 11,
        "adv_ratio": 45.55,
    }

    slots = {
        "factors": {
            "index_tech": {"details": {"_raw_data": indices_raw}},
            "amount": {
                "details": {
                    "amount_total": 20448.51,
                    "_raw_data": json.dumps(amount_raw, ensure_ascii=False),
                }
            },
            "unified_emotion": {"details": {"_raw_data": {"market_sentiment": market_sentiment}}},
            "etf_index_sync_daily": {"details": {"top20_amount_ratio": 0.7455}},
        },
        "structure": {
            "north_proxy_pressure": {
                "state": "pressure_low",
                "meaning": "北向代理压力不显著（未见明显撤退压力）。",
                "evidence": {"pressure_level": "NEUTRAL", "pressure_score": 27.9},
            },
            "_summary": {"tags": ["modifier_distribution_risk"]},
        },
    }

    return ReportContext(trade_date="2025-12-31", kind="EOD", slots=slots)


def test_market_overview_block() -> None:
    ctx = _mk_ctx()
    blk = MarketOverviewBlock().render(ctx, {})

    # 1) no missing warnings for key fields
    warn_text = "\n".join(blk.warnings or [])
    assert "missing:indices" not in warn_text, warn_text
    assert "missing:amount" not in warn_text, warn_text
    assert "missing:breadth" not in warn_text, warn_text

    # 2) has expected content lines
    content = blk.payload.get("content") if isinstance(blk.payload, dict) else None
    assert isinstance(content, list) and content, "empty content"

    joined = "\n".join(content)
    assert "指数表现" in joined, joined
    assert "成交额" in joined, joined
    assert "赚钱效应" in joined, joined
    assert "北向代理" in joined, joined

    # 3) pct conversion sanity (ratio -> percent)
    assert "SH 0.09%" in joined, joined
    assert "KC50 3.50%" in joined, joined


def test_structure_facts_skip_meta() -> None:
    ctx = _mk_ctx()
    blk = StructureFactsBlock().render(ctx, {})

    warn_text = "\n".join(blk.warnings or [])
    assert "missing_semantics" not in warn_text, warn_text

    content = blk.payload.get("content") if isinstance(blk.payload, dict) else None
    assert isinstance(content, list) and content
    joined = "\n".join(content)
    assert "_summary" not in joined, joined


def main() -> None:
    test_market_overview_block()
    test_structure_facts_skip_meta()
    print("UAT PASS: market_overview & structure_facts stability")


if __name__ == "__main__":
    main()
