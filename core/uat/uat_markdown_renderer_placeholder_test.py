# -*- coding: utf-8 -*-
"""UAT: MarkdownRenderer must never drop blocks when payload schema is non-standard.

Why:
- In UAT-P0 we often generate placeholder blocks (missing builder / builder exception).
- Renderer must keep producing the full document and show auditable placeholders.

Run:
    python core/uat/uat_markdown_renderer_placeholder_test.py
"""

from __future__ import annotations

from core.reporters.report_types import ReportBlock, ReportDocument
from core.reporters.renderers.markdown_renderer import MarkdownRenderer


def main() -> None:
    doc = ReportDocument(
        meta={"trade_date": "2099-01-01", "kind": "EOD"},
        actionhint={"reason": "UAT"},
        summary="N",
        blocks=[
            ReportBlock(
                block_alias="market.overview",
                title="大盘概述（收盘事实）",
                payload={"content": ["OK"]},
                warnings=[],
            ),
            # Placeholder-like payload (note-only) must be rendered and must not break later blocks.
            ReportBlock(
                block_alias="structure.facts",
                title="结构事实（技术轨）",
                payload={"note": "BLOCK_NOT_IMPLEMENTED"},
                warnings=["missing_builder:structure.facts"],
            ),
            ReportBlock(
                block_alias="summary",
                title="简要总结（A / N / D）",
                payload={"text": "OK"},
                warnings=[],
            ),
        ],
    )

    text = MarkdownRenderer().render(doc)

    assert "## 大盘概述（收盘事实）" in text
    assert "## 结构事实（技术轨）" in text
    assert "missing_builder:structure.facts" in text
    assert "BLOCK_NOT_IMPLEMENTED" in text
    assert "## 简要总结（A / N / D）" in text

    print("PASS: MarkdownRenderer renders placeholder blocks and keeps following blocks")


if __name__ == "__main__":
    main()
