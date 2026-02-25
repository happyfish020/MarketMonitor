#-*- coding: utf-8 -*-
"""core.reporters.report_blocks.market_mode_blk

Market Mode (Unified Decision Layer) · observe-only report block.

Frozen contract:
- Read-only rendering of slots['governance']['market_mode']
- Must NEVER crash report generation.
"""

from __future__ import annotations

from typing import Any, Dict, List

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase
from core.utils.logger import get_logger

LOG = get_logger("Block.MarketMode")


def _as_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        return str(v)
    except Exception:
        return ""


class MarketModeBlock(ReportBlockRendererBase):
    """Report block: governance.market_mode"""

    block_alias = "governance.market_mode"
    title = "市场制度状态（Market Mode）"

    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        warnings: List[str] = []

        gov = context.slots.get("governance") or {}
        mm = gov.get("market_mode") or context.slots.get("market_mode")

        if not isinstance(mm, dict) or not mm:
            warnings.append("missing:market_mode")
            payload = {
                "mode": "MISSING",
                "severity": "",
                "asof": _as_str(context.asof),
                "summary": "未生成 Market Mode（统一裁决层）区块：可能尚未接入或上游未注入。\n"
                           "说明：本块为只读解释层，不改变 Gate/DRS/Execution。",
                "reasons": [],
            }
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload=payload,
                warnings=warnings,
            )

        mode = _as_str(mm.get("mode"))
        severity = _as_str(mm.get("severity"))
        asof = _as_str(mm.get("asof") or context.asof)
        reasons = mm.get("reasons") or []
        inputs = mm.get("inputs") if isinstance(mm.get("inputs"), dict) else {}
        if not isinstance(reasons, list):
            reasons = [reasons]

        # Human text (short, report-friendly)
        line1 = f"- 当前阶段：**{mode or 'UNKNOWN'}**" + (f"（{severity}）" if severity else "")
        line2 = f"- asof: {asof}" if asof else "- asof: (missing)"
        if reasons:
            r_short = [ _as_str(x) for x in reasons if _as_str(x) ]
            if len(r_short) > 6:
                r_short = r_short[:6] + ["..."]  # keep concise
            line3 = "- 依据：" + "；".join(r_short)
        else:
            line3 = "- 依据：（缺失）"

        payload = {
            "schema_version": _as_str(mm.get("schema_version") or "MM_V1"),
            "mode": mode,
            "severity": severity,
            "asof": asof,
            "drs_level": _as_str(mm.get("drs_level") or inputs.get("drs_level")),
            "gate": _as_str(mm.get("gate") or inputs.get("gate")),
            "execution_band": _as_str(mm.get("execution_band") or inputs.get("execution_band")),
            "trend_state": _as_str(mm.get("trend_state") or inputs.get("trend_state")),
            "reasons": reasons,
            "human": "\n".join([line1, line2, line3]),
        }

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload=payload,
            warnings=warnings,
        )
