#-*- coding: utf-8 -*-
"""UnifiedRisk V12 · Rotation Market Facts block (Frozen)

- slot-only: context.slots["rotation_market_signal"]
- Read-only overlay: reflects "what the market is rotating into" based on facts table
  SECOPR.CN_SECTOR_ROTATION_MKT_SIGNAL_T.

Important:
- This block must NOT change strategy/backtest tables or execution rules.
"""

from __future__ import annotations

from typing import Any, Dict, List

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase


def _as_list(v: Any) -> List[Dict[str, Any]]:
    if isinstance(v, list):
        return [x for x in v if isinstance(x, dict)]
    return []


class RotationMarketSignalBlock(ReportBlockRendererBase):
    block_alias = "rotation.market_signal"
    title = "板块轮动事实（Market Rotation · Facts）"

    def render(self, context: ReportContext, doc_partial: Dict[str, Any]) -> ReportBlock:
        warnings: List[str] = []
        slots = context.slots if isinstance(context.slots, dict) else {}
        rs = slots.get("rotation_market_signal")
        if not isinstance(rs, dict) or not rs:
            warnings.append("missing:rotation_market_signal")
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                warnings=warnings,
                payload={"content": ["（未生成 rotation_market_signal：事实层轮动区块仅占位）"]},
            )

        meta = rs.get("meta") if isinstance(rs.get("meta"), dict) else {}
        trade_date = meta.get("trade_date") or getattr(context, "trade_date", None)
        run_id = meta.get("run_id") or getattr(context, "run_id", None)

        enter = _as_list(rs.get("enter"))
        watch = _as_list(rs.get("watch"))
        exit_ = _as_list(rs.get("exit"))
        cands = _as_list(rs.get("candidates"))

        lines: List[str] = []
        if trade_date:
            lines.append(f"- Trade Date: **{trade_date}**")
        if run_id:
            lines.append(f"- Rotation Baseline: `{run_id}`")

        lines.append(f"- Facts: ENTER={len(enter)} · WATCH={len(watch)} · EXIT={len(exit_)}")

        if cands:
            lines.append("- Candidates (Top3):")
            for i, r in enumerate(cands[:3], start=1):
                nm = r.get("SECTOR_NAME") or r.get("sector_name") or "UNKNOWN"
                sid = r.get("SECTOR_ID") or r.get("sector_id")
                tag = r.get("STRENGTH_TAG") or r.get("strength_tag")
                sc = r.get("SIGNAL_SCORE") or r.get("signal_score")
                act = r.get("ACTION") or r.get("action")
                extra = []
                if sid:
                    extra.append(str(sid))
                if tag:
                    extra.append(str(tag))
                if sc is not None:
                    extra.append(f"score={sc}")
                if act:
                    extra.append(f"{act}")
                lines.append(f"  - {i}. {nm} ({' · '.join(extra)})")
        else:
            lines.append("- Candidates: (none)")

        lines.append("")
        lines.append("- Note: 本块是“事实层轮动”呈现，不改变回测策略信号（entry/hold/exit）。")

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            warnings=warnings,
            payload={"content": lines, "raw": rs},
        )
