#-*- coding: utf-8 -*-
"""UnifiedRisk V12 · Market Rotation Facts block (Frozen)

- slot-only: context.slots["rotation_market_signal"]
- governance-only read: context.slots["governance"]["attack_permit"] (optional)
- NO DB/SP access in report layer

本块用于“事实层轮动”呈现：反映已经发生的轮动强弱，不改变回测策略信号（entry/hold/exit）。
"""

from __future__ import annotations

from typing import Any, Dict, List

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase
from core.utils.logger import get_logger


log = get_logger(__name__)


def _get_block(slots: Dict[str, Any], k: str) -> Any:
    if not isinstance(slots, dict):
        return None
    return slots.get(k)


class RotationMarketFactsBlock(ReportBlockRendererBase):
    block_alias = "rotation.market_facts"
    title = "板块轮动事实（Market Rotation · Facts）"

    def render(self, context: ReportContext, doc_partial: Dict[str, Any]) -> ReportBlock:
        warnings: List[str] = []
        try:
            rm = _get_block(context.slots, "rotation_market_signal")
            if not isinstance(rm, dict) or not rm:
                warnings.append("missing:rotation_market_signal")
                return ReportBlock(
                    block_alias=self.block_alias,
                    title=self.title,
                    warnings=warnings,
                    payload={
                        "content": ["（未生成 rotation_market_signal：事实层轮动区块仅占位）"],
                        "note": "注：本块是事实层轮动呈现，不改变回测策略信号（entry/hold/exit）。",
                    },
                )

            meta = rm.get("meta") if isinstance(rm.get("meta"), dict) else {}
            trade_date = meta.get("trade_date") or getattr(context, "trade_date", None)
            run_id = meta.get("run_id") or getattr(context, "run_id", None)

            enter = rm.get("enter") if isinstance(rm.get("enter"), list) else []
            watch = rm.get("watch") if isinstance(rm.get("watch"), list) else []
            exit_ = rm.get("exit") if isinstance(rm.get("exit"), list) else []
            candidates = rm.get("candidates") if isinstance(rm.get("candidates"), list) else []

            lines: List[str] = []
            if trade_date:
                lines.append(f"- Trade Date: **{trade_date}**")
            if run_id:
                lines.append(f"- Rotation Baseline: `{run_id}`")
            lines.append(
                f"- Facts: ENTER={len(enter)} · WATCH={len(watch)} · EXIT={len(exit_)}"
            )

            # RotationMode (observe-only) for readability
            gov = _get_block(context.slots, "governance")
            rm_mode = None
            if isinstance(gov, dict):
                rm_mode = gov.get("rotation_mode")
            if isinstance(rm_mode, dict) and rm_mode:
                mode = rm_mode.get("mode")
                days = rm_mode.get("persistence_days")
                reason = rm_mode.get("reason")
                if mode:
                    lines.append(f"- Rotation Mode (observe): **{mode}** · days={days} · {reason}")

            if candidates:
                lines.append("- Candidates (Top3):")
                for i, c in enumerate(candidates[:3], start=1):
                    if not isinstance(c, dict):
                        continue
                    nm = c.get("sector_name") or c.get("SECTOR_NAME") or "UNKNOWN"
                    sid = c.get("sector_id") or c.get("SECTOR_ID") or "?"
                    tag = c.get("strength_tag") or c.get("STRENGTH_TAG") or ""
                    sc = c.get("signal_score") if "signal_score" in c else c.get("SIGNAL_SCORE")
                    act = c.get("action") or c.get("ACTION") or ""
                    ed = c.get("enter_days_5d") if "enter_days_5d" in c else c.get("ENTER_DAYS_5D")
                    td = c.get("top3_days_5d") if "top3_days_5d" in c else c.get("TOP3_DAYS_5D")
                    ma3 = c.get("score_ma3") if "score_ma3" in c else c.get("SCORE_MA3")
                    stab = c.get("stability_score") if "stability_score" in c else c.get("STABILITY_SCORE")
                    lvl = c.get("boost_level") if "boost_level" in c else c.get("BOOST_LEVEL")
                    lines.append(
                        f"  - {i}. {nm} ({sid} · {tag} · score={sc} · {act} · L{lvl} · stab={stab} · 5d_enter={ed} · 5d_top3={td} · ma3={ma3})"
                    )
            else:
                lines.append("- Candidates: (none)")

            # Step2: show whether governance used facts boost
            ap = gov.get("attack_permit") if isinstance(gov, dict) else None
            boost = None
            if isinstance(ap, dict):
                ev = ap.get("evidence") if isinstance(ap.get("evidence"), dict) else {}
                boost = ev.get("rotation_market_facts_boost")
            if isinstance(boost, dict):
                lines.append("")
                lines.append(
                    f"- Boost (B+连续性): ok={boost.get('ok')} · level={boost.get('level')} · reason={boost.get('reason')}"
                )

            lines.append("")
            lines.append("- Note: 本块是“事实层轮动”呈现，不改变回测策略信号（entry/hold/exit）。")

            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                warnings=warnings,
                payload={"content": lines, "raw": rm},
            )

        except Exception as e:
            log.exception("RotationMarketFactsBlock.render failed: %s", e)
            warnings.append("exception:rotation_market_facts_render")
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                warnings=warnings,
                payload={"content": ["RotationMarketFacts 渲染异常（已捕获）。"]},
            )
