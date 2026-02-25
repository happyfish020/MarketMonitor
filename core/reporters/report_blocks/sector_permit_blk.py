#-*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List

from core.reporters.report_types import ReportBlock
from core.reporters.report_context import ReportContext
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase


def _as_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        return str(v)
    except Exception:
        return ""


def _fmt_num(v: Any, nd: int = 4) -> str:
    try:
        if v is None:
            return "-"
        f = float(v)
        if abs(f) >= 1000:
            return str(int(f))
        return str(round(f, nd))
    except Exception:
        return _as_str(v) or "-"


class SectorPermitBlock(ReportBlockRendererBase):
    """UnifiedRisk V12 · SectorPermit Block

    Purpose:
    - 将“板块轮动参与许可”显式展示出来：即便 Gate=CAUTION，也能看到哪些方向允许轻参与。
    - 只读：来源 governance.sector_permit（由 ReportEngine 构建）。
    """

    block_alias = "governance.sector_permit"
    title = "板块轮动参与许可（SectorPermit · Rotation）"

    def render(self, context: ReportContext, doc_partial: Dict[str, Any]) -> ReportBlock:
        warnings: List[str] = []

        gov = context.slots.get("governance")
        sp = gov.get("sector_permit") if isinstance(gov, dict) else None

        if not isinstance(sp, dict):
            warnings.append("missing:governance.sector_permit")
            payload = "SectorPermit 未生成（请检查 ReportEngine 是否写入 slots['governance']['sector_permit']）。"
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload=payload,
                warnings=warnings,
            )

        permit = _as_str(sp.get("permit") or "NO").upper()
        mode = _as_str(sp.get("mode") or "OFF").upper()
        label = _as_str(sp.get("label") or "")

        lines: List[str] = []
        lines.append(f"- 结论：{label}（permit={permit}, mode={mode}）")

        constraints = sp.get("constraints")
        if isinstance(constraints, list) and constraints:
            lines.append("- 约束：")
            for c in constraints[:6]:
                lines.append(f"  - {_as_str(c)}")

        candidates = sp.get("candidates")
        if isinstance(candidates, list) and candidates:
            lines.append("- 候选板块（Top）：")
            for r in candidates:
                if not isinstance(r, dict):
                    continue
                name = _as_str(r.get("SECTOR_NAME") or r.get("SECTOR_ID") or "?")
                rank = _as_str(r.get("ENTRY_RANK") or "?")
                w = _fmt_num(r.get("WEIGHT_SUGGESTED"), 4)
                tier = _as_str(r.get("ENERGY_TIER") or "")
                ep = _fmt_num(r.get("ENERGY_PCT"), 2)
                sc = _fmt_num(r.get("SIGNAL_SCORE"), 2)
                lines.append(f"  - #{rank} {name} | w={w} | energy={ep} | tier={tier} | score={sc}")

        exits = sp.get("exits") if isinstance(sp.get("exits"), dict) else {}
        ea = exits.get("exit_allowed") if isinstance(exits.get("exit_allowed"), list) else []
        epd = exits.get("exit_pending") if isinstance(exits.get("exit_pending"), list) else []
        if ea or epd:
            lines.append("- 退出优先级：")
            if ea:
                lines.append("  - ExitAllowed：")
                for x in ea[:6]:
                    if isinstance(x, dict):
                        lines.append(f"    - {_as_str(x.get('SECTOR_NAME'))} ({_as_str(x.get('EXIT_EXEC_STATUS'))})")
            if epd:
                lines.append("  - ExitPending：")
                for x in epd[:6]:
                    if isinstance(x, dict):
                        lines.append(f"    - {_as_str(x.get('SECTOR_NAME'))} ({_as_str(x.get('EXIT_EXEC_STATUS'))})")

        w = sp.get("warnings")
        if isinstance(w, list) and w:
            warnings.extend([_as_str(x) for x in w if _as_str(x)])

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload="\n".join(lines) if lines else "(empty)",
            warnings=warnings,
        )
