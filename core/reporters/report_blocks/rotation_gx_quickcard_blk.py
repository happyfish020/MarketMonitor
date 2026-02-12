# -*- coding: utf-8 -*-
"""UnifiedRisk V12 · Rotation GX Quickcard (Frozen)

展示三条执行模板（GX）在当前交易日的“是否触发/执行计划”：
- GX-ROT-ENTRY-SPLIT-V1
- GX-ROT-EXIT-T1-V1
- GX-ROT-HARDSTOP-V1

注意：
- 本 block 只读展示 slots['rotation_gx']（由 Engine 评估并注入）
- 任何异常必须吞掉并输出 warnings（永不影响其它区块）
"""

from __future__ import annotations

from typing import Any, Dict, List

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase


class RotationGXQuickcardBlock(ReportBlockRendererBase):
    block_alias = "rotation.gx.quickcard"
    title = "板块轮换执行卡（GX · Quickcard）"

    def render(self, context: ReportContext, doc_partial: Dict[str, Any]) -> ReportBlock:
        warnings: List[str] = []
        lines: List[str] = []

        try:
            gx = context.slots.get("rotation_gx") if isinstance(context.slots, dict) else None
            if not isinstance(gx, dict):
                warnings.append("missing:rotation_gx")
                return ReportBlock(block_alias=self.block_alias, title=self.title, payload={"content": lines, "warnings": warnings})

            meta = gx.get("meta") if isinstance(gx.get("meta"), dict) else {}
            lines.append(f"- Trade Date: **{meta.get('trade_date', context.trade_date)}**")
            sw = gx.get("switch") if isinstance(gx.get("switch"), dict) else {}
            lines.append(f"- Switch: **{sw.get('mode','UNKNOWN')}**")

            # ---- Entry Split ----
            es = gx.get("entry_split") if isinstance(gx.get("entry_split"), dict) else {}
            allowed = es.get("allowed")
            lines.append("\n### GX-ROT-ENTRY-SPLIT-V1")
            lines.append(f"- Allowed: **{'YES' if allowed else 'NO'}**")
            if es.get("reasons"):
                lines.append(f"- Reasons: {', '.join([str(x) for x in (es.get('reasons') or [])])}")
            tgt = es.get("target") if isinstance(es.get("target"), dict) else None
            if tgt:
                cap = es.get("cap_weight")
                lines.append(f"- Target: {tgt.get('sector_name')} (id={tgt.get('sector_id')}, rank={tgt.get('entry_rank')})")
                if cap is not None:
                    lines.append(f"- Cap Weight: {cap}")
                split = es.get("split") if isinstance(es.get("split"), dict) else {}
                sched = split.get("schedule") or []
                ratios = split.get("ratios") or []
                if isinstance(sched, list) and isinstance(ratios, list) and len(sched) == len(ratios):
                    parts = [f"{sched[i]}={ratios[i]}" for i in range(len(sched))]
                    lines.append(f"- Split: {', '.join(parts)}")

            # ---- Exit T1 ----
            ex = gx.get("exit_t1") if isinstance(gx.get("exit_t1"), dict) else {}
            acts = ex.get("actions") if isinstance(ex.get("actions"), list) else []
            lines.append("\n### GX-ROT-EXIT-T1-V1")
            if acts:
                lines.append(f"- Exit Actions: **{len(acts)}** (T+1 mandatory)")
                for a in acts:
                    if not isinstance(a, dict):
                        continue
                    lines.append(f"  - {a.get('sector_name')}: {a.get('status')} exec={a.get('exec_exit_date')} qty={a.get('qty')}")
            else:
                lines.append("- Exit Actions: **0**")

            # ---- Hardstop ----
            hs = gx.get("hardstop") if isinstance(gx.get("hardstop"), dict) else {}
            hs_acts = hs.get("actions") if isinstance(hs.get("actions"), list) else []
            hs_reasons = hs.get("reasons") if isinstance(hs.get("reasons"), list) else []
            lines.append("\n### GX-ROT-HARDSTOP-V1")
            if hs_acts:
                lines.append(f"- Hardstop Actions: **{len(hs_acts)}** (ASAP)")
                for a in hs_acts:
                    if not isinstance(a, dict):
                        continue
                    lines.append(f"  - {a.get('name') or a.get('symbol')}: {a.get('type')} action={a.get('action')} note={a.get('note')}")
            else:
                lines.append("- Hardstop Actions: **0**")
                if hs_reasons:
                    lines.append(f"- Notes: {', '.join([str(x) for x in hs_reasons])}")

        except Exception as e:
            warnings.append(f"exception:{e}")

        return ReportBlock(block_alias=self.block_alias, title=self.title, payload={"content": lines, "warnings": warnings})
