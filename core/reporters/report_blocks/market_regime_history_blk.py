# -*- coding: utf-8 -*-
"""UnifiedRisk V12 · Market Regime History Block (Human Layer) · v1.2

B-refactor:
- Report layer MUST NOT recompute shift/stats (single source of truth).
- Prefer slots['regime_shift'] / slots['regime_stats'] injected by engine/service.
- Stage/DRS/Gate/Execution labels rendered via state_render (ZH-first).
"""

from __future__ import annotations

from typing import Any, Dict, List

from core.reporters.report_types import ReportBlock
from core.reporters.report_context import ReportContext
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase
from core.reporters.utils.state_render import stage_zh, shift_zh, safe_dict


class MarketRegimeHistoryBlock(ReportBlockRendererBase):
    block_alias = "market.regime_history"
    title = "阶段轨迹（Regime History · Human Layer）"

    def render(self, context: ReportContext, doc_partial: Dict[str, Any]) -> ReportBlock:
        warnings: List[str] = []
        lines: List[str] = []

        hist = context.slots.get("regime_history")
        if not isinstance(hist, list) or not hist:
            warnings.append("missing:regime_history")
            lines.append("### 最近阶段轨迹（缺失历史数据）")
            lines.append("")
            lines.append("当前报告未注入历史序列，暂无法展示最近 N 天阶段轨迹。")
            lines.append("建议接线方式（任选其一）：")
            lines.append("- Engine 在生成报告前，从 Run→Persist / DB 读取最近 N 日的 (trend, drs, amount_ratio, adv_ratio) 并写入 slots['regime_history']")
            lines.append("- 或在缓存中读取最近 N 份 EOD 报告的结构字段，拼成 regime_history")
            note = "只读解释层：本块不访问数据库，等待上游注入 history。"
            return ReportBlock(self.block_alias, self.title, payload={"content": lines, "note": note}, warnings=warnings)

        tail = hist[-10:] if len(hist) > 10 else hist

        lines.append("### 最近阶段轨迹（近10日）")
        lines.append("")
        for it in tail:
            if not isinstance(it, dict):
                continue
            dt = it.get("trade_date") or it.get("date") or "NA"
            stage = stage_zh(it.get("stage_raw") or it.get("stage") or it.get("stage_name") or "UNKNOWN")
            drs = it.get("drs")
            trend = it.get("trend")
            extra = []
            if isinstance(drs, str) and drs:
                extra.append(f"drs={drs}")
            if isinstance(trend, str) and trend:
                extra.append(f"trend={trend}")
            suffix = f" ({', '.join(extra)})" if extra else ""
            lines.append(f"- {dt}: {stage}{suffix}")

        # Trigger: must prefer injected slots (no recompute)
        lines.append("")
        lines.append("### 阶段变更（Trigger）")
        shift = safe_dict(context.slots.get("regime_shift"))
        if shift and shift.get("from") and shift.get("to"):
            lines.append(f"- {stage_zh(shift.get('from'))} → {stage_zh(shift.get('to'))} | {shift_zh(shift.get('shift_type'), shift.get('severity'))}")
            rs = shift.get("reason")
            if isinstance(rs, list) and rs:
                lines.append(f"  - reason: {', '.join([str(x) for x in rs[:4]])}")
        else:
            warnings.append("missing:regime_shift")
            lines.append("- （缺失：regime_shift）")

        # Stats: must prefer injected slots (no recompute)
        lines.append("")
        lines.append("### Regime Stats（统计）")
        stats = safe_dict(context.slots.get("regime_stats"))
        if stats:
            consec = stats.get("consecutive_s5_days")
            dist20 = stats.get("stage_distribution_20d")
            if isinstance(dist20, dict):
                dist20_zh = {stage_zh(k): v for k, v in dist20.items()}
            else:
                dist20_zh = dist20
            lines.append(f"- 连续 S5 天数：{consec}")
            lines.append(f"- 近20日阶段分布：{dist20_zh}")
        else:
            warnings.append("missing:regime_stats")
            lines.append("- 连续 S5 天数：（缺失：regime_stats）")
            lines.append("- 近20日阶段分布：（缺失：regime_stats）")

        note = "只读解释层：历史轨迹由上游注入 slots['regime_history']（shift/stats 同样由上游注入，报告层不重算）。"
        return ReportBlock(self.block_alias, self.title, payload={"content": lines, "note": note}, warnings=warnings)
