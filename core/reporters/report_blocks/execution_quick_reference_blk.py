# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase


class ExecutionQuickReferenceBlock(ReportBlockRendererBase):
    """V12 执行速查卡（只读）

    目的：
    - 给用户一个“制度优先级 + Gate/Execution/DRS 含义”的固定解释卡
    - 只读 slots，不参与计算，不做推断
    - 返回 ReportBlock，字段名必须使用 block_alias（而不是 alias）
    """
    block_alias = "execution_quick_reference"
    title = "执行行为速查卡"

    def render(self, context: ReportContext, doc_partial: Dict[str, Any]) -> ReportBlock:
        warnings: List[str] = []

        # Highlight current gate/drs/execution (best-effort; missing is ok)
        gov = context.slots.get("governance") if isinstance(context.slots, dict) else None
        gate_final = None
        drs_band = None
        exe_band = None

        if isinstance(gov, dict):
            g = gov.get("gate")
            if isinstance(g, dict):
                gate_final = g.get("final_gate") or g.get("raw_gate")
            d = gov.get("drs")
            if isinstance(d, dict):
                drs_band = d.get("band")
            e = gov.get("execution")
            if isinstance(e, dict):
                exe_band = e.get("band")

        # Compose content (keep stable, readable, frozen)
        lines: List[str] = []
        lines.append("【决策优先级】")
        # Governance first: DRS is the hard veto; Gate defines permission boundary; Execution affects *how* to act.
        lines.append("DRS（否决） ＞ Gate（权限边界） ＞ Execution（摩擦/节奏）")
        lines.append("")
        lines.append("【Gate 含义】")
        lines.append("- ALLOW (A)：允许进攻")
        lines.append("👉 - NORMAL (N)：结构正常，但不鼓励进攻")
        lines.append("👉 - CAUTION：禁止加仓，只能防守或不动")
        lines.append("- D / FREEZE：必须防守")
        lines.append("")
        lines.append("【Execution（执行摩擦）】")
        lines.append("- Execution band = D1：轻摩擦/偏中性（仍需服从 Gate/DRS）")
        lines.append("- Execution band = D2：摩擦偏高（追价/频繁调仓胜率下降）")
        lines.append("- Execution band = D3：摩擦很高/结构压力大（制度倾向去风险）")
        lines.append("- Execution band = NA：数据不足（不影响 Gate/DRS）")
        lines.append("")
        lines.append("【DRS（日度风险信号）】")
        lines.append("👉 - GREEN：风险环境可控（不是进攻信号）")
        lines.append("- YELLOW：需降档执行")
        lines.append("- RED：否决一切进攻，仅允许防守")
        lines.append("")
        lines.append("【CAUTION 状态下的冻结规则】")
        lines.append("- 禁止：加仓、追高、放大试错")
        lines.append("- 允许：维持仓位、减仓、防守性调整")
        lines.append("")
        lines.append("【轻仓试错（严格定义）】")
        lines.append("- 不增加总风险敞口")
        lines.append("- 失败成本可忽略")
        lines.append("- 仅用于验证，不用于进攻")
        lines.append("👉 表示与当日制度状态直接相关的高亮提示（仅用于理解，不构成制度判断）。")

        payload: Any = "\n".join(lines)

        # Keep a small machine-friendly hint (optional) in doc_partial, do not affect rendering
        try:
            doc_partial.setdefault("_debug", {}).setdefault("execution_quick_reference", {
                "gate_final": gate_final,
                "drs_band": drs_band,
                "execution_band": exe_band,
            })
        except Exception:
            pass

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload=payload,
            warnings=warnings,
        )
