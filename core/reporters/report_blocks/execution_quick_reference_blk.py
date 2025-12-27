# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 · Execution Quick Reference Block (Read-Only)

冻结说明：
- 本 Block 为【只读说明块】
- 不读取 context
- 不读取 factor / structure / observation
- 不参与 Gate / Summary / ActionHint
- 永久静态内容，用于执行对照、防误操作

用途：
- 盘前 / 盘后报告尾部
- 为用户提供 V12 执行速查卡
"""

from typing import Any, Dict, Optional, List
 
from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase


class ExecutionQuickReferenceBlock(ReportBlockRendererBase):
    """
    V12 执行速查卡（只读说明 Block）
    """
    block_alias = "execution_quick_reference"
    title = "执行速查卡（只读）"

    def renderV0(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        """
        Render execution quick reference with highlight.

        冻结约束：
        - 不参与制度计算
        - 仅消费最终输出（execution / gate / drs）
        - 高亮仅为提示
        """

        # -------- 读取最终状态（只读） --------
        execution = doc_partial.get("execution")      # e.g. "A", "N", "D", "D2"
        summary_code = doc_partial.get("summary")     # e.g. "A", "N", "D"
        drs_signal: Optional[str] = None

        observations = context.slots.get("observations")
        if isinstance(observations, dict):
            drs = observations.get("drs")
            if isinstance(drs, dict):
                obs = drs.get("observation")
                if isinstance(obs, dict):
                    drs_signal = obs.get("signal")

        # -------- 高亮标记（纯展示） --------
        def mark(line: str, cond: bool) -> str:
            return f"👉 {line}" if cond else line

        content: List[str] = [
            "【决策优先级】",
            "Execution ＞ Gate ＞ DRS",
            "",
            "【Gate 含义】",
            mark("- ALLOW (A)：允许进攻", summary_code == "A"),
            mark("- NORMAL (N)：结构正常，但不鼓励进攻", summary_code == "N"),
            mark("- CAUTION：禁止加仓，只能防守或不动", summary_code == "N"),
            mark("- D / FREEZE：必须防守", summary_code == "D"),
            "",
            "【Execution（执行摩擦）】",
            mark("- Execution = A / N：执行顺", execution in ("A", "N")),
            mark("- Execution = D / D2：执行不顺", execution in ("D", "D2")),
            "",
            "【DRS（日度风险信号）】",
            mark("- GREEN：风险环境可控（不是进攻信号）", drs_signal == "GREEN"),
            mark("- YELLOW：需降档执行", drs_signal == "YELLOW"),
            mark("- RED：否决一切进攻，仅允许防守", drs_signal == "RED"),
            "",
            "【CAUTION 状态下的冻结规则】",
            "- 禁止：加仓、追高、放大试错",
            "- 允许：维持仓位、减仓、防守性调整",
            "",
            "【轻仓试错（严格定义）】",
            "- 不增加总风险敞口",
            "- 失败成本可忽略",
            "- 仅用于验证，不用于进攻",
        ]

        payload = {
            "meaning": "\n".join(content).strip(), 
            "note": "👉 表示与当日制度状态直接相关的高亮提示（仅用于理解，不构成制度判断）。",
        }

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload=payload,
            warnings=[],
        )
    
#############
    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        """
        Render execution quick reference with highlight.

        冻结约束：
        - 不参与制度计算
        - 仅消费最终输出（execution / gate / drs）
        - 高亮仅为提示
        """

        # -------- 读取最终状态（只读） --------
        execution = doc_partial.get("execution")      # e.g. "A", "N", "D", "D2"
        summary_code = doc_partial.get("summary")     # e.g. "A", "N", "D"
        drs_signal: Optional[str] = None

        observations = context.slots.get("observations")
        if isinstance(observations, dict):
            drs = observations.get("drs")
            if isinstance(drs, dict):
                obs = drs.get("observation")
                if isinstance(obs, dict):
                    drs_signal = obs.get("signal")

        # -------- 高亮标记（纯展示） --------
        def mark(line: str, cond: bool) -> str:
            return f"👉 {line}" if cond else line

        content: List[str] = [
            "【决策优先级】",
            "Execution ＞ Gate ＞ DRS",
            "",
            "【Gate 含义】",
            mark("- ALLOW (A)：允许进攻", summary_code == "A"),
            mark("- NORMAL (N)：结构正常，但不鼓励进攻", summary_code == "N"),
            mark("- CAUTION：禁止加仓，只能防守或不动", summary_code == "N"),
            mark("- D / FREEZE：必须防守", summary_code == "D"),
            "",
            "【Execution（执行摩擦）】",
            mark("- Execution = A / N：执行顺", execution in ("A", "N")),
            mark("- Execution = D / D2：执行不顺", execution in ("D", "D2")),
            "",
            "【DRS（日度风险信号）】",
            mark("- GREEN：风险环境可控（不是进攻信号）", drs_signal == "GREEN"),
            mark("- YELLOW：需降档执行", drs_signal == "YELLOW"),
            mark("- RED：否决一切进攻，仅允许防守", drs_signal == "RED"),
            "",
            "【CAUTION 状态下的冻结规则】",
            "- 禁止：加仓、追高、放大试错",
            "- 允许：维持仓位、减仓、防守性调整",
            "",
            "【轻仓试错（严格定义）】",
            "- 不增加总风险敞口",
            "- 失败成本可忽略",
            "- 仅用于验证，不用于进攻",
        ]
        note =  "👉 表示与当日制度状态直接相关的高亮提示（仅用于理解，不构成制度判断）。"
        content.append(note)
        #payload = {
        #    "meaning": "\n".join(content).strip(), 
        #    "note": "👉 表示与当日制度状态直接相关的高亮提示（仅用于理解，不构成制度判断）。",
        #}

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload="\n".join(content).strip(), 
            warnings=[],
        )    