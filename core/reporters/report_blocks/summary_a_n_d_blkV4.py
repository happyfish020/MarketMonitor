# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List

from core.reporters.report_types import ReportBlock
from core.reporters.report_context import ReportContext
from core.utils.logger import get_logger
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase

LOG = get_logger("Report.Summary")


class SummaryANDBlock(ReportBlockRendererBase):
    """
    UnifiedRisk V12 · Summary (A / N / D) Block（冻结完整版）

    设计铁律：
    - Summary code 来自 ActionHint（ReportEngine 内生成）
    - DRS / ExecutionSummary / Rebound-only 仅做解释与降级（只允许降级）
    - 不参与任何因子计算、预测或再裁决
    """

    block_alias = "summary"
    title = "简要总结（Summary · A / N / D）"

    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        warnings: List[str] = []

        # =====================================================
        # ① Summary Code（来自 ActionHint，最终裁决起点）
        # =====================================================
        summary_code = doc_partial.get("summary")
        actionhint = doc_partial.get("actionhint")

        if summary_code is None:
            raise ValueError("[SummaryANDBlock] missing summary code")

        # -----------------------------------------------------
        # ② 基础含义（来自 ActionHint.reason）
        # -----------------------------------------------------
        if not isinstance(actionhint, dict):
            warnings.append("missing:actionhint")
            meaning = "未提供 ActionHint，无法生成制度化摘要说明。"
        else:
            reason = actionhint.get("reason")
            if isinstance(reason, str) and reason.strip():
                meaning = reason
            else:
                warnings.append("missing:actionhint.reason")
                meaning = "系统未给出明确的制度裁决原因说明。"

        # =====================================================
        # ③ Trend-in-Force（结构补充 · 只读）
        # =====================================================
        trend_state = None
        structure = context.slots.get("structure")
        if isinstance(structure, dict):
            trend = structure.get("trend_in_force")
            if isinstance(trend, dict):
                trend_state = trend.get("state")

        trend_broken = trend_state == "broken"

        if trend_state:
            meaning = f"{meaning}\n{self._render_trend_hint(trend_state)}"

        # =====================================================
        # ④ DRS · 日度制度风险信号（只读）
        # =====================================================
        drs_signal = None
        drs_meaning = None

        drs = context.slots.get("drs")
        if isinstance(drs, dict):
            drs_signal = drs.get("signal")
            drs_meaning = drs.get("meaning")

        if isinstance(drs_signal, str):
            meaning = (
                f"{meaning}\n"
                f"【DRS · 日度风险信号】：{drs_signal} —— "
                f"{drs_meaning or '未提供风险说明'}"
            )

        # =====================================================
        # 🔴 降级规则（冻结）
        # Trend broken 或 DRS = RED → Summary = D
        # =====================================================
        if summary_code != "D" and (trend_broken or drs_signal == "RED"):
            summary_code = "D"

        # =====================================================
        # ⑤ Execution Summary（2–5D 执行维度 · 只读）
        # =====================================================
        execu = context.slots.get("execution_summary")
        execution_band = None

        if isinstance(execu, dict):
            exec_code = execu.get("code")
            execution_band = execu.get("band")
            exec_meaning = execu.get("meaning")

            if exec_code:
                meaning = (
                    f"{meaning}\n"
                    f"【Execution · 2–5D】{exec_code}"
                    f"{f'/{execution_band}' if execution_band else ''}"
                    f" —— {exec_meaning or '未提供短期执行风险说明'}"
                )

        # =====================================================
        # ⑥ Rebound-only Observation（反弹不可追 · 只读）
        # =====================================================
        rebound_only = context.slots.get("rebound_only")

        if isinstance(rebound_only, dict):
            flag = rebound_only.get("flag")
            severity = rebound_only.get("severity")
            ro_meaning = rebound_only.get("meaning")

            if flag:
                meaning = (
                    f"{meaning}\n"
                    f"【Rebound-only】{severity or 'NA'} —— "
                    f"{ro_meaning or '反弹阶段不支持追涨执行'}"
                )

        # =====================================================
        # ⑦ Gate 权限变化（Overlay 后 · 展示）
        # =====================================================
        gate_pre = context.slots.get("gate_pre")
        gate_final = context.slots.get("gate_final")

        if gate_pre and gate_final:
            meaning = (
                f"{meaning}\n"
                f"【制度权限（Gate）】\n"
                f"- 原始 Gate：{gate_pre}\n"
                f"- 执行后 Gate：{gate_final}"
            )

        # =====================================================
        # ⑧ D + RED + broken → 风险敞口边界说明（冻结）
        # =====================================================
        if summary_code == "D" and trend_broken and drs_signal == "RED":
            meaning = (
                f"{meaning}\n"
                "【制度说明｜风险敞口边界】\n"
                "当前处于 D + RED + broken 状态。\n"
                "趋势结构已失效，制度风险处于高位，\n"
                "系统不再支持维持现有风险敞口水平，\n"
                "制度上允许并偏向采取防守性调整（减少风险敞口）。"
            )

        # =====================================================
        # ⑨ 构造 payload（最终输出）
        # =====================================================
        #payload = {
        #    "code": summary_code,
        #    "meaning": meaning,
        #}
        payload = f" Code:{summary_code}\n {meaning}"
        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload=payload,
            warnings=warnings,
        )

    # ---------------------------------------------------------
    # helpers（冻结）
    # ---------------------------------------------------------
    def _render_trend_hint(self, state: Any) -> str:
        if state == "ok":
            return "趋势结构补充：当前趋势结构仍然成立，市场仍处于有效趋势环境中。"
        if state == "weak":
            return "趋势结构补充：趋势结构偏弱，仍需等待更明确的确认。"
        if state == "broken":
            return "趋势结构补充：趋势结构已被破坏，当前环境不再具备趋势确认条件。"
        return ""
