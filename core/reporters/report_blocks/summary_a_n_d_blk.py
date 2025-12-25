from __future__ import annotations

from typing import Any, Dict, List

from core.reporters.report_types import ReportBlock
from core.reporters.report_context import ReportContext 
from core.utils.logger import get_logger
from core.reporters.report_engine import BLOCK_SPECS
LOG = get_logger("Report.Summary")


class SummaryANDBlock:
    """
    UnifiedRisk V12 · Summary (A / N / D) Block（冻结版）

    设计原则：
    - Summary code 来自 ReportEngine（A / N / D）
    - 人话 meaning 来自 ActionHint.reason
    - 不重新解释 Gate / Structure
    - 不参与计算，只做制度解释
    """

    block_alias = "summary"
    title = "简要总结（Summary · A / N / D）"

###########
    def render_drs(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        warnings: List[str] = []
    
        summary_code = doc_partial.get("summary")
        actionhint = doc_partial.get("actionhint")
    
        if summary_code is None:
            raise ValueError("[SummaryANDBlock] missing summary code")
    
        # -------------------------------
        # 基础含义（来自 ActionHint）
        # -------------------------------
        if not actionhint:
            warnings.append("missing:actionhint")
            meaning = "未提供 ActionHint，无法生成摘要说明。"
        else:
            reason = actionhint.get("reason")
            if isinstance(reason, str) and reason.strip():
                meaning = reason
            else:
                warnings.append("missing:actionhint.reason")
                meaning = "系统未给出明确的裁决原因说明。"
    
        # ===============================
        # A1：Trend-in-Force 结构补充（冻结）
        # ===============================
        trend = (
            context.slots
            .get("structure", {})
            .get("trend_in_force")
        )
    
        trend_broken = False
        if isinstance(trend, dict):
            state = trend.get("state")
            trend_hint = self._render_trend_in_force_hint(state)
            if trend_hint:
                meaning = f"{meaning}\n{trend_hint}"
            if state == "broken":
                trend_broken = True
    
        # ===============================
        # A1b：DRS 风险补充（冻结）
        # ===============================
        drs_signal = None
        drs_meaning = None
    
        observations = context.slots.get("observations")
        if isinstance(observations, dict):
            drs = observations.get("drs")
            if isinstance(drs, dict):
                obs = drs.get("observation")
                if isinstance(obs, dict):
                    drs_signal = obs.get("signal")
                    drs_meaning = obs.get("meaning")
                else:
                    payload_obs = drs.get("payload")
                    if isinstance(payload_obs, dict):
                        drs_signal = payload_obs.get("signal")
                        drs_meaning = payload_obs.get("meaning")
    
                if isinstance(drs_signal, str):
                    meaning = (
                        f"{meaning}\n"
                        f"【DRS · 日度风险信号】：{drs_signal} —— "
                        f"{drs_meaning or '未提供风险说明'}"
                    )
    
        # ===============================
        # 🔴 冻结裁决降级规则（核心）
        # Trend = broken 或 DRS = RED → 必然 D
        # ===============================
        if trend_broken or drs_signal == "RED":
            summary_code = "D"
    
        # ===============================
        # A1c：D + RED + broken 减仓边界说明（冻结）
        # ⚠️ 必须在 payload 构造之前
        # ===============================
        if summary_code == "D" and trend_broken and drs_signal == "RED":
            meaning = (
                f"{meaning}\n"
                "【制度说明｜风险敞口边界】\n"
                "当前处于 D + RED + broken 状态。\n"
                "趋势结构已失效，制度风险处于高位，\n"
                "系统不再支持维持现有风险敞口水平，\n"
                "制度上允许并偏向采取防守性调整（减少风险敞口）。"
            )
    
        # ===============================
        # 最后再构造 payload
        # ===============================
        payload = {
            "code": summary_code,
            "meaning": meaning,
        }
    
        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload=payload,
            warnings=warnings,
        )
###########

    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        warnings: List[str] = []
    
        # ===============================
        # ① Summary Code（最终裁决入口）
        # ===============================
        summary_code = doc_partial.get("summary")
        actionhint = doc_partial.get("actionhint")
    
        if summary_code is None:
            raise ValueError("[SummaryANDBlock] missing summary code")
    
        # -------------------------------
        # ② 基础含义（来自 ActionHint）
        # -------------------------------
        if not actionhint:
            warnings.append("missing:actionhint")
            meaning = "未提供 ActionHint，无法生成摘要说明。"
        else:
            reason = actionhint.get("reason")
            if isinstance(reason, str) and reason.strip():
                meaning = reason
            else:
                warnings.append("missing:actionhint.reason")
                meaning = "系统未给出明确的裁决原因说明。"
    
        # ===============================
        # ③ Trend-in-Force（只读，一次性取）
        # ===============================
        trend = context.slots.get("structure", {}).get("trend_in_force")
        trend_state = trend.get("state") if isinstance(trend, dict) else None
        trend_broken = trend_state == "broken"
    
        if trend_state:
            trend_hint = self._render_trend_in_force_hint(trend_state)
            if trend_hint:
                meaning = f"{meaning}\n{trend_hint}"
    
        # ===============================
        # ④ DRS · 日度制度风险信号（标准化读取）
        # ===============================
        drs = context.slots.get("drs")
        drs_signal = None
        drs_meaning = None
    
        if isinstance(drs, dict):
            drs_signal = drs.get("signal")
            drs_meaning = drs.get("meaning")
    
        if isinstance(drs_signal, str):
            meaning = (
                f"{meaning}\n"
                f"【DRS · 日度风险信号】：{drs_signal} —— "
                f"{drs_meaning or '未提供风险说明'}"
            )
    
        # ===============================
        # 🔴 冻结降级规则（唯一裁决点）
        # A/N → D 只允许降级
        # ===============================
        if summary_code != "D" and (trend_broken or drs_signal == "RED"):
            summary_code = "D"
    
        # ===============================
        # ⑤ Execution Summary（2–5D）
        # ⚠️ 只解释，不参与裁决
        # ===============================
        execu = context.slots.get("execution_summary")
        if isinstance(execu, dict):
            exec_code = execu.get("code")
            exec_meaning = execu.get("meaning")
            if exec_code:
                meaning = (
                    f"{meaning}\n"
                    f"【Execution · 2–5D】{exec_code} —— "
                    f"{exec_meaning or '未提供短期执行风险说明'}"
                )
    
        # ===============================
        # ⑥ Gate 权限变化（pre → final）
        # ===============================
        gate_pre = context.slots.get("gate_pre")
        gate_final = context.slots.get("gate_final")
    
        if gate_pre and gate_final:
            meaning = (
                f"{meaning}\n"
                f"【制度权限（Gate）】\n"
                f"- 原始 Gate：{gate_pre}\n"
                f"- 执行后 Gate：{gate_final}"
            )
    
        # ===============================
        # A1c：D + RED + broken
        # 防守性减仓边界说明（冻结）
        # ===============================
        if summary_code == "D" and trend_broken and drs_signal == "RED":
            meaning = (
                f"{meaning}\n"
                "【制度说明｜风险敞口边界】\n"
                "当前处于 D + RED + broken 状态。\n"
                "趋势结构已失效，制度风险处于高位，\n"
                "系统不再支持维持现有风险敞口水平，\n"
                "制度上允许并偏向采取防守性调整（减少风险敞口）。"
            )
    
        # ===============================
        # ⑦ 构造 payload（不再修改 code）
        # ===============================
        payload = {
            "code": summary_code,
            "meaning": meaning,
        }
    
        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload=payload,
            warnings=warnings,
        )
    
 
###########
    def _render_trend_in_force_hint(self, state: str | None) -> str:
        if state == "in_force":
            return "趋势结构补充：当前趋势结构仍然成立，市场仍处于有效趋势环境中。"
    
        if state == "weakening":
            return "趋势结构补充：趋势动能减弱，结构进入观察阶段，对趋势确认的支持度下降。"
    
        if state == "broken":
            return "趋势结构补充：趋势结构已被破坏，当前环境不再具备趋势确认条件。"
    
        return ""
