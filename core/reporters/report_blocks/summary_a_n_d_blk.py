from __future__ import annotations

from typing import Any, Dict, List

from core.reporters.report_types import ReportBlock
from core.reporters.report_context import ReportContext 
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
    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        warnings: List[str] = []
    
        summary_code = doc_partial.get("summary")
        actionhint = doc_partial.get("actionhint")
    
        if summary_code is None:
            raise ValueError("[SummaryANDBlock] missing summary code")
    
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
    
        if isinstance(trend, dict):
            state = trend.get("state")
            trend_hint = self._render_trend_in_force_hint(state)
            if trend_hint:
                meaning = f"{meaning}\n{trend_hint}"
    
        # ===============================
        # A1-extension：DRS 日度风险信号（冻结）
        # ===============================
        drs_obs = (
            context.slots
            .get("observations", {})
            .get("drs")
        )
    
        if isinstance(drs_obs, dict):
            obs = drs_obs.get("observation", {})
            signal = obs.get("signal")
            drs_meaning = obs.get("meaning")
    
            if isinstance(signal, str) and isinstance(drs_meaning, str):
                meaning = (
                    f"{meaning}\n"
                    f"【DRS · 日度风险信号】：{signal} —— {drs_meaning}"
                )
    
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
