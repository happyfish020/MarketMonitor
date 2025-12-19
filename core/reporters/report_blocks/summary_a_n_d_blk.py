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
