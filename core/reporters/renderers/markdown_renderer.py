from __future__ import annotations

from typing import Any, List

from core.reporters.report_types import ReportDocument, ReportBlock


class MarkdownRenderer:
    """
    UnifiedRisk V12 · MarkdownRenderer（冻结版）

    铁律：
    - Renderer 不解释制度语义
    - 是否显示人话/技术话由 Block.payload 决定
    - dev_mode 只影响审计证据链，不影响制度描述
    """

    def render(self, doc: ReportDocument) -> str:
        lines: List[str] = []

        self._render_header(lines, doc)
        self._render_actionhint(lines, doc)

        for block in doc.blocks:
            self._render_block(lines, block)

        return "\n".join(lines).strip() + "\n"

    # ===============================
    # Header
    # ===============================
    def _render_header(self, lines: List[str], doc: ReportDocument) -> None:
        kind = doc.meta.get("kind")
        title = (
            "A股制度风险报告（Pre-open）"
            if kind == "PRE_OPEN"
            else "A股制度风险报告（EOD）"
        )

        lines.append(f"# {title}")
        lines.append("")
        lines.append(f"- Trade Date: **{doc.meta.get('trade_date')}**")
        lines.append(f"- Kind: **{kind}**")
        lines.append("")

    # ===============================
    # ActionHint
    # ===============================
    def _render_actionhint(self, lines: List[str], doc: ReportDocument) -> None:
        ah = doc.actionhint

        lines.append("## 系统裁决（ActionHint）")
        lines.append("")
        lines.append(f"**当前制度状态：{doc.summary}**")
        lines.append("")
        lines.append(f"**裁决依据：** {ah.get('reason')}")
        lines.append("")

    # ===============================
    # Block dispatcher
    # ===============================
    def _render_block(self, lines: List[str], block: ReportBlock) -> None:
        lines.append(f"## {block.title}")
        lines.append("")

        if block.warnings:
            for w in block.warnings:
                lines.append(f"> ⚠️ {w}")
            lines.append("")

        content = self._render_block_payload(block)
        if content:
            lines.append(content)
            lines.append("")

    # ===============================
    # Block payload rendering
    # ===============================
    def _render_block_payload(self, block: ReportBlock) -> str:
        alias = block.block_alias
        payload = block.payload

        #if alias == "execution.timing":
        if alias in {"execution.timing", "exposure.boundary"}:
            return self._render_execution_timing(payload)

        if isinstance(payload, str):
            return payload

        return self._fallback_render(payload)

    # ===============================
    # Execution timing (human only)
    # ===============================
    def _render_execution_timing(self, payload: Any) -> str:
        if not isinstance(payload, dict):
            return "（风险敞口变更行为说明不可用）"

        lines: List[str] = []

        intro = payload.get("intro")
        if intro:
            lines.append(intro)
            lines.append("")

        dominant = payload.get("dominant")
        if dominant:
            lines.append(f"**制度主导方向：** {dominant}")
            lines.append("")

        for b in payload.get("behaviors", []):
            lines.append(f"【{b.get('label')}】")
            lines.append(f"{b.get('status')}：{b.get('meaning')}")
            lines.append("")

        note = payload.get("note")
        if note:
            lines.append(f"**制度说明：** {note}")

        return "\n".join(lines)

    # ===============================
    # Fallback
    # ===============================
    def _fallback_render(self, payload: Any) -> str:
        if payload is None:
            return "（无内容）"
        return str(payload)
