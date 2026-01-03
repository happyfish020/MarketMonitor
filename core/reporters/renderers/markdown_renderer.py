from __future__ import annotations

from typing import Any, List, Optional

from core.reporters.report_types import ReportDocument, ReportBlock


class MarkdownRenderer:
    """UnifiedRisk V12 · MarkdownRenderer（冻结版）

    铁律：
    - Renderer 不解释制度语义
    - 是否显示人话/技术话由 Block.payload 决定
    - dev_mode 只影响审计证据链，不影响制度描述

    冻结约束：
    - 渲染层不得抛异常导致后续 block 丢失
    - payload 结构异常必须以可审计文本呈现（而不是报错/隐身）
    """

    def render(self, doc: ReportDocument) -> str:
        lines: List[str] = []

        self._render_header(lines, doc)
        self._render_actionhint(lines, doc)

        for block in (doc.blocks or []):
            try:
                self._render_block(lines, block)
            except Exception as e:
                # Renderer must never break the whole document.
                lines.append(f"## {getattr(block, 'title', 'UNKNOWN_BLOCK')}")
                lines.append("")
                lines.append("> ⚠️ exception:renderer")
                lines.append("")
                lines.append(f"渲染异常（已捕获）：{type(e).__name__}: {e}")
                lines.append("")

        return "\n".join(lines).strip() + "\n"

    # ===============================
    # Header
    # ===============================
    def _render_header(self, lines: List[str], doc: ReportDocument) -> None:
        kind = (doc.meta or {}).get("kind")
        title = "A股风险报告（Pre-open）" if kind == "PRE_OPEN" else "A股风险报告（EOD）"

        lines.append(f"# {title}")
        lines.append("本报告由个人程序自动生成，个人编程实验爱好，不构成投资建议。")
        lines.append("非预测系统，目的是提供大市的结构性风险情况，只提供风险事实检测，并提供操作指导，避免个人情绪，")
        lines.append("数据主要根据大盘，板块数据的相关性，如趋势是否仍成立、失败率/结构破坏、流动性与成交、北向代理压力、")
        lines.append("ETF–指数同步与拥挤/参与度等, 输出“允许/禁止加仓、防守/观望”等可执行边界。")
        lines.append("持续改时中。。。")

        lines.append("")
        lines.append(f"- Trade Date: **{(doc.meta or {}).get('trade_date')}**")
        lines.append(f"- Kind: **{kind}**")
        lines.append("")
        
    # ===============================
    # ActionHint
    # ===============================
    def _render_actionhint(self, lines: List[str], doc: ReportDocument) -> None:
        ah = doc.actionhint or {}

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
        if content and content.strip():
            lines.append(content)
            lines.append("")
        else:
            # Never let the block look "missing".
            lines.append("（无内容）")
            lines.append("")

    # ===============================
    # Block payload rendering
    # ===============================
    def _render_block_payload(self, block: ReportBlock) -> str:
        payload = block.payload

        # execution.timing may use a slightly different shape; keep a small adapter.
        if block.block_alias == "execution.timing":
            return self._render_execution_timing(payload)

        return self._render_common_payload(payload)

    def _render_common_payload(self, payload: Any) -> str:
        if isinstance(payload, dict):
            # Preferred: {"content": [str, ...], "note": str?}
            if isinstance(payload.get("content"), list):
                text = "\n".join(str(x) for x in (payload.get("content") or []))
                return self._append_note(text, payload.get("note"))

            # Alternate: {"meaning": [str, ...], "note": str?}
            if isinstance(payload.get("meaning"), list):
                text = "\n".join(str(x) for x in (payload.get("meaning") or []))
                return self._append_note(text, payload.get("note"))

            # Simple: {"text": "...", "note": "..."}
            if isinstance(payload.get("text"), str):
                text = payload.get("text") or ""
                return self._append_note(text, payload.get("note"))

            # Audit-friendly: note-only payload (placeholders, missing builder, etc.)
            note = payload.get("note")
            if isinstance(note, str) and note.strip():
                return f"> {note.strip()}"

            # Anything else: stable fallback (json-ish string)
            return self._fallback_render(payload)

        if isinstance(payload, list):
            return "\n".join(str(x) for x in payload)

        if isinstance(payload, str):
            return payload

        return self._fallback_render(payload)

    def _append_note(self, text: str, note: Any) -> str:
        base = text or ""
        if isinstance(note, str) and note.strip():
            if base.strip():
                return f"{base}\n\n> {note.strip()}"
            return f"> {note.strip()}"
        return base

    def _render_execution_timing(self, payload: Any) -> str:
        # Keep minimal adapter; most cases already covered by common payload rules.
        return self._render_common_payload(payload)

    def _fallback_render(self, payload: Any) -> str:
        if payload is None:
            return "（无内容）"
        try:
            return str(payload)
        except Exception:
            return "（payload 无法渲染）"
