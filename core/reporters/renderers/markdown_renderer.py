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
        lines.append("本报告由个人程序自动生成，个人代码实践，不构成投资建议。")
        lines.append("非预测系统，目的是提供大市的结构性风险情况，只提供风险事实检测，并提供操作指导，避免个人情绪，")
        lines.append("数据主要根据大盘，板块数据的相关性，如趋势是否仍成立、失败率/结构破坏、流动性与成交、北向代理压力、")
        lines.append("ETF–指数同步  与拥挤/参与度等, 输出“允许/禁止加仓、防守/观望”等可执行边界, 减少人为情绪操作。")
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

        # Summary (legacy, keep)
        lines.append(f"**当前制度状态：{doc.summary}**")
        lines.append("")

        # Explicit opportunity permission
        ap = ah.get("attack_permit")
        if isinstance(ap, dict):
            label = ap.get("label") if isinstance(ap.get("label"), str) else None
            permit = ap.get("permit") if isinstance(ap.get("permit"), str) else "-"
            mode = ap.get("mode") if isinstance(ap.get("mode"), str) else "-"
            lines.append(f"**进攻许可（AttackPermit）：** {label or (str(permit) + ' (' + str(mode) + ')')}")
            # show key evidence compactly (avoid noise)
            ev = ap.get("evidence") if isinstance(ap.get("evidence"), dict) else {}
            adv = ev.get("adv_ratio")
            top20 = ev.get("top20_ratio")
            if isinstance(adv, (int, float)) or isinstance(top20, (int, float)):
                parts = []
                if isinstance(adv, (int, float)):
                    parts.append(f"adv_ratio={float(adv):.3f}")
                if isinstance(top20, (int, float)):
                    parts.append(f"top20_ratio={float(top20):.4f} (strict)")
                if parts:
                    lines.append(f"- 关键证据：{', '.join(parts)}")
            # show warnings (if any)
            ws = ap.get("warnings")
            if isinstance(ws, list) and ws:
                for w in ws[:6]:
                    lines.append(f"> ⚠️ {w}")
            lines.append("")

        # DOS (Daily Opportunity Signal)
        dos = ah.get("dos")
        if isinstance(dos, dict):
            lvl = dos.get("level") if isinstance(dos.get("level"), str) else "-"
            mode = dos.get("mode") if isinstance(dos.get("mode"), str) else "-"
            allowed = dos.get("allowed") if isinstance(dos.get("allowed"), list) else []
            lines.append(f"**机会信号（DOS）：** {lvl} ({mode})")
            if allowed:
                al = ", ".join([str(x) for x in allowed[:10]])
                lines.append(f"- allowed: {al}")
            ws = dos.get("warnings")
            if isinstance(ws, list) and ws:
                for w in ws[:6]:
                    lines.append(f"> ⚠️ {w}")
            lines.append("")

        # Core reason
        reason = ah.get("reason")
        if reason:
            lines.append(f"**裁决依据：** {reason}")
            lines.append("")

        # Allowed / Forbidden / Limits (explicit)
        allowed = ah.get("allowed")
        if isinstance(allowed, list) and allowed:
            lines.append("**允许：**")
            for a in allowed:
                lines.append(f"- {a}")
            lines.append("")

        forbidden = ah.get("forbidden")
        if isinstance(forbidden, list) and forbidden:
            lines.append("**禁止：**")
            for x in forbidden:
                lines.append(f"- {x}")
            lines.append("")

        limits = ah.get("limits")
        if isinstance(limits, str) and limits.strip():
            lines.append(f"**行为边界：** {limits.strip()}")
            lines.append("")

        cond = ah.get("conditions")
        if isinstance(cond, str) and cond.strip():
            lines.append(f"**条件/备注：** {cond.strip()}")
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
