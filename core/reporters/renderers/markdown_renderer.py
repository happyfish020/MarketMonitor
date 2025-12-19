from __future__ import annotations

from typing import Any, Dict, List, Optional

from core.reporters.report_types import ReportDocument, ReportBlock


class MarkdownRenderer:
    """
    UnifiedRisk V12 · MarkdownRenderer（正式报告冻结版）

    冻结输出规则：
    - 正式报告不输出 JSON（dev.evidence 除外）
    - 所有标题/正文优先中文
    - Renderer 只做展示，不解释制度（不参与计算）
    """

    # block_alias → 标题（中文在前）
    REPORT_TITLES = {
        "actionhint": "系统裁决（ActionHint）",
        "summary": "简要总结（Summary · A / N / D）",
        "structure.facts": "结构事实（Structure Facts）",
        "context.overnight": "隔夜全球环境（Overnight Context）",
        "watchlist.sectors": "观察对象（Watchlist）",
        "conditions.runtime": "即时验证条件（Runtime Conditions）",
        "scenarios.forward": "情景说明（Scenarios · T+N）",
        "dev.evidence": "审计证据链（Dev / Evidence）",
    }

    # ===============================
    # Public API
    # ===============================
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
        title = "A股制度风险报告（Pre-open）" if kind == "PRE_OPEN" else "A股制度风险报告（EOD）"

        lines.append(f"# {title}")
        lines.append("")
        # 中文在前
        lines.append(f"- 交易日：**{doc.meta.get('trade_date')}**")
        lines.append(f"- 报告类型：**{kind}**")
        lines.append("")

    # ===============================
    # ActionHint（中文、人话、非 JSON）
    # ===============================
    def _render_actionhint(self, lines: List[str], doc: ReportDocument) -> None:
        ah = doc.actionhint or {}
        lines.append(f"## {self.REPORT_TITLES['actionhint']}")
        lines.append("")

        action = ah.get("action")
        reason = ah.get("reason")

        # 裁决主句（中文）
        if action == "HOLD":
            lines.append("**当前裁决：允许持有，但不允许主动加仓**")
        elif action == "ADD":
            lines.append("**当前裁决：允许参与 / 加仓**")
        elif action == "REDUCE":
            lines.append("**当前裁决：建议减仓或退出**")
        else:
            lines.append(f"**当前裁决：{action}**")

        lines.append("")
        if isinstance(reason, str) and reason.strip():
            lines.append(f"**裁决依据：** {reason}")
            lines.append("")

        # 允许/禁止（中文条列）
        allowed = ah.get("allowed")
        forbidden = ah.get("forbidden")

        if isinstance(allowed, list) and allowed:
            lines.append("**允许：**")
            for x in allowed:
                lines.append(f"- {self._as_cn_text(x)}")
            lines.append("")

        if isinstance(forbidden, list) and forbidden:
            lines.append("**禁止：**")
            for x in forbidden:
                lines.append(f"- {self._as_cn_text(x)}")
            lines.append("")

        # 执行边界说明（中文一句）
        limits = ah.get("limits")
        if isinstance(limits, dict):
            note = limits.get("notes")
            if isinstance(note, str) and note.strip():
                lines.append(f"**执行边界说明：** {self._as_cn_text(note)}")
                lines.append("")

        # 即时条件（若存在，用中文条列）
        conditions = ah.get("conditions")
        if isinstance(conditions, list) and conditions:
            # 这里不强制每条结构，只做展示
            lines.append("**即时条件：**")
            for c in conditions:
                if isinstance(c, dict):
                    status = c.get("status")
                    note = c.get("note")
                    if note:
                        lines.append(f"- {self._as_cn_text(note)}")
                    elif status:
                        lines.append(f"- 状态：{status}")
                    else:
                        lines.append("- （条件条目缺少可读说明）")
                else:
                    lines.append(f"- {self._as_cn_text(c)}")
            lines.append("")

        if doc.meta.get("kind") == "EOD":
            lines.append("> ⚠️ EOD ActionHint 仅用于审计，不可执行。")
            lines.append("")

    # ===============================
    # Block dispatcher
    # ===============================
    def _render_block(self, lines: List[str], block: ReportBlock) -> None:
        title = self.REPORT_TITLES.get(block.block_alias, block.block_alias)
        lines.append(f"## {title}")
        lines.append("")

        if block.warnings:
            lines.append("> Warnings:")
            for w in block.warnings:
                lines.append(f"> - {w}")
            lines.append("")

        content = self._render_block_payload(block)
        if content:
            lines.append(content)
            lines.append("")

    # ===============================
    # Block payload rendering policy（正式报告）
    # ===============================
    def _render_block_payload(self, block: ReportBlock) -> str:
        alias = block.block_alias
        payload = block.payload

        # Summary：只输出 meaning（人话）
        if alias == "summary":
            if isinstance(payload, dict):
                return self._as_cn_text(payload.get("meaning", ""))
            return ""

        # dev.evidence：允许 JSON（审计用途）
        if alias.startswith("dev."):
            return self._to_dev_json(payload)

        # structure.facts：类 YAML（中文在前）
        if alias == "structure.facts":
            return self._render_structure_facts(payload)

        # scenarios.forward：T+N 直出（中文句子）
        if alias == "scenarios.forward":
            return self._render_scenarios(payload)

        # context.overnight：优先 note/summary 文本
        if alias == "context.overnight":
            return self._render_note_block(payload)

        # watchlist.sectors：观察对象条列
        if alias == "watchlist.sectors":
            return self._render_watchlist(payload)

        # conditions.runtime：条件条列
        if alias == "conditions.runtime":
            return self._render_conditions(payload)

        # 其他：若 payload 本身就是 str，就直接输出（中文）
        if isinstance(payload, str):
            return self._as_cn_text(payload)

        return "（该区块当前无可读文本输出）"

    # ===============================
    # Render helpers (no JSON)
    # ===============================
    def _render_structure_facts(self, payload: Any) -> str:
        """
        目标：不输出 JSON，而用“key: state — meaning”的类 YAML 文本
        兼容 payload = {"structure": {...}, "note": "..."}
        """
        if not isinstance(payload, dict):
            return "（结构事实缺少结构化载荷）"

        structure = payload.get("structure")
        note = payload.get("note")

        lines: List[str] = []
        if isinstance(structure, dict) and structure:
            # summary 放最前（若存在）
            summary = structure.get("_summary")
            if isinstance(summary, dict):
                m = summary.get("meaning")
                if m:
                    lines.append(f"- 总结：{self._as_cn_text(m)}")
                    lines.append("")

            # 逐项输出
            for k, v in structure.items():
                if k == "_summary":
                    continue
                if isinstance(v, dict):
                    state = v.get("state")
                    meaning = v.get("meaning")
                    if meaning and state:
                        lines.append(f"- {k}: {state} — {self._as_cn_text(meaning)}")
                    elif meaning:
                        lines.append(f"- {k}: {self._as_cn_text(meaning)}")
                    else:
                        lines.append(f"- {k}: （无可读说明）")
                else:
                    lines.append(f"- {k}: {self._as_cn_text(v)}")
        else:
            lines.append("（结构事实为空或未接入）")

        if isinstance(note, str) and note.strip():
            lines.append("")
            lines.append(f"> 注：{self._as_cn_text(note)}")

        return "\n".join(lines).strip()

    def _render_scenarios(self, payload: Any) -> str:
        """
        目标：不输出 JSON
        兼容 payload 可以是 str，或 dict（从中提取人话字段）
        """
        if isinstance(payload, str):
            return self._as_cn_text(payload)

        if not isinstance(payload, dict):
            return "（情景说明缺少可读文本）"

        # 推荐：block builder 直接给 payload=str
        # 这里做兼容：如果有 scenario_text / note，就输出
        for key in ("scenario_text", "text", "note", "scenario_note"):
            v = payload.get(key)
            if isinstance(v, str) and v.strip():
                return self._as_cn_text(v)

        return "（情景说明当前未提供可读文本）"

    def _render_note_block(self, payload: Any) -> str:
        """
        通用：优先输出 note / meaning / summary 字段
        """
        if isinstance(payload, str):
            return self._as_cn_text(payload)

        if not isinstance(payload, dict):
            return "（该区块缺少可读文本）"

        # 常见字段尝试
        for key in ("meaning", "summary", "note"):
            v = payload.get(key)
            if isinstance(v, str) and v.strip():
                return self._as_cn_text(v)

        # 二层结构尝试（如 {"overnight": {"note": ...}, "note": ...}）
        for k, v in payload.items():
            if isinstance(v, dict):
                for key in ("meaning", "summary", "note"):
                    s = v.get(key)
                    if isinstance(s, str) and s.strip():
                        return self._as_cn_text(s)

        return "（该区块当前无可读文本输出）"

    def _render_watchlist(self, payload: Any) -> str:
        """
        目标：观察对象清单条列化，不输出 JSON
        兼容 payload={"watchlist": {...}, "note": "..."} 或直接 str
        """
        if isinstance(payload, str):
            return self._as_cn_text(payload)

        if not isinstance(payload, dict):
            return "（观察对象区块缺少可读文本）"

        watchlist = payload.get("watchlist")
        note = payload.get("note")

        lines: List[str] = []
        if isinstance(watchlist, dict) and watchlist:
            # 常见：watchlist 下有 sectors/stocks/indices/meta
            for group_key in ("sectors", "stocks", "indices"):
                grp = watchlist.get(group_key)
                if isinstance(grp, dict) and grp:
                    lines.append(f"- {group_key}:")
                    for obj_id, obj in grp.items():
                        if isinstance(obj, dict):
                            title = obj.get("title") or obj_id
                            summary = obj.get("summary") or obj.get("detail") or ""
                            lines.append(f"  - {self._as_cn_text(title)}：{self._as_cn_text(summary)}")
                        else:
                            lines.append(f"  - {obj_id}")
                    lines.append("")
            # 若没有分组，直接列键
            if not lines:
                lines.append("（观察对象已接入，但结构未分组展示）")
        else:
            lines.append("（观察对象为空或未接入）")

        if isinstance(note, str) and note.strip():
            lines.append("")
            lines.append(f"> 注：{self._as_cn_text(note)}")

        return "\n".join(lines).strip()

    def _render_conditions(self, payload: Any) -> str:
        """
        目标：即时验证条件条列化，不输出 JSON
        兼容 payload={"conditions_runtime": ... , "note": "..."} 或 str
        """
        if isinstance(payload, str):
            return self._as_cn_text(payload)

        if not isinstance(payload, dict):
            return "（即时验证条件缺少可读文本）"

        cr = payload.get("conditions_runtime")
        note = payload.get("note")

        lines: List[str] = []
        if isinstance(cr, dict):
            # 常见：{"status": "...", "note": "..."}
            n = cr.get("note")
            if isinstance(n, str) and n.strip():
                lines.append(f"- {self._as_cn_text(n)}")
            else:
                status = cr.get("status")
                if status:
                    lines.append(f"- 状态：{status}")
        elif isinstance(cr, list):
            for item in cr:
                if isinstance(item, dict):
                    n = item.get("note")
                    if n:
                        lines.append(f"- {self._as_cn_text(n)}")
                    else:
                        lines.append("- （条件条目缺少 note）")
                else:
                    lines.append(f"- {self._as_cn_text(item)}")
        elif cr is None:
            lines.append("（即时验证条件未接入）")
        else:
            lines.append("（即时验证条件结构无法识别）")

        if isinstance(note, str) and note.strip():
            lines.append("")
            lines.append(f"> 注：{self._as_cn_text(note)}")

        return "\n".join(lines).strip()

    # ===============================
    # Dev JSON (only for dev.evidence)
    # ===============================
    def _to_dev_json(self, payload: Any) -> str:
        import json
        try:
            s = json.dumps(payload, ensure_ascii=False, indent=2)
        except Exception:
            s = str(payload)
        return "```json\n" + s + "\n```"

    # ===============================
    # Simple CN text normalization
    # ===============================
    def _as_cn_text(self, x: Any) -> str:
        """
        轻量中文化兜底：仅处理你当前输出里出现的固定英文短句。
        其余不做翻译（避免引入主观性）。
        """
        if x is None:
            return ""

        s = str(x)

        # 固定英文短句兜底（来自你当前输出）
        replacements = {
            "Bypass ActionHint with manual interpretation": "不要绕过系统裁决，用主观解读替代 ActionHint",
            "Limits are policy-defined. This default implementation is conservative and non-prescriptive.": "执行边界由制度定义；当前实现采用保守默认值，不构成操作建议。",
        }
        return replacements.get(s, s)
