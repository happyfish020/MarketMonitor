# -*- coding: utf-8 -*-
"""
UnifiedRisk V12
MarkdownRenderer

职责（冻结）：
- 只做 ReportDocument -> Markdown 文本渲染
- 不参与任何制度计算
- 不补数据、不改 slots
- 仅负责“表达层格式”与“人话/技术双轨呈现”

关键修复点：
- 不再依赖 doc.title（避免 AttributeError）
- 强制 block 顺序（对齐冻结样板）
- 每个 block：人话在前、技术在后
- FactorResult / numpy 类型等做轻量可读化
"""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Optional

from core.reporters.report_types import ReportBlock


class MarkdownRenderer:
    """
    渲染规则：
    - 顶部固定标题 + 交易日 + 报告类型
    - Block 强制顺序
    - 每个 Block 两段：
        1) 人话轨（可读摘要）
        2) 技术轨（关键字段，精简）
    """

    # 冻结顺序（你样板的顺序语义）
    _BLOCK_ORDER: List[str] = [
        # 你现在系统里如果 ActionHint 是一个 block，也会被排到最前
        "actionhint",
        "summary",
        "structure.facts",
        "context.overnight",
        "watchlist.sectors",
        "conditions.runtime",
        "scenarios.forward",
        "dev.evidence",
    ]

    def render(self, doc: Any) -> str:
        """
        输入 doc：ReportDocument（具体字段不强依赖，使用 getattr 防御）
        必须返回 Markdown 字符串
        """
        kind = self._get_doc_kind(doc) or "PRE_OPEN"
        trade_date = self._get_doc_trade_date(doc) or "UNKNOWN"

        title = self._title_from_kind(kind)

        lines: List[str] = []
        lines.append(f"# {title}")
        lines.append("")
        lines.append(f"- 交易日：**{trade_date}**")
        lines.append(f"- 报告类型：**{kind}**")
        lines.append("")

        blocks = self._get_doc_blocks(doc)
        blocks_sorted = self._sort_blocks(blocks)

        for blk in blocks_sorted:
            lines.extend(self._render_block(blk))

        return "\n".join(lines).rstrip() + "\n"

    # =========================================================
    # Doc helpers (defensive)
    # =========================================================

    def _get_doc_kind(self, doc: Any) -> Optional[str]:
        for k in ("kind", "report_type", "type"):
            v = getattr(doc, k, None)
            if isinstance(v, str) and v.strip():
                return v.strip()
        # 有些实现把 meta 放在 doc.meta
        meta = getattr(doc, "meta", None)
        if isinstance(meta, dict):
            v = meta.get("kind") or meta.get("report_type")
            if isinstance(v, str) and v.strip():
                return v.strip()
        return None

    def _get_doc_trade_date(self, doc: Any) -> Optional[str]:
        for k in ("trade_date", "asof", "date"):
            v = getattr(doc, k, None)
            if isinstance(v, str) and v.strip():
                return v.strip()
        meta = getattr(doc, "meta", None)
        if isinstance(meta, dict):
            v = meta.get("trade_date") or meta.get("date")
            if isinstance(v, str) and v.strip():
                return v.strip()
        return None

    def _get_doc_blocks(self, doc: Any) -> List[ReportBlock]:
        blocks = getattr(doc, "blocks", None)
        if isinstance(blocks, list):
            return blocks
        # 兼容 doc.document / doc.payload 等结构
        blocks = getattr(doc, "report_blocks", None)
        if isinstance(blocks, list):
            return blocks
        return []

    def _title_from_kind(self, kind: str) -> str:
        k = (kind or "").upper()
        if "PRE" in k:
            return "A股制度风险报告（Pre-open）"
        if "EOD" in k or "CLOSE" in k:
            return "A股制度风险报告（盘后）"
        return "A股制度风险报告"

    # =========================================================
    # Sorting
    # =========================================================

    def _sort_blocks(self, blocks: List[ReportBlock]) -> List[ReportBlock]:
        order_index = {alias: i for i, alias in enumerate(self._BLOCK_ORDER)}

        def key_fn(b: ReportBlock) -> Any:
            alias = getattr(b, "block_alias", "") or ""
            if alias in order_index:
                return (0, order_index[alias])
            # 未识别的 block 放在后面，按 alias 字母序
            return (1, alias)

        return sorted(blocks, key=key_fn)

    # =========================================================
    # Block rendering (human first, then tech)
    # =========================================================

    def _render_block(self, blk: ReportBlock) -> List[str]:
        alias = getattr(blk, "block_alias", "") or ""
        title = getattr(blk, "title", "") or alias or "Block"
        payload = getattr(blk, "payload", None)
        warnings = getattr(blk, "warnings", None)

        out: List[str] = []
        out.append(f"## {title}")
        out.append("")

        # 1) 人话轨
        out.extend(self._render_human(alias=alias, payload=payload))
        out.append("")

        # 2) 技术轨（无旁白：只给关键字段）
        out.extend(self._render_tech(alias=alias, payload=payload))
        out.append("")

        # warnings（开发态）
        if isinstance(warnings, list) and warnings:
            out.append("#### Warnings")
            for w in warnings[:10]:
                out.append(f"- {self._safe_str(w)}")
            out.append("")

        return out

    # -------------------------
    # Human track
    # -------------------------

    def _render_human(self, *, alias: str, payload: Any) -> List[str]:
        p = payload if isinstance(payload, dict) else {}
        lines: List[str] = []

        # Summary block 常见结构：{code/meaning} 或 {summary: {...}}
        if alias == "summary":
            code = p.get("code") or (p.get("summary") or {}).get("code")
            meaning = p.get("meaning") or (p.get("summary") or {}).get("meaning")
            if code or meaning:
                lines.append("### 简要总结（Summary · A / N / D）")
                lines.append("")
                if code:
                    lines.append(f"- Summary：{code}")
                if meaning:
                    lines.append(f"- 解释：{meaning}")
                return lines

        if alias == "structure.facts":
            lines.append("### 结构事实（Structure Facts）")
            lines.append("")
            structure = p.get("structure")
            if not isinstance(structure, dict) or not structure:
                # 你冻结样板里这里要显示“空/未接入”的明确提示
                lines.append("（结构事实为空或未接入）")
                note = p.get("note")
                if isinstance(note, str) and note.strip():
                    lines.append("")
                    lines.append(f"> 注：{note.strip()}")
                return lines

            # 结构 facts：按 key 输出 state/meaning（人话）
            # 允许结构为：{name: {state, meaning, evidence}} 或 {name: "..."}
            for k in structure.keys():
                v = structure.get(k)
                if isinstance(v, dict):
                    state = v.get("state")
                    meaning = v.get("meaning")
                    # 只输出关键两项
                    if state is not None or meaning is not None:
                        s1 = f"- **{k}**"
                        if state is not None:
                            s1 += f"：{state}"
                        lines.append(s1)
                        if meaning:
                            lines.append(f"  - 含义：{meaning}")
                else:
                    lines.append(f"- **{k}**：{self._safe_str(v)}")

            note = p.get("note")
            if isinstance(note, str) and note.strip():
                lines.append("")
                lines.append(f"> 注：{note.strip()}")
            return lines

        if alias == "context.overnight":
            lines.append("### 隔夜全球环境（Overnight Context）")
            lines.append("")
            overnight = p.get("overnight")
            # overnight 为空时也必须输出人话句子（你指出的痛点）
            if not isinstance(overnight, dict) or not overnight:
                lines.append("（隔夜摘要暂缺或未接入）")
            note = p.get("note")
            if isinstance(note, str) and note.strip():
                lines.append("")
                lines.append(note.strip())
            return lines

        if alias == "watchlist.sectors":
            lines.append("### 观察对象（Watchlist）")
            lines.append("")
            watchlist = p.get("watchlist")
            if not isinstance(watchlist, dict) or not watchlist:
                lines.append("（观察对象为空或未接入）")
            else:
                # 期待 watchlist_state_builder 输出：
                # {obj_id: {title,state,summary,detail,...}}
                for obj_id, row in watchlist.items():
                    if not isinstance(row, dict):
                        lines.append(f"- {obj_id}：{self._safe_str(row)}")
                        continue
                    title = row.get("title") or obj_id
                    state = row.get("state")
                    summary = row.get("summary")
                    detail = row.get("detail")

                    head = f"- **{title}**"
                    if state:
                        head += f"（{state}）"
                    lines.append(head)
                    if summary:
                        lines.append(f"  - 结构验证：{summary}")
                    if detail:
                        lines.append(f"  - 风险提示/对照：{detail}")

            note = p.get("note")
            if isinstance(note, str) and note.strip():
                lines.append("")
                lines.append(f"> 注：{note.strip()}")
            return lines

        if alias == "conditions.runtime":
            lines.append("### 即时验证条件（Runtime Conditions）")
            lines.append("")
            status = p.get("status")
            note = p.get("note")
            if status:
                lines.append(f"- 状态：{self._safe_str(status)}")
            if note:
                lines.append(f"- 说明：{self._safe_str(note)}")
            return lines

        if alias == "scenarios.forward":
            lines.append("### 情景说明（Scenarios · T+N）")
            lines.append("")
            # 保留人话提示为主
            scenario_note = p.get("scenario_note") or p.get("note")
            gate = p.get("gate")
            summary = p.get("summary")
            if gate:
                lines.append(f"- 当前 Gate：{self._safe_str(gate)}")
            if summary:
                lines.append(f"- 当前 Summary：{self._safe_str(summary)}")
            if scenario_note:
                lines.append("")
                lines.append(self._safe_str(scenario_note))
            return lines

        if alias == "dev.evidence":
            lines.append("### 审计证据链（Dev / Evidence）")
            lines.append("")
            note = p.get("note")
            if note:
                lines.append(self._safe_str(note))
            else:
                lines.append("（审计证据链区块：用于制度与接线验证）")
            return lines

        # fallback：对未知 block，尽量给一句话
        lines.append("（该区块暂无人话摘要规则）")
        return lines

    # -------------------------
    # Tech track (no narration)
    # -------------------------

    def _render_tech(self, *, alias: str, payload: Any) -> List[str]:
        p = payload if isinstance(payload, dict) else {}
        lines: List[str] = []
        lines.append("### 技术轨（关键字段）")
        lines.append("")

        if alias == "structure.facts":
            structure = p.get("structure")
            if not isinstance(structure, dict) or not structure:
                lines.append("- structure: null_or_empty")
                return lines

            # 技术轨：只列 state + evidence(精简)
            for k, v in structure.items():
                if isinstance(v, dict):
                    state = v.get("state")
                    evidence = v.get("evidence")
                    # evidence 只展示关键小字段（避免爆炸）
                    ev_compact = self._compact_obj(evidence, max_depth=2, max_items=8)
                    lines.append(f"- {k}:")
                    lines.append(f"  - state: {self._safe_str(state)}")
                    if evidence is not None:
                        lines.append(f"  - evidence: {self._json(ev_compact)}")
                else:
                    lines.append(f"- {k}: {self._safe_str(v)}")

            return lines

        if alias == "context.overnight":
            overnight = p.get("overnight")
            ov_compact = self._compact_obj(overnight, max_depth=2, max_items=12)
            lines.append(f"- overnight: {self._json(ov_compact)}")
            return lines

        if alias == "watchlist.sectors":
            watchlist = p.get("watchlist")
            if not isinstance(watchlist, dict) or not watchlist:
                lines.append("- watchlist: null_or_empty")
                return lines

            # 技术轨：每个对象只留 state + summary（精简）
            for obj_id, row in watchlist.items():
                if isinstance(row, dict):
                    lines.append(f"- {obj_id}:")
                    lines.append(f"  - state: {self._safe_str(row.get('state'))}")
                    lines.append(f"  - summary: {self._safe_str(row.get('summary'))}")
                else:
                    lines.append(f"- {obj_id}: {self._safe_str(row)}")
            return lines

        if alias == "dev.evidence":
            # 技术轨：尽量原样 json（但仍做紧凑化）
            ev_compact = self._compact_obj(p, max_depth=3, max_items=30)
            lines.append("```json")
            lines.append(self._json(ev_compact, pretty=True))
            lines.append("```")
            return lines

        # 默认：把 payload 做 compact json
        compact = self._compact_obj(p, max_depth=2, max_items=20)
        lines.append("```json")
        lines.append(self._json(compact, pretty=True))
        lines.append("```")
        return lines

    # =========================================================
    # Utilities
    # =========================================================

    def _safe_str(self, x: Any) -> str:
        if x is None:
            return "null"
        try:
            s = str(x)
            return s
        except Exception:
            return "<unprintable>"

    def _json(self, obj: Any, pretty: bool = False) -> str:
        try:
            if pretty:
                return json.dumps(obj, ensure_ascii=False, indent=2, default=self._json_default)
            return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=self._json_default)
        except Exception:
            return self._safe_str(obj)

    def _json_default(self, o: Any) -> Any:
        # dataclass
        if is_dataclass(o):
            return asdict(o)
        # FactorResult 或类似对象：抓常见字段，避免 repr
        for fields in (("name", "score", "level", "details"), ("state", "meaning", "evidence")):
            ok = True
            out = {}
            for f in fields:
                if not hasattr(o, f):
                    ok = False
                    break
                out[f] = getattr(o, f)
            if ok:
                return out

        # numpy scalar
        try:
            import numpy as np  # type: ignore
            if isinstance(o, (np.integer, np.floating)):
                return float(o)
        except Exception:
            pass

        # fallback
        return self._safe_str(o)

    def _compact_obj(self, obj: Any, *, max_depth: int, max_items: int) -> Any:
        """
        目的：技术轨输出“关键字段”，防止 raw_data 爆炸。
        """
        def compact(x: Any, depth: int) -> Any:
            if depth <= 0:
                # 到深度就截断
                if isinstance(x, (dict, list)):
                    return "<truncated>"
                return self._json_default(x)

            # dict
            if isinstance(x, dict):
                out: Dict[str, Any] = {}
                for i, k in enumerate(list(x.keys())[:max_items]):
                    out[str(k)] = compact(x.get(k), depth - 1)
                if len(x) > max_items:
                    out["_truncated"] = f"{len(x) - max_items} more keys"
                return out

            # list/tuple
            if isinstance(x, (list, tuple)):
                out_list = [compact(v, depth - 1) for v in list(x)[:max_items]]
                if len(x) > max_items:
                    out_list.append(f"<truncated {len(x) - max_items} more items>")
                return out_list

            # string maybe huge
            if isinstance(x, str):
                s = x.strip()
                if len(s) > 260:
                    return s[:260] + "...<truncated>"
                return s

            # dataclass / objects
            return self._json_default(x)

        return compact(obj, max_depth)
