# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 FULL
CN A-Share Report Pipeline (Decision Expression Layer)

本文件职责（冻结）：
- 仅负责“表达层”报告生成（Markdown 文本 + 结构化 report dict）
- 只读输入（snapshot / policy_result / action_hint / context）
- 不做任何制度计算（不做 Gate / Regime / Factor 判断）
- 不反向影响 Gate / ActionHint
- Watchlist 展示遵循已冻结的“双轨人话 + 技术审计可选”规范

设计原则：
- Interface First（不假设上游对象具体实现）
- Defensive Extraction（通过 getattr / dict.get 读取可用字段）
- 可替换、可单测（纯函数式生成文本）
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List


# =========================================================
# Public Report Pipeline
# =========================================================

@dataclass(frozen=True, slots=True)
class DailyReport:
    """
    报告输出结构（冻结）：
    - text: Markdown 报告正文
    - meta: 仅用于上游归档与审计（不参与制度计算）
    """
    text: str
    meta: Dict[str, Any]
 

class AshareReportPipeline:
    """
    A股报告管线（表达层）

    调用契约（冻结）：
    callable(snapshot, policy_result, action_hint, trade_date, market, context) -> Dict[str, Any] | DailyReport
    """

    def __call__(
        self,
        *,
        snapshot: Any,
        policy_result: Any,
        action_hint: Any,
        trade_date: str,
        market: str = "CN_A",
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        report = self.build(
            snapshot=snapshot,
            policy_result=policy_result,
            action_hint=action_hint,
            trade_date=trade_date,
            market=market,
            context=context,
        )
        # 统一向外输出 dict，便于序列化与归档（表达层）
        return {"text": report.text, "meta": report.meta}

    def build(
        self,
        *,
        snapshot: Any,
        policy_result: Any,
        action_hint: Any,
        trade_date: str,
        market: str = "CN_A",
        context: Optional[Dict[str, Any]] = None,
    ) -> DailyReport:
        ctx = context or {}
        report_kind = _pick_str(ctx, "kind") or _pick_str(ctx, "report_type") or "PRE_OPEN"
        dev_mode = _pick_bool(ctx, "dev_mode", default=False)

        # 只读抽取：Gate / Slots / Watchlist
        gate_value, gate_source = _extract_gate(policy_result, snapshot)
        slots = _extract_slots(policy_result)
        watchlist = _extract_watchlist(slots)

        # 只读抽取：ActionHint（表达层结果）
        ah_allowed, ah_forbidden, ah_position, ah_explain, ah_risk_notes = _extract_actionhint(action_hint)

        # 生成 Markdown
        lines: List[str] = []
        lines.extend(_render_header(trade_date=trade_date, report_kind=report_kind))
        lines.append("")

        # 系统裁决（ActionHint）
        lines.extend(_render_actionhint_block(
            gate=gate_value,
            allowed=ah_allowed,
            forbidden=ah_forbidden,
            position_guidance=ah_position,
            explanation=ah_explain,
            risk_notes=ah_risk_notes,
        ))
        lines.append("")

        # Watchlist（冻结展示规范）
        lines.extend(_render_watchlist_block(watchlist=watchlist))
        lines.append("")

        # Dev / Evidence（仅 dev_mode 展示，且不影响人话）
        if dev_mode:
            lines.extend(_render_dev_evidence_block(
                gate=gate_value,
                gate_source=gate_source,
                watchlist=watchlist,
                market=market,
            ))
            lines.append("")

        text = "\n".join(lines).strip() + "\n"

        meta: Dict[str, Any] = {
            "trade_date": trade_date,
            "report_kind": report_kind,
            "market": market,
            "dev_mode": dev_mode,
            "gate": gate_value,
        }
        return DailyReport(text=text, meta=meta)


# =========================================================
# Renderers (Markdown)
# =========================================================

def _render_header(*, trade_date: str, report_kind: str) -> List[str]:
    return [
        "# A股制度风险报告（Pre-open）",
        "",
        f"- 交易日：**{trade_date}**",
        f"- 报告类型：**{report_kind}**",
    ]


def _render_actionhint_block(
    *,
    gate: str,
    allowed: List[str],
    forbidden: List[str],
    position_guidance: Dict[str, Any],
    explanation: str,
    risk_notes: List[str],
) -> List[str]:
    # 人话轨：不引入新判断，只表达输入结果
    lines: List[str] = []
    lines.append("## 系统裁决（ActionHint）")
    lines.append("")
    lines.append(f"**Gate：{gate}**")
    lines.append("")

    # 允许 / 禁止（表达层）
    if allowed:
        lines.append("**允许：**")
        for x in allowed:
            lines.append(f"- {x}")
        lines.append("")
    if forbidden:
        lines.append("**禁止：**")
        for x in forbidden:
            lines.append(f"- {x}")
        lines.append("")

    # 仓位边界（表达层只读）
    if isinstance(position_guidance, dict) and position_guidance:
        lines.append("**执行边界（仓位/约束）：**")
        for k in ("max_exposure", "position_note"):
            if k in position_guidance and position_guidance.get(k) is not None:
                lines.append(f"- {k}: {position_guidance.get(k)}")
        lines.append("")

    # 解释字段（表达层只读）
    if explanation:
        lines.append("**制度解释：**")
        lines.append(f"- {explanation}")
        lines.append("")

    # 风险提示（表达层只读）
    if risk_notes:
        lines.append("**风险提示：**")
        for x in risk_notes:
            lines.append(f"- {x}")
        lines.append("")

    # 固定边界文案（冻结）
    lines.append("**重要说明：**")
    lines.append("- 本段为“制度结果 → 行为表达”的只读翻译，不构成操作建议。")
    lines.append("- 若与主观判断冲突，必须以系统裁决为准。")

    return lines


def _render_watchlist_block(*, watchlist: Dict[str, Dict[str, str]]) -> List[str]:
    lines: List[str] = []
    lines.append("## 观察对象（Watchlist）")
    lines.append("")
    # 固定免责声明（冻结）
    lines.append("> **重要说明（冻结）**")
    lines.append("> Watchlist 模块仅用于结构验证与风险监控：")
    lines.append("> - NOT_ALLOWED ≠ 市场看空 ≠ 禁止持仓")
    lines.append("> - OBSERVE ≠ 允许参与 ≠ 交易信号")
    lines.append("> ")
    lines.append("> 是否允许操作，仅由 Gate / ActionHint 决定。")
    lines.append("")

    if not watchlist:
        lines.append("- 无观察对象输出（watchlist 为空或未接入）。")
        return lines

    # 单对象展示：严格只读 summary/detail，不扩写为新判断
    for obj_id, obj in watchlist.items():
        title = (obj or {}).get("title") or obj_id
        state = (obj or {}).get("state") or "OBSERVE"
        summary = (obj or {}).get("summary") or ""
        detail = (obj or {}).get("detail") or ""

        badge = "🔴" if state == "NOT_ALLOWED" else "🟡"
        lines.append(f"### 【{title}】")
        lines.append("")
        lines.append(f"**观察状态：** {badge} **{state}**")
        lines.append("")
        if summary:
            lines.append("**一句话结论：**")
            lines.append(f"{summary}")
            lines.append("")

        # 结构验证结果（展示框架冻结；内容仅引用 detail，不新增判断）
        lines.append("#### 1️⃣ 结构验证结果")
        if detail:
            lines.append(detail)
        else:
            lines.append("（该观察对象未提供 detail，当前仅作为占位观察。）")
        lines.append("")

        # 风险提示（冻结框架：不新增具体风险点）
        lines.append("#### 2️⃣ 风险提示")
        if state == "NOT_ALLOWED":
            lines.append("- 当前不具备参与前提，仅用于风险监控与结构跟踪。")
        else:
            lines.append("- 处于观察阶段，不等同于允许参与。")
        lines.append("")

        # 系统边界结论（固定句式冻结）
        lines.append("#### 3️⃣ 系统结论（固定边界）")
        lines.append("- ✅ 允许观察")
        if state == "NOT_ALLOWED":
            lines.append("- ❌ 不允许参与")
        else:
            lines.append("- ❌ 尚不允许参与")
        lines.append("- ❌ 不支持主动加仓或进攻性操作")
        lines.append("")

    # 与系统裁决关系说明（固定文案冻结）
    lines.append("### 与系统裁决的关系说明（冻结）")
    lines.append("")
    lines.append("> Watchlist 仅用于回答：某一主线/风格是否具备“参与前提”。")
    lines.append("> 系统是否允许操作，仅由 Gate / ActionHint 决定。")

    return lines


def _render_dev_evidence_block(
    *,
    gate: str,
    gate_source: str,
    watchlist: Dict[str, Dict[str, str]],
    market: str,
) -> List[str]:
    # 技术轨：精简、可复核、不产生新判断
    lines: List[str] = []
    lines.append("## 审计证据链（Dev / Evidence）")
    lines.append("")
    lines.append("```yaml")
    lines.append("decision_verification:")
    lines.append(f"  market: {market}")
    lines.append(f"  gate: {gate}")
    lines.append(f"  gate_source: {gate_source}")
    lines.append("watchlist_status:")
    if not watchlist:
        lines.append("  empty: true")
    else:
        lines.append("  empty: false")
        lines.append("  items:")
        for obj_id, obj in watchlist.items():
            title = (obj or {}).get("title") or obj_id
            state = (obj or {}).get("state") or "OBSERVE"
            lines.append(f"    - id: {obj_id}")
            lines.append(f"      title: {title}")
            lines.append(f"      state: {state}")
            lines.append("      decision_binding: false")
    lines.append("```")
    return lines


# =========================================================
# Extractors (defensive, no assumptions)
# =========================================================

def _extract_gate(policy_result: Any, snapshot: Any) -> Tuple[str, str]:
    """
    只读抽取 Gate：
    - 优先 policy_result.gate_decision.gate / level
    - fallback snapshot["gate"]["level"] / ["gate"]
    """
    # 1) policy_result.gate_decision.*
    gd = getattr(policy_result, "gate_decision", None)
    if gd is not None:
        g = getattr(gd, "gate", None)
        if isinstance(g, str) and g.strip():
            return g.strip(), "policy_result.gate_decision.gate"
        lv = getattr(gd, "level", None)
        if isinstance(lv, str) and lv.strip():
            return lv.strip(), "policy_result.gate_decision.level"

    # 2) snapshot dict
    if isinstance(snapshot, dict):
        gblk = snapshot.get("gate")
        if isinstance(gblk, dict):
            for key in ("gate", "level"):
                v = gblk.get(key)
                if isinstance(v, str) and v.strip():
                    return v.strip(), f"snapshot.gate.{key}"

    return "UNKNOWN", "fallback.UNKNOWN"


def _extract_slots(policy_result: Any) -> Dict[str, Any]:
    """
    从 policy_result 中提取“可读 slot 容器”（只读）：
    - 优先 slots / factors_bound / bound / policy_slots
    - 若不存在则返回空 dict
    """
    for attr in ("slots", "factors_bound", "bound", "policy_slots"):
        v = getattr(policy_result, attr, None)
        if isinstance(v, dict):
            return v
    # 兼容：policy_result 本身就是 dict
    if isinstance(policy_result, dict):
        return policy_result
    return {}


def _extract_watchlist(slots: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """
    Watchlist 槽位（冻结接口）：
    - slots["watchlist"] 为 dict(object_id -> {title,state,summary,detail})
    - 若缺失/不合法则返回空
    """
    wl = slots.get("watchlist")
    if isinstance(wl, dict):
        # 仅保留符合 dict 的 item（防污染）
        out: Dict[str, Dict[str, str]] = {}
        for k, v in wl.items():
            if not isinstance(k, str) or not k.strip():
                continue
            if not isinstance(v, dict):
                continue
            out[k] = {
                "title": str(v.get("title") or k),
                "state": str(v.get("state") or "OBSERVE"),
                "summary": str(v.get("summary") or ""),
                "detail": str(v.get("detail") or ""),
            }
        return out
    return {}


def _extract_actionhint(action_hint: Any) -> Tuple[List[str], List[str], Dict[str, Any], str, List[str]]:
    """
    ActionHintResult（冻结输出）：
    - allowed_actions: list[str]
    - forbidden_actions: list[str]
    - position_guidance: dict
    - explanation: str
    - risk_notes: list[str]
    """
    if not isinstance(action_hint, dict):
        return [], [], {}, "", []
    allowed = action_hint.get("allowed_actions") or []
    forbidden = action_hint.get("forbidden_actions") or []
    pos = action_hint.get("position_guidance") or {}
    explain = action_hint.get("explanation") or ""
    risk_notes = action_hint.get("risk_notes") or []

    return (
        [str(x) for x in allowed if x is not None],
        [str(x) for x in forbidden if x is not None],
        pos if isinstance(pos, dict) else {},
        str(explain) if explain is not None else "",
        [str(x) for x in risk_notes if x is not None],
    )


def _pick_str(d: Dict[str, Any], key: str) -> str:
    v = d.get(key)
    return v.strip() if isinstance(v, str) else ""


def _pick_bool(d: Dict[str, Any], key: str, default: bool = False) -> bool:
    v = d.get(key)
    if isinstance(v, bool):
        return v
    return default
