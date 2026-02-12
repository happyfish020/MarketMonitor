# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Optional

from core.reporters.report_types import ReportBlock
from core.reporters.report_context import ReportContext
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase


def _as_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        return str(v)
    except Exception:
        return ""


def _get_in(d: Any, path: List[str], default: Any = None) -> Any:
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    return cur if cur is not None else default


def _fmt_pct_from_ratio(v: Optional[float], *, decimals: int = 2) -> Optional[str]:
    if v is None:
        return None
    try:
        f = float(v)
    except Exception:
        return None
    # v may be ratio (0..1) or already percent (1..100)
    if f <= 1.0:
        f = f * 100.0
    return f"{round(f, decimals)}%"


def _fmt_pct_auto(v: Optional[float], *, decimals: int = 2, ratio_threshold: float = 0.02) -> Optional[str]:
    """Format a percentage with a safer heuristic.

    Used for fields that are *supposed to be percent* (0..100) but may occasionally
    appear as a ratio (0..1) when upstream mapping changes.
    - If v is extremely small (<= ratio_threshold), treat as ratio and scale.
    - Otherwise, treat as already-percent.
    """
    if v is None:
        return None
    try:
        f = float(v)
    except Exception:
        return None
    if f < 0:
        return None
    if f <= ratio_threshold:
        f = f * 100.0
    return f"{round(f, decimals)}%"


class AttackPermitBlock(ReportBlockRendererBase):
    """UnifiedRisk V12 · AttackPermit Block

    Purpose:
    - Explicitly surface DOS / AttackPermit in report, even when Gate=CAUTION.
    - Audit-friendly: show evidence + constraints + warnings.
    """

    block_alias = "governance.attack_permit"
    title = "进攻许可（AttackPermit · DOS）"

    def render(self, context: ReportContext, doc_partial: Dict[str, Any]) -> ReportBlock:
        warnings: List[str] = []

        gov = context.slots.get("governance")
        ap = gov.get("attack_permit") if isinstance(gov, dict) else None

        if not isinstance(ap, dict):
            warnings.append("missing:governance.attack_permit")
            payload = "AttackPermit 未生成（请检查 ReportEngine 是否调用 AttackPermitBuilder 并写入 slots['governance']['attack_permit']）。"
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload=payload,
                warnings=warnings,
            )

        permit = _as_str(ap.get("permit") or "NO").upper()
        mode = _as_str(ap.get("mode") or "NONE").upper()
        label = _as_str(ap.get("label") or "")

        ev = ap.get("evidence") if isinstance(ap.get("evidence"), dict) else {}
        adv = ev.get("adv_ratio")
        top20 = ev.get("top20_ratio")
        ma50 = ev.get("pct_above_ma50")
        nl = ev.get("new_low_ratio_pct")

        adv_s = _fmt_pct_from_ratio(adv)
        top20_s = _fmt_pct_from_ratio(top20)
        ma50_s = _fmt_pct_from_ratio(ma50, decimals=2) if ma50 is not None else None
        # new_low_ratio_pct is defined as *percent* in AttackPermitBuilder; use safer formatting
        # to avoid 0.54 -> 54% style unit bugs.
        nl_s = _fmt_pct_auto(nl, decimals=2) if nl is not None else None

        allowed = ap.get("allowed") if isinstance(ap.get("allowed"), list) else []
        allowed_s = [str(x) for x in allowed]

        cons = ap.get("constraints") if isinstance(ap.get("constraints"), list) else []
        cons_s = [str(x) for x in cons if str(x).strip()]

        ap_warn = ap.get("warnings") if isinstance(ap.get("warnings"), list) else []
        warnings.extend([str(x) for x in ap_warn if str(x).strip()])

        lines: List[str] = []
        headline = f"AttackPermit：{(label or (permit + ' ' + mode)).strip()}"
        lines.append(headline)
        lines.append(f"permit={permit} · mode={mode}")

        lines.append("")
        lines.append("证据（Evidence）：")
        lines.append(f"- adv_ratio：{adv_s or 'missing'}")
        lines.append(f"- top20_ratio（成交集中度）：{top20_s or 'missing'}")
        lines.append(f"- %>MA50：{ma50_s or 'missing'}")
        lines.append(f"- 50D New Lows（%）：{nl_s or 'missing'}")

        lines.append("")
        lines.append("允许（Allowed）：")
        if allowed_s:
            lines.append("- " + " / ".join(allowed_s))
        else:
            lines.append("- (empty)")

        # Hard-coded forbidden list (policy) for audit readability.
        lines.append("")
        lines.append("禁止（Forbidden）：")
        lines.append("- 追涨/追高加仓（CHASE_ADD）")
        lines.append("- 杠杆/融资扩大敞口（LEVERAGE）")
        lines.append("- 逆势抄底扩大敞口（BOTTOM_FISH_ADD）")

        if cons_s:
            lines.append("")
            lines.append("执行边界（Limits / Constraints）：")
            for c in cons_s[:8]:
                lines.append(f"- {c}")
            if len(cons_s) > 8:
                lines.append(f"- ...（共 {len(cons_s)} 条，仅展示前 8 条）")

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload="\n".join(lines),
            warnings=warnings,
        )
