# -*- coding: utf-8 -*-
"""UnifiedRisk V12 · Market Regime Repair Path Block (Human Layer) · v1.0

Read-only:
- Provide a "repair checklist" to move from current risk stage to next safer stage.
- Does NOT change Gate/DRS/Execution.
- Never throws; shows explicit warnings.

Inputs:
- slots["structure"] (trend/amount/crowding/failure_rate)
- slots["drs"] (signal)
- (optional) slots["attack_window"] for multi-day counters if present
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from core.reporters.report_types import ReportBlock
from core.reporters.report_context import ReportContext
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase


def _as_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _dig(d: Any, *path: str) -> Any:
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _detect_stage(trend: str, drs_sig: Optional[str], adv_ratio: Optional[float], amount_ratio: Optional[float]) -> str:
    if not trend or drs_sig is None:
        return "S_UNKNOWN"
    t = trend.strip().lower()
    s = (drs_sig or "").strip().upper()
    adv = adv_ratio if adv_ratio is not None else 0.0
    amt = amount_ratio if amount_ratio is not None else 0.0
    if t == "intact" and s != "RED" and adv >= 0.55 and amt >= 0.9:
        return "S1_ATTACK"
    if t == "intact" and s == "YELLOW":
        return "S2_RECOVERY"
    if t == "mixed" and amt < 0.9:
        return "S3_RANGE"
    if t == "broken" and s != "RED":
        return "S4_DEFENSE"
    if t == "broken" and s == "RED":
        return "S5_DERISK"
    return "S_UNKNOWN"


class MarketRegimeRepairPathBlock(ReportBlockRendererBase):
    block_alias = "market.regime_repair_path"
    title = "修复路径提示（Repair Path · Human Layer）"

    def render(self, context: ReportContext, doc_partial: Dict[str, Any]) -> ReportBlock:
        warnings: List[str] = []
        lines: List[str] = []

        structure = context.slots.get("structure")
        if not isinstance(structure, dict):
            structure = {}
            warnings.append("missing:structure")

        trend = _dig(structure, "trend_in_force", "state")
        trend_s = str(trend) if isinstance(trend, str) else ""

        drs_sig = None
        drs = context.slots.get("drs")
        if isinstance(drs, dict):
            sig = drs.get("signal")
            if isinstance(sig, str) and sig.strip():
                drs_sig = sig.strip().upper()
        if drs_sig is None:
            warnings.append("missing:drs")

        # amount_ratio
        amount_ratio = None
        amt_ev = _dig(structure, "amount", "evidence")
        if isinstance(amt_ev, dict):
            amount_ratio = _as_float(amt_ev.get("amount_ratio"))
        if amount_ratio is None:
            amount_ratio = _as_float(_dig(structure, "amount", "amount_ratio"))

        # adv_ratio
        adv_ratio = None
        cc_ev = _dig(structure, "crowding_concentration", "evidence")
        if isinstance(cc_ev, dict):
            adv_ratio = _as_float(cc_ev.get("adv_ratio"))
        if adv_ratio is None:
            mo = context.slots.get("market_overview")
            if isinstance(mo, dict):
                adv_ratio = _as_float(mo.get("adv_ratio"))

        # failure rate
        fr = _dig(structure, "failure_rate", "state")
        fr_s = str(fr) if isinstance(fr, str) else ""

        stage = _detect_stage(trend_s, drs_sig, adv_ratio, amount_ratio)

        lines.append("### 从当前状态走向“更安全”的修复路径")
        lines.append("")

        # Define repair targets (simple, frozen v1)
        # S5 -> S4: DRS must exit RED OR failure rate improves (soft), plus trend stop deteriorating.
        # S4/S5 -> S3/S2: need breadth+amount confirmation; we use adv_ratio/amount_ratio as proxies.
        def fmt(v, nd=2):
            if v is None:
                return "NA"
            try:
                return f"{v:.{nd}f}"
            except Exception:
                return str(v)

        # Current snapshot
        lines.append(f"当前读数：trend={trend_s or 'NA'} · drs={drs_sig or 'NA'} · amount_ratio={fmt(amount_ratio)} · adv_ratio={fmt(adv_ratio)} · failure_rate={fr_s or 'NA'}")
        lines.append("")

        if stage == "S5_DERISK":
            lines.append("**你现在在：去风险期（S5）**")
            lines.append("先目标不是进攻，而是把“红灯风险”从系统里退出来。")
            lines.append("")
            lines.append("**S5 → S4（去风险 → 防守）需要看到：**")
            lines.append("- DRS 从 RED 退出（变为 YELLOW/GREEN），或 failure_rate 明显改善")
            lines.append("- 成交缩量不再继续恶化（amount_ratio 由低位回升）")
            lines.append("")
            lines.append("**S4 → S3/S2（防守 → 震荡/修复）的确认门槛（简化版）：**")
            lines.append("- adv_ratio 连续2天 ≥ 0.55（参与度回到“多数股票能涨”）")
            lines.append("- amount_ratio ≥ 0.90（成交恢复到 MA20 的 9成以上）")
            lines.append("- trend_in_force 不再是 broken（至少回到 mixed / intact）")
        elif stage == "S4_DEFENSE":
            lines.append("**你现在在：防守期（S4）**")
            lines.append("目标是等“参与度 + 量能”的确认，而不是预判突破。")
            lines.append("")
            lines.append("**S4 → S3/S2（防守 → 震荡/修复）需要看到：**")
            lines.append("- adv_ratio 连续2天 ≥ 0.55")
            lines.append("- amount_ratio ≥ 0.90")
            lines.append("- failure_rate 下降（elevated → normal）")
        else:
            lines.append("当前阶段不是 S4/S5，修复路径提示会更偏“确认进攻条件”。")
            lines.append("（v1 只对防守/去风险阶段给出强约束提示。）")

        # Optional: show multi-day counters if attack_window is available
        aw = context.slots.get("attack_window")
        if isinstance(aw, dict):
            evidence = aw.get("evidence") if isinstance(aw.get("evidence"), dict) else None
            if evidence:
                passed = []
                for k in ("adv_ratio", "pct_above_ma20", "amount_ma20_ratio"):
                    if k in evidence:
                        passed.append(f"{k}={evidence.get(k)}")
                if passed:
                    lines.append("")
                    lines.append("附：AttackWindow evidence（仅供对照）： " + ", ".join(passed))

        #note = "只读解释层：不参与 Gate/DRS/Execution 计算。"
        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload={"content": lines, 
                    #"note": note
                    },
            warnings=warnings,
        )
