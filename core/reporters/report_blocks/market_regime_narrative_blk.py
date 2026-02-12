# -*- coding: utf-8 -*-
"""UnifiedRisk V12 · Market Regime Narrative Block (Human Layer) · v1.1

Read-only explanation layer:
- Translate structure + governance into human-friendly narrative
- DOES NOT change Gate/DRS/Execution
- Never throws; explicit warnings instead

Placement:
- Insert before Summary in config/report_blocks.yaml
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
    # Unknown if critical signals missing; do NOT downgrade to S4 by default.
    if not trend:
        return "S_UNKNOWN"
    if drs_sig is None:
        return "S_UNKNOWN"

    drs_sig = (drs_sig or "").strip().upper()
    trend = (trend or "").strip().lower()

    adv = adv_ratio if adv_ratio is not None else 0.0
    amt = amount_ratio if amount_ratio is not None else 0.0

    if trend == "intact" and drs_sig != "RED" and adv >= 0.55 and amt >= 0.9:
        return "S1_ATTACK"
    if trend == "intact" and drs_sig == "YELLOW":
        return "S2_RECOVERY"
    if trend == "mixed" and amt < 0.9:
        return "S3_RANGE"
    if trend == "broken" and drs_sig != "RED":
        return "S4_DEFENSE"
    if trend == "broken" and drs_sig == "RED":
        return "S5_DERISK"
    return "S_UNKNOWN"


class MarketRegimeNarrativeBlock(ReportBlockRendererBase):
    block_alias = "market.regime_narrative"
    title = "市场阶段判断（Human Layer）"

    def render(self, context: ReportContext, doc_partial: Dict[str, Any]) -> ReportBlock:
        warnings: List[str] = []

        structure = context.slots.get("structure")
        if not isinstance(structure, dict):
            structure = {}
            warnings.append("missing:structure")

        # Trend
        trend = _dig(structure, "trend_in_force", "state")
        trend_s = str(trend) if isinstance(trend, str) else ""

        # DRS (authoritative from slots["drs"] used by Summary block)
        drs = context.slots.get("drs")
        drs_sig = None
        if isinstance(drs, dict):
            sig = drs.get("signal")
            if isinstance(sig, str) and sig.strip():
                drs_sig = sig.strip().upper()
        if drs_sig is None:
            warnings.append("missing:drs")

        # Amount ratio
        amount_ratio = None
        amt_ev = _dig(structure, "amount", "evidence")
        if isinstance(amt_ev, dict):
            amount_ratio = _as_float(amt_ev.get("amount_ratio"))
        if amount_ratio is None:
            # fallback: sometimes stored directly
            amount_ratio = _as_float(_dig(structure, "amount", "amount_ratio"))

        # adv_ratio (prefer crowding_concentration evidence, fallback to market_overview)
        adv_ratio = None
        cc_ev = _dig(structure, "crowding_concentration", "evidence")
        if isinstance(cc_ev, dict):
            adv_ratio = _as_float(cc_ev.get("adv_ratio"))
        if adv_ratio is None:
            mo = context.slots.get("market_overview")
            if isinstance(mo, dict):
                adv_ratio = _as_float(mo.get("adv_ratio"))

        # failure rate label
        fr_state = _dig(structure, "failure_rate", "state")
        fr_s = str(fr_state) if isinstance(fr_state, str) else ""

        # Prefer persisted stage (single source of truth), injected by RegimeHistoryService.inject(...)
        # This avoids report-layer recomputation for stage.
        persisted_stage_raw = context.slots.get("regime_current_stage_raw")
        if isinstance(persisted_stage_raw, str) and persisted_stage_raw.strip().upper() in ("S1", "S2", "S3", "S4", "S5"):
            stage = persisted_stage_raw.strip().upper()
        else:
            stage = _detect_stage(trend_s, drs_sig, adv_ratio, amount_ratio)

        stage_name = {
            "S1": "进攻期（S1）",
            "S2": "修复期（S2）",
            "S3": "震荡期（S3）",
            "S4": "防守期（S4）",
            "S5": "去风险期（S5）",
            "S1_ATTACK": "进攻期（S1）",
            "S2_RECOVERY": "修复期（S2）",
            "S3_RANGE": "震荡期（S3）",
            "S4_DEFENSE": "防守期（S4）",
            "S5_DERISK": "去风险期（S5）",
            "S_UNKNOWN": "结构不明期（UNKNOWN）",
        }.get(stage, "结构不明期（UNKNOWN）")

        # ---- Narrative (human-friendly; minimal jargon) ----
        content: List[str] = []
        content.append(f"### 当前阶段：{stage_name}")
        content.append("")

        if stage in ("S5", "S5_DERISK"):
            content += [
                "趋势已经被打断，失败率上升，同时成交明显缩量。",
                "这不是恐慌性崩盘，但也不是修复起点。",
                "",
                "**周期位置**：更像进攻结束后的清理/去风险阶段，而不是“杀完马上进入主升”。",
                "",
                "**今天怎么做**：控制回撤优先。",
                "- 可以：HOLD；反弹走强时小幅 TRIM/再平衡（不追价）",
                "- 不可以：任何加仓/扩敞口；追涨；逆势抄底加仓",
            ]
        elif stage in ("S4", "S4_DEFENSE"):
            content += [
                "趋势偏弱或刚被破坏，市场进入防守状态。",
                "此时反弹更像“交易性反弹”，不适合扩大风险。",
                "",
                "- 可以：HOLD；反弹中小幅降档",
                "- 不可以：加仓、追高、用情绪交易",
            ]
        elif stage in ("S3", "S3_RANGE"):
            content += [
                "市场处在震荡区间，方向不明确。",
                "重点是等结构确认，不做预判式进攻。",
            ]
        elif stage in ("S2", "S2_RECOVERY"):
            content += [
                "结构在修复，但尚未进入全面进攻期。",
                "需要等待参与度与量能进一步确认。",
            ]
        elif stage in ("S1", "S1_ATTACK"):
            content += [
                "趋势完好，参与度与量能配合，属于更利于进攻的环境。",
                "仍需遵守个股/板块自身的结构条件与风控边界。",
            ]
        else:
            content += [
                "关键结构信息不足（或字段缺失），暂不判断市场阶段。",
                "建议先以 Gate/DRS 的制度边界为准，避免主观加戏。",
            ]

        # Small evidence footer (still readable)
        evidence_bits = []
        if trend_s:
            evidence_bits.append(f"trend={trend_s}")
        if drs_sig:
            evidence_bits.append(f"drs={drs_sig}")
        if amount_ratio is not None:
            evidence_bits.append(f"amount_ratio={amount_ratio:.2f}")
        if adv_ratio is not None:
            evidence_bits.append(f"adv_ratio={adv_ratio:.2f}")
        if fr_s:
            evidence_bits.append(f"failure_rate={fr_s}")

        note = "只读解释层：不参与 Gate/DRS/Execution 计算。"
        if evidence_bits:
            note += " | evidence: " + ", ".join(evidence_bits)

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload={"content": content, "note": note},
            warnings=warnings,
        )
