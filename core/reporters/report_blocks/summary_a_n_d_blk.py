# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from core.reporters.report_types import ReportBlock
from core.reporters.report_context import ReportContext
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase
from core.reporters.utils.state_render import actionhint_zh, gate_zh, drs_zh, execution_zh, stage_zh, safe_dict



# DRS fallback meaning (only used when payload meaning is missing).
# 设计：DRS meaning 的权威来源应来自 governance payload；这里仅做兜底，避免 GREEN/None 造成误读。
_DRS_MEANING_FALLBACK = {
    "GREEN": "未触发日度风险红旗（仍以 Gate / Execution 约束为准）。",
    "YELLOW": "日度风险抬升（偏防守，降低进攻倾向；仍以 Gate / Execution 约束为准）。",
    "ORANGE": "日度风险偏高（禁止加风险，优先防守；仍以 Gate / Execution 约束为准）。",
    "RED": "日度风险极高（执行去风险/防守；仍以 Gate / Execution 约束为准）。",
}

class SummaryANDBlock(ReportBlockRendererBase):
    """
    UnifiedRisk V12 · Summary (A / N / D) Block（语义一致性冻结版）

    Fixes (Frozen Engineering):
    - (B) Execution band must be a single value (D1/D2/D3/NA), never "A/D1".
    - (C) Summary must explicitly show Gate → Permission mapping when needed.
    - Add "体感" line when Execution indicates higher friction (D2/D3) to avoid optimistic misread.
    - Missing/invalid inputs MUST NOT crash report rendering; use warnings + placeholders.
    """

    block_alias = "summary"
    title = "简要总结（Summary · A / N / D）"

    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        warnings: List[str] = []

        # summary_code: never raise; fallback to N
        summary_code = doc_partial.get("summary")
        if summary_code not in ("A", "N", "D"):
            warnings.append("invalid:summary_code")
            summary_code = "N"

        # actionhint.reason: stable fallback
        actionhint = doc_partial.get("actionhint")
        reason = (
            actionhint.get("reason")
            if isinstance(actionhint, dict) and actionhint.get("reason")
            else "结构进入谨慎区间，制度上不支持主动扩大风险敞口。"
        )

        # Gate mapping (C fix)
        gate_pre = context.slots.get("gate_pre")
        gate_final = context.slots.get("gate_final")
        gate_for_perm = gate_final or gate_pre
        if not gate_for_perm:
            warnings.append("empty:gate")
        gate_perm = self._gate_permission_text(gate_for_perm) if gate_for_perm else None

        # Execution band (single value)
        execu = context.slots.get("execution_summary")
        exec_band = self._extract_execution_band(execu, warnings)

        lines: List[str] = []


        # Header (structured): avoid misreading Code as Gate
        lines.append(f"ActionHintCode: {summary_code} — {actionhint_zh(summary_code)}")
        ms = safe_dict(context.slots.get("market_state"))
        if ms:
            stg = ms.get("stage") or ms.get("stage_raw")
            sev = ms.get("severity")
            label = ms.get("label") or "MarketState"
            lines.append(f"{label}: {stage_zh(stg)} | 严重度={sev}")
        # Gate permission mapping (explicit)
        if gate_for_perm and gate_perm:
            lines.append(f"Gate: {gate_for_perm} — {gate_zh(gate_for_perm)} — {gate_perm}")
        elif gate_for_perm:
            lines.append(f"Gate: {gate_for_perm} — {gate_zh(gate_for_perm)}")
        else:
            lines.append("Gate: N/A")

        # AttackPermit (DOS) — explicit overlay indicator (read-only)
        gov = context.slots.get("governance")
        ap = gov.get("attack_permit") if isinstance(gov, dict) else None
        if isinstance(ap, dict) and str(ap.get("permit") or "").upper() == "YES":
            ap_label = ap.get("label") or "YES"
            lines.append(f"AttackPermit: {str(ap_label).strip()}（允许底仓小步；禁止追涨/杠杆/逆势抄底扩大敞口）")

        # Main meaning
        lines.append(reason)

        # Trend-in-force summary must not contradict structure facts.
        trend_line = self._trend_line(context)
        if trend_line:
            lines.append(trend_line)
        else:
            # conservative fallback (avoid optimistic wording)
            lines.append("趋势结构处于谨慎区间，制度不支持主动扩大风险敞口。")

        # "体感" line（证据驱动；避免仅凭 Execution 推断盘面事实）
        if exec_band in ("D2", "D3"):
            feeling = self._derive_execution_feeling(context)
            if feeling:
                lines.append(feeling)
            else:
                lines.append("体感：执行摩擦偏高，盘面更偏结构性分化/轮动，追价与频繁进攻性调仓胜率偏低。")

        # DRS（只读）
        drs = context.slots.get("drs")
        if isinstance(drs, dict):
            sig = drs.get("signal")
            m = drs.get("meaning")
            sig_s = sig.strip() if isinstance(sig, str) else ""
            m_s = m.strip() if isinstance(m, str) else ""
            # fallback: avoid empty meaning (e.g., GREEN/None)
            if sig_s and not m_s:
                m_s = _DRS_MEANING_FALLBACK.get(sig_s.upper(), "")
            if sig_s and m_s:
                lines.append(f"【DRS · 日度风险信号】{drs_zh(sig_s)} —— {m_s}")
            elif sig_s:
                lines.append(f"【DRS · 日度风险信号】{drs_zh(sig_s)}")
            elif m_s:
                lines.append(f"【DRS · 日度风险信号】{m_s}")
            else:
                warnings.append("invalid:drs_payload")
        elif drs is not None:
            warnings.append("invalid:drs_format")

        # Execution line
        if exec_band:
            lines.append(
                f"【Execution · 2–5D】{execution_zh(exec_band)} —— "
                "Execution 仅评估执行摩擦；在 Gate 约束下不构成放行或进攻依据。"
            )
        elif execu is not None:
            warnings.append("invalid:execution_summary_format")
        else:
            warnings.append("empty:execution_summary")

        # Gate block (keep explicit; do not overload Execution)
        if gate_pre or gate_final:
            lines.append("【制度权限（Gate）】")
            if gate_pre:
                lines.append(f"- 原始 Gate：{gate_pre}")
            else:
                warnings.append("empty:gate_pre")
                lines.append("- 原始 Gate：N/A")
            if gate_final:
                lines.append(f"- 执行后 Gate：{gate_final}")
            else:
                warnings.append("empty:gate_final")
                lines.append("- 执行后 Gate：N/A")

        payload = "\n".join(lines)

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload=payload,
            warnings=warnings,
        )

    # -----------------------------
    # Helpers
    # -----------------------------
    @staticmethod
    def _get_field(obj: Any, key: str) -> Optional[Any]:
        if obj is None:
            return None
        if isinstance(obj, dict):
            return obj.get(key)
        return getattr(obj, key, None)

    def _trend_line(self, context: ReportContext) -> Optional[str]:
        """Return a single summary line for trend-in-force, without contradicting structure facts."""
        structure = context.slots.get("structure")
        if not isinstance(structure, dict):
            return None
        trend = structure.get("trend_in_force")
        if not isinstance(trend, dict):
            return None
        state = trend.get("state")
        if not isinstance(state, str) or not state.strip():
            return None

        s = state.strip().lower()
        if s in ("broken", "fail", "failed", "invalid"):
            return "趋势结构已破坏（trend_in_force=broken），制度上不支持主动扩大风险敞口。"
        if s in ("weakening", "weak", "fragile"):
            return "趋势结构走弱（trend_in_force=weakening），成功率下降，避免追涨与放大试错。"
        if s in ("in_force", "inforce", "strong", "healthy"):
            return "趋势结构仍在（trend_in_force=in_force），但需结合成功率/执行摩擦控制追价风险。"

        # Unknown state: do not guess
        return None

    def _extract_execution_band(self, execu: Any, warnings: List[str]) -> Optional[str]:
        """
        Extract a single execution band string.

        Priority:
        - execu.band
        - execu.code (fallback)
        """
        band = self._get_field(execu, "band")
        if not band:
            band = self._get_field(execu, "code")

        if band is None:
            return None

        if not isinstance(band, str):
            try:
                band = str(band)
            except Exception:
                warnings.append("invalid:execution_band_type")
                return None

        s = band.strip().upper()

        # Hard rule: never allow combined strings like "A/D1" or "A / D1"
        if "/" in s:
            warnings.append("invalid:execution_band_combined")
            s = s.split("/", 1)[0].strip().upper()

        # Normalize spaces
        s = s.replace(" ", "")

        # Canonical allowed bands
        allowed = {"D1", "D2", "D3", "NA"}
        if s in allowed:
            return s

        # Legacy code mapping (conservative)
        if s in ("A", "N"):
            warnings.append("fallback:execution_code_mapped")
            return "D1"
        if s in ("D",):
            warnings.append("fallback:execution_code_mapped")
            return "D2"

        warnings.append("invalid:execution_band_value")
        # return raw normalized so UX has something to show (frozen principle)
        return s

    def _gate_permission_text(self, gate: Any) -> str:
        if gate is None:
            return "权限未知"

        g = str(gate).strip().upper()

        mapping = {
            "ALLOW": "允许 ADD-RISK / REBALANCE（受 Execution 约束）",
            "A": "允许 ADD-RISK / REBALANCE（受 Execution 约束）",
            "NORMAL": "允许 HOLD；ADD-RISK 不鼓励/需满足附加条件",
            "N": "允许 HOLD；ADD-RISK 不鼓励/需满足附加条件",
            "CAUTION": "允许 HOLD / DEFENSE；禁止 ADD-RISK",
            "C": "允许 HOLD / DEFENSE；禁止 ADD-RISK",
            "PLANB": "以防守为主；禁止 ADD-RISK（仅允许结构性切换/降风险）",
            "FREEZE": "必须 DEFENSE / DE-RISK；禁止一切进攻",
            "D": "必须 DEFENSE / DE-RISK；禁止 ADD-RISK",
        }

        return mapping.get(g, "权限未知")


    # -----------------------------
    # Evidence-driven "feeling" (summary only)
    # -----------------------------
    def _derive_execution_feeling(self, context: ReportContext) -> Optional[str]:
        """
        Summary 内的“体感”必须证据驱动（best-effort）：
        - 仅当可从 etf_spot_sync / intraday_overlay / market_overview 提取到关键证据时才输出具体描述
        - 避免仅凭 Execution band 直接“推断盘面事实”
        """
        details = self._extract_etf_sync_details(context)
        interp = details.get("interpretation") if isinstance(details.get("interpretation"), dict) else {}

        adv_ratio = self._pick_number(
            details.get("adv_ratio"),
            self._pick_number(self._get_nested(context.slots.get("market_overview"), ("breadth", "adv_ratio")), None),
        )
        top20_ratio = self._pick_number(
            details.get("top20_ratio"),
            self._pick_number(self._get_nested(context.slots.get("market_overview"), ("amount", "top20_ratio")), None),
        )
        top20_proxy = self._pick_number(
            details.get("top20_amount_ratio"),
            self._pick_number(self._get_nested(context.slots.get("market_overview"), ("amount", "top20_amount_ratio")), None),
        )

        participation = str(interp.get("participation", "")).lower()
        crowding = str(interp.get("crowding", "")).lower()

        weak = (participation in ("weak", "low")) or (isinstance(adv_ratio, (int, float)) and adv_ratio <= 0.42)
        crowded = (
            (crowding in ("high", "very_high"))
            or (isinstance(top20_ratio, (int, float)) and top20_ratio >= 0.12)
            or (isinstance(top20_proxy, (int, float)) and top20_proxy >= 0.70)
        )

        # evidence sufficiency guard
        has_ev = (
            isinstance(adv_ratio, (int, float))
            or isinstance(top20_ratio, (int, float))
            or isinstance(top20_proxy, (int, float))
            or participation
            or crowding
        )
        if not has_ev:
            return None

        if weak and crowded:
            return (
                "体感：指数可能较稳，但涨少跌多；盘面更像调仓轮动/兑现，"
                "而非全面风险偏好抬升（因此不利于追价执行）。"
            )
        if weak:
            return "体感：扩散偏弱，赚钱效应不普遍；更像结构性轮动，追价胜率偏低。"
        if crowded:
            return "体感：拥挤度高/窄领涨，轮动快；追价与频繁进攻性调仓胜率偏低。"

        return None

    def _extract_etf_sync_details(self, context: ReportContext) -> Dict[str, Any]:
        """
        提取 etf_spot_sync / crowding_concentration 的 details（best-effort）。
        """
        v = context.slots.get("etf_spot_sync")
        d = self._unwrap_details(v)
        if d:
            return d

        overlay = context.slots.get("intraday_overlay") or context.slots.get("overlay") or context.slots.get("intraday")
        if isinstance(overlay, dict):
            for k in ("etf_spot_sync",  "crowding_concentration"):
                d = self._unwrap_details(overlay.get(k))
                if d:
                    return d
            d = self._unwrap_details(overlay)
            if d:
                return d

        obs = context.slots.get("observations")
        if isinstance(obs, dict):
            for k in ("etf_spot_sync", "crowding_concentration", "intraday_overlay", "overlay"):
                d = self._unwrap_details(obs.get(k))
                if d:
                    return d

        return {}

    @staticmethod
    def _unwrap_details(obj: Any) -> Dict[str, Any]:
        if not isinstance(obj, dict) or not obj:
            return {}
        if isinstance(obj.get("details"), dict):
            return obj.get("details")  # type: ignore[return-value]
        keys = ("adv_ratio", "top20_ratio", "top20_amount_ratio", "interpretation")
        if any(k in obj for k in keys):
            return obj
        return {}

    @staticmethod
    def _pick_number(*vals: Any) -> Optional[float]:
        for v in vals:
            if isinstance(v, (int, float)):
                return float(v)
        return None

    @staticmethod
    def _get_nested(obj: Any, path: Tuple[str, ...]) -> Optional[Any]:
        cur = obj
        for k in path:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(k)
        return cur
