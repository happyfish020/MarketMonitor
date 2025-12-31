# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase

LOG = logging.getLogger("ReportBlock.StructureFacts")


class StructureFactsBlock(ReportBlockRendererBase):
    """
    UnifiedRisk V12 · Structure Facts Block（语义一致性冻结版 v3）

    v3 Fix:
    - 对 data_status != OK 的因子：不再展示为 neutral/healthy 等“看似正常”的状态，
      而是降级展示为 DATA_NOT_CONNECTED / UNKNOWN，并明确“该项解释力不足/不参与解读”。
    - 对 north_nps：若缺少趋势/窗口字段（仅单日读数），强制语义降级为 UNKNOWN，避免 neutral 误导。

    Frozen Engineering 原则：
    - 只读 slots["structure"]，slot 缺失≠错误 → warnings + 占位
    - 永不抛异常、不返回 None
    """

    block_alias = "structure.facts"
    title = "结构事实（技术轨）"

    # 统一语义（Single Source of Truth）
    # - 结构 slot 只需要提供 state / data_status / evidence
    # - 文案解释统一在本 Block 内维护，避免 mapper/builder 重复维护导致口径漂移
    _MEANING_TABLE = {
        "breadth": {
            "healthy": "市场广度未见系统性破坏，但扩散程度有限，需结合其他结构指标判断。",
            "not_broken": "市场广度偏弱但尚未出现趋势性破坏，需关注是否继续恶化。",
            "neutral": "市场广度处于中性状态，需结合成交/成功率判断。",
            "UNKNOWN": "市场广度语义不可用（数据不足/未计算）。",
        },
        "turnover": {
            "expanding": "成交放大，但更可能反映分歧或调仓轮动，而非新增进攻性资金。",
            "contracting": "成交缩量，参与度下降，风险偏好偏弱。",
            "neutral": "成交量处于中性水平，需结合广度/成功率判断。",
            "UNKNOWN": "成交量语义不可用（数据不足/未计算）。",
        },
        "index_tech": {
            "strong": "指数技术面偏强（趋势/均线/动量占优），但仅作结构解释，不构成进攻或调仓依据。",
            "weak": "指数技术面偏弱（趋势/均线/动量走弱），需谨慎解读。",
            "neutral": "指数技术面中性：趋势/均线未给出明确方向，更多呈震荡/整理。",
            "UNKNOWN": "指数技术面语义不可用（数据不足/未计算）。",
        },
        "north_proxy_pressure": {
            "pressure_low": "北向代理压力不显著（未见明显撤退压力），仍需结合广度/成功率判断。",
            "pressure_medium": "北向代理压力中性，需结合广度/成交/成功率等结构指标综合判断。",
            "pressure_high": "北向代理压力偏高，风险偏好可能收缩，进攻成功率偏低。",
            "UNKNOWN": "北向代理压力语义不可用（数据不足/未计算）。",
        },
        "trend_in_force": {
            "in_force": "趋势结构仍然成立，但需结合成功率/执行环境控制追价风险。",
            "weakening": "趋势动能减弱，结构进入观察与评估阶段。",
            "broken": "趋势结构已被破坏，需警惕趋势性风险。",
            "UNKNOWN": "趋势语义不可用（数据不足/未计算）。",
        },
        "failure_rate": {
            "stable": "未观察到趋势结构失效迹象，结构保持稳定。",
            "watch": "趋势结构存在失效迹象，但尚未形成连续性破坏。",
            "elevated_risk": "近期趋势结构失效频繁，结构性风险上升。",
            "UNKNOWN": "成功率/失效率语义不可用（数据不足/未计算）。",
        },
    }

    def _meaning_from_table(self, key: str, state: str, item: Dict[str, Any]) -> str:
        # 仅对已纳入表的 key 强制使用本 Block 的统一文案
        table = self._MEANING_TABLE.get(key)
        if not isinstance(table, dict):
            return ""
        s = str(state or "").strip()
        if not s:
            s = "UNKNOWN"
        # normalize some states
        s_norm = s
        if s_norm == "DATA_NOT_CONNECTED":
            return "该因子数据未接入/不可用，当前仅占位显示，不参与解读。"
        return str(table.get(s_norm) or table.get("UNKNOWN") or "")

    _EVIDENCE_KEYS = (
        "modifier",
        "pressure_level",
        "pressure_score",
        "trend",
        "trend_5d",
        "north_trend_5d",
        "window",
        "series",
        "history_len",
        "signal",
        "score",
        "adv_ratio",
        "new_low_ratio",
        "count_new_low",
        "turnover_total",
        "turnover_chg",
        "north_net",
    )

    _FORBIDDEN_PHRASES = (
        "动能改善",
        "结构偏强",
        "成交活跃",
        "资金参与度较高",
        "趋势向上",
    )

    _POSITIVE_STATES = ("healthy", "strong", "expanding")
    # Under caution context, some "positive" states are easily misread as "allowed to attack".
    # Use per-key suffix to prevent semantic drift (2025-12-29 case anchor).
    _CAUTION_SUFFIX_BY_KEY: Dict[str, str] = {
        "breadth": "（仅表示未观察到系统性破坏，不代表可进攻）",
        "turnover": "（仅表示未观察到系统性破坏，不代表可进攻）",
        "index_tech": "（指数技术偏强，不代表可进攻）",
        "trend_in_force": "（趋势仍在，不代表可进攻）",
    }




    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        warnings: List[str] = []
        structure = context.slots.get("structure")

        if not isinstance(structure, dict) or not structure:
            warnings.append("structure_missing_or_invalid")
            payload = (
                "- 结构事实：未提供或格式非法\n"
                "  含义：该区块仅用于占位，不影响 Gate / ActionHint\n"
            )
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload=payload,
                warnings=warnings,
            )

        gate_pre = context.slots.get("gate_pre")
        gate_final = context.slots.get("gate_final")
        gate = (gate_final or gate_pre or "")
        gate_u = str(gate).strip().upper()

        exec_band = self._extract_execution_band(context.slots.get("execution_summary"))
        caution_ctx = (gate_u in ("CAUTION", "FREEZE", "D", "PLANB")) or (exec_band in ("D1", "D2"))

        lines: List[str] = []
        lines.append("- 结构事实：")

        keys = list(structure.keys())
        keys_sorted = sorted([k for k in keys if k != "_summary"]) + (
            ["_summary"] if "_summary" in structure else []
        )

        for key in keys_sorted:
            item = structure.get(key)
            if not isinstance(item, dict):
                continue

            # v5: if north_proxy_pressure exists, hide deprecated north_nps (RAW_ONLY/NOT_COMPUTED) to avoid clutter/false comfort
            if key in ("north_nps", "north_nps_raw"):
                if isinstance(structure.get("north_proxy_pressure"), dict) and self._north_nps_trend_not_computed(item):
                    warnings.append("hidden:north_nps_deprecated_replaced_by_north_proxy_pressure")
                    continue

            # raw state / meaning
            raw_state = item.get("state") or item.get("status") or "unknown"
            meaning = self._meaning_from_table(key, raw_state, item) or (item.get("meaning") or item.get("reason") or "")

            # fallback: missing factor without meaning
            if not meaning and str(raw_state).strip().lower() == "missing":
                meaning = f"{key} 因子缺失，结构判断受限。"


            # data_status: if present and not OK, downgrade semantics
            data_status = item.get("data_status")
            if isinstance(data_status, str) and data_status and data_status.upper() != "OK":
                warnings.append(f"data_status:{key}:{data_status}")
                raw_state = "DATA_NOT_CONNECTED"
                if not meaning:
                    meaning = "该因子数据未接入/不可用，当前仅占位显示，不参与解读。"
                else:
                    meaning = meaning.strip()
                    if meaning and not meaning.endswith("。"):
                        meaning += "。"
                    meaning += "（提示：该因子数据未接入/不可用，解释力不足，仅占位。）"

            # north_nps special: if only single-day (no trend/window/series/history), degrade to UNKNOWN
            if key in ("north_nps", "north_nps_raw"):
                # v4: if upstream explicitly marks RAW_ONLY / NOT_COMPUTED, downgrade regardless of window presence
                if self._north_nps_trend_not_computed(item):
                    warnings.append("north_nps_trend_not_computed_downgraded")
                    raw_state = "UNKNOWN"
                    if meaning:
                        meaning = meaning.strip()
                        if meaning and not meaning.endswith("。"):
                            meaning += "。"
                        meaning += "（提示：已接入 window/series，但趋势/连续性尚未计算（NOT_COMPUTED），避免 neutral 误导，语义已降级为 UNKNOWN。）"
                    else:
                        meaning = "已接入 window/series，但趋势/连续性尚未计算（NOT_COMPUTED），语义降级为 UNKNOWN（避免 neutral 误导）。"
                # legacy: single-day guard (no window/series/history)
                elif self._north_nps_is_single_day(item) and str(raw_state).lower() in ("neutral", "strong", "weak"):
                    warnings.append("north_nps_single_day_downgraded")
                    raw_state = "UNKNOWN"
                    if meaning:
                        meaning = meaning.strip()
                        if meaning and not meaning.endswith("。"):
                            meaning += "。"
                        meaning += "（提示：仅单日读数，趋势/窗口未接入，语义已降级为 UNKNOWN。）"
                    else:
                        meaning = "仅单日读数，趋势/窗口未接入，语义降级为 UNKNOWN（避免 neutral 误导）。"
                if self._north_nps_is_single_day(item) and str(raw_state).lower() in ("neutral", "strong", "weak"):
                    warnings.append("north_nps_single_day_downgraded")
                    raw_state = "UNKNOWN"
                    if meaning:
                        meaning = meaning.strip()
                        if meaning and not meaning.endswith("。"):
                            meaning += "。"
                        meaning += "（提示：仅单日读数，趋势/窗口未接入，语义已降级为 UNKNOWN。）"
                    else:
                        meaning = "仅单日读数，趋势/窗口未接入，语义降级为 UNKNOWN（避免 neutral 误导）。"

            # sanitize aggressive phrases
            for p in self._FORBIDDEN_PHRASES:
                if p in meaning:
                    warnings.append(f"semantic_sanitized:{p}")
                    meaning = meaning.replace(p, "")

            # evidence whitelist
            ev: Dict[str, Any] = {}
            ev_src = item.get("evidence")
            if isinstance(ev_src, dict):
                for k in self._EVIDENCE_KEYS:
                    if k in ev_src and ev_src.get(k) is not None:
                        ev[k] = ev_src.get(k)
            # legacy compatibility: also read evidence keys from top-level
            for k in self._EVIDENCE_KEYS:
                if k not in ev and k in item and item.get(k) is not None:
                    ev[k] = item.get(k)

            if key == "_summary":
                lines.append("  - 总述：结构未坏，但扩散不足，结构同步性与成功率下降。")

                # 体感（证据驱动：返回 text/tag/evidence；避免凭 Gate/Execution 直接“推断盘面”）
                feeling = self._derive_market_feeling(context, structure, caution_ctx)
                if feeling:
                    txt = feeling.get("text")
                    tag = feeling.get("tag")
                    evl = feeling.get("evidence") or []
                    if isinstance(txt, str) and txt.strip():
                        lines.append(f"  - 体感：{txt.strip()}")
                        # 将 tag/evidence 写入 payload（Markdown 注释，不影响阅读，但可被回归与持久化解析）
                        try:
                            import json as _json
                            ev_json = _json.dumps(evl, ensure_ascii=False)
                        except Exception:
                            ev_json = str(evl)
                        if isinstance(tag, str) and tag.strip():
                            lines.append(f"  <!-- feeling_tag:{tag.strip()} evidence:{ev_json} -->")
                continue

            lines.append(f"  - {key}:")

            state_str = str(raw_state)

            # Avoid optimistic misread under caution context
            suffix = ""
            if caution_ctx and state_str in self._POSITIVE_STATES:
                suffix = self._CAUTION_SUFFIX_BY_KEY.get(key, "（不代表可进攻）")

            lines.append(f"      状态：{state_str}{suffix}")

            if meaning:
                if caution_ctx and state_str in self._POSITIVE_STATES:
                    # v6: idempotent guard — avoid duplicated "不等于允许进攻/不代表可进攻" hints
                    meaning2 = meaning.strip()
                    already = ("不等于允许进攻" in meaning2) or ("不代表可进攻" in meaning2) or ("不构成进攻" in meaning2)
                    if not already:
                        if meaning2 and not meaning2.endswith("。"):
                            meaning2 += "。"
                        meaning2 += "（提示：该状态不等于允许进攻。）"
                    meaning = meaning2
                lines.append(f"      含义：{meaning}")

            if ev:
                lines.append("      关键证据：")
                for ek, evv in ev.items():
                    lines.append(f"        - {ek}: {self._fmt_value(evv)}")

        lines.append("")
        lines.append(
            "说明：以上为已冻结的结构事实，仅用于解释当前制度背景，"
            "不构成预测、进攻信号或任何形式的操作建议。"
        )

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload="\n".join(lines).strip(),
            warnings=warnings,
        )

    # -----------------------------
    # helpers
    # -----------------------------
    
    def _north_nps_trend_not_computed(self, item: Dict[str, Any]) -> bool:
        # treat as NOT_COMPUTED if upstream marks it explicitly (direct or nested in details)
        for k in ("trend_status", "data_mode"):
            v = item.get(k)
            if isinstance(v, str):
                if k == "trend_status" and v.upper() == "NOT_COMPUTED":
                    return True
                if k == "data_mode" and v.upper() == "RAW_ONLY":
                    return True
        det = item.get("details")
        if isinstance(det, dict):
            ts = det.get("trend_status")
            dm = det.get("data_mode")
            if isinstance(ts, str) and ts.upper() == "NOT_COMPUTED":
                return True
            if isinstance(dm, str) and dm.upper() == "RAW_ONLY":
                return True
        return False
    
    def _north_nps_is_single_day(self, item: Dict[str, Any]) -> bool:
        # treat as single-day if no trend/window/series/history_len evidence exists
        for k in ("trend_5d", "north_trend_5d", "trend", "window", "series", "history_len"):
            if k in item and item.get(k) is not None:
                return False
        ev = item.get("evidence")
        if isinstance(ev, dict):
            for k in ("trend_5d", "north_trend_5d", "trend", "window", "series", "history_len"):
                if k in ev and ev.get(k) is not None:
                    return False
        # some implementations put history in details sub-dict
        det = item.get("details")
        if isinstance(det, dict):
            for k in ("trend_5d", "north_trend_5d", "window", "series", "history_len"):
                if k in det and det.get(k) is not None:
                    return False
        return True

    @staticmethod
    def _fmt_value(v: Any) -> str:
        if isinstance(v, float):
            return f"{v:.4f}"
        if isinstance(v, (list, tuple)):
            return f"[{', '.join(map(str, v[:6]))}{'...' if len(v) > 6 else ''}]"
        if isinstance(v, dict):
            keys = list(v.keys())
            short = {k: v[k] for k in keys[:6]}
            return f"{short}{'...' if len(keys) > 6 else ''}"
        return str(v)

    @staticmethod
    def _get_field(obj: Any, key: str) -> Optional[Any]:
        if obj is None:
            return None
        if isinstance(obj, dict):
            return obj.get(key)
        return getattr(obj, key, None)

    

    # -------------------------
    # feeling (evidence-based)
    # -------------------------
    def _derive_market_feeling(
        self,
        context: ReportContext,
        structure: Dict[str, Any],
        caution_ctx: bool,
    ) -> Optional[Dict[str, Any]]:
        """
        体感输出（Market Feeling Lexicon）——证据驱动，可回归

        返回：
            {"text": str, "tag": str, "evidence": [str,...]}

        约束：
        - 不允许“仅凭 Gate/Execution”直接推断“涨少跌多/轮动”等盘面事实
        - 必须存在可验证证据（adv_ratio/top20/crowding/direction/dispersion 等）才输出具体描述
        - 若处于谨慎区间但证据不足，仅输出泛化句（不带具体事实）

        证据来源（只读）：
        - slots["etf_spot_sync"] / intraday_overlay 内嵌 etf_index_sync
        - slots["market_overview"]（若你已接入 breadth/turnover/fundflow）
        """
        ev: List[str] = []

        # ---- extract evidence candidates ----
        d = self._extract_etf_sync_details(context)
        interp = d.get("interpretation") if isinstance(d.get("interpretation"), dict) else {}

        adv_ratio = self._pick_number(
            d.get("adv_ratio"),
            self._pick_number(self._get_nested(context.slots.get("market_overview"), ("breadth", "adv_ratio")), None),
            self._pick_number(self._get_nested(context.slots.get("market_overview"), ("adv_ratio",)), None),
        )
        if isinstance(adv_ratio, (int, float)):
            ev.append(f"adv_ratio={adv_ratio:.4f}")

        top20_ratio = self._pick_number(
            d.get("top20_turnover_ratio"),
            self._pick_number(self._get_nested(context.slots.get("market_overview"), ("turnover", "top20_turnover_ratio")), None),
            self._pick_number(self._get_nested(context.slots.get("market_overview"), ("top20_turnover_ratio",)), None),
        )
        if isinstance(top20_ratio, (int, float)):
            ev.append(f"top20_turnover_ratio={top20_ratio:.3f}")

        dispersion_num = self._pick_number(
            d.get("dispersion"),
            self._pick_number(self._get_nested(context.slots.get("market_overview"), ("breadth", "dispersion")), None),
            self._pick_number(self._get_nested(context.slots.get("market_overview"), ("dispersion",)), None),
        )
        if isinstance(dispersion_num, (int, float)):
            ev.append(f"dispersion={dispersion_num:.4f}")

        divergence_index = self._pick_number(d.get("divergence_index"), None)
        if isinstance(divergence_index, (int, float)):
            ev.append(f"divergence_index={divergence_index:.4f}")

        same_direction = d.get("same_direction")
        if isinstance(same_direction, bool):
            ev.append(f"same_direction={same_direction}")

        crowding = str(interp.get("crowding", "")).lower()
        direction = str(interp.get("direction", "")).lower()
        participation = str(interp.get("participation", "")).lower()
        disp_label = str(interp.get("dispersion", "")).lower()
        div_label = str(interp.get("divergence", "")).lower()

        if crowding:
            ev.append(f"crowding={crowding}")
        if direction:
            ev.append(f"direction={direction}")
        if participation:
            ev.append(f"participation={participation}")
        if disp_label:
            ev.append(f"dispersion_label={disp_label}")
        if div_label:
            ev.append(f"divergence_label={div_label}")

        # breadth up/down (optional)
        up = self._pick_number(self._get_nested(context.slots.get("market_overview"), ("breadth", "up")), None)
        down = self._pick_number(self._get_nested(context.slots.get("market_overview"), ("breadth", "down")), None)
        if isinstance(up, (int, float)) and isinstance(down, (int, float)):
            ev.append(f"up={int(up)}")
            ev.append(f"down={int(down)}")

        # north proxy pressure state (optional)
        npp_state = None
        npp = structure.get("north_proxy_pressure")
        if isinstance(npp, dict):
            npp_state = str(npp.get("state") or npp.get("status") or "").lower()
            if npp_state:
                ev.append(f"north_proxy_pressure={npp_state}")

        # ---- derived flags ----
        weak_participation = (participation in ("weak", "low")) or (
            isinstance(adv_ratio, (int, float)) and adv_ratio <= 0.42
        ) or (
            isinstance(up, (int, float)) and isinstance(down, (int, float)) and up < down
        )

        crowded = (crowding in ("high", "very_high")) or (
            isinstance(top20_ratio, (int, float)) and top20_ratio >= 0.70
        )

        diverged = (direction in ("diverged", "mixed", "diverge")) or (same_direction is False)

        soft_rotation = (disp_label in ("moderate", "mid", "medium")) and (div_label in ("low", "small")) and (
            isinstance(adv_ratio, (int, float)) and 0.45 <= adv_ratio <= 0.55
        )

        high_disagreement = (
            isinstance(dispersion_num, (int, float)) and dispersion_num >= 2.5 and
            isinstance(adv_ratio, (int, float)) and 0.45 <= adv_ratio <= 0.55
        )

        risk_off_broad = (isinstance(adv_ratio, (int, float)) and adv_ratio <= 0.35) or (
            npp_state in ("pressure_high", "high", "severe", "pressure_severe")
        )

        risk_on_broad = (isinstance(adv_ratio, (int, float)) and adv_ratio >= 0.58) and (
            not crowded
        ) and (
            not diverged
        )

        # Evidence sufficiency: if no meaningful evidence at all, do not output concrete feeling
        has_any_evidence = isinstance(adv_ratio, (int, float)) or isinstance(top20_ratio, (int, float)) or isinstance(same_direction, bool) or bool(interp) or (isinstance(up, (int, float)) and isinstance(down, (int, float)))

        # ---- lexicon decision (priority order) ----
        if risk_off_broad and has_any_evidence:
            return {
                "tag": "RISK_OFF_BROAD",
                "text": "普跌扩散或撤退压力抬升，盘面更像风险释放阶段；优先防守，减少追价与执行摩擦。",
                "evidence": ev,
            }

        if risk_on_broad and has_any_evidence:
            return {
                "tag": "RISK_ON_BROAD",
                "text": "扩散良好且不拥挤，赚钱效应更“普遍”，更像风险偏好抬升而非单点主题。",
                "evidence": ev,
            }

        if weak_participation and (crowded or diverged) and has_any_evidence:
            return {
                "tag": "INDEX_STABLE_STOCKS_WEAK",
                "text": "指数可能较稳，但涨少跌多；盘面更像调仓轮动/兑现，而非全面风险偏好抬升。",
                "evidence": ev,
            }

        if crowded and has_any_evidence:
            return {
                "tag": "INDEX_UP_NARROW",
                "text": "成交高度集中（窄领涨/拥挤），轮动快、追价胜率偏低；更适合等确认而非追热点。",
                "evidence": ev,
            }

        if diverged and has_any_evidence:
            return {
                "tag": "DIVERGENCE_SLIPPAGE",
                "text": "方向不同步、同步性下降，执行摩擦上升；更像结构性轮动，追价容易买在错误一侧。",
                "evidence": ev,
            }

        if high_disagreement and has_any_evidence:
            return {
                "tag": "HIGH_DISAGREEMENT",
                "text": "分歧放大但扩散一般，容易冲高回落/反复；执行摩擦上升，降低进攻性调仓频率。",
                "evidence": ev,
            }

        if soft_rotation and has_any_evidence:
            return {
                "tag": "SOFT_ROTATION",
                "text": "温和分化与结构性轮动为主；选错方向回撤不一定大，但会耗时间，避免频繁追涨。",
                "evidence": ev,
            }

        # Generic caution message: only if caution_ctx, and no concrete evidence (or evidence insufficient)
        if caution_ctx:
            return {
                "tag": "CAUTION_GENERIC",
                "text": "处于谨慎区间，盘面偏结构性分化/轮动；在证据不足时避免用“体感”替代事实判断。",
                "evidence": ev,
            }

        return None

    def _extract_etf_sync_details(self, context: ReportContext) -> Dict[str, Any]:
        """
        从 slots 中提取 etf_spot_sync / intraday_overlay 的 details。
        - 兼容 FactorResult wrapper: {"details": {...}}
        - 兼容 overlay container: overlay["etf_spot_sync"] / overlay["etf_index_sync"]
        """
        # direct slot
        v = context.slots.get("etf_spot_sync")
        if isinstance(v, dict) and v:
            details = v.get("details")
            if isinstance(details, dict) and details:
                return details
            return v

        # overlay container
        overlay = context.slots.get("intraday_overlay") or context.slots.get("intraday") or context.slots.get("overlay")
        if isinstance(overlay, dict) and overlay:
            for k in ("etf_spot_sync", "etf_index_sync", "etf_index_sync_daily", "etf_spot_sync_raw"):
                vv = overlay.get(k)
                if isinstance(vv, dict) and vv:
                    dd = vv.get("details")
                    if isinstance(dd, dict) and dd:
                        return dd
                    return vv
            # overlay itself is details
            if "interpretation" in overlay or "top20_turnover_ratio" in overlay:
                return overlay

        # observations fallback
        obs = context.slots.get("observations")
        if isinstance(obs, dict):
            vv = obs.get("etf_spot_sync") or obs.get("etf_index_sync") or obs.get("intraday_overlay")
            if isinstance(vv, dict) and vv:
                dd = vv.get("details")
                if isinstance(dd, dict) and dd:
                    return dd
                return vv

        return {}

    def _get_nested(self, obj: Any, path: Tuple[str, ...]) -> Any:
        cur = obj
        for k in path:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(k)
        return cur

    def _pick_number(self, *vals: Any) -> Optional[float]:
        for v in vals:
            if isinstance(v, (int, float)):
                return float(v)
        return None

    def _extract_execution_band(self, execu: Any) -> Optional[str]:
        band = self._get_field(execu, "band")
        if not band:
            band = self._get_field(execu, "code")
        if band is None:
            return None
        try:
            s = str(band).strip().upper().replace(" ", "")
        except Exception:
            return None
        if "/" in s:
            s = s.split("/", 1)[0].strip().upper()
        return s
