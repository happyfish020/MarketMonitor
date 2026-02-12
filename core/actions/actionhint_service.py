from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, TypedDict, Literal

from core.actions.summary_mapper import SummaryMapper

logger = logging.getLogger(__name__)

GateLevel = Literal["NORMAL", "CAUTION", "FREEZE"]
Action = Literal["BUY", "HOLD", "SELL", "FREEZE"]
SummaryCode = Literal["A", "N", "D"]


class ActionHint(TypedDict):
    gate: GateLevel
    action: Action
    summary: SummaryCode
    reason: str
    allowed: List[str]
    forbidden: List[str]
    limits: str
    conditions: str


class ActionHintService:
    """
    UnifiedRisk V12 · ActionHintService（冻结版）

    铁律：
    - ActionHint 只做“制度权限 × 行为边界”裁决，不消费 observations / execution_summary
    - 输入只允许：gate + structure(可选) + watchlist(可选) + conditions_runtime(可选)
    - 输出必须是可读中文，且可审计（reason/allowed/forbidden/limits 固定字典）

    Hotfix (DOS_V1):
    - 允许在 Gate=CAUTION 时，根据 slots['governance']['dos'] 放行：
      * BASE_ETF_ADD（底仓参与）
      * PULLBACK_ADD（卫星仓回撤确认加）
      同时继续禁止 CHASE_ADD（追涨式加仓）与 LEVER_ADD（杠杆）。
    """

    # 冻结文本字典（不要在 Block 层拼这些）
    _REASON_BY_GATE: Dict[GateLevel, str] = {
        "NORMAL": "当前未触发制度性风险限制，可按既定结构计划执行（以纪律为先，不追价）。",
        "CAUTION": "结构进入谨慎区间，制度上不支持主动扩大风险敞口，优先控制执行摩擦与回撤风险。",
        "FREEZE": "制度风险处于高位，进入防守状态：暂停新增风险敞口，仅允许防守性调整。",
    }

    _ALLOWED_BY_GATE: Dict[GateLevel, List[str]] = {
        "NORMAL": ["按计划分批执行", "维持或小幅调整结构（需有计划）"],
        "CAUTION": ["维持核心仓位", "利用反弹做降风险/再平衡", "计划内小幅微调（不追价）"],
        "FREEZE": ["仅允许防守性操作（减仓/降波动/清高β）", "被动持有核心（必要时）"],
    }

    _FORBIDDEN_BY_GATE: Dict[GateLevel, List[str]] = {
        "NORMAL": ["情绪化追涨", "无计划加仓"],
        "CAUTION": ["任何主动加仓/扩敞口", "追涨式买入", "逆势抄底式加仓"],
        "FREEZE": ["任何新增风险敞口", "抄底式买入", "高β扩大仓位", "杠杆/融资扩大风险"],
    }

    _LIMITS_BY_GATE: Dict[GateLevel, str] = {
        "NORMAL": "行为边界：允许按计划执行，但必须避免追价与无计划扩敞口。",
        "CAUTION": "行为边界：禁止加仓，优先防守与降摩擦；允许利用反弹做减仓/再平衡。",
        "FREEZE": "行为边界：仅防守（减仓/降风险）；任何新增风险敞口均不被制度支持。",
    }

    def build_actionhint(
        self,
        *,
        gate: GateLevel,
        structure: Optional[Dict[str, Any]] = None,
        watchlist: Optional[Dict[str, Any]] = None,
        conditions_runtime: Optional[Any] = None,
        governance: Optional[Dict[str, Any]] = None,
    ) -> ActionHint:
        self._validate_inputs(gate=gate)

        action = self._decide_action(gate)
        summary = SummaryMapper().map_gate_to_summary(gate)

        reason = self._REASON_BY_GATE[gate]
        allowed = list(self._ALLOWED_BY_GATE[gate])
        forbidden = list(self._FORBIDDEN_BY_GATE[gate])
        limits = self._LIMITS_BY_GATE[gate]
        conditions = self._build_conditions_text(conditions_runtime)  # base notes (non-silent)

        # ------------------------------------------------------------------
        # DOS overlay (Frozen V1): when Gate=CAUTION, allow base-index participation / pullback add,
        # while still forbidding chase adds. This avoids "always defensive" outputs in rising index regimes.
        # Source: slots['governance']['dos']
        # ------------------------------------------------------------------
        dos_allowed: List[str] = []
        dos_level: Optional[str] = None
        dos_mode: Optional[str] = None

        if isinstance(governance, dict):
            dos = governance.get("dos")
            if isinstance(dos, dict):
                da = dos.get("allowed")
                if isinstance(da, list):
                    dos_allowed = [str(x) for x in da if isinstance(x, (str, int, float))]
                dl = dos.get("level")
                if isinstance(dl, str):
                    dos_level = dl
                dm = dos.get("mode")
                if isinstance(dm, str):
                    dos_mode = dm

        # ------------------------------------------------------------------
        # AttackPermit overlay (Route-A):
        # - AttackPermit does NOT relax Gate, but provides explicit permission boundaries.
        # - When permit=YES, align allowed/forbidden/limits with the permission (LIMITED/FULL).
        # Source: slots['governance']['attack_permit']
        # ------------------------------------------------------------------
        ap: Optional[Dict[str, Any]] = None
        ap_permit: Optional[str] = None
        ap_mode: Optional[str] = None
        ap_label: Optional[str] = None
        ap_allowed: List[str] = []
        ap_constraints: List[str] = []
        ap_warnings: List[str] = []

        if isinstance(governance, dict):
            _ap = governance.get("attack_permit")
            if isinstance(_ap, dict):
                ap = _ap
                ap_permit = str(_ap.get("permit") or "").upper() if _ap.get("permit") is not None else None
                ap_mode = str(_ap.get("mode") or "")
                ap_label = str(_ap.get("label") or "")
                aa = _ap.get("allowed")
                if isinstance(aa, list):
                    ap_allowed = [str(x) for x in aa if isinstance(x, (str, int, float))]
                cc = _ap.get("constraints")
                if isinstance(cc, list):
                    ap_constraints = [str(x) for x in cc if isinstance(x, (str, int, float))]
                ww = _ap.get("warnings")
                if isinstance(ww, list):
                    ap_warnings = [str(x) for x in ww if isinstance(x, (str, int, float))]


        if gate == "CAUTION" and any(x in dos_allowed for x in ("BASE_ETF_ADD", "PULLBACK_ADD")):
            # Rewrite CAUTION semantics: allow controlled participation (base / pullback), forbid chase.
            reason = (
                "结构进入谨慎区间：默认不支持追涨式扩大风险敞口；"
                "但当前制度允许“底仓参与/回撤确认加仓”（不追价、分批、小步）。"
            )

            # Allowed actions enriched by DOS
            if "允许上证/宽基底仓分批参与（不追涨）" not in allowed:
                allowed.insert(0, "允许上证/宽基底仓分批参与（不追涨）")
            if "BASE_ETF_ADD" in dos_allowed and "允许底仓小步加（BASE_ETF_ADD）" not in allowed:
                allowed.append("允许底仓小步加（BASE_ETF_ADD）")
            if "PULLBACK_ADD" in dos_allowed and "允许卫星仓仅在回撤确认后小步加（PULLBACK_ADD）" not in allowed:
                allowed.append("允许卫星仓仅在回撤确认后小步加（PULLBACK_ADD）")

            # Remove the blanket ban on all adds; keep chase/contrarian bans
            forbidden = [x for x in forbidden if x != "任何主动加仓/扩敞口"]
            if "追涨式买入" not in forbidden:
                forbidden.append("追涨式买入")
            if "追涨加仓/突破追价加仓" not in forbidden:
                forbidden.append("追涨加仓/突破追价加仓")
            if "逆势抄底式加仓" not in forbidden:
                forbidden.append("逆势抄底式加仓")

            limits = (
                "行为边界：允许底仓参与与回撤确认加仓（小步/分批/不追价）；"
                "禁止追涨式加仓与逆势抄底扩大敞口。"
            )

            # Keep summary code stable (Gate=CAUTION => Summary=N), but add a short condition note.
            if dos_level or dos_mode:
                conditions = conditions + f" ｜DOS={dos_level or '-'}({dos_mode or '-'})"

        
        # ------------------------------------------------------------------
        # Apply AttackPermit (higher priority than DOS overlay for wording):
        # - When permit=YES(LIMITED/FULL), explicitly allow BASE_ETF_ADD / PULLBACK_ADD (and maybe SATELLITE_ADD),
        #   while still forbidding chase/leveraged adds.
        # - Keep Summary mapped from Gate (Gate=CAUTION => Summary=N); this avoids semantic drift.
        # ------------------------------------------------------------------
        if gate == "CAUTION" and ap_permit == "YES":
            # Replace reason to be explicit and auditable
            reason = (
                "结构进入谨慎区间：默认不支持追涨式扩大风险敞口；"
                "但当前满足进攻许可（仅底仓参与/回撤确认加仓），须小步分批、不追价。"
            )

            # Ensure base participation headline
            if "允许上证/宽基底仓分批参与（不追涨）" not in allowed:
                allowed.insert(0, "允许上证/宽基底仓分批参与（不追涨）")

            # Allowed actions based on permit mode
            if "BASE_ETF_ADD" in ap_allowed and "允许底仓小步加（BASE_ETF_ADD）" not in allowed:
                allowed.append("允许底仓小步加（BASE_ETF_ADD）")
            if "PULLBACK_ADD" in ap_allowed and "允许卫星仓仅在回撤确认后小步加（PULLBACK_ADD）" not in allowed:
                allowed.append("允许卫星仓仅在回撤确认后小步加（PULLBACK_ADD）")
            if "SATELLITE_ADD" in ap_allowed and "允许卫星仓分批参与（SATELLITE_ADD，不追涨）" not in allowed:
                allowed.append("允许卫星仓分批参与（SATELLITE_ADD，不追涨）")

            # Remove blanket ban on all adds, but keep chase/contrarian/lever restrictions
            forbidden = [x for x in forbidden if x != "任何主动加仓/扩敞口"]
            if "追涨式买入" not in forbidden:
                forbidden.append("追涨式买入")
            if "追涨加仓/突破追价加仓" not in forbidden:
                forbidden.append("追涨加仓/突破追价加仓")
            if "逆势抄底式加仓" not in forbidden:
                forbidden.append("逆势抄底式加仓")
            if "杠杆/融资扩大风险" not in forbidden:
                forbidden.append("杠杆/融资扩大风险")

            limits = (
                "行为边界：允许底仓参与与回撤确认加仓（小步/分批/不追价）；"
                "禁止追涨式加仓与逆势抄底扩大敞口。"
            )

            # Attach short audit notes to conditions
            ap_label_txt = ap_label or (f"YES({ap_mode})" if ap_mode else "YES")
            ap_warn_txt = ";".join(ap_warnings[:6]) if ap_warnings else ""
            ap_cons_txt = ";".join(ap_constraints[:3]) if ap_constraints else ""
            conditions = (
                (conditions or '')
                + f" ｜AttackPermit={ap_label_txt}"
                + (f" ｜AP_warnings={ap_warn_txt}" if ap_warn_txt else "")
                + (f" ｜AP_constraints={ap_cons_txt}" if ap_cons_txt else "")
            )


        hint: ActionHint = {
            "gate": gate,
            "action": action,
            "summary": summary,
            "reason": reason,
            "allowed": allowed,
            "forbidden": forbidden,
            "limits": limits,
            "conditions": conditions,
        }

        logger.info("[ActionHint] gate=%s summary=%s action=%s", gate, summary, action)
        return hint

    def _validate_inputs(self, *, gate: Any) -> None:
        if gate not in ("NORMAL", "CAUTION", "FREEZE"):
            raise ValueError(f"Invalid gate level: {gate}")

    def _decide_action(self, gate: GateLevel) -> Action:
        # V12 冻结：ActionHint 不做买卖推荐，仅给权限边界
        if gate == "FREEZE":
            return "FREEZE"
        return "HOLD"

    def _build_conditions_text(self, conditions_runtime: Any, *, ap_permit: Optional[str] = None) -> str:
        if conditions_runtime is None:
            return "执行时点校验未启用：当前依据 Gate + AttackPermit（DOS）输出行为边界（Execution/时点确认未启用）。"
        return "执行时点校验已预留：当前未纳入强制判断。"