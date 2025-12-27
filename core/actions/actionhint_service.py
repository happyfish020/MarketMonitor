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
    ) -> ActionHint:
        self._validate_inputs(gate=gate)

        action = self._decide_action(gate)
        summary = SummaryMapper().map_gate_to_summary(gate)

        reason = self._REASON_BY_GATE[gate]
        allowed = list(self._ALLOWED_BY_GATE[gate])
        forbidden = list(self._FORBIDDEN_BY_GATE[gate])
        limits = self._LIMITS_BY_GATE[gate]
        conditions = self._build_conditions_text(conditions_runtime)

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

        logger.info(
            "[ActionHint] gate=%s summary=%s action=%s",
            gate,
            summary,
            action,
        )
        return hint

    def _validate_inputs(self, *, gate: Any) -> None:
        if gate not in ("NORMAL", "CAUTION", "FREEZE"):
            raise ValueError(f"Invalid gate level: {gate}")

    def _decide_action(self, gate: GateLevel) -> Action:
        # V12 冻结：ActionHint 不做买卖推荐，仅给权限边界
        if gate == "FREEZE":
            return "FREEZE"
        return "HOLD"

    def _build_conditions_text(self, conditions_runtime: Any) -> str:
        if conditions_runtime is None:
            return "执行时点校验未启用：当前仅依据制度权限（Gate）输出行为边界。"
        return "执行时点校验已预留：当前未纳入强制判断。"
