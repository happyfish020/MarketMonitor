# core/actions/actionhint_service.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional
from core.actions.summary_mapper import SummaryMapper

Action = Literal["HOLD", "ETF_COND_ADD", "ETF_LADDER", "FREEZE"]

ActionHint = Dict[str, Any] 

logger = logging.getLogger("UnifiedRisk.ActionHintService")



@dataclass(frozen=True)
class ActionPolicy:
    """
    可注入的裁决策略（不改接口，不越权）。
    """
    normal_action: Action = "HOLD"
    caution_action: Action = "HOLD"
    freeze_action: Action = "FREEZE"

class ActionHintService:
    def __init__(self, *, policy: Optional[ActionPolicy] = None) -> None:
        self._policy = policy or ActionPolicy()

 
##3
    def build_actionhint(
        self,
        *,
        gate: Any,
        structure: Any,
        watchlist: Any,
        conditions_runtime: Any,
    ) -> ActionHint:
        """
        Phase-3 ActionHint 构建（冻结版，兼容 gate=str / gate=dict）
    
        设计原则：
        - 兼容旧路径 gate=str
        - 内部统一为 dict 形态
        - 不在此层强制上游重构
        """
    
        # -------- ① 统一 gate 形态（关键修复点）--------
        if isinstance(gate, str):
            gate = {
                "level": gate,
                "reasons": [],
                "evidence": {},
            }
        elif isinstance(gate, dict):
            if "level" not in gate:
                raise ValueError("gate dict missing required field: level")
        else:
            raise TypeError(f"invalid gate type: {type(gate)}")
    
        # -------- 输入校验（原有）--------
        self._validate_inputs(
            gate=gate,
            structure=structure,
            watchlist=watchlist,
            conditions_runtime=conditions_runtime,
        )
    
        # -------- 动作裁决 --------
        action = self._decide_action(gate=gate, structure=structure)
    
        # -------- Summary（A / N / D）--------
        summary = SummaryMapper().map_gate_to_summary(gate=gate["level"])
    
        # -------- Reason（人话）--------
        reason = self._build_reason_text(gate=gate)
    
        # -------- 其余字段 --------
        limits = self._build_limits(action=action, gate=gate)
        allowed = self._build_allowed(action=action, gate=gate)
        forbidden = self._build_forbidden(action=action, gate=gate)
        conditions = self._build_conditions(conditions_runtime=conditions_runtime)
    
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
    
        self._validate_actionhint(hint)
    
        logger.info(
            "ActionHint built gate=%s action=%s summary=%s",
            gate["level"],
            action,
            summary,
        )
    
        return hint
     
    # ----------------- internal (frozen hooks) -----------------

    def _build_reason_text(self, *, gate: Any) -> str:
        """
        将 Gate 中的 reasons 精简为 ActionHint 可读原因（≤2 条）
        gate = snapshot["gate"]，必须包含 level / reasons
        """
    
        level = gate.get("level")
        reasons = gate.get("reasons", [])
    
        if not reasons:
            return f"Gate={level}。未触发明确的风险或放行条件。"
    
        trimmed = reasons[:2]
    
        if len(trimmed) == 1:
            return f"Gate={level}。原因：{trimmed[0]}。"
    
        return f"Gate={level}。主要原因：{trimmed[0]}；{trimmed[1]}。"
    

###
    def _validate_inputs(
        self,
        *,
        gate: Any,
        structure: Any,
        watchlist: Any,
        conditions_runtime: Any,
    ) -> None:
        """
        Phase-3 输入校验（冻结版）
    
        原则：
        - 校验“是否明显非法”
        - 不强制 Phase-2 的具体数据结构
        """
    
        # ---- gate ----
        if not isinstance(gate, dict):
            raise ValueError(f"gate must be dict, got {type(gate)}")
    
        level = gate.get("level")
        if level not in ("NORMAL", "CAUTION", "FREEZE"):
            raise ValueError(f"Invalid gate level: {level}")
    
        reasons = gate.get("reasons")
        if reasons is not None and not isinstance(reasons, list):
            raise ValueError("gate.reasons must be list")
    
        evidence = gate.get("evidence")
        if evidence is not None and not isinstance(evidence, dict):
            raise ValueError("gate.evidence must be dict")
    
        # ---- structure ----
        if structure is not None and not isinstance(structure, dict):
            raise ValueError("structure must be dict or None")
    
        # ---- watchlist ----
        if watchlist is not None and not isinstance(watchlist, dict):
            raise ValueError("watchlist must be dict or None")
    
        # ---- conditions_runtime ----
        # 允许 dict / list / None（仅展示用途）
        if conditions_runtime is not None and not isinstance(
            conditions_runtime, (dict, list)
        ):
            raise ValueError(
                f"conditions_runtime must be dict, list or None, got {type(conditions_runtime)}"
            )
    

###
    def _decide_action(self, *, gate: Any, structure: Any) -> Action:
        g = str(gate).upper()
        if g == "FREEZE":
            return self._policy.freeze_action
        if g == "CAUTION":
            return self._policy.caution_action
        return self._policy.normal_action

    def _build_reason(self, *, gate: Any, structure: Any) -> str:
        g = str(gate).upper()

        # Only read “conclusion-like” fields if present; do NOT read factor/prediction/raw.
        summary_bits: List[str] = []
        if isinstance(structure, dict):
            for key in ("participation", "breadth", "regime", "correlation", "note"):
                v = structure.get(key)
                if v is not None:
                    summary_bits.append(f"{key}={v}")

        core = f"Gate={g}. Phase-3 action is derived from frozen Phase-2 Gate/Structure slots only."
        if summary_bits:
            core += " Structure: " + ", ".join(summary_bits[:6])
        return core

    def _build_limits(self, *, action: Action, gate: Any) -> Dict[str, Any]:
        # Keep it explicit & auditable. Real numeric limits should be injected later (policy/config).
        g = str(gate).upper()
        return {
            "gate": g,
            "action": action,
            "notes": "Limits are policy-defined. This default implementation is conservative and non-prescriptive.",
        }

    def _build_allowed(self, *, action: Action, gate: Any) -> Dict[str, Any]:
        # Allowed is structural: what category of actions are allowed.
        return {"action": action}

    def _build_forbidden(self, *, action: Action, gate: Any) -> List[str]:
        g = str(gate).upper()
        if g == "FREEZE":
            return [
                "Any new risk add",
                "Any ladder add",
                "Any discretionary override outside ActionHint",
            ]
        return [
            "Bypass ActionHint with manual interpretation",
        ]

    def _build_conditions(self, *, conditions_runtime: Any) -> List[Any]:
        # conditions_runtime is precomputed upstream (Phase-2/bridge). We only pass through.
        if conditions_runtime is None:
            return []
        if isinstance(conditions_runtime, list):
            return conditions_runtime
        return [conditions_runtime]

    def _validate_actionhint(self, hint: ActionHint) -> None:
        required = {"action", "reason", "allowed", "forbidden", "limits", "conditions"}
        missing = required - set(hint.keys())
        if missing:
            raise ValueError(f"ActionHint missing keys: {sorted(list(missing))}")
        
        if hint.get("summary") not in ("A", "N", "D"):
            raise ValueError(f"Invalid summary in ActionHint: {hint.get('summary')}")

        a = hint["action"]
        if a not in ("HOLD", "ETF_COND_ADD", "ETF_LADDER", "FREEZE"):
            raise ValueError(f"ActionHint.action invalid: {a}")


