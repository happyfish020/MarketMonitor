from __future__ import annotations

from typing import Dict


class ExecutionGuard:
    """
    execution guard（冻结）

    职责：
    - 基于 Gate × 行为类型，做制度合规校验
    - 不计算、不读取行情、不判断市场
    """

    # 冻结的 Gate × 行为矩阵
    _MATRIX: Dict[str, Dict[str, str]] = {
        "NORMAL": {
            "INCREASE_EXPOSURE": "ALLOW",
            "REDUCE_EXPOSURE": "ALLOW",
            "SWITCH_EXPOSURE": "ALLOW",
        },
        "CAUTION": {
            "INCREASE_EXPOSURE": "BLOCK",
            "REDUCE_EXPOSURE": "ALLOW",
            "SWITCH_EXPOSURE": "WARN",
        },
        "FREEZE": {
            "INCREASE_EXPOSURE": "BLOCK",
            "REDUCE_EXPOSURE": "ALLOW",
            "SWITCH_EXPOSURE": "BLOCK",
        },
    }

    def check(self, gate: str, action_intent: str) -> Dict[str, str | bool]:
        if not isinstance(gate, str):
            return {
                "allowed": False,
                "severity": "HARD_BLOCK",
                "reason": "Gate 类型异常，执行被制度拦截。",
            }

        gate = gate.upper()
        action = action_intent.upper()

        rule = self._MATRIX.get(gate, {}).get(action)

        if rule is None:
            return {
                "allowed": False,
                "severity": "HARD_BLOCK",
                "reason": f"未知 Gate 或行为类型（Gate={gate}, Action={action}）。",
            }

        if rule == "ALLOW":
            return {
                "allowed": True,
                "severity": "ALLOW",
                "reason": f"当前 Gate={gate}，该行为在制度上允许。",
            }

        if rule == "WARN":
            return {
                "allowed": False,
                "severity": "SOFT_WARN",
                "reason": f"当前 Gate={gate}，该行为仅在不放大整体风险的前提下可被谨慎考虑。",
            }

        # BLOCK
        return {
            "allowed": False,
            "severity": "HARD_BLOCK",
            "reason": f"当前 Gate={gate}，制度不支持该风险敞口变更行为。",
        }
