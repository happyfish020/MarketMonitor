from typing import Literal

Gate = Literal["NORMAL", "CAUTION", "PLANB", "FREEZE"]


# Gate 等级顺序（越右越严格）
_GATE_ORDER = {
    "NORMAL": 0,
    "CAUTION": 1,
    "PLANB": 2,
    "FREEZE": 3,
}


def _min_gate(g1: Gate, g2: Gate) -> Gate:
    """
    返回更严格（风险更高）的 gate
    """
    return g1 if _GATE_ORDER[g1] >= _GATE_ORDER[g2] else g2


def apply_execution_overlay(
    gate_pre: Gate,
    execution_code: str,
) -> Gate:
    """
    使用 Execution Summary 对 GateDecision 结果进行覆盖（只降级，不升级）

    execution_code:
        "A" | "N" | "D1" | "D2" | "D3"
    """

    # --- Execution → 最低 Gate 要求 ---
    if execution_code == "D3":
        gate_required: Gate = "FREEZE"

    elif execution_code == "D2":
        gate_required = "PLANB"

    elif execution_code == "D1":
        gate_required = "CAUTION"

    else:
        # A / N 不额外施压
        gate_required = gate_pre

    # --- 返回更严格的 gate ---
    return _min_gate(gate_pre, gate_required)
