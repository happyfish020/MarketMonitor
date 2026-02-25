from typing import Dict, List
import pandas as pd


RISK_GATES = {"PlanB", "Freeze"}
CRITICAL_BREADTH = {"Confirmed", "Breakdown"}


def max_consecutive(series: List[str], value: str) -> int:
    max_len = 0
    curr = 0
    for v in series:
        if v == value:
            curr += 1
            max_len = max(max_len, curr)
        else:
            curr = 0
    return max_len


def detect_zigzag(gates: List[str]) -> bool:
    """
    Detect patterns like:
    PlanB/Freeze -> Normal -> PlanB/Freeze
    or Freeze -> Normal
    """
    n = len(gates)
    for i in range(n - 2):
        if gates[i] in RISK_GATES and gates[i + 1] == "Normal" and gates[i + 2] in RISK_GATES:
            return True
    return False


def run_health_check(df_gate: pd.DataFrame) -> Dict:
    """
    df_gate must contain columns:
    - date
    - breadth_damage_state
    - participation_state
    - correlation_regime_state
    - H4_gate
    """

    df = df_gate.sort_values("date").reset_index(drop=True)

    gates = df["H4_gate"].tolist()

    # 1. Gate distribution
    gate_distribution = df["H4_gate"].value_counts().to_dict()

    # 2. Max consecutive PlanB / Freeze
    max_consecutive_planb = max_consecutive(gates, "PlanB")
    max_consecutive_freeze = max_consecutive(gates, "Freeze")

    # 3. Zigzag detection (hard redline)
    zigzag_detected = detect_zigzag(gates)

    # 4. Breadth -> Gate consistency
    breadth_violations = df[
        (df["breadth_damage_state"].isin(CRITICAL_BREADTH)) &
        (df["H4_gate"] == "Normal")
    ]
    breadth_consistency_ok = len(breadth_violations) == 0

    # 5. Hidden Weakness capture ratio
    hidden_df = df[df["participation_state"] == "HiddenWeakness"]
    if len(hidden_df) == 0:
        hidden_capture_ratio = 1.0
    else:
        captured = hidden_df[hidden_df["H4_gate"].isin(RISK_GATES)]
        hidden_capture_ratio = len(captured) / len(hidden_df)

    # 6. Summary judgement (machine only)
    reasons = []
    status = "HEALTHY"

    if zigzag_detected:
        status = "FAIL"
        reasons.append("zigzag_detected")

    if not breadth_consistency_ok:
        status = "FAIL"
        reasons.append("breadth_gate_inconsistency")

    if hidden_capture_ratio < 0.8:
        if status != "FAIL":
            status = "WARNING"
        reasons.append("hidden_weakness_not_fully_captured")

    return {
        "gate_distribution": gate_distribution,
        "max_consecutive": {
            "PlanB": max_consecutive_planb,
            "Freeze": max_consecutive_freeze
        },
        "zigzag_detected": zigzag_detected,
        "breadth_consistency_ok": breadth_consistency_ok,
        "hidden_weakness_capture_ratio": round(hidden_capture_ratio, 3),
        "summary": {
            "status": status,
            "reasons": reasons
        }
    }
