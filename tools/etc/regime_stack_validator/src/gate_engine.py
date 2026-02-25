def compute_gate(df):
    df = df.copy()
    gates = []

    prev_gate = None

    for _, r in df.iterrows():
        # === 原始 Gate 规则（不变） ===
        if r.breadth_damage_state == "Breakdown":
            raw_gate = "Freeze"
        elif r.breadth_damage_state == "Confirmed":
            raw_gate = "PlanB"
        elif r.participation_state == "HiddenWeakness":
            raw_gate = "PlanB"
        elif r.breadth_damage_state == "Early" or r.participation_state == "Narrow":
            raw_gate = "Caution"
        elif r.breadth_damage_state == "Healthy" and r.participation_state in {"BroadUp", "Neutral"}:
            raw_gate = "Normal"
        else:
            raw_gate = "Caution"

        # === 🔒 Phase-2 恢复粘性规则（新增，仅此一条） ===
        if prev_gate == "PlanB" and raw_gate == "Normal":
            gate = "Caution"
        else:
            gate = raw_gate

        gates.append(gate)
        prev_gate = gate

    df["H4_gate"] = gates
    return df[[
        "date",
        "breadth_damage_state",
        "participation_state",
        "correlation_regime_state",
        "H4_gate"
    ]]
