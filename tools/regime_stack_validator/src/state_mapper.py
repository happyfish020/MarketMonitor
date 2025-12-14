def map_states(df):
    out = df.copy()

    def breadth(row):
        if row.new_low_ratio >= 0.12 and row.new_low_persistence >= 5:
            return "Breakdown"
        if row.new_low_ratio >= 0.07 and row.new_low_persistence >= 3:
            return "Confirmed"
        if row.new_low_ratio >= 0.03:
            return "Early"
        return "Healthy"

    def participation(row):
        if row.adv_ratio <= 0.4 and row.median_return < 0:
            return "HiddenWeakness"
        if row.adv_ratio < 0.6 and row.median_return >= 0:
            return "Narrow"
        if row.adv_ratio >= 0.6 and row.median_return >= 0:
            return "BroadUp"
        if row.adv_ratio <= 0.4 and row.median_return < 0:
            return "BroadDown"
        return "Neutral"

    out["breadth_damage_state"] = out.apply(breadth, axis=1)
    out["participation_state"] = out.apply(participation, axis=1)
    out["correlation_regime_state"] = "Stable"

    return out
