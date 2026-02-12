# UPDATED: Rotation Switch governance integration
def apply_rotation_governance(action, context):
    rs = context.get("slots", {}).get("rotation_switch", {})
    mode = rs.get("mode")
    if mode == "OFF" and action in ("ENTER", "ROTATE"):
        return "BLOCKED_BY_ROTATION_OFF"
    if mode == "PARTIAL" and action == "ENTER":
        return "BLOCKED_BY_ROTATION_PARTIAL"
    return None
