def render_block_note(actionhint):
    if actionhint.get("allowed") is False and actionhint.get("reason","").startswith("BLOCKED_BY_ROTATION"):
        return "- RotationSwitch: 已拦截进攻动作（%s）" % actionhint.get("reason")
    return ""
