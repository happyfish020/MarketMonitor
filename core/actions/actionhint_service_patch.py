# PATCH: call rotation governance before emitting action hints
from core.actions.rotation_governance import apply_rotation_governance

def guard_action(action, context):
    reason = apply_rotation_governance(action, context)
    if reason:
        return {
            "allowed": False,
            "reason": reason
        }
    return {"allowed": True}
