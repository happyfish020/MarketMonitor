# Report block: Rotation Entry Priority
from core.reporters.base_block import ReportBlock

class RotationEntryPriorityBlock(ReportBlock):
    alias = "rotation.entry_priority"

    def build(self, context):
        rows = context.slots.get("rotation_entry_priority_raw", [])
        return {
            "title": "Rotation Entry Priority",
            "rows": rows,
            "note": "priority_rank=1 indicates the top candidate."
        }
