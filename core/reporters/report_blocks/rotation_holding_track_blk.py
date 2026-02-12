# Report block: Rotation Holding Track
from core.reporters.base_block import ReportBlock

class RotationHoldingTrackBlock(ReportBlock):
    alias = "rotation.holding_track"

    def build(self, context):
        rows = context.slots.get("rotation_holding_track_raw", [])
        return {
            "title": "Rotation Holding Track",
            "rows": rows,
            "note": "Empty rows indicate no holdings for the trade_date."
        }
