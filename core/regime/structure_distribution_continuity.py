import json
import os
from datetime import datetime

class StructureDistributionContinuity:
    @staticmethod
    def apply(
        *,
        factors: dict,
        asof: str,
        state_path: str,
        window: int = 3,
        threshold: int = 2,
    ) -> dict | None:
        """
        Phase-3 Structure Distribution Continuity
        """

        sync = factors.get("crowding_concentration")
        part = factors.get("participation")

        is_bad = False
        reason = None

        if sync and sync.level == "HIGH":
            is_bad = True
            reason = "sync_high"

        if part and part.level == "LOW":
            is_bad = True
            reason = "participation_low"

        today = {
            "date": asof,
            "bad": is_bad,
            "reason": reason,
        }

        os.makedirs(os.path.dirname(state_path), exist_ok=True)

        try:
            with open(state_path, "r", encoding="utf-8") as f:
                history = json.load(f)
        except Exception:
            history = []

        history.append(today)
        history = history[-window:]

        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)

        bad_days = [d for d in history if d.get("bad")]

        if len(bad_days) >= threshold:
            return {
                "state": "DISTRIBUTION_RISK",
                "window": f"{window}D",
                "count": len(bad_days),
                "evidence": bad_days,
            }

        return None
