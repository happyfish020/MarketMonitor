class StructureDistributionEvaluator:
    """
    Phase-3 Structure Distribution Evaluator (Frozen)

    Detects distribution risk via multi-day deterioration
    of participation and ETF index synchronization.
    """

    def __init__(self, window: int = 3, threshold: int = 2):
        self.window = window
        self.threshold = threshold

    def evaluate_1(
        self,
        history: list[dict],
    ) -> dict | None:
        """
        history: list of daily snapshots (latest last)
        each item must include:
          - date
          - factors:
              - etf_index_sync_daily.level
              - participation.level
        """

        if len(history) < self.window:
            return None

        recent = history[-self.window :]
        bad_days = []
        evidence = []

        for day in recent:
            factors = day.get("factors", {})
            sync = factors.get("etf_index_sync_daily")
            part = factors.get("participation")

            is_bad = False
            reason = None

            if sync and sync.level == "HIGH":
                is_bad = True
                reason = "sync_high"

            if part and part.level == "LOW":
                is_bad = True
                reason = "participation_low"

            if is_bad:
                bad_days.append(day.get("date"))
                evidence.append(
                    {
                        "date": day.get("date"),
                        "reason": reason,
                    }
                )

        if len(bad_days) >= self.threshold:
            return {
                "state": "DISTRIBUTION_RISK",
                "window": f"{self.window}D",
                "count": len(bad_days),
                "evidence": evidence,
            }

        return None

####
    def evaluate(self, history: list[dict]) -> dict | None:
        if len(history) < self.window:
            return None

        recent = history[-self.window:]
        bad_days = []
        evidence = []

        for day in recent:
            factors = day.get("factors", {})
            sync = factors.get("etf_index_sync_daily")
            part = factors.get("participation")

            is_bad = False
            reason = None

            if sync and sync.level == "HIGH":
                is_bad = True
                reason = "sync_high"

            if part and part.level == "LOW":
                is_bad = True
                reason = "participation_low"

            if is_bad:
                bad_days.append(day.get("date"))
                evidence.append(
                    {"date": day.get("date"), "reason": reason}
                )

        if len(bad_days) >= self.threshold:
            return {
                "state": "DISTRIBUTION_RISK",
                "window": f"{self.window}D",
                "count": len(bad_days),
                "evidence": evidence,
            }

        return None
    
 #########