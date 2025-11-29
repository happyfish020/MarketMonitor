from __future__ import annotations

from datetime import date

from unified_risk.common.logger import get_logger
from unified_risk.core.ashare.ashare_daily_engine import AShareDailyEngine
from unified_risk.core.ashare.report_writer import write_daily_report

LOG = get_logger("UnifiedRisk.Main")


def main(run_date: date | None = None) -> None:
    engine = AShareDailyEngine()
    res = engine.run(run_date)
    LOG.info(
        "[Main] Finished: total_risk_score=%.3f",
        res.get("total_risk_score", 0.0),
    )
    write_daily_report(res)


if __name__ == "__main__":
    main()
