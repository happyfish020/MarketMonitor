
import sys
import logging

from unifiedrisk.utils.logger import setup_logger
from unifiedrisk.core.ashare.engine import AShareDailyEngine
from unifiedrisk.reporting.daily_writer import write_daily_report

def main():
    level = logging.DEBUG if "--debug" in sys.argv else logging.INFO
    setup_logger(level=level)
    log = logging.getLogger("UnifiedRisk.main")

    log.info("UnifiedRisk starting (v3.4, mode=ashare)")
    engine = AShareDailyEngine()
    result = engine.run()

    raw = result.get("raw", {})
    score = result.get("score", {})

    log.info("UnifiedRisk result: %s", result)

    try:
        report_path = write_daily_report(raw, score)
        log.info("Daily report written to %s", report_path)
    except Exception:
        log.exception("Failed to write daily report")

    print("\n=== UnifiedRisk v4.0 Output ===")
    print(result)

if __name__ == "__main__":
    main()
