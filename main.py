"""
UnifiedRisk main entry.
"""

import argparse
import sys
from unifiedrisk.common.logger import get_logger
from unifiedrisk.core.midterm.engine import MidtermEngine
from unifiedrisk.core.shortterm.engine import ShorttermEngine
from unifiedrisk.core.global_daily_risk.engine import GlobalDailyRiskEngine
from unifiedrisk.core.ashare.engine import AShareDailyEngine
from unifiedrisk.common.scoring import aggregate_horizons


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--mode",
        type=str,
        default="all",
        choices=["midterm", "shortterm", "global_daily", "ashare", "all"],
        help="Which engine to run",
    )

    args = parser.parse_args()

    # MUST initialize logger AFTER args.debug is available
    LOG = get_logger("UnifiedRisk.main", debug=args.debug)

    LOG.info("UnifiedRisk starting with mode=%s date=None", args.mode)

    horizon_results = {}

    if args.mode in ("midterm", "all"):
        m = MidtermEngine().run()
        horizon_results["midterm"] = m

    if args.mode in ("shortterm", "all"):
        s = ShorttermEngine().run()
        horizon_results["shortterm"] = s

    if args.mode in ("global_daily", "all"):
        gd = GlobalDailyRiskEngine().run()
        horizon_results["global_daily"] = gd

    if args.mode in ("ashare", "all"):
        a = AShareDailyEngine().run()
        horizon_results["ashare_daily"] = a

    if args.mode != "all":
        LOG.info("Result = %s", horizon_results)
        print("\n=== UnifiedRisk Output ===")
        print(horizon_results)
        return

    # aggregate results
    agg = aggregate_horizons(horizon_results)
    LOG.info(
        "Unified aggregated risk level: %s (score=%.2f)",
        agg["risk_level"],
        agg["total_score"],
    )

    print("\n=== UnifiedRisk Aggregated Result ===")
    print(agg)


if __name__ == "__main__":
    main()
