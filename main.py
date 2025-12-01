from unified_risk.core.engines.ashare_daily_engine import run_ashare_daily
from unified_risk.common.logging_utils import log_info
import argparse


def run_ashare_daily_mode(force_refresh=False):
    log_info("[MAIN] Running AShareDaily mode")
    result = run_ashare_daily(force_refresh=force_refresh)

    print("\n====== META ======")
    print(result.get("meta"))

    print("\n====== SNAPSHOT ======")
    snap = result.get("snapshot")
    if snap:
        for k, v in snap.items():
            print(f"{k}: {v}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, default="ashare_daily")
    parser.add_argument("--force-refresh", action="store_true")
    args = parser.parse_args()

    if args.mode == "ashare_daily":
        run_ashare_daily_mode(force_refresh=args.force_refresh)
    else:
        print(f"Unknown mode: {args.mode}")


if __name__ == "__main__":
    main()
