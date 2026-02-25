from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure repo root is on sys.path so `core.*` imports work when running as a script
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.personal_tactical.cn_entry_pool_rotation.engine import build_engine


def main() -> None:
    parser = argparse.ArgumentParser(prog="run_cn_epr_eod.py")
    parser.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    engine = build_engine()
    out = engine.run_eod(args.trade_date)
    print(out)


if __name__ == "__main__":
    main()
