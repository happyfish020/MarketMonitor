from __future__ import annotations

import argparse
import uuid
from pathlib import Path

from core.personal_tactical.cn_position_governance.engine import CnPositionGovernanceEngine
from core.personal_tactical.cn_position_governance.sqlite_store import SqliteStore
from core.personal_tactical.cn_position_governance.reporting import render_t1


def main() -> None:
    p = argparse.ArgumentParser(prog="run_cn_pg_t1.py")
    p.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    args = p.parse_args()

    run_id = str(uuid.uuid4())
    store = SqliteStore(
        sqlite_path=Path("data") / "cn_position_governance.db",
        schema_sql_path=Path("core") / "personal_tactical" / "cn_position_governance" / "resources" / "sqlite_schema.sql",
    )
    engine = CnPositionGovernanceEngine(store)

    sigs = engine.run_t1(trade_date=args.trade_date, run_id=run_id)
    print(render_t1(sigs))
    print(f"run_id={run_id}")


if __name__ == "__main__":
    main()
