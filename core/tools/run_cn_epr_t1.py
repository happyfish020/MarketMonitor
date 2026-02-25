from __future__ import annotations

import argparse
from pathlib import Path
import sys

# Ensure project root is on sys.path and avoid shadowed 'core' modules.
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
if 'core' in sys.modules and not hasattr(sys.modules['core'], '__path__'):
    del sys.modules['core']


from core.personal_tactical.cn_entry_pool_rotation.bootstrap import bootstrap
from core.personal_tactical.cn_entry_pool_rotation.db import connect, fetch_all
from core.personal_tactical.cn_entry_pool_rotation.engine import run_t1
from core.personal_tactical.cn_entry_pool_rotation.io_loaders import load_executions, load_positions
from core.personal_tactical.cn_entry_pool_rotation.oracle_facts import fetch_prev_trade_date_oracle


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--trade-date", required=True)
    p.add_argument("--prev-trade-date", default=None)
    # Defaults are resolved relative to project root (鈥?MarketMonitor),
    # so user can run with only --trade-date.
    project_root = Path(__file__).resolve().parents[1]
    p.add_argument("--db", default=str(project_root / "data" / "cn_entry_pool_rotation.db"))
    p.add_argument(
        "--config",
        default=str(project_root / "config" / "cn_entry_pool_rotation.yaml"),
    )

    p.add_argument("--positions-json", default=None)
    # Optional: if omitted, will read executions from SQLite for the trade_date.
    p.add_argument("--executions-json", default=None)

    # Optional: use Oracle to find prev_trade_date automatically
    p.add_argument(
        "--oracle-dsn",
        default="oracle+oracledb://secopr:secopr@localhost:1521/xe",
        help="SQLAlchemy DSN for Oracle (python-oracledb).",
    )
    p.add_argument("--facts-table", default="SECOPR.CN_STOCK_DAILY_PRICE")
    p.add_argument("--date-col", default="TRADE_DATE")

    args = p.parse_args()

    db_path = Path(args.db)
    cfg_path = Path(args.config)
    bootstrap(db_path, cfg_path)

    # Load active pool symbols from SQLite
    conn = connect(db_path)
    pool_rows = fetch_all(conn, "SELECT symbol FROM cn_epr_entry_pool WHERE is_active=1", ())
    conn.close()
    symbols = [str(r["symbol"]) for r in pool_rows]

    # prev_trade_date
    prev_trade_date = args.prev_trade_date
    if not prev_trade_date:
        prev_trade_date = fetch_prev_trade_date_oracle(
            oracle_dsn=args.oracle_dsn,
            facts_table=args.facts_table,
            date_col=args.date_col,
            trade_date=args.trade_date,
        )

    # Positions: prefer JSON if provided; otherwise try SQLite snapshot; else default 0.
    holding_lots_by_symbol = {s: 0 for s in symbols}
    if args.positions_json:
        holding_lots_by_symbol.update(load_positions(Path(args.positions_json)))
    else:
        conn = connect(db_path)
        try:
            rows = fetch_all(
                conn,
                "SELECT symbol, holding_lots FROM cn_epr_position_snap WHERE trade_date=?",
                (args.trade_date,),
            )
            for r in rows:
                holding_lots_by_symbol[str(r["symbol"])] = int(r["holding_lots"])
        finally:
            conn.close()

    # Executions: prefer JSON if provided; otherwise read from SQLite for the trade_date.
    if args.executions_json:
        executions = load_executions(Path(args.executions_json))
    else:
        conn = connect(db_path)
        try:
            rows = fetch_all(
                conn,
                "SELECT symbol, action, lots, price_ref, source, note FROM cn_epr_execution WHERE trade_date=?",
                (args.trade_date,),
            )
            executions = [
                {
                    "symbol": str(r["symbol"]),
                    "action": str(r["action"]),
                    "lots": int(r["lots"]),
                    "price_ref": r["price_ref"],
                    "source": r["source"],
                    "note": r["note"],
                }
                for r in rows
            ]
        finally:
            conn.close()

    report = run_t1(
        db_path=db_path,
        config_path=cfg_path,
        trade_date=args.trade_date,
        prev_trade_date=prev_trade_date,
        holding_lots_by_symbol=holding_lots_by_symbol,
        executions=executions,
        report_kind="T1",
    )

    items = report.get("items") or []
    lines = [f"[CN_EPR] T1 trade_date={args.trade_date} prev={prev_trade_date}"]
    for it in items:
        lines.append(
            f"- {it.get('symbol')} | state={it.get('state')} | hold={it.get('holding_lots')} | action={it.get('suggested_action')}"
        )
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
