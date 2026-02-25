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
from core.personal_tactical.cn_entry_pool_rotation.config_loader import load_config
from core.personal_tactical.cn_entry_pool_rotation.db import connect, fetch_all
from core.personal_tactical.cn_entry_pool_rotation.engine import run_eod
from core.personal_tactical.cn_entry_pool_rotation.io_loaders import load_facts, load_positions
from core.personal_tactical.cn_entry_pool_rotation.oracle_facts import (
    fetch_oracle_facts,
    fetch_prev_trade_date_oracle,
)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--trade-date", required=True)
    p.add_argument("--prev-trade-date", default=None)
    # Defaults are resolved relative to project root (鈥?MarketMonitor),
    # so user only needs to pass --trade-date.
    project_root = Path(__file__).resolve().parents[1]
    p.add_argument("--db", default=str(project_root / "data" / "cn_entry_pool_rotation.db"))
    p.add_argument(
        "--config",
        default=str(project_root / "config" / "cn_entry_pool_rotation.yaml"),
    )

    # Facts input: either JSON or Oracle
    p.add_argument("--facts-json", default=None)
    p.add_argument("--facts-provider", choices=["json", "oracle"], default="oracle")
    p.add_argument(
        "--oracle-dsn",
        default="oracle+oracledb://secopr:secopr@localhost:1521/xe",
        help="SQLAlchemy DSN for Oracle (python-oracledb).",
    )
    p.add_argument(
        "--facts-table",
        default="SECOPR.CN_STOCK_DAILY_PRICE",
        help="Oracle daily price table to read close/volume.",
    )
    p.add_argument("--symbol-col", default="SYMBOL")
    p.add_argument("--date-col", default="TRADE_DATE")
    p.add_argument("--close-col", default="CLOSE")
    p.add_argument("--volume-col", default="VOLUME")

    # Positions: optional; if omitted defaults to 0 for all symbols
    p.add_argument("--positions-json", default=None)

    args = p.parse_args()

    db_path = Path(args.db)
    cfg_path = Path(args.config)
    bootstrap(db_path, cfg_path)
    _ = load_config(cfg_path)  # config is used by bootstrap + future extensions

    # Load active pool symbols from SQLite
    conn = connect(db_path)
    pool_rows = fetch_all(conn, "SELECT symbol FROM cn_epr_entry_pool WHERE is_active=1", ())
    conn.close()
    symbols = [str(r["symbol"]) for r in pool_rows]

    # Determine prev_trade_date
    prev_trade_date = args.prev_trade_date
    if not prev_trade_date:
        if args.facts_provider == "oracle":
            prev_trade_date = fetch_prev_trade_date_oracle(
                oracle_dsn=args.oracle_dsn,
                facts_table=args.facts_table,
                date_col=args.date_col,
                trade_date=args.trade_date,
            )
        else:
            # For JSON provider, caller should pass --prev-trade-date for correct holiday/weekend handling.
            prev_trade_date = args.trade_date

    # Load facts
    if args.facts_provider == "json":
        if not args.facts_json:
            raise SystemExit("--facts-json is required when --facts-provider=json")
        facts_by_symbol = load_facts(Path(args.facts_json))
    else:
        facts_by_symbol = fetch_oracle_facts(
            oracle_dsn=args.oracle_dsn,
            facts_table=args.facts_table,
            symbol_col=args.symbol_col,
            date_col=args.date_col,
            close_col=args.close_col,
            volume_col=args.volume_col,
            trade_date=args.trade_date,
            symbols=symbols,
        )

    # Load positions (optional)
    holding_lots_by_symbol = {s: 0 for s in symbols}
    if args.positions_json:
        holding_lots_by_symbol.update(load_positions(Path(args.positions_json)))

    report = run_eod(
        db_path=db_path,
        config_path=cfg_path,
        trade_date=args.trade_date,
        prev_trade_date=prev_trade_date,
        facts_by_symbol=facts_by_symbol,
        holding_lots_by_symbol=holding_lots_by_symbol,
        report_kind="EOD",
    )

    # Simple CLI output
    items = report.get("items") or []
    lines = [f"[CN_EPR] EOD trade_date={args.trade_date} prev={prev_trade_date}"]
    for it in items:
        lines.append(
            f"- {it.get('symbol')} {it.get('name') or ''} | {it.get('group_code')} | state={it.get('state')} | hold={it.get('holding_lots')} | action={it.get('suggested_action')}"
        )
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
