# -*- coding: utf-8 -*-
"""
tools/analyze_cn_epr_hits.py

Option-1: Statistical validation for CN_ENTRY_POOL_ROTATION_V1 breakout hits.

What it does
- Read HIT lines from data/cn_epr_breakout_scan.log
- For each (trade_date, symbol) hit, pull forward close series from Oracle:
    SECOPR.CN_STOCK_DAILY_PRICE (SYMBOL, TRADE_DATE, CLOSE, VOLUME)
  using SQLAlchemy + python-oracledb, with DATE binds (no string implicit compare).
- Compute:
    * ret_5d, ret_10d, ret_20d (next N trading closes vs hit close)
    * max_dd_10d, max_dd_20d (max drawdown from hit close over next N trading days)
- Write:
    data/cn_epr_hit_stats.csv (row-level)
    data/cn_epr_hit_stats_summary.csv (symbol-level aggregates)

Usage
  python tools/analyze_cn_epr_hits.py

Optional:
  python tools/analyze_cn_epr_hits.py --log-path data/cn_epr_breakout_scan.log
  python tools/analyze_cn_epr_hits.py --out-prefix data/cn_epr_hit_stats

Notes
- Non-trading days are naturally handled because we use Oracle trading rows.
- If forward window has insufficient trading rows, metrics are left blank.
"""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.engine import Engine


DSN = "oracle+oracledb://secopr:secopr@localhost:1521/xe"
TABLE = "SECOPR.CN_STOCK_DAILY_PRICE"

HIT_RE = re.compile(r"trade_date=(\d{4}-\d{2}-\d{2}).*symbols=([0-9A-Z,\.]+)")


@dataclass(frozen=True)
class Hit:
    trade_date: date
    symbol: str


def parse_hits(log_path: Path) -> List[Hit]:
    if not log_path.exists():
        raise FileNotFoundError(f"log not found: {log_path}")

    hits: List[Hit] = []
    for line in log_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if "[HIT]" not in line:
            continue
        m = HIT_RE.search(line)
        if not m:
            continue
        d = date.fromisoformat(m.group(1))
        syms = [s.strip() for s in m.group(2).split(",") if s.strip()]
        for s in syms:
            hits.append(Hit(trade_date=d, symbol=s))
    hits = sorted(set(hits), key=lambda x: (x.trade_date, x.symbol))
    return hits


def get_engine() -> Engine:
    return create_engine(
        DSN,
        pool_pre_ping=True,
        future=True,
    )


def fetch_close_series(engine: Engine, symbol: str, start: date, end: date) -> List[Tuple[date, float]]:
    """
    Fetch (TRADE_DATE, CLOSE) for a symbol between [start, end] inclusive.
    Uses DATE binds.
    """
    sql = text(f"""
        SELECT TRADE_DATE, CLOSE
        FROM {TABLE}
        WHERE SYMBOL = :sym
          AND TRADE_DATE >= :d0
          AND TRADE_DATE <= :d1
        ORDER BY TRADE_DATE
    """).bindparams(
        bindparam("sym"),
        bindparam("d0"),
        bindparam("d1"),
    )

    with engine.connect() as conn:
        rows = conn.execute(sql, {"sym": symbol, "d0": start, "d1": end}).fetchall()

    out: List[Tuple[date, float]] = []
    for td, cl in rows:
        if td is None or cl is None:
            continue
        if isinstance(td, datetime):
            td = td.date()
        out.append((td, float(cl)))
    return out


def compute_forward_metrics(series: List[Tuple[date, float]], hit_d: date) -> Dict[str, Optional[float]]:
    idx = None
    for i, (d, _) in enumerate(series):
        if d == hit_d:
            idx = i
            break
    if idx is None:
        return {
            "hit_close": None,
            "ret_5d": None, "ret_10d": None, "ret_20d": None,
            "max_dd_10d": None, "max_dd_20d": None,
        }

    hit_close = series[idx][1]
    forward = [c for (_, c) in series[idx + 1:]]

    def ret_n(n: int) -> Optional[float]:
        if len(forward) < n:
            return None
        return (forward[n - 1] / hit_close) - 1.0

    def max_dd_n(n: int) -> Optional[float]:
        if len(forward) < 1:
            return None
        w = forward[:n] if len(forward) >= n else forward
        min_close = min(w)
        return (min_close / hit_close) - 1.0

    return {
        "hit_close": hit_close,
        "ret_5d": ret_n(5),
        "ret_10d": ret_n(10),
        "ret_20d": ret_n(20),
        "max_dd_10d": max_dd_n(10),
        "max_dd_20d": max_dd_n(20),
    }


def write_csv(path: Path, rows: List[Dict[str, object]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def summarize_by_symbol(detail_rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    """
    Per-symbol aggregates:
      hit_count
      avg_ret_5d / avg_ret_10d / avg_ret_20d
      win_rate_10d (ret_10d > 0)
      avg_max_dd_10d / avg_max_dd_20d
    """
    bucket: Dict[str, List[Dict[str, object]]] = {}
    for r in detail_rows:
        sym = str(r["symbol"])
        bucket.setdefault(sym, []).append(r)

    out: List[Dict[str, object]] = []

    def _avg(vals: List[Optional[float]]) -> Optional[float]:
        vv = [v for v in vals if isinstance(v, (int, float))]
        if not vv:
            return None
        return sum(vv) / len(vv)

    for sym, rows in sorted(bucket.items()):
        ret10 = [r.get("ret_10d") for r in rows]
        win10 = [v for v in ret10 if isinstance(v, (int, float))]
        win_rate_10d = None
        if win10:
            win_rate_10d = sum(1 for v in win10 if v > 0) / len(win10)

        out.append({
            "symbol": sym,
            "hit_count": len(rows),
            "avg_ret_5d": _avg([r.get("ret_5d") for r in rows]),
            "avg_ret_10d": _avg(ret10),
            "avg_ret_20d": _avg([r.get("ret_20d") for r in rows]),
            "win_rate_10d": win_rate_10d,
            "avg_max_dd_10d": _avg([r.get("max_dd_10d") for r in rows]),
            "avg_max_dd_20d": _avg([r.get("max_dd_20d") for r in rows]),
        })
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log-path", default="data/cn_epr_breakout_scan.log")
    ap.add_argument("--out-prefix", default="data/cn_epr_hit_stats")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    log_path = (repo_root / args.log_path).resolve()
    out_prefix = (repo_root / args.out_prefix).resolve()

    hits = parse_hits(log_path)
    if not hits:
        print(f"No HIT lines found in log: {log_path}")
        return

    engine = get_engine()

    detail_rows: List[Dict[str, object]] = []
    for h in hits:
        d0 = h.trade_date
        d1 = h.trade_date + timedelta(days=40)
        series = fetch_close_series(engine, h.symbol, d0, d1)
        metrics = compute_forward_metrics(series, h.trade_date)

        detail_rows.append({
            "trade_date": h.trade_date.isoformat(),
            "symbol": h.symbol,
            **metrics,
        })

    detail_fields = ["trade_date", "symbol", "hit_close", "ret_5d", "ret_10d", "ret_20d", "max_dd_10d", "max_dd_20d"]
    write_csv(Path(str(out_prefix) + ".csv"), detail_rows, detail_fields)

    summary_rows = summarize_by_symbol(detail_rows)
    summary_fields = ["symbol", "hit_count", "avg_ret_5d", "avg_ret_10d", "avg_ret_20d", "win_rate_10d", "avg_max_dd_10d", "avg_max_dd_20d"]
    write_csv(Path(str(out_prefix) + "_summary.csv"), summary_rows, summary_fields)

    print(f"OK: wrote detail -> {out_prefix}.csv")
    print(f"OK: wrote summary -> {out_prefix}_summary.csv")


if __name__ == "__main__":
    main()
