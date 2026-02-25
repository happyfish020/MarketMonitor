# -*- coding: utf-8 -*-
"""
tools/analyze_cn_epr_hits_v2.py

Enhanced statistical validation for CN_ENTRY_POOL_ROTATION_V1 breakout hits.

Input
- data/cn_epr_breakout_scan.log   (lines like: [HIT] ... trade_date=YYYY-MM-DD symbols=300054,603986)

Oracle Source (frozen)
- SECOPR.CN_STOCK_DAILY_PRICE(SYMBOL, TRADE_DATE, CLOSE, VOLUME)
- DSN: oracle+oracledb://secopr:secopr@localhost:1521/xe
- Uses DATE binds (no implicit string compare)

Outputs (CSV)
- <out_prefix>.csv                        (detail rows)
- <out_prefix>_summary_by_symbol.csv      (symbol aggregates)
- <out_prefix>_summary_overall.csv        (overall aggregates)
- <out_prefix>_summary_by_strata.csv      (strata aggregates, based on next-day move)
- <out_prefix>_worst_hits.csv             (worst hits by max_dd_10d, then ret_10d)

Strata definition (close-to-close proxy, no open price needed)
- nextday_surge:   ret_1d >= +5%
- normal:          -5% < ret_1d < +5%
- immediate_fail:  ret_1d <= -5%

Usage
  python tools/analyze_cn_epr_hits_v2.py

Options
  --log-path     default: data/cn_epr_breakout_scan.log
  --out-prefix   default: data/cn_epr_hit_stats_v2
  --worst-n      default: 20
  --calendar-window-days default: 60   (calendar days to fetch forward series; should cover 20 trading days)
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


def _find_index(series: List[Tuple[date, float]], hit_d: date) -> Optional[int]:
    for i, (d, _) in enumerate(series):
        if d == hit_d:
            return i
    return None


def compute_forward_metrics(series: List[Tuple[date, float]], hit_d: date) -> Dict[str, Optional[float]]:
    idx = _find_index(series, hit_d)
    if idx is None:
        return {
            "hit_close": None,
            "ret_1d": None, "ret_5d": None, "ret_10d": None, "ret_20d": None,
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
        "ret_1d": ret_n(1),
        "ret_5d": ret_n(5),
        "ret_10d": ret_n(10),
        "ret_20d": ret_n(20),
        "max_dd_10d": max_dd_n(10),
        "max_dd_20d": max_dd_n(20),
    }


def classify_strata(ret_1d: Optional[float]) -> str:
    if ret_1d is None:
        return "unknown"
    if ret_1d >= 0.05:
        return "nextday_surge"
    if ret_1d <= -0.05:
        return "immediate_fail"
    return "normal"


def write_csv(path: Path, rows: List[Dict[str, object]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _avg(vals: List[Optional[float]]) -> Optional[float]:
    vv = [v for v in vals if isinstance(v, (int, float))]
    if not vv:
        return None
    return sum(vv) / len(vv)


def _pct(vals: List[float], p: float) -> Optional[float]:
    if not vals:
        return None
    s = sorted(vals)
    k = (len(s) - 1) * p
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


def summarize_overall(detail_rows: List[Dict[str, object]]) -> Dict[str, object]:
    ret10 = [r.get("ret_10d") for r in detail_rows]
    ret10v = [v for v in ret10 if isinstance(v, (int, float))]
    win10 = None
    if ret10v:
        win10 = sum(1 for v in ret10v if v > 0) / len(ret10v)

    dd10 = [r.get("max_dd_10d") for r in detail_rows]
    dd10v = [v for v in dd10 if isinstance(v, (int, float))]

    return {
        "hit_count": len(detail_rows),
        "valid_ret10_count": len(ret10v),
        "win_rate_10d": win10,
        "avg_ret_5d": _avg([r.get("ret_5d") for r in detail_rows]),
        "avg_ret_10d": _avg(ret10),
        "avg_ret_20d": _avg([r.get("ret_20d") for r in detail_rows]),
        "p25_ret_10d": _pct(ret10v, 0.25) if ret10v else None,
        "p50_ret_10d": _pct(ret10v, 0.50) if ret10v else None,
        "p75_ret_10d": _pct(ret10v, 0.75) if ret10v else None,
        "avg_max_dd_10d": _avg(dd10),
        "p50_max_dd_10d": _pct(dd10v, 0.50) if dd10v else None,
        "worst_max_dd_10d": min(dd10v) if dd10v else None,
        "best_ret_20d": max([v for v in [r.get("ret_20d") for r in detail_rows] if isinstance(v,(int,float))], default=None),
        "worst_ret_20d": min([v for v in [r.get("ret_20d") for r in detail_rows] if isinstance(v,(int,float))], default=None),
    }


def summarize_by_symbol(detail_rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    bucket: Dict[str, List[Dict[str, object]]] = {}
    for r in detail_rows:
        sym = str(r["symbol"])
        bucket.setdefault(sym, []).append(r)

    out: List[Dict[str, object]] = []
    for sym, rows in sorted(bucket.items()):
        ret10 = [r.get("ret_10d") for r in rows]
        win10v = [v for v in ret10 if isinstance(v, (int, float))]
        win_rate_10d = None
        if win10v:
            win_rate_10d = sum(1 for v in win10v if v > 0) / len(win10v)

        out.append({
            "symbol": sym,
            "hit_count": len(rows),
            "avg_ret_1d": _avg([r.get("ret_1d") for r in rows]),
            "avg_ret_5d": _avg([r.get("ret_5d") for r in rows]),
            "avg_ret_10d": _avg(ret10),
            "avg_ret_20d": _avg([r.get("ret_20d") for r in rows]),
            "win_rate_10d": win_rate_10d,
            "avg_max_dd_10d": _avg([r.get("max_dd_10d") for r in rows]),
            "avg_max_dd_20d": _avg([r.get("max_dd_20d") for r in rows]),
        })
    return out


def summarize_by_strata(detail_rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    bucket: Dict[str, List[Dict[str, object]]] = {}
    for r in detail_rows:
        st = str(r.get("strata") or "unknown")
        bucket.setdefault(st, []).append(r)

    out: List[Dict[str, object]] = []
    for st, rows in sorted(bucket.items()):
        out.append({
            "strata": st,
            "hit_count": len(rows),
            "avg_ret_10d": _avg([r.get("ret_10d") for r in rows]),
            "win_rate_10d": (lambda vv: (sum(1 for v in vv if v > 0) / len(vv)) if vv else None)(
                [v for v in [r.get("ret_10d") for r in rows] if isinstance(v,(int,float))]
            ),
            "avg_max_dd_10d": _avg([r.get("max_dd_10d") for r in rows]),
        })
    return out


def build_worst_hits(detail_rows: List[Dict[str, object]], worst_n: int) -> List[Dict[str, object]]:
    def key(r):
        dd = r.get("max_dd_10d")
        rt = r.get("ret_10d")
        ddv = dd if isinstance(dd,(int,float)) else 999.0
        rtv = rt if isinstance(rt,(int,float)) else 999.0
        return (ddv, rtv)

    rows = sorted(detail_rows, key=key)[:worst_n]
    out = []
    for r in rows:
        out.append({
            "trade_date": r.get("trade_date"),
            "symbol": r.get("symbol"),
            "hit_close": r.get("hit_close"),
            "ret_1d": r.get("ret_1d"),
            "ret_10d": r.get("ret_10d"),
            "ret_20d": r.get("ret_20d"),
            "max_dd_10d": r.get("max_dd_10d"),
            "max_dd_20d": r.get("max_dd_20d"),
            "strata": r.get("strata"),
        })
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log-path", default="data/cn_epr_breakout_scan.log")
    ap.add_argument("--out-prefix", default="data/cn_epr_hit_stats_v2")
    ap.add_argument("--worst-n", type=int, default=20)
    ap.add_argument("--calendar-window-days", type=int, default=60)
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
        d1 = h.trade_date + timedelta(days=int(args.calendar_window_days))
        series = fetch_close_series(engine, h.symbol, d0, d1)
        metrics = compute_forward_metrics(series, h.trade_date)
        strata = classify_strata(metrics.get("ret_1d"))

        detail_rows.append({
            "trade_date": h.trade_date.isoformat(),
            "symbol": h.symbol,
            "strata": strata,
            **metrics,
        })

    # Detail
    detail_fields = [
        "trade_date", "symbol", "strata",
        "hit_close",
        "ret_1d", "ret_5d", "ret_10d", "ret_20d",
        "max_dd_10d", "max_dd_20d",
    ]
    write_csv(Path(str(out_prefix) + ".csv"), detail_rows, detail_fields)

    # Summary overall
    overall = summarize_overall(detail_rows)
    write_csv(Path(str(out_prefix) + "_summary_overall.csv"), [overall], list(overall.keys()))

    # Summary by symbol
    sym_rows = summarize_by_symbol(detail_rows)
    sym_fields = ["symbol","hit_count","avg_ret_1d","avg_ret_5d","avg_ret_10d","avg_ret_20d","win_rate_10d","avg_max_dd_10d","avg_max_dd_20d"]
    write_csv(Path(str(out_prefix) + "_summary_by_symbol.csv"), sym_rows, sym_fields)

    # Summary by strata
    st_rows = summarize_by_strata(detail_rows)
    st_fields = ["strata","hit_count","avg_ret_10d","win_rate_10d","avg_max_dd_10d"]
    write_csv(Path(str(out_prefix) + "_summary_by_strata.csv"), st_rows, st_fields)

    # Worst hits
    worst_rows = build_worst_hits(detail_rows, int(args.worst_n))
    worst_fields = ["trade_date","symbol","hit_close","ret_1d","ret_10d","ret_20d","max_dd_10d","max_dd_20d","strata"]
    write_csv(Path(str(out_prefix) + "_worst_hits.csv"), worst_rows, worst_fields)

    print(f"OK: wrote detail -> {out_prefix}.csv")
    print(f"OK: wrote overall -> {out_prefix}_summary_overall.csv")
    print(f"OK: wrote by_symbol -> {out_prefix}_summary_by_symbol.csv")
    print(f"OK: wrote by_strata -> {out_prefix}_summary_by_strata.csv")
    print(f"OK: wrote worst_hits -> {out_prefix}_worst_hits.csv")


if __name__ == "__main__":
    main()
