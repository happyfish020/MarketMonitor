import argparse
import csv
import datetime as dt
import json
import logging
import os
from dataclasses import dataclass
from typing import Dict, Any, List

try:
    import oracledb  # pip install oracledb
except ImportError as e:
    raise SystemExit("Missing dependency: pip install oracledb") from e


@dataclass
class RunRow:
    run_id: str
    p_start_dt: str
    p_end_dt: str
    p_sector_type: str
    p_enter_p: float
    p_exit_p: float
    p_exit_consecutive: int
    p_top_k: int
    p_min_hold: int
    p_rebalance_freq: int
    p_weight_mode: str
    p_full_rebuild: int
    p_cost_rate: float


SUMMARY_SQL = r"""
WITH x AS (
  SELECT
    b.run_id,
    COUNT(*) AS n_days,
    SUM(b.exposed_flag) AS exposed_days,
    ROUND(SUM(b.exposed_flag)/COUNT(*),4) AS exposure_ratio,
    MAX(b.nav) AS nav_end,
    POWER(MAX(b.nav), 252/COUNT(*)) - 1 AS cagr_252,
    MIN(b.nav) AS nav_min,
    (1 - MIN(b.nav)/MAX(b.nav)) AS mdd_approx
  FROM SECOPR.CN_SECTOR_ROT_BT_DAILY_T b
  WHERE b.run_id = :run_id
  GROUP BY b.run_id
),
a AS (
  SELECT
    p.run_id,
    SUM(CASE WHEN p.action='ENTER' THEN 1 ELSE 0 END) AS n_enter,
    SUM(CASE WHEN p.action='EXIT'  THEN 1 ELSE 0 END) AS n_exit,
    SUM(CASE WHEN p.action='KEEP'  THEN 1 ELSE 0 END) AS n_keep
  FROM SECOPR.CN_SECTOR_ROT_POS_DAILY_T p
  WHERE p.run_id = :run_id
  GROUP BY p.run_id
)
SELECT
  x.run_id,
  x.n_days, x.exposed_days, x.exposure_ratio,
  x.nav_end, x.cagr_252,
  ROUND(x.mdd_approx,4) AS mdd_approx,
  a.n_enter, a.n_exit, a.n_keep,
  ROUND(a.n_keep / NULLIF(a.n_enter,0), 2) AS keep_per_enter
FROM x
LEFT JOIN a ON a.run_id = x.run_id
"""

BASELINE_SQL = r"""
SELECT run_id
FROM SECOPR.CN_SECTOR_ROT_BASELINE_T
WHERE baseline_key = :baseline_key
"""


def _now_tag() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def setup_logger(logdir: str) -> logging.Logger:
    os.makedirs(logdir, exist_ok=True)
    log_path = os.path.join(logdir, f"runner_{_now_tag()}.log")

    logger = logging.getLogger("sector_rot_runner")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)
    logger.info("log_path=%s", log_path)
    return logger


def get_env(name: str, required: bool = True) -> str:
    v = os.environ.get(name, "").strip()
    if required and not v:
        raise SystemExit(f"Missing env var: {name}")
    return v


def connect():
    user = get_env("ORACLE_USER")
    password = get_env("ORACLE_PASSWORD")
    dsn = get_env("ORACLE_DSN")
    return oracledb.connect(user=user, password=password, dsn=dsn)


def fetch_baseline_id(cur, baseline_key: str) -> str:
    cur.execute(BASELINE_SQL, baseline_key=baseline_key)
    row = cur.fetchone()
    if not row or not row[0]:
        raise RuntimeError(f"baseline_key not found: {baseline_key}")
    return row[0]


def parse_runs_csv(path: str) -> List[RunRow]:
    rows: List[RunRow] = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            r = {k.strip().lower(): (v.strip() if isinstance(v, str) else v) for k, v in raw.items() if k}

            run_id = r["run_id"]
            ep = float(r.get("ep") or r.get("p_enter_p") or r.get("enter_p") or 0)
            xp = float(r.get("xp") or r.get("p_exit_p") or r.get("exit_p") or 0)
            xc = int(float(r.get("xc") or r.get("p_exit_consecutive") or r.get("exit_consecutive") or 2))
            k = int(float(r.get("k") or r.get("p_top_k") or r.get("top_k") or 2))

            start = r.get("start") or r.get("p_start_dt")
            end = r.get("end") or r.get("p_end_dt")
            sector_type = (r.get("sector_type") or r.get("p_sector_type") or "ALL").upper()

            min_hold = int(float(r.get("min_hold") or r.get("p_min_hold") or 5))
            rf = int(float(r.get("rebalance_freq") or r.get("p_rebalance_freq") or 5))
            weight_mode = (r.get("weight_mode") or r.get("p_weight_mode") or "EQ").upper()
            full_rebuild = int(float(r.get("full_rebuild") or r.get("p_full_rebuild") or 1))
            cost_rate = float(r.get("cost_rate") or r.get("p_cost_rate") or 0.0005)

            rows.append(RunRow(
                run_id=run_id,
                p_start_dt=start,
                p_end_dt=end,
                p_sector_type=sector_type,
                p_enter_p=ep,
                p_exit_p=xp,
                p_exit_consecutive=xc,
                p_top_k=k,
                p_min_hold=min_hold,
                p_rebalance_freq=rf,
                p_weight_mode=weight_mode,
                p_full_rebuild=full_rebuild,
                p_cost_rate=cost_rate,
            ))
    return rows


def call_backtest(cur, rr: RunRow):
    # Use an anonymous PL/SQL block with named parameters (explicit + stable).
    plsql = """
    BEGIN
      SECOPR.SP_RUN_SECTOR_ROT_BACKTEST(
        p_run_id=>:p_run_id,
        p_start_dt=>TO_DATE(:p_start_dt,'YYYY-MM-DD'),
        p_end_dt=>TO_DATE(:p_end_dt,'YYYY-MM-DD'),
        p_sector_type=>:p_sector_type,
        p_enter_p=>:p_enter_p,
        p_exit_p=>:p_exit_p,
        p_exit_consecutive=>:p_exit_consecutive,
        p_top_k=>:p_top_k,
        p_min_hold=>:p_min_hold,
        p_rebalance_freq=>:p_rebalance_freq,
        p_weight_mode=>:p_weight_mode,
        p_full_rebuild=>:p_full_rebuild,
        p_cost_rate=>:p_cost_rate
      );
    END;
    """
    cur.execute(plsql, dict(
        p_run_id=rr.run_id,
        p_start_dt=rr.p_start_dt,
        p_end_dt=rr.p_end_dt,
        p_sector_type=rr.p_sector_type,
        p_enter_p=rr.p_enter_p,
        p_exit_p=rr.p_exit_p,
        p_exit_consecutive=rr.p_exit_consecutive,
        p_top_k=rr.p_top_k,
        p_min_hold=rr.p_min_hold,
        p_rebalance_freq=rr.p_rebalance_freq,
        p_weight_mode=rr.p_weight_mode,
        p_full_rebuild=rr.p_full_rebuild,
        p_cost_rate=rr.p_cost_rate,
    ))


def call_validate(cur, run_id: str):
    cur.execute("BEGIN SECOPR.SP_VALIDATE_SECTOR_ROT_RUN(:rid); END;", rid=run_id)


def call_validate_against_baseline(cur, run_id: str, baseline_id: str, min_alpha: float):
    cur.execute(
        "BEGIN SECOPR.SP_VALIDATE_AGAINST_BASELINE(p_run_id=>:rid, p_baseline_id=>:bid, p_min_alpha=>:ma); END;",
        rid=run_id, bid=baseline_id, ma=min_alpha
    )


def fetch_summary(cur, run_id: str) -> Dict[str, Any]:
    cur.execute(SUMMARY_SQL, run_id=run_id)
    row = cur.fetchone()
    if not row:
        return {"run_id": run_id, "status": "NO_BT_ROW"}
    cols = [d[0].lower() for d in cur.description]
    return dict(zip(cols, row))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", required=True, help="runs.csv path")
    ap.add_argument("--logdir", default="logs", help="log output dir")
    ap.add_argument("--baseline-key", default="DEFAULT_BASELINE", help="baseline_key in CN_SECTOR_ROT_BASELINE_T")
    ap.add_argument("--min-alpha", type=float, default=0.0, help="candidate_cagr - baseline_cagr must be >= this")
    ap.add_argument("--skip-run", action="store_true", help="skip SP_RUN, only validate + summary")
    args = ap.parse_args()

    logger = setup_logger(args.logdir)

    runs = parse_runs_csv(args.runs)
    logger.info("loaded %d runs from %s", len(runs), args.runs)

    out_csv = os.path.join(args.logdir, "results_summary.csv")
    out_jsonl = os.path.join(args.logdir, "results_summary.jsonl")

    with connect() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            baseline_id = fetch_baseline_id(cur, args.baseline_key)
            logger.info("baseline_key=%s baseline_id=%s", args.baseline_key, baseline_id)

            with open(out_csv, "w", encoding="utf-8", newline="") as fcsv,                  open(out_jsonl, "w", encoding="utf-8") as fjsonl:

                writer = None

                for rr in runs:
                    rec: Dict[str, Any] = {
                        "run_id": rr.run_id,
                        "baseline_id": baseline_id,
                        "baseline_key": args.baseline_key,
                        "min_alpha": args.min_alpha,
                        "ts": dt.datetime.now().isoformat(timespec="seconds"),
                        "params": rr.__dict__,
                    }
                    try:
                        logger.info("RUN %s", rr.run_id)

                        if not args.skip_run:
                            call_backtest(cur, rr)
                            conn.commit()
                            logger.info("SP_RUN ok: %s", rr.run_id)

                        call_validate(cur, rr.run_id)
                        logger.info("SP_VALIDATE ok: %s", rr.run_id)

                        call_validate_against_baseline(cur, rr.run_id, baseline_id, args.min_alpha)
                        logger.info("SP_VALIDATE_AGAINST_BASELINE ok: %s", rr.run_id)

                        summ = fetch_summary(cur, rr.run_id)
                        rec.update({"status": "OK", "summary": summ})

                    except Exception as e:
                        conn.rollback()
                        rec.update({"status": "ERROR", "error": str(e)})
                        logger.exception("ERROR %s", rr.run_id)

                    fjsonl.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    fjsonl.flush()

                    flat = {
                        "run_id": rec["run_id"],
                        "status": rec["status"],
                        "error": rec.get("error", ""),
                        "baseline_id": baseline_id,
                        "min_alpha": args.min_alpha,
                    }
                    summ = rec.get("summary", {}) if rec["status"] == "OK" else {}
                    for k in ["n_days","exposed_days","exposure_ratio","nav_end","cagr_252","mdd_approx","n_enter","n_exit","n_keep","keep_per_enter"]:
                        flat[k] = summ.get(k)

                    if writer is None:
                        writer = csv.DictWriter(fcsv, fieldnames=list(flat.keys()))
                        writer.writeheader()
                    writer.writerow(flat)
                    fcsv.flush()

    logger.info("done. summary_csv=%s jsonl=%s", out_csv, out_jsonl)


if __name__ == "__main__":
    main()
