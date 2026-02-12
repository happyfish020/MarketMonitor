#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Sector Rotation XP Sweep Runner (Python v2)

Goal
- Second-stage local sweep focused on XP (exit_p), keeping EP/XC fixed.
- Writes logs + runs.csv + summary.sql.

Default sweep (based on your v1 grid results)
- EP=0.90 (fixed; EP not sensitive in v1)
- XC=2   (fixed; good tradeoff in v1)
- K in {2,3}
- XP from 0.46 to 0.58 step 0.01  (13 values)
Total runs: 26

Env vars (required)
- ORA_USER, ORA_PASS, ORA_DSN

Run
- pip install -r requirements.txt
- python xp_sweep_runner.py --prefix SR_XP_SWEEP_V1 --tag XPSWEEP
"""

import os, sys, csv, json, time, argparse, logging
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Tuple

import oracledb
try:
    import pandas as pd
except Exception:
    pd = None


@dataclass(frozen=True)
class SweepConfig:
    start_dt: str
    end_dt: str
    sector_type: str
    min_hold: int
    rebalance_freq: int
    weight_mode: str
    full_rebuild: int
    prefix: str
    tag: str

    ep: float
    xc: int
    k_list: List[int]
    xp_list: List[float]


def setup_logging(log_dir: str) -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger("xp_sweep")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(os.path.join(log_dir, "xp_sweep.log"), encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(logging.INFO)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    sh.setLevel(logging.INFO)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger

 
def get_conn(logger: logging.Logger):
    #user = os.environ.get("ORA_USER")
    #pwd  = os.environ.get("ORA_PASS")
    #dsn  = os.environ.get("ORA_DSN")
    DB_USER = "secopr"
    DB_PASSWORD = "secopr"
    DB_HOST = "localhost"
    DB_PORT = "1521"
    DB_SERVICE = "xe"
    
    DSN = f"{DB_HOST}:{DB_PORT}/{DB_SERVICE}"

    if not (DB_USER and DB_PASSWORD and DSN):
        raise RuntimeError("Missing ORA_USER / ORA_PASS / ORA_DSN env vars.")

    logger.info(f"Connecting to Oracle DSN={DSN} as {DB_USER}")
    return oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=DSN)

def run_id_of(prefix: str, ep: float, xp: float, xc: int, mh: int, rf: int, k: int, tag: str) -> str:
    ep_i = int(round(ep * 100))
    xp_i = int(round(xp * 100))
    return f"{prefix}_EP{ep_i}_XP{xp_i}_XC{xc}_MH{mh}_RF{rf}_K{k}_{tag}"


def call_backtest(cur, cfg: SweepConfig, run_id: str, xp: float, k: int, logger: logging.Logger):
    """
    Assumed signature:
      (p_run_id, p_start_dt, p_end_dt, p_sector_type, p_enter_p, p_exit_p, p_exit_consecutive,
       p_top_k, p_min_hold, p_rebalance_freq, p_weight_mode, p_full_rebuild)
    """
    logger.info(f"[CALL] SP_RUN_SECTOR_ROT_BACKTEST run_id={run_id} ep={cfg.ep} xp={xp} xc={cfg.xc} k={k}")
    cur.callproc("SECOPR.SP_RUN_SECTOR_ROT_BACKTEST", [
        run_id,
        datetime.strptime(cfg.start_dt, "%Y-%m-%d").date(),
        datetime.strptime(cfg.end_dt, "%Y-%m-%d").date(),
        cfg.sector_type,
        float(cfg.ep),
        float(xp),
        int(cfg.xc),
        int(k),
        int(cfg.min_hold),
        int(cfg.rebalance_freq),
        cfg.weight_mode,
        int(cfg.full_rebuild),
    ])


def call_validate(cur, run_id: str, logger: logging.Logger):
    logger.info(f"[CALL] SP_VALIDATE_SECTOR_ROT_RUN run_id={run_id}")
    cur.callproc("SECOPR.SP_VALIDATE_SECTOR_ROT_RUN", [run_id])


def build_combos(cfg: SweepConfig) -> List[Tuple[float, int]]:
    return [(xp, k) for xp in cfg.xp_list for k in cfg.k_list]


def write_summary_sql(out_path: str, prefix: str):
    sql = f"""
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
  WHERE b.run_id LIKE '{prefix}%'
  GROUP BY b.run_id
),
a AS (
  SELECT
    p.run_id,
    SUM(CASE WHEN p.action='ENTER' THEN 1 ELSE 0 END) AS n_enter,
    SUM(CASE WHEN p.action='EXIT'  THEN 1 ELSE 0 END) AS n_exit,
    SUM(CASE WHEN p.action='KEEP'  THEN 1 ELSE 0 END) AS n_keep
  FROM SECOPR.CN_SECTOR_ROT_POS_DAILY_T p
  WHERE p.run_id LIKE '{prefix}%'
  GROUP BY p.run_id
)
SELECT
  x.run_id,
  x.exposure_ratio, x.exposed_days, x.nav_end, x.cagr_252,
  ROUND(x.mdd_approx,4) AS mdd_approx,
  a.n_enter, a.n_exit, a.n_keep,
  ROUND(a.n_keep / NULLIF(a.n_enter,0), 2) AS keep_per_enter
FROM x
LEFT JOIN a ON a.run_id = x.run_id
ORDER BY
  x.cagr_252 DESC, x.mdd_approx ASC;
"""
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(sql.strip() + "\n")


def frange(start: float, end: float, step: float) -> List[float]:
    vals = []
    x = start
    while x <= end + 1e-9:
        vals.append(round(x, 2))
        x += step
    return vals


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2022-11-07")
    ap.add_argument("--end", default="2026-01-23")
    ap.add_argument("--sector-type", default="ALL")
    ap.add_argument("--min-hold", type=int, default=5)
    ap.add_argument("--rebalance-freq", type=int, default=5)
    ap.add_argument("--weight-mode", default="EQ")
    ap.add_argument("--full-rebuild", type=int, default=1)
    ap.add_argument("--prefix", default="SR_XP_SWEEP_V1")
    ap.add_argument("--tag", default="XPSWEEP")
    ap.add_argument("--ep", type=float, default=0.90)
    ap.add_argument("--xc", type=int, default=2)
    ap.add_argument("--k-list", default="2,3")
    ap.add_argument("--xp-start", type=float, default=0.46)
    ap.add_argument("--xp-end", type=float, default=0.58)
    ap.add_argument("--xp-step", type=float, default=0.01)
    ap.add_argument("--sleep-sec", type=float, default=0.0)
    args = ap.parse_args()

    k_list = [int(x.strip()) for x in args.k_list.split(",") if x.strip()]
    xp_list = frange(args.xp_start, args.xp_end, args.xp_step)

    cfg = SweepConfig(
        start_dt=args.start,
        end_dt=args.end,
        sector_type=args.sector_type,
        min_hold=args.min_hold,
        rebalance_freq=args.rebalance_freq,
        weight_mode=args.weight_mode,
        full_rebuild=args.full_rebuild,
        prefix=args.prefix,
        tag=args.tag,
        ep=float(args.ep),
        xc=int(args.xc),
        k_list=k_list,
        xp_list=xp_list,
    )

    root = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(root, "logs")
    out_dir = os.path.join(root, "outputs")
    os.makedirs(out_dir, exist_ok=True)

    logger = setup_logging(log_dir)
    logger.info("=== XP Sweep Runner (Python v2) ===")
    logger.info("Config: " + json.dumps(asdict(cfg), ensure_ascii=False))

    write_summary_sql(os.path.join(out_dir, "summary.sql"), cfg.prefix)

    combos = build_combos(cfg)
    logger.info(f"Total runs: {len(combos)} (xp={len(cfg.xp_list)} * k={len(cfg.k_list)})")

    results = []
    conn = get_conn(logger)
    try:
        conn.autocommit = False
        cur = conn.cursor()

        for i, (xp, k) in enumerate(combos, start=1):
            run_id = run_id_of(cfg.prefix, cfg.ep, xp, cfg.xc, cfg.min_hold, cfg.rebalance_freq, k, cfg.tag)
            logger.info(f"--- [{i}/{len(combos)}] run_id={run_id} ---")

            row = {
                "run_id": run_id,
                "ep": cfg.ep, "xp": xp, "xc": cfg.xc, "k": k,
                "start": cfg.start_dt, "end": cfg.end_dt,
                "sector_type": cfg.sector_type,
                "min_hold": cfg.min_hold,
                "rebalance_freq": cfg.rebalance_freq,
                "weight_mode": cfg.weight_mode,
                "full_rebuild": cfg.full_rebuild,
                "status": "INIT",
                "error": "",
                "ts_start": datetime.now().isoformat(timespec="seconds"),
                "ts_end": ""
            }

            try:
                call_backtest(cur, cfg, run_id, xp, k, logger)
                conn.commit()
                call_validate(cur, run_id, logger)
                conn.commit()
                row["status"] = "OK"
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                row["status"] = "FAIL"
                row["error"] = str(e).replace("\\n", " ")[:1500]
                logger.error(f"[FAIL] run_id={run_id} err={row['error']}")
            finally:
                row["ts_end"] = datetime.now().isoformat(timespec="seconds")
                results.append(row)

            if args.sleep_sec and args.sleep_sec > 0:
                time.sleep(args.sleep_sec)

    finally:
        try:
            conn.close()
        except Exception:
            pass

    csv_path = os.path.join(out_dir, "runs.csv")
    if pd is not None:
        pd.DataFrame(results).to_csv(csv_path, index=False, encoding="utf-8-sig")
    else:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            w.writeheader()
            w.writerows(results)

    ok = sum(1 for r in results if r["status"] == "OK")
    logger.info(f"Done. OK={ok} FAIL={len(results)-ok}")
    logger.info(f"Outputs: {csv_path} and outputs/summary.sql")


if __name__ == "__main__":
    main()
