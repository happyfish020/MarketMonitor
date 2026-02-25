
"""
Step 3b (P1): Compare a candidate run against the pinned DEFAULT_BASELINE and record a PASS/REJECT decision.

Usage:
    python run_compare_and_decide.py <CANDIDATE_RUN_ID>

Assumptions (frozen):
- Baseline is stored in SECOPR.CN_BASELINE_REGISTRY_T with BASELINE_KEY='DEFAULT_BASELINE'.
- Backtest metrics are derived from SECOPR.CN_SECTOR_ROT_BT_DAILY_T.
- Decision is recorded into SECOPR.CN_BASELINE_DECISION_T.
- DB connectivity uses SQLAlchemy + python-oracledb (NO cx_Oracle).
"""

import sys
from sqlalchemy import create_engine, text

# ==============================
#        Database Configuration
# ==============================
DB_USER = "secopr"
DB_PASSWORD = "secopr"
DB_HOST = "localhost"
DB_PORT = "1521"
DB_SERVICE = "xe"

DSN = f"{DB_HOST}:{DB_PORT}/{DB_SERVICE}"
CONNECTION_STRING = f"oracle+oracledb://{DB_USER}:{DB_PASSWORD}@{DSN}"

engine = create_engine(
    CONNECTION_STRING,
    pool_pre_ping=True,
    future=True,
)

SCHEMA_NAME = "SECOPR"

def main():
    if len(sys.argv) != 2:
        print("Usage: python run_compare_and_decide.py <CANDIDATE_RUN_ID>")
        sys.exit(2)

    cand_run_id = sys.argv[1].strip()
    if not cand_run_id:
        print("[ERROR] empty run_id")
        sys.exit(2)

    # Run SP_COMPARE_AND_DECIDE_BASELINE
    plsql = text(f"""
    BEGIN
        {SCHEMA_NAME}.SP_COMPARE_AND_DECIDE_BASELINE(p_candidate_run_id => :rid);
    END;
    """)

    with engine.begin() as conn:
        conn.execute(plsql, {"rid": cand_run_id})

        # Fetch latest decision row for this candidate
        q = text(f"""
            SELECT baseline_run_id, candidate_run_id, decision, reason,
                   baseline_cagr_252, cand_cagr_252,
                   baseline_mdd_approx, cand_mdd_approx,
                   baseline_exposure_ratio, cand_exposure_ratio,
                   create_ts
            FROM {SCHEMA_NAME}.CN_BASELINE_DECISION_T
            WHERE candidate_run_id = :rid
            ORDER BY create_ts DESC
            FETCH FIRST 1 ROWS ONLY
        """)
        row = conn.execute(q, {"rid": cand_run_id}).mappings().first()

    if not row:
        print(f"[ERROR] No decision row found for {cand_run_id}. Check SP/table permissions.")
        sys.exit(1)

    print("[DECISION]"
          f" baseline={row['baseline_run_id']} candidate={row['candidate_run_id']}"
          f" decision={row['decision']} reason={row['reason']}")
    print("[METRIC]"
          f" cagr_252 base={row['baseline_cagr_252']} cand={row['cand_cagr_252']} |"
          f" mdd base={row['baseline_mdd_approx']} cand={row['cand_mdd_approx']} |"
          f" exposure base={row['baseline_exposure_ratio']} cand={row['cand_exposure_ratio']}")
    print("[OK] Step3b compare+decide completed")


if __name__ == "__main__":
    main()
