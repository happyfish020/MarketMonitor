
"""
Batch Step3b: run SP_COMPARE_AND_DECIDE_BASELINE for each run_id in a CSV file.

CSV requirement:
- Must contain a header with column 'run_id' OR first column is run_id.

Usage:
    python validate_runs_csv_step3b.py runs.csv

Outputs:
- step3b_pass.csv
- step3b_fail.csv
- step3b_compare.log
"""

import csv
import sys
from pathlib import Path
from sqlalchemy import create_engine, text

DB_USER = "secopr"
DB_PASSWORD = "secopr"
DB_HOST = "localhost"
DB_PORT = "1521"
DB_SERVICE = "xe"

DSN = f"{DB_HOST}:{DB_PORT}/{DB_SERVICE}"
CONNECTION_STRING = f"oracle+oracledb://{DB_USER}:{DB_PASSWORD}@{DSN}"

engine = create_engine(CONNECTION_STRING, pool_pre_ping=True, future=True)
SCHEMA_NAME = "SECOPR"

def iter_run_ids(csv_path: Path):
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
    if not rows:
        return
    header = [c.strip().lower() for c in rows[0]]
    if "run_id" in header:
        idx = header.index("run_id")
        for r in rows[1:]:
            if len(r) > idx and r[idx].strip():
                yield r[idx].strip()
    else:
        for r in rows[1:]:
            if r and r[0].strip():
                yield r[0].strip()

def main():
    if len(sys.argv) != 2:
        print("Usage: python validate_runs_csv_step3b.py <runs.csv>")
        sys.exit(2)

    csv_path = Path(sys.argv[1])
    if not csv_path.exists():
        print(f"[ERROR] file not found: {csv_path}")
        sys.exit(2)

    pass_rows = []
    fail_rows = []

    log_path = Path("step3b_compare.log")
    with log_path.open("w", encoding="utf-8") as log:
        for rid in iter_run_ids(csv_path):
            try:
                plsql = text(f"BEGIN {SCHEMA_NAME}.SP_COMPARE_AND_DECIDE_BASELINE(p_candidate_run_id => :rid); END;")
                with engine.begin() as conn:
                    conn.execute(plsql, {"rid": rid})
                    q = text(f"""
                        SELECT decision, reason
                        FROM {SCHEMA_NAME}.CN_BASELINE_DECISION_T
                        WHERE candidate_run_id = :rid
                        ORDER BY create_ts DESC
                        FETCH FIRST 1 ROWS ONLY
                    """)
                    row = conn.execute(q, {"rid": rid}).mappings().first()

                decision = (row or {}).get("decision", "UNKNOWN")
                reason = (row or {}).get("reason", "NO_ROW")
                log.write(f"{rid}\t{decision}\t{reason}\n")
                if decision == "PASS":
                    pass_rows.append({"run_id": rid, "decision": decision, "reason": reason})
                else:
                    fail_rows.append({"run_id": rid, "decision": decision, "reason": reason})
            except Exception as e:
                msg = str(e)
                log.write(f"{rid}\tERROR\t{msg}\n")
                fail_rows.append({"run_id": rid, "decision": "ERROR", "reason": msg})

    def write_out(path, rows):
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["run_id", "decision", "reason"])
            w.writeheader()
            w.writerows(rows)

    write_out("step3b_pass.csv", pass_rows)
    write_out("step3b_fail.csv", fail_rows)

    print(f"[OK] batch completed: pass={len(pass_rows)} fail={len(fail_rows)} (see step3b_compare.log)")

if __name__ == "__main__":
    main()
