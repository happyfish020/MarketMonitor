
from sqlalchemy import create_engine, text
import sys, datetime

DB_USER = "secopr"
DB_PASSWORD = "secopr"
DB_HOST = "localhost"
DB_PORT = "1521"
DB_SERVICE = "xe"

DSN = f"{DB_HOST}:{DB_PORT}/{DB_SERVICE}"
CONNECTION_STRING = f"oracle+oracledb://{DB_USER}:{DB_PASSWORD}@{DSN}"

engine = create_engine(CONNECTION_STRING, pool_pre_ping=True, future=True)
SCHEMA_NAME = "SECOPR"

def persist_param(run_id: str, key: str, value: str):
    sql = text(f"""
        MERGE INTO {SCHEMA_NAME}.CN_PARAM_KV_T t
        USING (SELECT :run_id run_id, :k k FROM dual) s
        ON (t.run_id = s.run_id AND t.param_key = s.k)
        WHEN MATCHED THEN UPDATE SET
            param_value = :v,
            update_ts = SYSTIMESTAMP
        WHEN NOT MATCHED THEN INSERT
            (run_id, param_key, param_value, create_ts)
        VALUES
            (:run_id, :k, :v, SYSTIMESTAMP)
    """)
    with engine.begin() as conn:
        conn.execute(sql, {"run_id": run_id, "k": key, "v": value})

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_step3b_param_persist.py <RUN_ID>")
        sys.exit(1)

    run_id = sys.argv[1]

    persist_param(run_id, "STEP3B_STATUS", "OK")
    persist_param(run_id, "STEP3B_TS", datetime.datetime.now().isoformat())

    print(f"[OK] Step3b param persisted for {run_id}")
