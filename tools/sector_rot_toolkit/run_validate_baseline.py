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


def validate_against_baseline(run_id: str) -> None:
    # Use an anonymous PL/SQL block to call the procedure
    plsql = text(f"""
    BEGIN
        {SCHEMA_NAME}.SP_VALIDATE_AGAINST_BASELINE(:run_id);
    END;
    """)
    with engine.begin() as conn:
        conn.execute(plsql, {"run_id": run_id})


def main() -> None:
    import argparse

    p = argparse.ArgumentParser(description="Validate a sector-rotation run against DEFAULT_BASELINE.")
    p.add_argument("run_id", help="Run ID to validate, e.g. SR_BASE_V535_EP90_XP55_XC2_MH5_RF5_K2_COST5BPS")
    args = p.parse_args()

    validate_against_baseline(args.run_id)
    print("[OK] Baseline validation completed for", args.run_id)
    # >>>>>>>>[OK] Baseline validation completed for SR_BASE_V535_EP90_XP55_XC2_MH5_RF5_K2_COST5BPS  

if __name__ == "__main__":
    main()
  