
# Step 3b (P1) — Baseline Compare & Decide (UnifiedRisk / Sector Rotation)

This toolkit provides a **single, managed** implementation of Step 3b:
- Compare a candidate run against the pinned baseline (DEFAULT_BASELINE)
- Produce a **PASS/REJECT** decision using **SQL facts**
- Persist the decision into `SECOPR.CN_BASELINE_DECISION_T`

## Prerequisites
- python >= 3.10
- `pip install -r requirements.txt`
- Oracle connectivity via **SQLAlchemy + python-oracledb**
- Baseline pinned in `SECOPR.CN_BASELINE_REGISTRY_T` where `BASELINE_KEY='DEFAULT_BASELINE'`

## Install
```bash
pip install -r requirements.txt
```

## 1) Create/Replace SP + Decision Table
Run the SQL in `sp/` (SQL*Plus/SQLcl/DBeaver):
- `sp/DDL_CN_BASELINE_DECISION_T.sql`
- `sp/SP_COMPARE_AND_DECIDE_BASELINE.sql`

## 2) Compare a single run
```bash
python run_compare_and_decide.py <CANDIDATE_RUN_ID>
```

## 3) Batch compare by runs.csv (optional)
```bash
python validate_runs_csv_step3b.py runs.csv
```
Outputs:
- `step3b_pass.csv`
- `step3b_fail.csv`
- `step3b_compare.log`
