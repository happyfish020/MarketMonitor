Step 3 (P1) Toolkit (SQLAlchemy Edition)
=======================================

Contents
--------
- SP_VALIDATE_AGAINST_BASELINE.sql
- run_validate_baseline.py

Prereqs
-------
- Python package: sqlalchemy
- Oracle driver: python-oracledb (SQLAlchemy dialect: oracle+oracledb)

Usage
-----
1) Install dependencies
   pip install sqlalchemy oracledb

2) Create/replace procedure
   Run SP_VALIDATE_AGAINST_BASELINE.sql in your SQL client.

3) Validate a run_id from Python
   python run_validate_baseline.py SR_BASE_V535_EP90_XP55_XC2_MH5_RF5_K2_COST5BPS

4) Or validate from SQL
   BEGIN SECOPR.SP_VALIDATE_AGAINST_BASELINE('SR_BASE_...'); END;
