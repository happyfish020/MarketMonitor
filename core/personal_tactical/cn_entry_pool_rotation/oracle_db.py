from __future__ import annotations

from sqlalchemy import create_engine


def create_oracle_engine():
    # User frozen constraint: SQLAlchemy + python-oracledb
    dsn = "oracle+oracledb://secopr:secopr@localhost:1521/xe"
    return create_engine(dsn, pool_pre_ping=True, future=True)
