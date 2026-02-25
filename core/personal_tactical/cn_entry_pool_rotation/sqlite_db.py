from __future__ import annotations

from pathlib import Path
from sqlalchemy import create_engine


def create_sqlite_engine(db_path: str | Path):
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    # future=True for SQLAlchemy 2.0 style
    return create_engine(f"sqlite:///{p.as_posix()}", future=True)
