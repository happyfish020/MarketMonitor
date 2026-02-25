from __future__ import annotations

from pathlib import Path
from typing import Union, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, Connection


SQLiteArg = Union[str, Path, Engine]


def _is_engine(obj) -> bool:
    # Avoid importing SQLAlchemy internals; be permissive.
    return hasattr(obj, "connect") and obj.__class__.__name__.lower().endswith("engine")


def ensure_sqlite_engine(db: SQLiteArg, *, echo: bool = False) -> Engine:
    """
    Accept either:
      - a SQLAlchemy Engine (returned as-is)
      - a filesystem path / string path to sqlite file (creates an Engine)

    This function exists to make the subsystem resilient to call-site drift:
    some callers pass db_path (Path/str), others pass an Engine.
    """
    if _is_engine(db):
        return db  # type: ignore[return-value]

    db_path = Path(db)  # type: ignore[arg-type]
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Use absolute path to avoid cwd surprises.
    uri = f"sqlite:///{db_path.resolve().as_posix()}"
    return create_engine(uri, future=True, echo=echo)


def connect(db: SQLiteArg, *, echo: bool = False) -> Connection:
    """
    Backward compatible helper used by older bootstrap code.
    - If db is Engine: returns db.connect()
    - Else: creates sqlite engine from path then connects
    """
    eng = ensure_sqlite_engine(db, echo=echo)
    return eng.connect()
