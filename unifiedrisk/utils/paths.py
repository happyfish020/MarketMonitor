
import os
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = _THIS_FILE.parents[2]

def get_project_root() -> Path:
    return PROJECT_ROOT

def get_data_dir() -> Path:
    d = PROJECT_ROOT / "data"
    d.mkdir(parents=True, exist_ok=True)
    return d

def get_logs_dir() -> Path:
    d = PROJECT_ROOT / "logs"
    d.mkdir(parents=True, exist_ok=True)
    return d

def get_reports_dir() -> Path:
    d = PROJECT_ROOT / "reports"
    d.mkdir(parents=True, exist_ok=True)
    return d

def ensure_dir(path: os.PathLike) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)
