
from pathlib import Path

VERSION = "GlobalMultiRisk v5.4.full"

BASE_DIR = Path(".").resolve()
LOG_DIR = BASE_DIR / "logs"
REPORT_DIR = BASE_DIR / "reports"
HISTORY_DIR = BASE_DIR / "history"

LOG_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)
HISTORY_DIR.mkdir(parents=True, exist_ok=True)
