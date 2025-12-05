import os
import json
from typing import Any, Optional

from core.utils.config_loader import load_paths
from core.utils.logger import log

_paths = load_paths()
BASE_CACHE_DIR = _paths.get("cache_dir", "data/cache/")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _abs(path: str) -> str:
    try:
        return os.path.abspath(path)
    except Exception:
        return path


def load_json(path: str) -> Optional[Any]:
    abs_path = _abs(path)
    if not os.path.exists(path):
        log(f"[IO] Load JSON skipped (not exists) ← {abs_path}")
        return None
    log(f"[IO] Loading JSON ← {abs_path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    extra = ""
    try:
        if isinstance(data, dict):
            extra = f" (keys={len(data)})"
        elif isinstance(data, list):
            extra = f" (len={len(data)})"
    except Exception:
        extra = ""
    log(f"[IO] Load JSON OK ← {abs_path}{extra}")
    return data


def save_json(path: str, data: Any) -> None:
    abs_path = _abs(path)
    ensure_dir(os.path.dirname(path))
    log(f"[IO] Writing JSON → {abs_path}")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log(f"[IO] Write JSON OK → {abs_path}")
