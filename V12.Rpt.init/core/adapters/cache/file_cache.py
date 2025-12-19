# core/adapters/cache/file_cache.py

import os
import json
from typing import Any, Optional

from core.utils.logger import get_logger

LOG = get_logger("DS.Cache")


# ==========================================================
# JSON Load
# ==========================================================
def load_json(path: str) -> Optional[Any]:
    abs_path = os.path.abspath(path)
    LOG.info("CacheRead: path=%s", abs_path)

    if not os.path.exists(abs_path):
        LOG.warning("CacheReadFailed: file not found path=%s", abs_path)
        return None

    try:
        with open(abs_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        LOG.error("CacheReadError: path=%s error=%s", abs_path, e)
        return None


# ==========================================================
# JSON Save
# ==========================================================
def save_json(path: str, data: Any):
    abs_path = os.path.abspath(path)
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)

    LOG.info("CacheWrite: path=%s", abs_path)

    try:
        with open(abs_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        LOG.error("CacheWriteError: path=%s error=%s", abs_path, e)
        raise
