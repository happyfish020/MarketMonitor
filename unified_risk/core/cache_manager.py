from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from unified_risk.global_core.config import BASE_DIR

# 根目录: 项目根 -> data/cache
DATA_DIR: Path = BASE_DIR / "data"
CACHE_ROOT: Path = DATA_DIR / "cache"

MODULES = ["ashare", "us", "global", "common"]
FREQS = ["day", "intraday"]

for m in MODULES:
    for f in FREQS:
        (CACHE_ROOT / m / f).mkdir(parents=True, exist_ok=True)


@dataclass
class CacheManager:
    """统一文件缓存管理:
    - 根目录: data/cache
    - 路径: data/cache/{module}/{freq}/{date_str}_{key}.json
    """
    base_dir: Path = CACHE_ROOT

    def _build_path(self, module: str, freq: str, key: str, date_str: str) -> Path:
        module = module.strip().lower()
        freq = freq.strip().lower()
        key = key.strip().replace(" ", "_")
        return self.base_dir / module / freq / f"{date_str}_{key}.json"

    def write(self, module: str, freq: str, key: str, payload: Any, date_str: str) -> Path:
        path = self._build_path(module, freq, key, date_str)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return path

    def read(self, module: str, freq: str, key: str, date_str: str) -> Optional[Any]:
        path = self._build_path(module, freq, key, date_str)
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)


GLOBAL_CACHE = CacheManager()


def write_day(module: str, key: str, payload: Any, date_str: str) -> Path:
    """按交易日写入日级缓存 (data/cache/{module}/day/{date_str}_{key}.json)。"""
    return GLOBAL_CACHE.write(module=module, freq="day", key=key, payload=payload, date_str=date_str)


def read_day(module: str, key: str, date_str: str) -> Optional[Any]:
    """读取日级缓存，若不存在返回 None。"""
    return GLOBAL_CACHE.read(module=module, freq="day", key=key, date_str=date_str)


def write_intraday(module: str, key: str, payload: Any, date_str: str) -> Path:
    """写入盘中缓存 (按日期分目录；具体时间信息可以放在 key/payload 内)。"""
    return GLOBAL_CACHE.write(module=module, freq="intraday", key=key, payload=payload, date_str=date_str)


def read_intraday(module: str, key: str, date_str: str) -> Optional[Any]:
    """读取盘中缓存 (按日期)。"""
    return GLOBAL_CACHE.read(module=module, freq="intraday", key=key, date_str=date_str)
