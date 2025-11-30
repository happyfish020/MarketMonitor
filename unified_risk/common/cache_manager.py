# unified_risk/common/cache_manager.py

from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional
import json
import os

from unified_risk.common.logger import get_logger

LOG = get_logger("UnifiedRisk.Cache")


@dataclass
class DayCacheManager:
    """
    简单日级缓存：
    根目录: data/cache/day_cache/YYYYMMDD/
    文件名按功能自定义，如 "ashare_index.json", "turnover.json", "northbound_etf.json"
    """

    root: Path = Path("data/cache/day_cache")

    def _day_dir(self, d: date) -> Path:
        return self.root / d.strftime("%Y%m%d")

    def read_json(self, d: date, name: str) -> Optional[Dict[str, Any]]:
        day_dir = self._day_dir(d)
        path = day_dir / name
        if not path.exists():
            LOG.info(f"[CACHE] miss day {path}, no file.")
            return None
        try:
            text = path.read_text(encoding="utf-8")
            data = json.loads(text)
            LOG.info(f"[CACHE] hit day {path}")
            return data
        except Exception as e:
            LOG.warning(f"[CACHE] read error {path}: {e}")
            return None

    def write_json(self, d: date, name: str, data: Dict[str, Any]) -> Path:
        day_dir = self._day_dir(d)
        os.makedirs(day_dir, exist_ok=True)
        path = day_dir / name
        text = json.dumps(data, ensure_ascii=False, indent=2)
        path.write_text(text, encoding="utf-8")
        LOG.info(f"[CACHE] write day {path}")
        return path
