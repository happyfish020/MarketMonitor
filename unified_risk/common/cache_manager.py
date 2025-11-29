import json
from pathlib import Path
from typing import Any, Dict, Optional

from .logger import get_logger
from .config_loader import get_path

LOG = get_logger("UnifiedRisk.Cache")

# 从配置获取 day_cache 目录
CACHE_DIR: Path = get_path("cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)


class CacheManager:
    """Day-cache 管理器：一个交易日一个 JSON 文件。"""

    def __init__(self, base_dir: Optional[Path] = None) -> None:
        self.base_dir = Path(base_dir) if base_dir else CACHE_DIR

    def _file_path(self, date_str: str) -> Path:
        return self.base_dir / f"{date_str}.json"

    def load_day_cache(self, date_str: str) -> Dict[str, Any]:
        path = self._file_path(date_str)
        if not path.exists():
            return {"date": date_str}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            LOG.error(f"Failed reading cache {path}: {e}")
            return {"date": date_str}

    def save_day_cache(self, date_str: str, data: Dict[str, Any]) -> None:
        path = self._file_path(date_str)
        try:
            path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            LOG.error(f"Failed writing cache {path}: {e}")

    def read_section(self, date_str: str, section: str) -> Dict[str, Any]:
        return self.load_day_cache(date_str).get(section, {})

    def write_section(self, date_str: str, section: str, payload: Dict[str, Any]) -> None:
        cache = self.load_day_cache(date_str)
        sec = cache.get(section, {})
        sec.update(payload)
        cache[section] = sec
        self.save_day_cache(date_str, cache)

    def read_key(self, date_str: str, section: str, key: str) -> Any:
        return self.read_section(date_str, section).get(key)

    def write_key(self, date_str: str, section: str, key: str, value: Any) -> None:
        sec = self.read_section(date_str, section)
        sec[key] = value
        self.write_section(date_str, section, sec)
