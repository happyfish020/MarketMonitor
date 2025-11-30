# unified_risk/common/cache_manager.py

from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional
import json
import os

from unified_risk.common.logging_utils import get_logger

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

# ------------------------------------------------------------
# 超强兼容旧版接口：get_or_fetch_json
# 支持旧 TRM / v7 / v8 的所有参数：bj_time, cache_minutes, etc.
# ------------------------------------------------------------
def get_or_fetch_json(path: str, fetch_func, *args, **kwargs):
    """
    兼容旧版 UnifiedRisk / TRM 的万能数据提取函数。

    旧系统有的调用方式：
        get_or_fetch_json(path, fetch_func)
        get_or_fetch_json(path, fetch_func, bj_time=bj_time)
        get_or_fetch_json(path, fetch_func, force=True)
        get_or_fetch_json(path, fetch_func, cache_minutes=30)
        ...

    v9.1 只需要 path + fetch_func，其他参数自动忽略。
    """

    p = Path(path)

    # --------------------------
    # 1) 如果文件存在 → 直接读取
    # --------------------------
    if p.exists() and not kwargs.get("force", False):
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass  # 如果文件损坏，fallback 到 fetch_func()

    # --------------------------
    # 2) 文件不存在 → 调用 fetch_func()
    # --------------------------
    data = fetch_func()

    # 自动创建目录
    p.parent.mkdir(parents=True, exist_ok=True)

    p.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return data
