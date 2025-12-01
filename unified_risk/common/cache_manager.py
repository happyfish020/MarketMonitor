# unified_risk/common/cache_manager.py

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
from typing import Any, Callable, Optional, Union
import json

from .config_loader import get_path
from .logging_utils import get_logger

LOG = get_logger("UnifiedRisk.Cache")

BJ_TZ = timezone(timedelta(hours=8))


def _date_str(d: Union[str, date, datetime]) -> str:
    """
    转换为 YYYYMMDD 字符串格式
    """
    if isinstance(d, datetime):
        return d.astimezone(BJ_TZ).strftime("%Y%m%d")
    if isinstance(d, date):
        return d.strftime("%Y%m%d")
    return str(d)


# =====================
# 日级缓存管理器（JSON）
# =====================

@dataclass
class DayCacheManager:
    date_str: str

    def __init__(self, d: Union[str, date, datetime]):
        self.date_str = _date_str(d)
        self.root = get_path("day_cache_dir") / self.date_str
        self.root.mkdir(parents=True, exist_ok=True)

    def path(self, name: str) -> Path:
        return self.root / name

    def load(self, name: str) -> Optional[Any]:
        p = self.path(name)
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            LOG.warning(f"[DayCache] 读取失败 {p}: {e}")
            return None

    def save(self, name: str, data: Any) -> Path:
        p = self.path(name)
        try:
            p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            LOG.warning(f"[DayCache] 写入失败 {p}: {e}")
        return p

    # ⭐⭐ 支持 force_refresh 参数（兼容 fetcher v9.5.1）
    def get_or_fetch(self, name: str, fetch_func: Callable[[], Any],
                     force=False, force_refresh=None):

        # 兼容 force_refresh 作为最终优先参数
        if force_refresh is not None:
            force = force_refresh

        if not force:
            cached = self.load(name)
            if cached is not None:
                return cached

        data = fetch_func()
        self.save(name, data)
        return data


# =====================
# 全市场行情本地 DB（PARQUET）
# =====================

class AshareDailyDB:
    """
    data/database/ashare_daily/all_stocks_YYYYMMDD.parquet
    """

    def __init__(self, d: Union[str, date, datetime]):
        self.date_str = _date_str(d)

        try:
            root = get_path("ashare_daily_db")
        except KeyError:
            root = get_path("data_dir") / "database" / "ashare_daily"

        root.mkdir(parents=True, exist_ok=True)
        self.root = root
        self.file = root / f"all_stocks_{self.date_str}.parquet"

    def exists(self) -> bool:
        return self.file.exists()

    def load(self):
        import pandas as pd
        if not self.file.exists():
            return None
        try:
            return pd.read_parquet(self.file)
        except Exception as e:
            LOG.warning(f"[DailyDB] 读取失败 {self.file}: {e}")
            return None

    def save(self, df, overwrite=True):
        import pandas as pd
        if self.file.exists() and not overwrite:
            return self.file

        try:
            df.to_parquet(self.file, index=False)
        except Exception as e:
            LOG.warning(f"[DailyDB] 写入失败 {self.file}: {e}")
        return self.file
