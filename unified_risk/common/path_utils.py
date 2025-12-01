from pathlib import Path
from datetime import datetime

# 项目根目录：自动从当前文件向上查找
ROOT_DIR = Path(__file__).resolve().parent.parent.parent

def day_cache_root() -> Path:
    """
    返回 /data/day_cache/ 根路径
    """
    path = ROOT_DIR / "data" / "day_cache"
    path.mkdir(parents=True, exist_ok=True)
    return path

def day_cache_path(date: datetime) -> Path:
    """
    返回某一天的 day cache 目录，例如：
    2025-11-30 → data/day_cache/20251130/
    """
    day_str = date.strftime("%Y%m%d")
    path = day_cache_root() / day_str
    path.mkdir(parents=True, exist_ok=True)
    return path
