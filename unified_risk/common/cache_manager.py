import os, json
from datetime import datetime, timezone, timedelta

BJ_TZ = timezone(timedelta(hours=8))


class CacheManager:
    """
    v5.0.1 修复版：
    - data_root= 和 base_dir= 两种参数都支持（向下兼容）
    - subdir 默认为 "default_cache"
    - 自动创建目录
    """

    def __init__(self, data_root: str = None, subdir: str = "default_cache", base_dir: str = None):
        # ---- 参数兼容处理 ----
        if data_root is None and base_dir is None:
            raise ValueError("CacheManager requires either data_root= or base_dir= argument.")

        if data_root is None:
            data_root = base_dir

        # ---- 最终缓存根目录 ----
        self.root = os.path.join(data_root, subdir)
        os.makedirs(self.root, exist_ok=True)

    # ---------------------------------------------
    # 内部工具函数
    # ---------------------------------------------
    def _day(self):
        return datetime.now(BJ_TZ).strftime("%Y%m%d")

    def _path(self, key):
        # 安全 key（不允许路径）
        safe = key.replace("/", "_")

        # 当日的目录
        day_dir = os.path.join(self.root, self._day())
        os.makedirs(day_dir, exist_ok=True)

        return os.path.join(day_dir, safe + ".json")

    # ---------------------------------------------
    # 对外接口
    # ---------------------------------------------
    def get(self, key):
        p = self._path(key)
        if not os.path.exists(p):
            return None
        try:
            return json.load(open(p, "r", encoding="utf8"))
        except:
            return None

    def set(self, key, value):
        p = self._path(key)
        try:
            json.dump(value, open(p, "w", encoding="utf8"), ensure_ascii=False)
        except:
            pass
