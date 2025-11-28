
import os, json
from datetime import datetime, timezone, timedelta
BJ_TZ = timezone(timedelta(hours=8))

class CacheManager:
    def __init__(self, data_root:str, subdir:str):
        self.root = os.path.join(data_root, subdir)
        os.makedirs(self.root, exist_ok=True)

    def _day(self):
        return datetime.now(BJ_TZ).strftime("%Y%m%d")

    def _path(self, key):
        safe = key.replace("/", "_")
        day_dir = os.path.join(self.root, self._day())
        os.makedirs(day_dir, exist_ok=True)
        return os.path.join(day_dir, safe + ".json")

    def get(self, key):
        p = self._path(key)
        if not os.path.exists(p): return None
        try: return json.load(open(p, "r", encoding="utf8"))
        except: return None

    def set(self, key, value):
        p = self._path(key)
        try: json.dump(value, open(p, "w", encoding="utf8"), ensure_ascii=False)
        except: pass
