
try:
    import akshare as ak
except:
    ak = None

from .cache_manager import CacheManager

class AkCache:
    def __init__(self, data_root:str):
        self.cm = CacheManager(data_root, "cache_ak")

    def _wrap(self, key, func):
        data = self.cm.get(key)
        if data is not None:
            return data
        if ak is None:
            return None
        data = func()
        if data is not None:
            self.cm.set(key, data)
        return data

    def spot(self):
        return self._wrap("spot_all", lambda: ak.stock_zh_a_spot().to_dict(orient="records"))

    def sse(self):
        return self._wrap("sse_deal_daily", lambda: ak.stock_sse_deal_daily().to_dict(orient="records"))

    def szse(self):
        return self._wrap("szse_summary", lambda: ak.stock_szse_summary().to_dict(orient="records"))

    def index_daily(self, symbol:str):
        return self._wrap(f"index_daily_{symbol}", lambda: ak.stock_zh_index_daily(symbol=symbol).to_dict(orient="records"))
