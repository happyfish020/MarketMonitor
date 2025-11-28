
import pandas as pd
from unifiedrisk.common.ak_cache import AkCache

DATA_ROOT = r"D:/LHJ/PythonWS/MarketMon/MarketMonitor/data"
ak_cache = AkCache(DATA_ROOT)

class DataFetcher:
    def get_spot_all(self):
        rec = ak_cache.spot() or []
        return pd.DataFrame(rec)

    def get_sse(self):
        rec = ak_cache.sse() or []
        return pd.DataFrame(rec)

    def get_szse(self):
        rec = ak_cache.szse() or []
        return pd.DataFrame(rec)

    def get_index_daily(self, symbol):
        rec = ak_cache.index_daily(symbol) or []
        return pd.DataFrame(rec)
