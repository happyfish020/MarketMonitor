try:
    import akshare as ak
except:
    ak = None

import os
from .cache_manager import CacheManager


class AkCache:
    """
    v5.0.1a — 完整增强版
    - 支持 data_root= 与 base_dir=
    - 自动创建目录
    - 新接口：spot / sse / szse / index_daily
    - 兼容旧接口 *_cached
    """

    def __init__(self, data_root: str = None, base_dir: str = None):
        if data_root is None and base_dir is None:
            raise ValueError("AkCache requires either data_root= or base_dir= argument.")
        if data_root is None:
            data_root = base_dir

        os.makedirs(data_root, exist_ok=True)
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

    # ===== 新接口 =====
    def spot(self):
        return self._wrap(
            "spot_all",
            lambda: ak.stock_zh_a_spot().to_dict(orient="records"),
        )

    def sse(self):
        return self._wrap(
            "sse_deal_daily",
            lambda: ak.stock_sse_deal_daily().to_dict(orient="records"),
        )

    def szse(self):
        return self._wrap(
            "szse_summary",
            lambda: ak.stock_szse_summary().to_dict(orient="records"),
        )

    def index_daily(self, symbol: str):
        return self._wrap(
            f"index_daily_{symbol}",
            lambda: ak.stock_zh_index_daily(symbol=symbol).to_dict(orient="records"),
        )

    # ===== 旧接口（兼容 legacy）=====
    def stock_zh_a_spot_cached(self):
        return self.spot()

    def stock_sse_deal_daily_cached(self):
        return self.sse()

    def stock_szse_summary_cached(self):
        return self.szse()

    def stock_zh_index_daily_cached(self, symbol: str):
        return self.index_daily(symbol)
