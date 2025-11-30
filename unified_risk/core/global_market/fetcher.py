# unified_risk/core/global_market/fetcher.py

from __future__ import annotations
from datetime import datetime
from typing import Dict, Any

from unified_risk.common.cache_manager import DayCacheManager
from unified_risk.common.logger import get_logger

LOG = get_logger("UnifiedRisk.GlobalFetcher")


class GlobalMarketFetcher:
    """
    统一封装美股 / 全球市场数据（从 YF / 其他接口获取，先写入 day_cache，再读取）。
    """

    def __init__(self) -> None:
        self.cache = DayCacheManager()

    # ---------------- 公共入口 ----------------

    def build_global_snapshot(self, bj_now: datetime) -> Dict[str, Any]:
        d = bj_now.date()

        # 1) 先尝试从缓存读取
        cached = self.cache.read_json(d, "global_market.json")
        if cached is not None:
            return cached

        # 2) 缓存不存在 → 重新抓取
        data = {
            "us10y": self._fetch_us10y_yield(),
            "us5y": self._fetch_us5y_yield(),
            "nasdaq_pct": self._fetch_index_daily_change("^IXIC"),
            "spy_pct": self._fetch_index_daily_change("SPY"),
            "vix": self._fetch_vix(),
            # 短期 & 中期，可以视情况在 later 版本里做完整实现
            "nasdaq_5d_pct": 0.0,
            "spy_5d_pct": 0.0,
            "nasdaq_4w_pct": 0.0,
            "spy_4w_pct": 0.0,
        }

        # A50 夜盘变动（你现在 log 里用 HSI 代理）
        data["a50_night_pct"] = self.get_a50_night_change()

        # 写入缓存
        self.cache.write_json(d, "global_market.json", data)
        return data

    # ---------------- 对外给 A 股用的 A50 夜盘 ----------------

    def get_a50_night_change(self) -> float:
        """
        返回 A50 夜盘涨跌幅（%）。
        目前你 log 中是 HSI 作为 proxy: [RAW] HSI | A50Night% : -0.336
        这里保留函数接口，内部你可以按自己的逻辑实现。
        """
        # 占位实现：可以改成你现有的 HSI/A50 逻辑
        try:
            # TODO: 用你自己的数据源
            return 0.0
        except Exception as e:
            LOG.warning(f"[GLOBAL] get_a50_night_change error: {e}")
            return 0.0

    # ---------------- 下面这些方法请用你现有逻辑填充 ----------------

    def _fetch_us10y_yield(self) -> float:
        """
        返回美国 10Y 国债收益率（%）
        """
        # TODO: 使用你现有 YF / 数据源实现
        return 0.0

    def _fetch_us5y_yield(self) -> float:
        """
        返回美国 5Y 国债收益率（%）
        """
        # TODO
        return 0.0

    def _fetch_index_daily_change(self, symbol: str) -> float:
        """
        返回某指数/ETF 当日涨跌幅（%），如 ^IXIC, SPY。
        """
        # TODO: 使用你的 YF fetcher（统一通过缓存）
        return 0.0

    def _fetch_vix(self) -> float:
        """
        返回 VIX 最新价。
        """
        # TODO
        return 0.0
