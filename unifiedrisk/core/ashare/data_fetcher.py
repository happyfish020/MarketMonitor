import logging
from dataclasses import dataclass
from datetime import datetime

import requests
from pytz import timezone

from .index_turnover_cache import (
    is_trading_time,
    write_turnover_cache,
    load_turnover_cache,
    load_latest_cache,
    load_default_cache,
)

BJ_TZ = timezone("Asia/Shanghai")
log = logging.getLogger(__name__)


@dataclass
class YahooQuote:
    symbol: str
    last: float
    change_pct: float


class AShareDataFetcher:
    def fetch_ashare_daily_raw(self):
        bj_now = datetime.now(BJ_TZ)
        return {
            "meta": {
                "bj_time": bj_now.isoformat(),
                "version": "UnifiedRisk_v3.4",
                "yahoo_enabled": True,
            },
            "index_turnover": self._fetch_turnover(bj_now),
            "global": self._fetch_global(),
            "macro": self._fetch_macro(),
        }

    # ========= 全球指数：纳指 / SPY / VIX =========
    def _fetch_global(self):
        syms = {"nasdaq": "^IXIC", "spy": "SPY", "vix": "^VIX"}
        out = {}
        for key, sym in syms.items():
            try:
                q = self._fetch_chart(sym, range_="5d")
                out[key] = {
                    "symbol": sym,
                    "last": q.last,
                    "change_pct": q.change_pct,
                }
            except Exception:
                log.exception("Chart API failed for %s", sym)
        return out

    # ========= 宏观因子：美元 + 大宗 =========
    def _fetch_macro(self):
        symbols = {
            "usd": "DX-Y.NYB",  # 美元指数
            "gold": "GC=F",     # COMEX 黄金
            "oil": "CL=F",      # WTI 原油
            "copper": "HG=F",   # COMEX 期铜
        }
        out = {}
        for key, sym in symbols.items():
            try:
                q = self._fetch_chart(sym, range_="5d")
                out[key] = {
                    "symbol": sym,
                    "last": q.last,
                    "change_pct": q.change_pct,
                }
            except Exception:
                log.exception("Macro chart API failed for %s", sym)
        return out

    def _fetch_chart(self, sym: str, range_: str) -> YahooQuote:
        """
        使用 v8 chart + 5d 日线数据，用最后两根 close 计算日涨跌幅，
        避免收盘后 meta 里的 previousClose/regularMarketPrice 导致涨跌幅 = 0 的问题。
        """
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
        resp = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            params={"range": range_, "interval": "1d"},
            timeout=8,
        )
        resp.raise_for_status()
        root = resp.json()["chart"]["result"][0]
        meta = root["meta"]

        closes = (
            root.get("indicators", {})
            .get("quote", [{}])[0]
            .get("close", [])
            or []
        )
        # 过滤 None
        closes = [c for c in closes if c is not None]

        if len(closes) >= 2:
            last = float(closes[-1])
            prev = float(closes[-2])
        else:
            # 兜底逻辑：退回 meta
            last = float(meta.get("regularMarketPrice") or 0.0)
            prev = float(meta.get("previousClose") or 0.0) or last

        pct = (last - prev) / prev * 100 if prev else 0.0
        return YahooQuote(sym, last, round(pct, 3))

    # ========= 成交额（ETF 代理 + 缓存） =========
    def _fetch_turnover(self, bj_now: datetime):
        date = bj_now.strftime("%Y-%m-%d")

        if is_trading_time(bj_now):
            live = self._fetch_turnover_live(bj_now)
            write_turnover_cache(date, live)
            return live

        cached = load_turnover_cache(date)
        if cached:
            return cached

        latest = load_latest_cache()
        if latest:
            return latest

        default = load_default_cache()
        if default:
            return default

        log.warning("No turnover cache/default found, index_turnover will be empty.")
        return {}

    def _fetch_turnover_live(self, bj_now: datetime):
        etf = {
            "shanghai": "510300.SS",
            "shenzhen": "159901.SZ",
            "chi_next": "159915.SZ",
        }
        out = {}
        for key, sym in etf.items():
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
                resp = requests.get(
                    url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    params={"range": "1d", "interval": "1m"},
                    timeout=8,
                )
                resp.raise_for_status()
                root = resp.json()["chart"]["result"][0]
                meta = root["meta"]
                price = float(meta.get("regularMarketPrice") or 0.0)
                vol = meta.get("regularMarketVolume", 0) or 0
                out[key] = {
                    "symbol": sym,
                    "price": price,
                    "volume": vol,
                    "turnover": price * vol,
                    "date": bj_now.strftime("%Y-%m-%d"),
                }
            except Exception:
                log.exception("ETF turnover fetch fail for %s", sym)
        return out