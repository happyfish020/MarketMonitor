
import requests, logging
from datetime import datetime
from pytz import timezone
from dataclasses import dataclass

from .index_turnover_cache import (
    is_trading_time, write_turnover_cache,
    load_turnover_cache, load_latest_cache
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
                "version": "UnifiedRisk_v2.1",
                "yahoo_enabled": True
            },
            "index_turnover": self._fetch_turnover(bj_now),
            "global": self._fetch_global()
        }

    def _fetch_global(self):
        syms = {"nasdaq": "^IXIC", "spy": "SPY", "vix": "^VIX"}
        out = {}
        for k, s in syms.items():
            try:
                q = self._fetch_chart(s)
                out[k] = {"symbol": s, "last": q.last, "change_pct": q.change_pct}
            except:
                log.exception("Global fetch fail %s", s)
        return out

    def _fetch_chart(self, sym):
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
        resp = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            params={"range": "1d", "interval": "1d"},
            timeout=8
        )
        resp.raise_for_status()
        data = resp.json()["chart"]["result"][0]["meta"]
        last = float(data["regularMarketPrice"])
        prev = float(data.get("previousClose", last))
        pct = (last - prev) / prev * 100 if prev else 0
        return YahooQuote(sym, last, round(pct, 3))

    def _fetch_turnover(self, bj_now):
        date = bj_now.strftime("%Y-%m-%d")
        if is_trading_time(bj_now):
            live = self._fetch_turnover_live(bj_now)
            write_turnover_cache(date, live)
            return live
        c = load_turnover_cache(date)
        if c:
            return c
        latest = load_latest_cache()
        return latest or {}

    def _fetch_turnover_live(self, bj_now):
        etf = {"shanghai": "510300.SS", "shenzhen": "159901.SZ", "chi_next": "159915.SZ"}
        out = {}
        for k, sym in etf.items():
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
                resp = requests.get(
                    url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    params={"range": "1d", "interval": "1m"},
                    timeout=8
                )
                resp.raise_for_status()
                meta = resp.json()["chart"]["result"][0]["meta"]
                price = float(meta["regularMarketPrice"])
                vol = meta.get("regularMarketVolume", 0)
                out[k] = {
                    "symbol": sym,
                    "price": price,
                    "volume": vol,
                    "turnover": price * vol,
                    "date": bj_now.strftime("%Y-%m-%d")
                }
            except:
                log.exception("ETF turnover fail %s", sym)
        return out
