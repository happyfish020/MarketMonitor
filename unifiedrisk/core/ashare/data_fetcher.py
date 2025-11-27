
"""UnifiedRisk A-share DataFetcher v4 (修复版)

特点：
- 保留旧版本里用到的 `_fetch_global` 结构（global / macro）以兼容你现有的 v3.x / v4.x 逻辑；
- 去掉会触发 401 的 Yahoo v7 quote 接口，只用 v8 chart 接口；
- 同时提供 v4 用到的 `build_payload()` + 11 因子占位结构；
- 所有网络请求都有 try/except，出错时返回 None，不会抛异常导致程序崩溃。
"""
from __future__ import annotations

import datetime as _dt
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests

LOG = logging.getLogger("unifiedrisk.core.ashare.data_fetcher")

BJ_TZ = _dt.timezone(_dt.timedelta(hours=8))


def now_bj() -> _dt.datetime:
    return _dt.datetime.now(BJ_TZ)


# ---------------------------- Yahoo helpers ---------------------------- #

YF_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


def _fetch_chart_series(
    symbol: str,
    range_: str = "1d",
    interval: str = "1d",
) -> Optional[Dict[str, Any]]:
    """只使用 v8 chart 接口，避免 v7 quote 的 401 问题。

    返回格式：
    {
        "last": float | None,
        "change_pct": float | None,
    }
    """
    params = {
        "range": range_,
        "interval": interval,
    }
    url = YF_CHART_URL.format(symbol=symbol)
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        result = data.get("chart", {}).get("result")
        if not result:
            LOG.warning("Empty chart result for %s", symbol)
            return None
        result = result[0]
        closes = result.get("indicators", {}).get("quote", [{}])[0].get("close")
        timestamps = result.get("timestamp")
        if not closes or not timestamps:
            LOG.warning("No closes/timestamps in chart for %s", symbol)
            return None

        last = closes[-1]
        # 如果只有一个点，就没有涨跌幅，返回 0
        if len(closes) < 2 or last is None or closes[-2] is None:
            change_pct = 0.0
        else:
            prev = closes[-2]
            change_pct = (last / prev - 1.0) * 100.0 if prev else 0.0
        return {"last": float(last), "change_pct": float(change_pct)}
    except Exception as exc:
        LOG.error("chart API failed for %s: %s", symbol, exc)
        return None


# ------------------------------- DataFetcher ---------------------------- #


@dataclass
class DataFetcher:
    """A-share DataFetcher v4（兼容旧结构 + 新因子结构）。

    兼容点：
    - 保留 `raw['global']` 结构：
        {
            'nasdaq': {'symbol': '^IXIC', 'last': ..., 'change_pct': ...},
            'spy':    {'symbol': 'SPY',   'last': ..., 'change_pct': ...},
            'vix':    {'symbol': '^VIX',  'last': ..., 'change_pct': ...},
        }
    - 保留 `raw['macro']` 结构：
        {
            'usd':   {'symbol': 'DX-Y.NYB', 'last': ..., 'change_pct': ...},
            'gold':  {'symbol': 'GC=F',     'last': ..., 'change_pct': ...},
            'oil':   {'symbol': 'CL=F',     'last': ..., 'change_pct': ...},
            'copper':{'symbol': 'HG=F',     'last': ..., 'change_pct': ...},
        }

    新增点：
    - 提供 `build_payload()` 返回 v4 统一结构（meta + 各因子）。
    """

    as_of: Optional[_dt.date] = None

    # ------------------------------------------------------------------ #
    #  v4 统一入口：build_payload
    # ------------------------------------------------------------------ #

    def build_payload(self, as_of: Optional[_dt.date] = None) -> Dict[str, Any]:
        if as_of is None:
            as_of = self.as_of or now_bj().date()

        bj_now = now_bj()
        date_str = as_of.isoformat()

        # 旧结构（index_turnover / global / macro）—— 可以被你现有 v3.x 逻辑直接使用
        index_turnover = self._fetch_index_turnover(as_of)
        global_view = self._fetch_global()
        macro_view = self._fetch_macro()

        # v4 因子占位结构（目前简单从 global / macro / index_turnover 派生）
        # 你可以后续自己在这里接入更精细的因子逻辑
        return {
            "meta": {
                "bj_time": bj_now.isoformat(),
                "trade_date": date_str,
                "version": "UnifiedRisk_v4.0",
                "yahoo_enabled": True,
            },
            "index_turnover": index_turnover,
            "global": global_view,
            "macro": macro_view,
            # 下面这些 key 是给 v4 Scorer 用的占位结构，可逐步细化
            "northbound": {"ok": False, "data": None, "error": "TODO: northbound not wired"},
            "margin": {"ok": False, "data": None, "error": "TODO: margin not wired"},
            "liquidity": {"ok": False, "data": None, "error": "TODO: liquidity not wired"},
            "fund_flow": {"ok": False, "data": None, "error": "TODO: fund_flow not wired"},
            "style": {"ok": False, "data": None, "error": "TODO: style not wired"},
            "valuation": {"ok": False, "data": None, "error": "TODO: valuation not wired"},
            "volume_price": {"ok": False, "data": None, "error": "TODO: volume/price not wired"},
            "macro_reflection": {"ok": True, "data": macro_view},
            "tech_pattern": {"ok": False, "data": None, "error": "TODO: tech pattern not wired"},
        }

    # ------------------------------------------------------------------ #
    #  旧结构：指数成交额代理
    # ------------------------------------------------------------------ #

    def _fetch_index_turnover(self, as_of: _dt.date) -> Dict[str, Any]:
        """指数成交额 / 换手率代理。

        为了保持和你现有日志一致，这里保留：
        - 510300.SS （沪深300，代表上证）
        - 159901.SZ （深证成指）
        - 159915.SZ （创业板）

        这里仅用 1m 分钟级别做一个简单的当日成交额估算。
        """
        symbols = {
            "shanghai": "510300.SS",
            "shenzhen": "159901.SZ",
            "chi_next": "159915.SZ",
        }

        result: Dict[str, Any] = {}
        for key, sym in symbols.items():
            data = _fetch_chart_series(sym, range_="1d", interval="1m")
            if not data:
                result[key] = {
                    "symbol": sym,
                    "price": None,
                    "volume": None,
                    "turnover": None,
                    "date": as_of.isoformat(),
                }
                continue

            # 注意：chart 的 volume 是逐时间点成交量，这里简单求和近似为当日总量
            try:
                # 为了不再额外请求一次，这里简化：只返回 last 价格，volume/turnover 先留 None
                # 你之前的 v3 版本有更精确的成交额计算，可以在这里自己补回去。
                result[key] = {
                    "symbol": sym,
                    "price": data["last"],
                    "volume": None,
                    "turnover": None,
                    "date": as_of.isoformat(),
                }
            except Exception as exc:
                LOG.error("index_turnover calc failed for %s: %s", sym, exc)
                result[key] = {
                    "symbol": sym,
                    "price": None,
                    "volume": None,
                    "turnover": None,
                    "date": as_of.isoformat(),
                }

        return result

    # ------------------------------------------------------------------ #
    #  旧结构：global（纳指 / SPY / VIX）
    # ------------------------------------------------------------------ #

    def _fetch_global(self) -> Dict[str, Any]:
        """兼容旧版本的 global 结构。

        返回：
        {
            'nasdaq': {'symbol': '^IXIC', 'last': ..., 'change_pct': ...},
            'spy':    {'symbol': 'SPY',   'last': ..., 'change_pct': ...},
            'vix':    {'symbol': '^VIX',  'last': ..., 'change_pct': ...},
        }
        """
        pairs = {
            "nasdaq": "^IXIC",
            "spy": "SPY",
            "vix": "^VIX",
        }
        out: Dict[str, Any] = {}
        for key, sym in pairs.items():
            data = _fetch_chart_series(sym, range_="5d", interval="1d")
            if not data:
                out[key] = {"symbol": sym, "last": None, "change_pct": None}
            else:
                out[key] = {
                    "symbol": sym,
                    "last": data["last"],
                    "change_pct": data["change_pct"],
                }
        return out

    # ------------------------------------------------------------------ #
    #  旧结构：macro（美元指数 / 黄金 / 原油 / 铜）
    # ------------------------------------------------------------------ #

    def _fetch_macro(self) -> Dict[str, Any]:
        """兼容旧版本的 macro 结构。"""
        pairs = {
            "usd": "DX-Y.NYB",
            "gold": "GC=F",
            "oil": "CL=F",
            "copper": "HG=F",
        }
        out: Dict[str, Any] = {}
        for key, sym in pairs.items():
            data = _fetch_chart_series(sym, range_="5d", interval="1d")
            if not data:
                out[key] = {"symbol": sym, "last": None, "change_pct": None}
            else:
                out[key] = {
                    "symbol": sym,
                    "last": data["last"],
                    "change_pct": data["change_pct"],
                }
        return out

    # ------------------------------------------------------------------ #
    #  兼容：如果有旧代码直接调用 _fetch_quote，不再使用 v7 接口，直接走 chart
    # ------------------------------------------------------------------ #

    def _fetch_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """兼容旧接口名称，但内部直接走 chart，避免 401。"""
        LOG.debug("compat _fetch_quote via chart for %s", symbol)
        return _fetch_chart_series(symbol, range_="5d", interval="1d")
