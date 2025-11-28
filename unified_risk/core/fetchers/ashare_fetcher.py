from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, Union

import requests
import yfinance as yf
import pandas as pd

from unified_risk.global_core.datasources.commodity_fetcher import get_commodity_snapshot
from unified_risk.core.logging_utils import log_info, log_warning, log_error
from unified_risk.core.cache_manager import write_day, read_day

BJ_TZ = timezone(timedelta(hours=8))


def safe_int(value) -> int:
    """
    robust int converter for f49/f50:
    '-', '--', None, ''  → 0
    '123'                → 123
    123.0                → 123
    """
    try:
        if value in [None, "", "-", "--"]:
            return 0
        # float-like?
        if isinstance(value, float):
            return int(value)
        # string number?
        s = str(value).strip()
        if s in ["", "-", "--"]:
            return 0
        return int(float(s))     # handles "12.0"
    except Exception:
        return 0


class AshareDataFetcher:
    """
    A 股数据抓取统一入口（修复版）：
      - 指数涨跌：上证 / 创业板（优先 push2，失败回退 Yahoo）
      - 涨跌家数：使用 f49/f50
      - 成交额：使用 f164（元）并换算为亿元
      - ETF：510300 等
      - 流动性枯竭：基于 510300 成交量
      - 外围：美股 / 欧股 / 亚洲 / 美债 / A50 夜盘
    """

    def __init__(self) -> None:
        # 禁用系统代理
        for k in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
            os.environ[k] = ""

        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "*/*",
                "Referer": "https://quote.eastmoney.com/",
                "Connection": "keep-alive",
            }
        )
        self.session.keep_alive = False

        # 通用 cache
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_ttl = 300.0  # 秒
        self._raw_logged_keys: set[str] = set()

        # yfinance/ETF cache
        self._yf_cache: Dict[str, Any] = {}
        self._yf_cache_expire: Dict[str, float] = {}
        self._yf_ttl = 600.0

    # ---------- 通用 cache / 日志 ----------

    def _get_cache(self, key: str):
        try:
            entry = self._cache.get(key)
            if not entry:
                return None
            ts = entry.get("ts")
            if ts is None:
                return None
            if time.time() - ts > self._cache_ttl:
                self._cache.pop(key, None)
                return None
            return entry.get("value")
        except Exception:
            return None

    def _set_cache(self, key: str, value):
        try:
            self._cache[key] = {"value": value, "ts": time.time()}
        except Exception:
            pass

    def _log_raw_data(self, source: str, key: str, value: Union[float, str, int]):
        try:
            tag = f"{source}|{key}"
            if tag in self._raw_logged_keys:
                return
            self._raw_logged_keys.add(tag)
        except Exception:
            pass

        if isinstance(value, float):
            value_str = f"{value: >8.3f}"
        else:
            value_str = str(value).rjust(8)
        log_info(f"  [RAW] {source.ljust(15)} | {key.ljust(12)}: {value_str}")

    # ---------- Yahoo 封装 ----------

    def _fetch_yahoo_data(self, symbol: str) -> Dict[str, Any]:
        """
        统一封装一个 Yahoo 当前 / 最近日线数据获取。
        返回:
          {"price": float, "changePct": float}
        """
        cache_key = f"yf:{symbol}"
        cached = self._get_cache(cache_key)
        if cached is not None:
            return cached

        data: Dict[str, Any] = {}
        try:
            tk = yf.Ticker(symbol)
            hist = tk.history(period="2d")
            if hist is None or hist.empty:
                log_warning(f"Yahoo history empty for {symbol}")
            else:
                hist = hist.tail(2)
                last = hist["Close"].iloc[-1]
                prev = hist["Close"].iloc[-2] if len(hist) >= 2 else last
                if prev:
                    change_pct = (last - prev) / prev * 100.0
                else:
                    change_pct = 0.0
                data = {"price": float(last), "changePct": float(change_pct)}
        except Exception as e:
            log_warning(f"Yahoo fetch failed for {symbol}: {e}")
            data = {}

        self._set_cache(cache_key, data)
        return data

    # ---------- 美债 / 欧洲 / 亚洲 / 美股 ----------

    def get_treasury_yield(self) -> Dict[str, float]:
        ten_data = self._fetch_yahoo_data("^TNX")
        five_data = self._fetch_yahoo_data("^FVX")

        ten = ten_data.get("price", 0.0)
        five = five_data.get("price", 0.0)
        if not ten or not five:
            self._log_raw_data("Treasury(YF)", "STATUS", "Data Error/Missing")
            return {"yield_jump": 0.0, "yield_curve_diff": 0.0}

        yield_curve_diff = (ten - five) * 100.0
        self._log_raw_data("Treasury(YF)", "10Y(%)", ten)
        self._log_raw_data("Treasury(YF)", "5Y(%)", five)
        self._log_raw_data("Treasury(YF)", "Y.Curve(bps)", yield_curve_diff)
        return {"yield_jump": 0.0, "yield_curve_diff": yield_curve_diff}

    def get_us_equity_snapshot(self) -> Dict[str, Any]:
        snap: Dict[str, Any] = {}

        ndx = self._fetch_yahoo_data("^IXIC")
        spy = self._fetch_yahoo_data("SPY")
        vix = self._fetch_yahoo_data("^VIX")

        if ndx:
            self._log_raw_data("^IXIC", "Change%", ndx.get("changePct", 0.0))
        if spy:
            self._log_raw_data("SPY", "Change%", spy.get("changePct", 0.0))
        if vix:
            self._log_raw_data("^VIX", "Price", vix.get("price", 0.0))

        snap["nasdaq"] = {
            "price": float(ndx.get("price", 0.0)) if ndx else 0.0,
            "changePct": float(ndx.get("changePct", 0.0)) if ndx else 0.0,
        }
        snap["spy"] = {
            "price": float(spy.get("price", 0.0)) if spy else 0.0,
            "changePct": float(spy.get("changePct", 0.0)) if spy else 0.0,
        }
        snap["vix"] = {
            "price": float(vix.get("price", 0.0)) if vix else 0.0,
            "changePct": float(vix.get("changePct", 0.0)) if vix else 0.0,
        }
        return snap

    def get_eu_futures(self) -> float:
        dax = self._fetch_yahoo_data("^GDAXI")
        ftse = self._fetch_yahoo_data("^FTSE")
        dax_chg = dax.get("changePct", 0.0)
        ftse_chg = ftse.get("changePct", 0.0)
        self._log_raw_data("^GDAXI", "Change%", dax_chg)
        self._log_raw_data("^FTSE", "Change%", ftse_chg)
        # 取 DAX 为主，失败则用 FTSE
        return dax_chg if dax_chg != 0.0 else ftse_chg

    def get_asian_market(self) -> Dict[str, float]:
        out = {"nikkei_vol": 0.0, "kospi_vol": 0.0}
        nk = self._fetch_yahoo_data("^N225")
        ks = self._fetch_yahoo_data("^KS11")
        out["nikkei_vol"] = abs(nk.get("changePct", 0.0)) if nk else 0.0
        out["kospi_vol"] = abs(ks.get("changePct", 0.0)) if ks else 0.0
        return out

    # ---------- ETF / 北向代理 ----------

    def get_northbound_etf_proxy(self) -> Dict[str, Any]:
        """
        盘中使用 510300 + 510500 的 f62/f184 作为北向代理。
        返回:
          - proxy_etf_flow_yi: 资金流向（亿元）
          - proxy_etf_volume : 成交量
        """
        def _one(code: str):
            url = "https://push2.eastmoney.com/api/qt/stock/get"
            params = {
                "secid": f"1.{code}",
                "fields": "f62,f184",
                "_": int(time.time() * 1000),
            }
            try:
                r = self.session.get(url, params=params, timeout=5)
                j = r.json().get("data", {})
                return j.get("f62", 0), j.get("f184", 0)
            except Exception:
                return 0, 0

        flow1, vol1 = _one("510300")
        flow2, vol2 = _one("510500")

        total_flow = (flow1 + flow2) / 1e8
        self._log_raw_data("NBProxyETF", "FlowYi", total_flow)

        return {
            "proxy_etf_flow_yi": round(total_flow, 2),
            "proxy_etf_volume": (vol1 + vol2),
        }

    # ---------- A50 夜盘（多重 fallback） ----------

    def get_a50_night_session(self) -> Dict[str, Any]:
        def _ret1(symbol: str) -> Optional[float]:
            try:
                data = self._fetch_yahoo_data(symbol)
                return float(data.get("changePct", 0.0)) / 100.0
            except Exception:
                return None

        for symbol, tag in (("^FTXIN9", "FTXIN9"), ("^HSI", "HSI")):
            r = _ret1(symbol)
            if r is not None and r != 0.0:
                r = max(min(r, 0.08), -0.08)
                self._log_raw_data(tag, "A50Night%", r * 100.0)
                return {"ret": r, "source": tag}

        proxy = self.get_northbound_etf_proxy()
        flow_yi = float(proxy.get("proxy_etf_flow_yi", 0.0) or 0.0)
        if flow_yi != 0.0:
            approx = max(min(flow_yi / 100.0, 0.03), -0.03)
            self._log_raw_data("A50Proxy", "FlowYi", flow_yi)
            self._log_raw_data("A50Proxy", "Approx%", approx * 100.0)
            return {"ret": approx, "source": "ETF_PROXY"}

        self._log_raw_data("A50Night", "STATUS", "No valid data, use 0.0")
        return {"ret": 0.0, "source": "NONE"}

    # ---------- 指数涨跌（修复版） ----------

    def _get_index_change_push2(self, secid: str) -> Optional[float]:
        """
        通过 push2 获取指数涨跌幅（%），使用 f3 字段。
        """
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        params = {
            "secid": secid,
            "fields": "f3",
            "_": int(time.time() * 1000),
        }
        try:
            r = self.session.get(url, params=params, timeout=5)
            j = r.json().get("data", {})
            val = j.get("f3", None)
            if val is None:
                return None
            return float(val)
        except Exception as e:
            log_warning(f"index push2 fetch failed for {secid}: {e}")
            return None

    def get_china_index_snapshot(self, bj_time: datetime) -> Dict[str, Any]:
        """
        上证、创业板涨跌幅快照。
        优先使用东方财富 push2，失败时回退到 Yahoo。
        """
        sh_chg = self._get_index_change_push2("1.000001")
        cyb_chg = self._get_index_change_push2("0.399006")

        if sh_chg is None or cyb_chg is None:
            # fallback to Yahoo
            sh = self._fetch_yahoo_data("000001.SS")
            cyb = self._fetch_yahoo_data("399006.SZ")
            sh_chg = float(sh.get("changePct", 0.0)) if sh else 0.0
            cyb_chg = float(cyb.get("changePct", 0.0)) if cyb else 0.0

        self._log_raw_data("SH", "Change%", sh_chg)
        self._log_raw_data("CYB", "Change%", cyb_chg)

        return {"sh_change": sh_chg, "cyb_change": cyb_chg}

    # ---------- 涨跌家数（修复版：f49 / f50） ----------

    def get_advance_decline(self) -> Dict[str, int]:
        """
        全市场涨跌家数：f49 / f50
        """
        url = "https://push2.eastmoney.com/api/qt/ulist.np/get"
        params = {
            "fltt": "2",
            "invt": "2",
            "fid": "f3",
            "pn": "1",
            "pz": "1",
            "secids": "1.000001",
            "fields": "f49,f50",
            "_": int(time.time() * 1000),
        }
        try:
            r = self.session.get(url, params=params, timeout=5)
            j = r.json().get("data", {})
            diff = j.get("diff", [])
            if not diff:
                raise ValueError("no diff in adv/dec")
            row = diff[0]
    
            adv = safe_int(row.get("f49"))
            dec = safe_int(row.get("f50"))
    
        except Exception as e:
            log_warning(f"advance/decline fetch failed: {e}")
            adv, dec = 0, 0
    
        self._log_raw_data("ADV", "Count", adv)
        self._log_raw_data("DEC", "Count", dec)
        return {"advance": adv, "decline": dec}

    # ---------- 成交额（修复版：f164） ----------

    def get_turnover(self) -> float:
        """
        上证成交额（亿元）。
        使用 f164 字段（单位：元），换算为亿元。
        """
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        params = {
            "secid": "1.000001",
            "fields": "f164",
            "_": int(time.time() * 1000),
        }
        try:
            r = self.session.get(url, params=params, timeout=5)
            j = r.json().get("data", {})
            turnover = float(j.get("f164", 0.0) or 0.0) / 1e8
        except Exception as e:
            log_warning(f"turnover fetch failed: {e}")
            turnover = 0.0

        self._log_raw_data("Turnover", "Shanghai(Yi)", turnover)
        return turnover

    # ---------- ETF 日线 ----------

    def get_etf_daily(self, symbol: str):
        """
        ETF 日线数据：优先用本地缓存，再用 yfinance。
        返回 DataFrame: [date, close, volume]
        """
        now = time.time()
        if symbol in self._yf_cache and now < self._yf_cache_expire.get(symbol, 0):
            return self._yf_cache[symbol]

        try:
            yf_map = {
                "510300": "510300.SS",
                "510050": "510050.SS",
                "512880": "512880.SS",
                "159915": "159915.SZ",
            }
            yf_symbol = yf_map.get(symbol, f"{symbol}.SS")
            tk = yf.Ticker(yf_symbol)
            hist = tk.history(period="90d")
            if hist is None or hist.empty:
                log_warning(f"yfinance ETF 返回空数据: {symbol} ({yf_symbol})")
                return None

            tmp = hist[["Close", "Volume"]].copy()
            tmp.reset_index(inplace=True)
            tmp.rename(columns={"Date": "date", "Close": "close", "Volume": "volume"}, inplace=True)
            df = tmp.sort_values("date").reset_index(drop=True)

            self._yf_cache[symbol] = df
            self._yf_cache_expire[symbol] = now + self._yf_ttl

            log_info(f"[ETF] {symbol} rows={len(df)} cols={list(df.columns)}")
            return df
        except Exception as e:
            log_warning(f"yfinance ETF 获取失败 {symbol}: {e}")
            return None

    # ---------- 流动性枯竭信号 ----------

    def get_liquidity_drying_signal(self) -> dict:
        """
        使用 510300 成交量作为流动性代理。
        """
        try:
            df = self.get_etf_daily(symbol="510300")
            if df is None or len(df) < 30:
                log_warning("510300 ETF 数据不足，流动性因子返回中性")
                return {
                    "liquidity_risk": False,
                    "risk_score": 0.0,
                    "signal_desc": "流动性数据不足，无法判断",
                    "detail": {"error": "data too short"},
                }

            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            vol = df["volume"].astype(float)

            vol_20_mean = vol.tail(20).mean()
            vol_20_min = vol.tail(20).min()
            vol_recent_3 = vol.tail(3)
            vol_today = vol.iloc[-1]

            cond1 = vol_recent_3.mean() < vol_20_mean * 0.85
            cond2 = vol_today <= vol_20_min * 1.05

            liquidity_risk = cond1 and cond2
            volume_ratio = round(vol_recent_3.mean() / (vol_20_mean + 1e-6), 3)
            risk_score = 2.0 if liquidity_risk else 0.0
            signal_desc = (
                "【重大风险】流动性严重枯竭（成交量连续萎缩+创阶段新低）"
                if liquidity_risk
                else "市场流动性正常"
            )

            log_info(
                f"[LIQ] drying={liquidity_risk}, "
                f"vol_3d/20d={volume_ratio}, today_vs_20d_min={vol_today / (vol_20_min + 1e-6):.3f}"
            )

            return {
                "liquidity_risk": liquidity_risk,
                "risk_score": risk_score,
                "signal_desc": signal_desc,
                "volume_drying": cond1,
                "volume_ratio": volume_ratio,
                "today_below_20d_min": cond2,
                "current_volume": float(vol_today),
                "vol_20_mean": float(vol_20_mean),
                "vol_20_min": float(vol_20_min),
                "detail": {
                    "vol_3d_mean": float(vol_recent_3.mean()),
                    "vol_20_mean": float(vol_20_mean),
                    "volume_ratio": volume_ratio,
                    "today_vs_20d_min": round(vol_today / (vol_20_min + 1e-6), 3),
                },
            }
        except Exception as e:
            log_error(f"流动性枯竭因子异常: {e}")
            return {
                "liquidity_risk": False,
                "risk_score": 0.0,
                "signal_desc": "流动性因子计算失败",
                "detail": {"error": str(e)},
            }

    # ---------- 快照接口（供因子引擎调用） ----------

    def prepare_daily_market_snapshot(self, bj_time: datetime) -> Dict[str, Any]:
    """构建 A 股日级市场快照，并使用文件缓存：
    - 缓存键: module='ashare', key='daily_snapshot'
    - 日期: 使用 bj_time 的日期 (YYYYMMDD)
    """
    date_str = bj_time.strftime("%Y%m%d")
    cached = read_day("ashare", "daily_snapshot", date_str)
    if isinstance(cached, dict) and cached:
        return cached

    snapshot: Dict[str, Any] = {}
    snapshot["treasury"] = self.get_treasury_yield()
    snapshot["us_equity"] = self.get_us_equity_snapshot()
    snapshot["eu_futures"] = self.get_eu_futures()
    snapshot["asia"] = self.get_asian_market()
    snapshot["a50_night"] = self.get_a50_night_session()
    snapshot["index"] = self.get_china_index_snapshot(bj_time)
    snapshot["advdec"] = self.get_advance_decline()
    snapshot["turnover"] = self.get_turnover()
    snapshot["liquidity"] = self.get_liquidity_drying_signal()
    snapshot["commodities"] = get_commodity_snapshot()

    # 写入日级缓存
    try:
        write_day("ashare", "daily_snapshot", snapshot, date_str)
    except Exception as e:
        log_warning(f"Failed to write ashare daily snapshot cache for {date_str}: {e}")

    return snapshot

    def prepare_intraday_snapshot(self, bj_time: datetime, slot: str) -> Dict[str, Any]:
        snapshot: Dict[str, Any] = {}
        snapshot["slot"] = slot
        snapshot["treasury"] = self.get_treasury_yield()
        snapshot["us_equity"] = self.get_us_equity_snapshot()
        snapshot["asia"] = self.get_asian_market()
        snapshot["eu_futures"] = self.get_eu_futures()
        snapshot["a50_night"] = self.get_a50_night_session()
        return snapshot
