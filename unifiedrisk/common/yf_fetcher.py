# -*- coding: utf-8 -*-
"""
UnifiedRisk v4.3.8 - DataFetcher
--------------------------------
负责获取 A 股全市场所需的全部数据，包括：
    • 指数行情（上证/深证/创业板/科创50）
    • 东方财富北向资金（日级最终）
    • 两融（融资融券）日级 + 历史
    • 全市场情绪（涨跌家数/平均涨跌幅）
    • 市场成交额 & 市值（上交所 / 深交所）
    • 大盘主力资金（主力/超大/大/中/小单）
    • 行业/概念主力资金（板块轮动）

本文件是 UnifiedRisk 的数据基础模块。
"""

import time
import json
import re
import requests
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import urlencode
import os
import akshare as ak
from typing import List, Dict, Any, Optional
from pathlib import Path

from unifiedrisk.common.ak_cache import AkCache
from unifiedrisk.common.cache_manager import CacheManager
try:
    import logging
    LOG = logging.getLogger("unifiedrisk.core.ashare.data_fetcher")
except Exception:
    class _Dummy:
        def info(self, *a, **k): pass
        def warning(self, *a, **k): pass
        def error(self, *a, **k): pass
    LOG = _Dummy()

#import logging
LOG = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parents[3]
ak_cache = AkCache(base_dir=str(BASE_DIR))
day_cache = CacheManager(base_dir=str(BASE_DIR))


BJ_TZ = datetime.now().astimezone().tzinfo


class DataFetcher:
    """
    核心数据采集类。
    每次运行 A-Share Daily Engine 时，由 DataFetcher 统一抓取数据。
    """

    def __init__(self):
        # 复用 session，减少延迟
        self.s = requests.Session()
        self.s.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/129.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
            "Connection": "keep-alive",
        })

    # ------------------------------------------------------------
    # 工具函数：安全 GET
    # ------------------------------------------------------------
    def _safe_get(self, url, headers=None, params=None, timeout=10, try_times=3):
        """
        更稳健的 GET：重试 + 捕获异常
        """
        for i in range(try_times):
            try:
                r = self.s.get(url, headers=headers, params=params, timeout=timeout)
                r.raise_for_status()
                return r.text
            except Exception as e:
                LOG.warning(f"[safe_get] Try {i+1}/{try_times} failed: {e}")
                time.sleep(0.6)
        return None

    # ------------------------------------------------------------
    # Ⅰ. 指数行情
    # ------------------------------------------------------------
    def get_index_daily(self):
        """
        获取 4 大指数的最新日行情：
            sh000001  上证
            sz399001  深证成指
            sz399006  创业板指
            sh000688  科创50
        """

        def _fetch(symbol):
            try:
                #df = ak.stock_zh_index_daily(symbol=symbol)
                records = ak_cache.stock_zh_index_daily_cached(symbol)
                df = pd.DataFrame(records)
                if df is None or df.empty:
                    return None
                row = df.iloc[-1]
                return {
                    "date": str(row["date"]),
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row["volume"]),
                }
            except Exception as e:
                LOG.warning(f"index fetch failed: {symbol}: {e}")
                return None

        return {
            "sh000001": _fetch("sh000001"),
            "sz399001": _fetch("sz399001"),
            "sz399006": _fetch("sz399006"),
            "sh000688": _fetch("sh000688"),
        }

    # ------------------------------------------------------------
    # Ⅱ. 全市场情绪：上涨 / 下跌 / 平盘 + 平均涨跌幅
    # ------------------------------------------------------------
    def get_market_breadth(self):
        """
        使用 ak.stock_zh_a_spot() 获取全市场情绪：
            - 上涨家数
            - 下跌家数
            - 平盘家数
            - 全市场平均涨跌幅
        """
        try:
            #df = ak.stock_zh_a_spot()
            df = self.get_spot_all_with_cache()
            if df is None or df.empty:
                return {}

            # 涨跌幅字段：通常叫 "涨跌幅" 或 "changepercent"
            col_change = None
            for c in ["涨跌幅", "changepercent", "change"]:
                if c in df.columns:
                    col_change = c
                    break

            if col_change is None:
                return {}

            ups = (df[col_change] > 0).sum()
            downs = (df[col_change] < 0).sum()
            flats = (df[col_change] == 0).sum()
            avg_chg = df[col_change].mean()

            return {
                "ups": int(ups),
                "downs": int(downs),
                "flats": int(flats),
                "avg_change_pct": float(avg_chg),
                "up_ratio": ups / max(1, (ups + downs + flats)),
            }

        except Exception as e:
            LOG.warning(f"market breadth failed: {e}")
            return {}

    # ------------------------------------------------------------
    # Ⅲ. 上交所行情（成交额、市值、换手率）
    # ------------------------------------------------------------
    def get_sse_summary(self):
        """
        使用 ak.stock_sse_deal_daily() 获取上交所：
            - 股票市值
            - 流通市值
            - 成交金额
        """
        try:
            #df = ak.stock_sse_deal_daily()
            records = ak_cache.stock_sse_deal_daily_cached()
            df = pd.DataFrame(records)
            if df is None or df.empty:
                return {}

            # 结构示例：
            #   1   市价总值   529411.2
            #   2   流通市值   496863.92
            #   3   成交金额   6759.46

            rec = {}
            for _, row in df.iterrows():
                name = str(row["单日情况"])
                val = float(row["股票"])
                if "市价总值" in name:
                    rec["total_mv"] = val
                elif "流通市值" in name:
                    rec["float_mv"] = val
                elif "成交金额" in name:
                    rec["turnover"] = val

            return rec
        except Exception as e:
            LOG.warning(f"SSE summary failed: {e}")
            return {}

    # ------------------------------------------------------------
    # Ⅳ. 深交所行情（成交额、市值）
    # ------------------------------------------------------------
    def get_szse_summary(self, date_str=None):
        """
        使用 ak.stock_szse_summary(date) 获取深交所：
            - 股票总市值
            - 流通市值
            - 成交金额
        """
        try:
            if date_str is None:
                date_str = datetime.now().strftime("%Y%m%d")

            #df = ak.stock_szse_summary(date=date_str)
            
            records = ak_cache.stock_szse_summary_cached()
            df = pd.DataFrame(records)

            if df is None or df.empty:
                return {}

            # 返回结构类似：
            #   股票 2915    1.011943e+12   4.133826e+13   3.587433e+13
            stock_row = df[df["证券类别"] == "股票"]
            if stock_row.empty:
                return {}

            row = stock_row.iloc[0]
            return {
                "total_mv": float(row["总市值"]),
                "float_mv": float(row["流通市值"]),
                "turnover": float(row["成交金额"]) / 1e8,  # 转换为亿元单位
            }

        except Exception as e:
            LOG.warning(f"SZSE summary failed: {e}")
            return {}

    # ------------------------------------------------------------
    # Ⅴ. 北向资金（使用 Datacenter RPT_MUTUAL_DEALAMT）
    # ------------------------------------------------------------
    def get_northbound_daily(self, start_date="2023-01-01"):
        """
        使用你已测试成功的 Datacenter RPT_MUTUAL_DEALAMT 接口。
        自动解析 JSONP。
        返回最近几天的北向日级数据。
        """

        url = (
            "https://datacenter-web.eastmoney.com/web/api/data/v1/get"
            "?reportName=RPT_MUTUAL_DEALAMT"
            "&columns=ALL"
            f"&filter=(TRADE_DATE>='{start_date}')"
            "&sortTypes=-1"
            "&sortColumns=TRADE_DATE"
            "&source=WEB&client=WEB"
        )

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Referer": "https://datacenter-web.eastmoney.com/",
            "Accept": "*/*",
        }

        text = self._safe_get(url, headers=headers, timeout=8, try_times=3)
        if not text:
            return {}

        # 去除 JSONP
        try:
            json_str = re.sub(r"^[^(]*\((.*)\)[^)]*$", r"\1", text)
            data = json.loads(json_str)
            result = data.get("result", {})
            rows = result.get("data", [])
        except Exception as e:
            LOG.warning(f"north json decode failed: {e}")
            return {}

        north_list = []
        for item in rows:
            try:
                north_list.append({
                    "date": item.get("TRADE_DATE"),
                    "fund_net": float(item.get("FUND_NET_BUY", 0.0)),
                    "fund_buy": float(item.get("FUND_BUY", 0.0)),
                    "fund_sell": float(item.get("FUND_SELL", 0.0)),
                    "hk2sh": float(item.get("HK2SH_NETBUY", 0.0)),
                    "hk2sz": float(item.get("HK2SZ_NETBUY", 0.0)),
                })
            except Exception:
                continue

        return {"north": north_list}


    # ------------------------------------------------------------
    # Ⅶ. 大盘主力资金（主力 / 超大单 / 大单 / 中单 / 小单）
    # ------------------------------------------------------------
    def get_market_mainflow(self):
        """
        使用 push2.eastmoney.com 全市场主力资金（日级）
        secid=1.000001 → 上证综合（全市场聚合）
        """

        url = "https://push2.eastmoney.com/api/qt/stock/fflow/kline/get"

        params = {
            "lmt": "0",           # 0=全部，推荐只取所有再取最后一条
            "klt": "1",           # 1=日K
            "fields1": "f1,f2,f3,f7",
            "fields2": (
                "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,"
                "f63,f64,f65"
            ),
            "ut": "b2884a393a59ad64002292a3e90d46a5",
            "secid": "1.000001",
            "cb": "jQuery11230987654321_1731234567890",
            "_": int(time.time() * 1000),
        }

        text = self._safe_get(url, params=params, timeout=10, try_times=3)
        if not text:
            return {}

        # 去 JSONP
        try:
            json_str = re.search(r"\((.*)\)", text).group(1)
            data = json.loads(json_str)
            kl = data.get("data", {}).get("klines", [])
            if not kl:
                return {}
        except Exception as e:
            LOG.warning(f"market mainflow json failed: {e}")
            return {}

        # 取最后一天
        latest = kl[-1]  # "2025-11-27 15:00,主力净额,主力净比,..."
        items = latest.split(",")
        if len(items) < 10:
            return {}

        try:
            return {
                "date": items[0],
                "main_net": float(items[2]),
                "super_net": float(items[4]),
                "large_net": float(items[6]),
                "mid_net": float(items[8]),
                "small_net": float(items[10]) if len(items) > 10 else 0.0,
            }
        except Exception:
            return {}

    # ------------------------------------------------------------
    # Ⅷ. 行业资金（你测试成功的 clist → fs=m:90+t:2）
    # ------------------------------------------------------------
    def get_sector_fund_flow(self, page_size=100):
        """
        行业/概念主力净流入（万元 → 亿元）
        包含：
            f62 主力
            f66 超大单
            f72 大单
            f78 中单
            f84 小单
        """

        ts = int(time.time() * 1000)
        callback = f"jQuery1123{int(time.time())%10000000000}_{ts}"

        params = {
            "np": "1",
            "fltt": "2",
            "invt": "2",
            "cb": callback,
            "fs": "m:90+t:2",
            "stat": "1",
            "fields": "f12,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87",
            "fid": "f62",
            "pn": "1",
            "pz": str(page_size),
            "po": "1",
            "ut": "8dec03ba335b81bf4ebdf7b29ec27d15",
        }

        url = "https://push2.eastmoney.com/api/qt/clist/get?" + urlencode(params)

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/130.0.0.0 Safari/537.36"
            ),
            "Referer": "https://data.eastmoney.com/",
            "Accept": "*/*",
        }

        text = self._safe_get(url, headers=headers, timeout=10, try_times=3)
        if not text:
            return {}

        try:
            m = re.search(r"^\w+\((.*)\);?$", text.strip())
            json_str = m.group(1) if m else text
            data = json.loads(json_str)
            diff = data.get("data", {}).get("diff", [])
        except Exception as e:
            LOG.warning(f"sector fund json failed: {e}")
            return {}

        out = []
        for item in diff:
            def _f(k):
                try:
                    return float(item.get(k, 0.0))
                except:
                    return 0.0

            out.append({
                "code": item.get("f12"),
                "name": item.get("f14"),
                "change_pct": _f("f3") or _f("f2"),
                "main_100m": _f("f62") / 1e4,
                "super_100m": _f("f66") / 1e4,
                "large_100m": _f("f72") / 1e4,
                "mid_100m": _f("f78") / 1e4,
                "small_100m": _f("f84") / 1e4,
                "main_ratio": _f("f184"),
                "super_ratio": _f("f69"),
                "large_ratio": _f("f75"),
                "mid_ratio": _f("f81"),
                "small_ratio": _f("f87"),
            })

        return {"sectors": out}

    # ------------------------------------------------------------
    # Ⅸ. fetch_daily_snapshot（核心入口）
    # ------------------------------------------------------------
    def fetch_daily_snapshot(self, date=None):
        """
        A 股日级别核心数据总入口。
        引擎 Engine 会调用本函数获取全部 raw 输入数据。

        返回结构（示例）：
        {
            "meta": {...},
            "index": {...},
            "breadth": {...},
            "sse": {...},
            "szse": {...},
            "north": {...},
            "margin": {...},
            "mainflow": {...},
            "sector": {...},
        }
        """

        bj_now = datetime.now(BJ_TZ)
        date_str = bj_now.strftime("%Y-%m-%d")
        date_int = bj_now.strftime("%Y%m%d")

        # 一：指数行情
        index_data = self.get_index_daily()

        # 二：全市场情绪
        breadth = self.get_market_breadth()

        # 三：上交所&深交所
        sse = self.get_sse_summary()
        szse = self.get_szse_summary(date_int)

        # 四：北向资金
        north = self.get_northbound_daily(start_date="2024-01-01")

        # 五：两融
        margin = self.get_margin_all(start_date="2024-01-01")

        # 六：大盘主力资金
        mainflow = self.get_market_mainflow()

        # 七：行业主力资金
        sector = self.get_sector_fund_flow(page_size=60)

        # --------------------------------------------------------
        # 汇总
        # --------------------------------------------------------
        return {
            "meta": {
                "bj_time": bj_now.strftime("%Y-%m-%d %H:%M:%S"),
                "date": date_str,
                "version": "UnifiedRisk_v4.3.8",
            },
            "index": index_data,
            "breadth": breadth,
            "sse": sse,
            "szse": szse,
            "north": north,
            "margin": margin,
            "mainflow": mainflow,
            "sector": sector,
        }

     
 
    def get_spot_all_with_cache(self) -> pd.DataFrame:
        """
        使用 AkCache 缓存的全市场快照。
        """
        records: List[Dict[str, Any]] = ak_cache.stock_zh_a_spot_cached()
        if not records:
            LOG.warning("[spot] cached spot_all is empty")
            return pd.DataFrame()
        return pd.DataFrame(records)

    # ------------------------------------------------------------
    # Ⅵ. 两融（RPTA_RZRQ_LSDB） - 日级 & 历史
    # ------------------------------------------------------------

    def get_margin_all(self, start_date: str = "2023-01-01", page_size: int = 100) -> Dict[str, Any]:
        """
        使用 EastMoney Datacenter RPTA_RZRQ_LSDB 获取两融历史数据（带缓存）。
        缓存 key: margin_all_{start_date}_{page_size}
        """
        cache_key = f"margin_all_{start_date}_{page_size}"
        cached = day_cache.get(cache_key)
        if cached is not None:
            return cached

        all_rows: List[Dict[str, Any]] = []
        page: int = 1

        while True:
            params = {
                "reportName": "RPTA_RZRQ_LSDB",
                "columns": "ALL",
                "source": "WEB",
                "sortColumns": "DIM_DATE",
                "sortTypes": "-1",
                "pageNumber": page,
                "pageSize": page_size,
                "filter": f"(DIM_DATE>='{start_date}')",
                "_": int(time.time() * 1000),
            }
            url = "https://datacenter-web.eastmoney.com/api/data/v1/get?" + urlencode(params)

            # 假定你原有类中已有 _safe_get
            text = self._safe_get(url, headers=None, timeout=10, try_times=3)
            if not text:
                LOG.warning(f"[margin] page {page} no response → break")
                break

            try:
                m = re.search(r"\(\s*(\{.*\})\s*\)", text)
                json_str = m.group(1) if m else text
                js = json.loads(json_str)
            except Exception as e:
                LOG.warning(f"[margin] decode error page {page}: {e} → break")
                break

            result = js.get("result")
            success = js.get("success", True)

            if not success:
                LOG.warning(
                    f"[margin] page {page} returned success=False, "
                    f"code={js.get('code')}, msg={js.get('message')} → break"
                )
                break

            if result in (None, "null"):
                LOG.warning(f"[margin] page {page} result is null → break")
                break

            rows = result.get("data") or []
            if not rows:
                LOG.warning(f"[margin] page {page} has no data → break")
                break

            for it in rows:
                try:
                    all_rows.append(
                        {
                            "date": it.get("DIM_DATE", "").split(" ")[0],
                            "rzye_100m": float(it.get("RZYE", 0.0)) / 1e8,
                            "rqye_100m": float(it.get("RQYE", 0.0)) / 1e8,
                            "rzmre_100m": float(it.get("RZMRE", 0.0)) / 1e8,
                            "rzrqye_100m": float(it.get("RZRQYE", 0.0)) / 1e8,
                            "rzrqycz_100m": float(it.get("RZRQYECZ", 0.0)) / 1e8,
                            "rzye_ratio": float(it.get("RZYEZB", 0.0)),
                        }
                    )
                except Exception:
                    continue

            page += 1
            time.sleep(0.25)

        all_rows = sorted(all_rows, key=lambda x: x["date"])
        result_dict = {"margin": all_rows}
        day_cache.set(cache_key, result_dict)
        return result_dict

    # ========= 预留：Northbound Proxy / Sector / Mainflow 缓存位 =========
    # 下面是示例接口，你可以按自己现有实现补充 fetch_xxx 的细节，然后复用 day_cache。
    def get_northbound_proxy_with_cache(self) -> Optional[Dict[str, Any]]:
        """
        示例：北向代理因子使用缓存（请在内部调用你现有的 fetch_northbound_proxy 实现）。
        """
        cache_key = "northbound_proxy"
        cached = day_cache.get(cache_key)
        if cached is not None:
            return cached

        try:
            data = self.fetch_northbound_proxy()  # <-- 需要你在原类中实现
        except Exception as e:
            LOG.warning(f"[northbound] fetch_northbound_proxy failed: {e}")
            return cached  # 若有旧缓存则用旧的

        if data is not None:
            day_cache.set(cache_key, data)
        return data

    def get_sector_fund_flow_with_cache(self) -> Optional[Dict[str, Any]]:
        """
        示例：板块资金流缓存入口。
        你可以把原来的板块主力/ETF 逻辑抽到 fetch_sector_fund_flow，再通过缓存包装。
        """
        cache_key = "sector_fund_flow"
        cached = day_cache.get(cache_key)
        if cached is not None:
            return cached

        try:
            data = self.fetch_sector_fund_flow()  # <-- 需要你在原类中实现
        except Exception as e:
            LOG.warning(f"[sector] fetch_sector_fund_flow failed: {e}")
            return cached

        if data is not None:
            day_cache.set(cache_key, data)
        return data


    def get_sse_turnover_no(self):
        records = ak_cache.stock_sse_deal_daily_cached()
        if not records:
            LOG.warning("sse_deal_daily empty or failed")
            return pd.DataFrame()
    
        return pd.DataFrame(records)

    def stock_szse_summary_cached(self):
        """
        深交所每日统计数据缓存 (ak.stock_szse_summary)
        """
        if ak is None:
            raise RuntimeError("akshare is not installed")

        def _fetch():
            #df = ak.stock_szse_summary()
            records = ak_cache.stock_szse_summary_cached()
            df = pd.DataFrame(records)

            return df.to_dict(orient="records")

        return self._get_or_fetch("szse_summary", _fetch) or []


    def stock_sse_deal_daily_cached(self):
        """
        上交所成交数据缓存 (ak.stock_sse_deal_daily)
        """
        if ak is None:
            raise RuntimeError("akshare is not installed")

        def _fetch():
            #df = ak.stock_sse_deal_daily()
            records = ak_cache.stock_sse_deal_daily_cached()
            df = pd.DataFrame(records)
            return df.to_dict(orient="records")

        return self._get_or_fetch("sse_deal_daily", _fetch) or []

    def get_sse_turnover(self):
        records = ak_cache.stock_sse_deal_daily_cached()
        if not records:
            LOG.warning("sse_deal_daily empty or failed")
            return pd.DataFrame()
    
        return pd.DataFrame(records)


    def get_cn_index_snap_with_cache(self):
        sh = ak_cache.stock_zh_index_daily_cached("sh000001")
        sz = ak_cache.stock_zh_index_daily_cached("sz399001")
    
        df_sh = pd.DataFrame(sh)
        df_sz = pd.DataFrame(sz)
    
        return {
            "shanghai": df_sh.iloc[-1].to_dict() if not df_sh.empty else None,
            "shenzhen": df_sz.iloc[-1].to_dict() if not df_sz.empty else None,
        }


# 结束：DataFetcher 类

