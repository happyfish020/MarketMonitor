import requests
import datetime as dt
import logging
import json
import re
import time
from urllib.parse import urlencode
from typing import Any, Dict, List, Optional


BJ_TZ = dt.timezone(dt.timedelta(hours=8))

LOG = logging.getLogger("UnifiedRisk.DataFetcher")
if not LOG.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )


class DataFetcher:
    """
    v4.3.7 数据抓取层：
      - ETF 成交额/成交量（510300.SS / 159901.SZ, 来自 Yahoo）
      - 北向资金日级（RPT_MUTUAL_DEALAMT, /web/api）
      - 两融日级（RPTA_RZRQ_LSDB, /api）
      - 大盘快照（push2.clist，主板股票涨跌情况）
    """

    def __init__(self, session: Optional[requests.Session] = None):
        self.s = session or requests.Session()
        self.s.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        })

    # --------- 时间工具 ---------
    def _today_cst(self) -> dt.date:
        return dt.datetime.now(BJ_TZ).date()

    def _ensure_date(self, d: Optional[dt.date]) -> dt.date:
        return d or self._today_cst()

    # --------- Yahoo ETF 抓取：510300.SS / 159901.SZ ---------
    def _fetch_yahoo_etf(self, symbol: str) -> Dict[str, Any]:
        """
        使用 Yahoo Finance v8 chart 接口获取日级最后一条数据：
        - close
        - volume
        - 简单 5 日平均量 → volume_ratio 代理
        """
        url = (
            "https://query1.finance.yahoo.com/v8/finance/chart/"
            f"{symbol}?interval=1d&range=6d"
        )
        try:
            resp = self.s.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            result_list = data.get("chart", {}).get("result") or []
            if not result_list:
                return {}

            result = result_list[0]
            indicator = (result.get("indicators") or {}).get("quote") or []
            if not indicator:
                return {}

            quote = indicator[0]
            closes = quote.get("close") or []
            volumes = quote.get("volume") or []

            if not closes or not volumes:
                return {}

            last_close = closes[-1] or 0.0
            last_vol = volumes[-1] or 0
            prev_close = 0.0
            if len(closes) >= 2 and closes[-2] is not None:
                prev_close = closes[-2] or 0.0

            prev_vols = [v for v in volumes[:-1] if v]
            if prev_vols:
                avg_vol = sum(prev_vols) / len(prev_vols)
            else:
                avg_vol = last_vol or 1

            if avg_vol:
                vol_ratio = float(last_vol or 0) / float(avg_vol)
            else:
                vol_ratio = 0.0

            turnover_100m = (last_close * last_vol) / 1e8  # 亿元级

            return {
                "symbol": symbol,
                "close": float(last_close or 0.0),
                "prev_close": float(prev_close or 0.0),
                "volume": int(last_vol or 0),
                "turnover_100m": float(turnover_100m),
                "volume_ratio": float(vol_ratio),
            }
        except Exception as e:
            LOG.warning("fetch yahoo etf failed: %s %s", symbol, e)
            return {}

    def get_etf_market_snapshot(self) -> Dict[str, Any]:
        """
        使用 ETF 代理沪深市场：
          - 510300.SS 作为沪深300代理（偏沪）
          - 159901.SZ 作为深市代理
        """
        sh = self._fetch_yahoo_etf("510300.SS")
        sz = self._fetch_yahoo_etf("159901.SZ")
        return {"sh": sh, "sz": sz}

    # --------- 北向资金（RPT_MUTUAL_DEALAMT, /web/api） ---------
    def get_northbound_daily(self, date: Optional[dt.date] = None) -> List[Dict[str, Any]]:
        d = self._ensure_date(date)
        s = d.strftime("%Y-%m-%d")

        url = (
            "https://datacenter-web.eastmoney.com/web/api/data/v1/get"
            "?reportName=RPT_MUTUAL_DEALAMT"
            "&columns=ALL"
            f"&filter=(TRADE_DATE>='{s}')(TRADE_DATE<='{s}')"
            "&sortTypes=-1"
            "&sortColumns=TRADE_DATE"
            "&source=WEB&client=WEB"
        )

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Referer": "https://datacenter-web.eastmoney.com/",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Connection": "keep-alive",
        }

        try:
            resp = self.s.get(url, headers=headers, timeout=8)
            text = resp.text

            # 处理 JSONP： callback(...json...)
            json_str = re.sub(r"^[^(]*\((.*)\)[^)]*$", r"\1", text)
            data = json.loads(json_str)

            rows = (data.get("result") or {}).get("data") or []
            out: List[Dict[str, Any]] = []
            for r in rows:
                out.append(
                    {
                        "northbound_net": float(r.get("S2N_NetBuyAmt") or 0.0),
                        "northbound_total": float(r.get("S2N_Turnover") or 0.0),
                        "raw": r,
                    }
                )
            return out
        except Exception as e:
            LOG.warning("fetch northbound failed: %s", e)
            return []

    # --------- 两融（日级, RPTA_RZRQ_LSDB, /api） ---------
    def get_margin_daily(self, date: Optional[dt.date] = None) -> Dict[str, Any]:
        """
        参考你提供的 get_rzrq_all_history 代码，做成单日查询：
          - 使用 RPTA_RZRQ_LSDB
          - sort DIM_DATE desc, pageNumber=1
          - filter: DIM_DATE >= date
        取出匹配 date 的那一条（沪深合计）
        """
        d = self._ensure_date(date)
        ds = d.strftime("%Y-%m-%d")

        session = self.s
        session.headers.update({
            "Referer": "https://data.eastmoney.com/rzrq/",
        })

        params = {
            "reportName": "RPTA_RZRQ_LSDB",
            "columns": "ALL",
            "source": "WEB",
            "sortColumns": "DIM_DATE",
            "sortTypes": "-1",
            "pageNumber": 1,
            "pageSize": 500,
            "filter": f"(DIM_DATE>='{ds}')",
            "_": int(time.time() * 1000),
        }

        url = "https://datacenter-web.eastmoney.com/api/data/v1/get?" + urlencode(params)

        try:
            r = session.get(url, timeout=15)
            text = r.text

            # 稳健解析 JSONP：抓取第一个 {...} 块
            m = re.search(r"(\{.*\})", text)
            if not m:
                LOG.warning("margin json not found in response")
                return {}

            data = json.loads(m.group(1))
            records = (data.get("result") or {}).get("data") or []
            if not records:
                return {}

            # 查找匹配日期的记录（DIM_DATE 带时间，取前 10 位比较）
            target = None
            for rec in records:
                dim_date = str(rec.get("DIM_DATE") or "")[:10]
                if dim_date == ds:
                    target = rec
                    break

            if target is None:
                # 找不到当日，就用第一条（最新）兜底
                target = records[0]

            # 关键字段转为 亿元
            def _to_100m(x):
                try:
                    return float(x) / 1e8
                except Exception:
                    return 0.0

            rzye = _to_100m(target.get("RZYE"))
            rqye = _to_100m(target.get("RQYE"))
            rzmre = _to_100m(target.get("RZMRE"))
            rzrqye = _to_100m(target.get("RZRQYE"))
            rzrqyecz = _to_100m(target.get("RZRQYECZ"))
            ltsz = _to_100m(target.get("LTSZ"))
            rzyezb = float(target.get("RZYEZB") or 0.0)

            return {
                "date": ds,
                "融资余额_亿": rzye,
                "融资买入额_亿": rzmre,
                "融券余额_亿": rqye,
                "两融余额_亿": rzrqye,
                "两融余额增减_亿": rzrqyecz,
                "融资余额占比_pct": rzyezb,
                "证券出借余额_亿": ltsz,
                "raw": target,
            }
        except Exception as e:
            LOG.warning("fetch margin failed: %s", e)
            return {}

    # --------- 大盘快照：push2.clist（主板股票涨跌情况） ---------
    def get_market_snapshot_clist(self, fs: str = "b:MK0010", page_size: int = 200) -> Dict[str, Any]:
        """
        使用东财 push2.clist 获取主板股票列表：
          - 统计上涨 / 下跌 / 平盘家数
          - 计算平均涨跌幅
          - 计算 sample 成交额（亿元）
          - 提取 Top 涨幅 / 跌幅 / 振幅 股票
        """
        timestamp = int(time.time() * 1000)
        callback = "jQuery1123%d_%d" % (
            int(time.time()) % 10000000000,
            timestamp,
        )

        params = {
            "np": "1",
            "fltt": "1",
            "invt": "2",
            "cb": callback,
            "fs": fs,  # b:MK0010=主板
            "fields": "f12,f13,f14,f1,f2,f4,f3,f152,f5,f6,f18,f17,f15,f16",
            "fid": "f3",  # 排序字段（这里用涨跌幅或其他字段都可以）
            "pn": "1",
            "pz": str(page_size),
            "po": "1",
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "dect": "1",
            "wbp2u": "9890067320575914|0|1|0|web",
            "_": str(timestamp),
        }

        url = "https://push2.eastmoney.com/api/qt/clist/get?" + urlencode(params)

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "Referer": "https://quote.eastmoney.com/center/gridlist.html#hs_a_board",
            "Accept": "*/*",
        }

        try:
            resp = self.s.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            text = resp.text

            # 稳健 JSONP 解析
            if "(" in text and ")" in text:
                start = text.find("(") + 1
                end = text.rfind(")")
                if start < end:
                    json_str = text[start:end]
                else:
                    raise ValueError("JSONP 括号不匹配")
            else:
                json_str = text

            data = json.loads(json_str)
            diff = (data.get("data") or {}).get("diff") or []
            if not diff:
                return {}

            up = down = flat = 0
            sum_change = 0.0
            sum_amt_100m = 0.0

            records: List[Dict[str, Any]] = []

            for item in diff:
                try:
                    code = item.get("f2")       # 代码
                    name = item.get("f3")       # 名称
                    price = float(item.get("f4") or 0.0)  # 现价
                    change_rate = float(item.get("f1") or 0.0)  # 涨跌幅 %
                    amt_wan = float(item.get("f13") or 0.0)     # 成交额 万元
                    vol_wan = float(item.get("f12") or 0.0)     # 成交量 万股
                    amp = float(item.get("f14") or 0.0)         # 振幅 %
                except Exception:
                    continue

                if change_rate > 0:
                    up += 1
                elif change_rate < 0:
                    down += 1
                else:
                    flat += 1

                sum_change += change_rate
                sum_amt_100m += amt_wan / 100.0  # 万元 → 亿元

                records.append({
                    "code": code,
                    "name": name,
                    "price": price,
                    "change_pct": change_rate,
                    "amount_100m": amt_wan / 100.0,
                    "volume_wan": vol_wan,
                    "amp_pct": amp,
                })

            n = len(records)
            mean_change = sum_change / n if n > 0 else 0.0

            # TOP 统计
            # 涨幅前 10
            top_gainers = sorted(records, key=lambda x: x["change_pct"], reverse=True)[:10]
            # 跌幅前 10
            top_losers = sorted(records, key=lambda x: x["change_pct"])[:10]
            # 振幅前 10
            top_amp = sorted(records, key=lambda x: x["amp_pct"], reverse=True)[:10]

            return {
                "up": up,
                "down": down,
                "flat": flat,
                "breadth": up - down,
                "mean_change": mean_change,
                "total_amt_100m": sum_amt_100m,
                "top_gainers": top_gainers,
                "top_losers": top_losers,
                "top_amplitude": top_amp,
                "sample_size": n,
            }
        except Exception as e:
            LOG.warning("fetch market snapshot (clist) failed: %s", e)
            return {}

    # --------- 汇总：日级快照（给 engine 用） ---------
    def fetch_daily_snapshot(self, date: Optional[dt.date] = None) -> Dict[str, Any]:
        d = self._ensure_date(date)
        LOG.info("Fetching daily snapshot for %s", d)

        etf = self.get_etf_market_snapshot()
        north = self.get_northbound_daily(d)
        margin = self.get_margin_daily(d)
        market = self.get_market_snapshot_clist()

        return {
            "date": d.strftime("%Y-%m-%d"),
            "bj_time": dt.datetime.now(BJ_TZ).isoformat(timespec="seconds"),
            "version": "UnifiedRisk_v4.3.7",
            "etf": etf,
            "north": north,
            "margin": margin,
            "market": market,
        }
