# core/adapters/datasources/providers/provider_em.py
# UnifiedRisk V12 - EastMoney Provider (EM)

from __future__ import annotations

import time
import requests
from typing import List, Dict, Any
from datetime import date, datetime, timedelta

from core.adapters.providers.provider_base import ProviderBase
from core.utils.logger import get_logger

LOG = get_logger("Provider.EM")

import pandas as pd
import requests
from akshare.utils.tqdm import get_tqdm

class EMProvider(ProviderBase):
    """
    EastMoney Provider
    ------------------
    - 结构型数据 Provider（两融等）
    - 不提供通用行情 series
    """

    BASE_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        ),
        "Referer": "https://data.eastmoney.com/",
    }

    def __init__(self):
        # ⭐ 关键修复点
        super().__init__(name="em")

    # ------------------------------------------------------------------
    # 必须实现的抽象方法（但明确声明不支持）
    # ------------------------------------------------------------------
    def fetch_series_raw(self, *args, **kwargs):
        raise NotImplementedError(
            "EMProvider does not support fetch_series_raw(). "
            "Use EM-specific methods like fetch_margin_series()."
        )

    # ------------------------------------------------------------------
    # EM 专用方法：两融
    # ------------------------------------------------------------------
    def fetch_margin_series(self, days: int = 40) -> List[Dict[str, Any]]:
        params = {
            "reportName": "RPTA_RZRQ_LSDB",
            "columns": "ALL",
            "sortColumns": "DIM_DATE",
            "sortTypes": "-1",
            "pageNumber": 1,
            "pageSize": days,
            "source": "WEB",
            "_": int(time.time() * 1000),
        }

        rows = self._fetch_raw(params)
        if not rows:
            LOG.error("[EMProvider] empty margin data")
            return []

        out: List[Dict[str, Any]] = []
        for r in rows:
            try:
                date = str(r.get("DIM_DATE"))[:10]
                out.append(
                    {
                        "date": date,
                        "rz_balance": self._to_e8(r.get("TOTAL_RZYE")),
                        "rq_balance": self._to_e8(r.get("TOTAL_RQYE")),
                        "total": self._to_e8(r.get("TOTAL_RZRQYE")),
                        "rz_buy": self._to_e8(r.get("TOTAL_RZMRE")),
                        "total_chg": self._to_e8(r.get("TOTAL_RZRQYECZ")),
                        "rz_ratio": float(r.get("TOTAL_RZYEZB") or 0.0),
                    }
                )
            except Exception as e:
                LOG.error("[EMProvider] parse row failed: %s", e)

        out.sort(key=lambda x: x["date"])
        return out

    # ------------------------------------------------------------------
    def _fetch_raw(self, params: Dict[str, Any], retry: int = 3) -> List[Dict[str, Any]]:
        for i in range(retry):
            try:
                resp = requests.get(
                    self.BASE_URL,
                    params=params,
                    headers=self.HEADERS,
                    timeout=15,
                )
                resp.raise_for_status()
                js = resp.json()
                data = (js.get("result") or {}).get("data") or []
                if data:
                    return data
            except Exception as e:
                LOG.warning("[EMProvider] fetch retry=%s err=%s", i + 1, e)
                time.sleep(1)
        return []

    @staticmethod
    def _to_e8(v: Any) -> float:
        try:
            return round(float(v) / 1e8, 2)
        except Exception:
            return 0.0
    # ------------------------------------------------------------------
    # EM 专用方法：Watchlist Supply（董监高增减持 + 大宗交易）
    #
    # 说明（冻结工程/层次规范）：
    # - Provider 负责“外部数据获取 + 最小清洗 + JSON-safe 转换”
    # - 不写 cache；cache 由 DataSource 层负责
    # - 不抛 silent exception；失败返回 data_status=ERROR + warnings
    # ------------------------------------------------------------------
    def fetch_watchlist_supply(
        self,
        trade_date: str,
        symbols: List[str],
        cfg: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        返回 raw dict（供 DataSource 落库/写 cache）：
        {
          "meta": {...},
          "warnings": [...],
          "symbols": {
             "300750.SZ": {
                 "insider": {"data_status": "OK|ERROR|MISSING", "rows": [...], "warnings":[...], "meta": {...}},
                 "block_trade": {"data_status": "...", ...}
             },
             ...
          }
        }

        注意：
        - 即使某标的近期无事件，也返回 data_status=OK + rows=[]
        - 只有“抓取失败/接口异常/字段缺失无法解析”才返回 ERROR/MISSING
        """
        lookback_days = int(cfg.get("lookback_days", 60))
        max_rows = int(cfg.get("max_rows", 60))
        insider_mode = str(cfg.get("insider_mode", "全部"))
        dzjy_scope = str(cfg.get("dzjy_scope", "A股"))

        insider_date_cols = cfg.get("insider_date_cols") or ["公告日期", "公告日", "公告时间", "变动日期", "日期"]
        dzjy_date_cols = cfg.get("dzjy_date_cols") or ["交易日期", "成交日期", "日期"]

        # trade_date -> date
        asof = self._parse_ymd(trade_date)
        cutoff = asof - timedelta(days=lookback_days)
        start_date = cutoff.strftime("%Y%m%d")
        end_date = asof.strftime("%Y%m%d")

        sym_list = [s for s in (symbols or []) if isinstance(s, str) and s.strip()]
        sym6_set = {self._sym6(s) for s in sym_list}

        raw: Dict[str, Any] = {
            "meta": {
                "provider": "em",
                "trade_date": trade_date,
                "lookback_days": lookback_days,
                "max_rows": max_rows,
                "insider_mode": insider_mode,
                "dzjy_scope": dzjy_scope,
                "generated_at": datetime.now().isoformat(timespec="seconds"),
            },
            "warnings": [],
            "symbols": {},
        }

        # 预置 per-symbol 默认结构（避免下游 KeyError）
        for s in sym_list:
            raw["symbols"][s] = {
                "insider": {"data_status": "OK", "rows": [], "warnings": [], "meta": {"cutoff": cutoff.isoformat()}},
                "block_trade": {"data_status": "OK", "rows": [], "warnings": [], "meta": {"cutoff": cutoff.isoformat()}},
            }

        ak = self._import_akshare()
        if ak is None:
            raw["warnings"].append("missing_dep:akshare")
            # 依赖缺失属于系统不可用：对所有 symbol 标记 ERROR
            for s in sym_list:
                raw["symbols"][s]["insider"]["data_status"] = "ERROR"
                raw["symbols"][s]["insider"]["warnings"].append("akshare_unavailable")
                raw["symbols"][s]["block_trade"]["data_status"] = "ERROR"
                raw["symbols"][s]["block_trade"]["warnings"].append("akshare_unavailable")
            return raw

        # --------------------------------------------------------------
        # 1) 董监高增减持：stock_ggcg_em 一次性拉取，再按代码过滤
        # --------------------------------------------------------------
        insider_df = None
        try:
            insider_df = self.stock_ggcg_em(symbol=insider_mode)  # type: ignore
        except Exception as e:
            raw["warnings"].append(f"fetch_failed:ggcg:{type(e).__name__}")
            insider_df = None

        if insider_df is None or getattr(insider_df, "empty", False):
            # 这里“全量为空”可能是接口异常，也可能是当期无数据。
            # 为了不误伤，标记为 OK+空，并给 warning（便于定位）。
            raw["warnings"].append("empty:ggcg_all")
        else:
            try:
                # 统一列名：至少保证 code/date 可用
                insider_df = self._normalize_cols(insider_df)
                code_col = self._pick_col(insider_df, ["代码", "证券代码", "股票代码", "symbol", "Symbol"])
                if code_col is None:
                    raw["warnings"].append("missing_col:ggcg:code")
                else:
                    insider_df["_code6"] = insider_df[code_col].astype(str).str.zfill(6)
                    # 日期过滤（在 df 层做一次，减少后续 per-symbol 子集规模）
                    insider_df = self._filter_by_date(insider_df, insider_date_cols, cutoff, asof)

                    for s in sym_list:
                        s6 = self._sym6(s)
                        sub = insider_df[insider_df["_code6"] == s6]
                        rows = self._df_to_rows_json_safe(sub, max_rows=max_rows)
                        # 即使 rows 为空也算 OK（表示“无事件”）
                        raw["symbols"][s]["insider"]["rows"] = rows
                        raw["symbols"][s]["insider"]["meta"].update(
                            {"rows": len(rows), "insider_mode": insider_mode}
                        )
                        if not rows:
                            raw["symbols"][s]["insider"]["warnings"].append("no_events")
            except Exception as e:
                LOG.debug(e)
                raw["warnings"].append(f"parse_failed:ggcg:{type(e).__name__}")
                for s in sym_list:
                    raw["symbols"][s]["insider"]["data_status"] = "ERROR"
                    raw["symbols"][s]["insider"]["warnings"].append(f"parse_failed:{type(e).__name__}")

        # --------------------------------------------------------------
        # 2) 大宗交易：stock_dzjy_mrmx 一次性拉取，再按代码过滤
        # --------------------------------------------------------------
        dzjy_df = None
        try:
            dzjy_df = ak.stock_dzjy_mrmx(symbol=dzjy_scope, start_date=start_date, end_date=end_date)  # type: ignore
        except Exception as e:
            raw["warnings"].append(f"fetch_failed:dzjy:{type(e).__name__}")
            dzjy_df = None

        if dzjy_df is None or getattr(dzjy_df, "empty", False):
            raw["warnings"].append("empty:dzjy_all")
        else:
            try:
                dzjy_df = self._normalize_cols(dzjy_df)
                code_col = self._pick_col(dzjy_df, ["代码", "证券代码", "股票代码", "symbol", "Symbol"])
                if code_col is None:
                    raw["warnings"].append("missing_col:dzjy:code")
                else:
                    dzjy_df["_code6"] = dzjy_df[code_col].astype(str).str.zfill(6)
                    # 仅保留 watchlist codes（否则量太大）
                    dzjy_df = dzjy_df[dzjy_df["_code6"].isin(sym6_set)]
                    dzjy_df = self._filter_by_date(dzjy_df, dzjy_date_cols, cutoff, asof)

                    for s in sym_list:
                        s6 = self._sym6(s)
                        sub = dzjy_df[dzjy_df["_code6"] == s6]
                        rows = self._df_to_rows_json_safe(sub, max_rows=max_rows)
                        raw["symbols"][s]["block_trade"]["rows"] = rows
                        raw["symbols"][s]["block_trade"]["meta"].update(
                            {"rows": len(rows), "dzjy_scope": dzjy_scope, "start_date": start_date, "end_date": end_date}
                        )
                        if not rows:
                            raw["symbols"][s]["block_trade"]["warnings"].append("no_events")
            except Exception as e:
                raw["warnings"].append(f"parse_failed:dzjy:{type(e).__name__}")
                for s in sym_list:
                    raw["symbols"][s]["block_trade"]["data_status"] = "ERROR"
                    raw["symbols"][s]["block_trade"]["warnings"].append(f"parse_failed:{type(e).__name__}")

        return raw

    # -------------------------- helpers (watchlist_supply) --------------------------
    @staticmethod
    def _import_akshare():
        try:
            import akshare as ak  # type: ignore
            return ak
        except Exception:
            return None

    @staticmethod
    def _parse_ymd(s: str) -> date:
        try:
            return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()
        except Exception:
            # 容错：尝试 yyyymmdd
            return datetime.strptime(str(s)[:8], "%Y%m%d").date()

    @staticmethod
    def _sym6(symbol: str) -> str:
        s = str(symbol).strip()
        # "300750.SZ" -> "300750"
        if "." in s:
            s = s.split(".", 1)[0]
        return s.zfill(6)

    @staticmethod
    def _normalize_cols(df):
        try:
            df = df.copy()
            df.columns = [str(c).strip() for c in df.columns]
            return df
        except Exception:
            return df

    @staticmethod
    def _pick_col(df, candidates: List[str]):
        ########cols = set(getattr(df, "columns", []) or [])
        if hasattr(df, "columns") and df is not None:
            cols = set(df.columns)
        else:
            cols = set()
        for c in candidates:
            if c in cols:
                return c
        # fuzzy match
        for c in cols:
            for cand in candidates:
                if cand.lower() == str(c).lower():
                    return c
        return None

    @classmethod
    def _filter_by_date(cls, df, date_cols: List[str], cutoff: date, asof: date):
        """
        在 df 中选一个可用的 date 列，做 [cutoff, asof] 过滤。
        若找不到可用列，则原样返回。
        """
        try:
            import pandas as pd  # type: ignore
            for c in date_cols:
                if c in df.columns:
                    dt = pd.to_datetime(df[c], errors="coerce")
                    mask = (dt.dt.date >= cutoff) & (dt.dt.date <= asof)
                    return df.loc[mask].copy()
        except Exception:
            return df
        return df

    @classmethod
    def _df_to_rows_json_safe(cls, df, max_rows: int = 60) -> List[Dict[str, Any]]:
        """
        DataFrame -> rows(list[dict]) 并确保 JSON 可序列化（date/NaT/Timestamp -> str/None）。
        """
        try:
            import pandas as pd  # type: ignore
            if df is None or getattr(df, "empty", False):
                return []
            # 按日期列（若存在）降序
            for c in ["交易日期", "公告日期", "公告日", "变动日期", "日期"]:
                if c in df.columns:
                    try:
                        df = df.copy()
                        df["_dt_sort"] = pd.to_datetime(df[c], errors="coerce")
                        df = df.sort_values("_dt_sort", ascending=False).drop(columns=["_dt_sort"])
                    except Exception:
                        pass
                    break
            df = df.head(max_rows)
            rows: List[Dict[str, Any]] = []
            for _, r in df.iterrows():
                d = {}
                for k, v in r.to_dict().items():
                    if k == "_code6":
                        continue
                    d[str(k)] = cls._json_safe(v)
                rows.append(d)
            return rows
        except Exception:
            return []

    @staticmethod
    def _json_safe(v: Any) -> Any:
        try:
            import pandas as pd  # type: ignore
            # NaT / NaN
            if v is None:
                return None
            if isinstance(v, float) and (v != v):  # NaN
                return None
            if "NaTType" in type(v).__name__:
                return None
            if hasattr(pd, "isna") and pd.isna(v):
                return None
        except Exception:
            pass

        # date / datetime / pandas timestamp
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        tn = type(v).__name__
        if tn in ("Timestamp",):
            try:
                return v.to_pydatetime().isoformat()  # type: ignore
            except Exception:
                return str(v)

        # numpy scalar
        try:
            import numpy as np  # type: ignore
            if isinstance(v, np.generic):
                return v.item()
        except Exception:
            pass

        # others
        if isinstance(v, (int, float, str, bool)):
            return v
        return str(v)


    
    def stock_ggcg_em(self, symbol: str = "股东减持") -> pd.DataFrame:
        """
        东方财富网-数据中心-特色数据-高管持股
        https://data.eastmoney.com/executive/gdzjc.html
        :param symbol: choice of {"全部", "股东增持", "股东减持"}
        :type symbol: str
        :return: 高管持股
        :rtype: pandas.DataFrame
        """
        symbol_map = {
            "全部": "",
            "股东增持": '(DIRECTION="增持")',
            "股东减持": '(DIRECTION="减持")',
        }
        url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
        params = {
            "sortColumns": "END_DATE,SECURITY_CODE,EITIME",
            "sortTypes": "-1,-1,-1",
            "pageSize": "500",
            "pageNumber": "1",
            "reportName": "RPT_SHARE_HOLDER_INCREASE",
            "quoteColumns": "f2~01~SECURITY_CODE~NEWEST_PRICE,f3~01~SECURITY_CODE~CHANGE_RATE_QUOTES",
            "quoteType": "0",
            "columns": "ALL",
            "source": "WEB",
            "client": "WEB",
            "filter": symbol_map[symbol],
        }
        r = requests.get(url, params=params)
        data_json = r.json()
        total_page = data_json["result"]["pages"]
        total_page = 1
        # 该 fetcher 用于系统化拉取，避免在运行时打印噪音
        LOG.debug("stock_ggcg_em total_page=%s", total_page)
        
        big_df = pd.DataFrame()
        tqdm = get_tqdm()
        for page in tqdm(range(1, total_page + 1), leave=False):
            params.update(
                {
                    "pageNumber": page,
                }
            )
            r = requests.get(url, params=params)
            data_json = r.json()
            temp_df = pd.DataFrame(data_json["result"]["data"])
            big_df = pd.concat(objs=[big_df, temp_df], ignore_index=True)
    
        big_df.columns = [
            "持股变动信息-变动数量",
            "公告日",
            "代码",
            "股东名称",
            "持股变动信息-占总股本比例",
            "_",
            "-",
            "变动截止日",
            "-",
            "变动后持股情况-持股总数",
            "变动后持股情况-占总股本比例",
            "_",
            "变动后持股情况-占流通股比例",
            "变动后持股情况-持流通股数",
            "_",
            "名称",
            "持股变动信息-增减",
            "_",
            "持股变动信息-占流通股比例",
            "变动开始日",
            "_",
            "最新价",
            "涨跌幅",
            "_",
        ]
        big_df = big_df[
            [
                "代码",
                "名称",
                "最新价",
                "涨跌幅",
                "股东名称",
                "持股变动信息-增减",
                "持股变动信息-变动数量",
                "持股变动信息-占总股本比例",
                "持股变动信息-占流通股比例",
                "变动后持股情况-持股总数",
                "变动后持股情况-占总股本比例",
                "变动后持股情况-持流通股数",
                "变动后持股情况-占流通股比例",
                "变动开始日",
                "变动截止日",
                "公告日",
            ]
        ]
    
        big_df["最新价"] = pd.to_numeric(big_df["最新价"], errors="coerce")
        big_df["涨跌幅"] = pd.to_numeric(big_df["涨跌幅"], errors="coerce")
        big_df["持股变动信息-变动数量"] = pd.to_numeric(big_df["持股变动信息-变动数量"])
        big_df["持股变动信息-占总股本比例"] = pd.to_numeric(
            big_df["持股变动信息-占总股本比例"]
        )
        big_df["持股变动信息-占流通股比例"] = pd.to_numeric(
            big_df["持股变动信息-占流通股比例"]
        )
        big_df["变动后持股情况-持股总数"] = pd.to_numeric(big_df["变动后持股情况-持股总数"])
        big_df["变动后持股情况-占总股本比例"] = pd.to_numeric(
            big_df["变动后持股情况-占总股本比例"]
        )
        big_df["变动后持股情况-持流通股数"] = pd.to_numeric(
            big_df["变动后持股情况-持流通股数"]
        )
        big_df["变动后持股情况-占流通股比例"] = pd.to_numeric(
            big_df["变动后持股情况-占流通股比例"]
        )
        big_df["变动开始日"] = pd.to_datetime(big_df["变动开始日"]).dt.date
        big_df["变动截止日"] = pd.to_datetime(big_df["变动截止日"]).dt.date
        big_df["公告日"] = pd.to_datetime(big_df["公告日"]).dt.date
    
        return big_df

