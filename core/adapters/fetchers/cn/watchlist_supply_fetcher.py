# -*- coding: utf-8 -*-
"""
core/adapters/fetchers/cn/watchlist_supply_fetcher.py

WatchlistSupply Fetcher (RAW MVP)

【定位】
- 仅负责“抓取原始数据 + 轻量字段标准化 + 稳定窗口过滤（基于 trade_date）”
- 不做任何解释/打分/聚合（这些必须放在更上层：Factor / WatchlistLeadBuilder）
- 不写 cache（cache 由 DataSource 统一负责）
- 永不抛异常：失败返回结构化默认块 + warnings（禁止 silent exception）

【MVP 数据源】
1) 董监高增减持（akshare: stock_em_ggcg）
2) 大宗交易每日明细（akshare: stock_dzjy_mrmx）

【输出契约（RAW）】
fetch(trade_date, symbols, cfg_dict) -> dict:
{
  "meta": {"trade_date": "YYYY-MM-DD", "start_date": "YYYYMMDD", "end_date": "YYYYMMDD", "source": "akshare"},
  "symbols": {
     "300394.SZ": {
        "insider": {"data_status": "OK|MISSING|ERROR", "rows": [...], "warnings": [...]},
        "block_trade": {"data_status": "OK|MISSING|ERROR", "rows": [...], "warnings": [...]},
        "warnings": [...]
     }
  },
  "warnings": [...]
}
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import math

import pandas as pd
import requests
from akshare.utils.tqdm import get_tqdm


try:
    from core.utils.logger import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging
    def get_logger(name: str):
        return logging.getLogger(name)


LOG = get_logger("Fetcher.WatchlistSupply")


def _parse_trade_date(trade_date: str) -> datetime:
    s = str(trade_date).strip()
    if len(s) == 8 and s.isdigit():
        return datetime.strptime(s, "%Y%m%d")
    return datetime.strptime(s, "%Y-%m-%d")


def _yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def _sym6(symbol: str) -> str:
    return str(symbol).split(".")[0]


def _df_to_rows(df: Any, limit: int) -> List[Dict[str, Any]]:
    """将 DataFrame 轻量化为 list[dict]，避免 cache 写入巨大对象。"""
    out: List[Dict[str, Any]] = []
    try:
        if df is None:
            return out
        # pandas DataFrame
        import pandas as pd  # type: ignore

        try:
            import numpy as np  # type: ignore
        except Exception:  # pragma: no cover
            np = None  # type: ignore

        def _jsonable(v: Any) -> Any:
            # None
            if v is None:
                return None

            # pandas 缺失值（pd.NaT / pd.NA / numpy NaN 等）
            # 注意：某些列会出现 NaTType（不是 Timestamp），仅靠 `v is pd.NaT` 不够
            try:
                if pd.isna(v):
                    return None
            except Exception:
                pass
            # pandas NaT / Timestamp
            try:
                if isinstance(v, pd.Timestamp):
                    # 统一到 YYYY-MM-DD（供给类事件无需到秒）
                    return v.to_pydatetime().date().isoformat()
            except Exception:
                pass

            # datetime/date
            if isinstance(v, (datetime, date)):
                return v.isoformat()

            # float NaN/Inf
            if isinstance(v, float):
                if math.isnan(v) or math.isinf(v):
                    return None
                return v

            # numpy scalar
            if np is not None:
                try:
                    if isinstance(v, np.generic):
                        vv = v.item()
                        return _jsonable(vv)
                except Exception:
                    pass

            # container types
            if isinstance(v, dict):
                return {str(k): _jsonable(val) for k, val in v.items()}
            if isinstance(v, (list, tuple)):
                return [_jsonable(x) for x in v]

            # default
            return v

        cols = list(getattr(df, "columns", []))
        for _, row in df.head(limit).iterrows():
            item: Dict[str, Any] = {}
            for c in cols:
                v = None
                try:
                    v = row.get(c)
                except Exception:
                    try:
                        v = row[c]
                    except Exception:
                        v = None
                item[str(c)] = _jsonable(v)
            out.append(item)
        return out
    except Exception as e:
        # 最坏情况：返回空 + 在上层 warnings 记录
        LOG.warning("df_to_rows failed: %s", e)
        return out


def _filter_rows_by_trade_date(df: Any, date_col_candidates: List[str], cutoff: datetime, warnings: List[str]) -> Any:
    """
    基于 trade_date 计算的 cutoff 做稳定过滤：
    - 有 pandas 且能解析日期列：过滤到 dt >= cutoff
    - 否则不筛，并给 warnings（但不抛异常）
    """
    try:
        if df is None:
            return df
        import pandas as pd  # type: ignore

        # 找到第一个存在的日期列
        date_col = None
        cols = set(map(str, getattr(df, "columns", [])))
        for c in date_col_candidates:
            if c in cols:
                date_col = c
                break
        if not date_col:
            warnings.append("date_col_missing")
            return df

        dt_series = pd.to_datetime(df[date_col], errors="coerce")
        return df[dt_series >= cutoff]
    except Exception as e:
        warnings.append(f"date_filter_skipped:{type(e).__name__}")
        return df


@dataclass
class WatchlistSupplyFetchCfg:
    lookback_days: int = 60
    max_rows: int = 60
    # date col candidates
    insider_date_cols: List[str] = None  # type: ignore
    dzjy_date_cols: List[str] = None  # type: ignore

    def __post_init__(self) -> None:
        if self.insider_date_cols is None:
            self.insider_date_cols = ["公告日期", "公告日", "公告时间", "变动日期", "日期"]
        if self.dzjy_date_cols is None:
            self.dzjy_date_cols = ["交易日期", "成交日期", "日期"]


class WatchlistSupplyFetcher:
    def fetch(self, trade_date: str, symbols: List[str], cfg: Dict[str, Any]) -> Dict[str, Any]:
        warnings: List[str] = []
        td = _parse_trade_date(trade_date)
        lookback_days = int(cfg.get("lookback_days", 60) or 60)
        max_rows = int(cfg.get("max_rows", cfg.get("top_n_records", 60)) or 60)

        cutoff = td - timedelta(days=lookback_days)
        start_date = _yyyymmdd(cutoff)
        end_date = _yyyymmdd(td)

        out: Dict[str, Any] = {
            "meta": {
                "trade_date": td.strftime("%Y-%m-%d"),
                "start_date": start_date,
                "end_date": end_date,
                "source": "akshare",
                "lookback_days": lookback_days,
                "max_rows": max_rows,
            },
            "symbols": {},
            "warnings": warnings,
        }

        try:
            import akshare as ak  # type: ignore
        except Exception as e:
            warnings.append(f"missing:akshare:{type(e).__name__}")
            # 所有 symbol 直接返回 MISSING
            for sym in symbols:
                out["symbols"][sym] = {
                    "insider": {"data_status": "MISSING", "rows": [], "warnings": ["missing:akshare"]},
                    "block_trade": {"data_status": "MISSING", "rows": [], "warnings": ["missing:akshare"]},
                    "warnings": ["missing:akshare"],
                }
            return out

        # ==============================
        # 预拉取：避免对“同一 API（返回全表）”在 symbols 上重复调用
        # ==============================
        insider_df_all = None
        dzjy_df_all = None

        # 1) 董监高/股东增减持：全表一次性返回 -> 过滤 code
        insider_mode = str(cfg.get("insider_mode") or "股东减持")
        try:
            insider_df_all = stock_ggcg_em(symbol=insider_mode)
        except Exception as e:
            warnings.append(f"prefetch_failed:ggcg:{type(e).__name__}")
            insider_df_all = None

        # 2) 大宗交易明细：按 A 股范围拉取（窗口过滤后再按 code 过滤）
        dzjy_scope = str(cfg.get("dzjy_scope") or "A股")
        try:
            dzjy_df_all = ak.stock_dzjy_mrmx(symbol=dzjy_scope, start_date=start_date, end_date=end_date)  # type: ignore
        except Exception as e:
            warnings.append(f"prefetch_failed:dzjy:{type(e).__name__}")
            dzjy_df_all = None

        def _code6(s: str) -> str:
            x = _sym6(s)
            return x.zfill(6)

        for sym in symbols:
            sym_warn: List[str] = []
            s6 = _code6(sym)

            # 1) 董监高/股东增减持（供给压力）
            insider_rows: List[Dict[str, Any]] = []
            insider_status = "OK"  # 事件型数据：空记录是正常状态
            insider_warn: List[str] = []
            try:
                df = insider_df_all
                if df is None or getattr(df, "empty", False):
                    insider_status = "MISSING"
                    insider_warn.append("missing:ggcg")
                else:
                    # 按 code 过滤（该 API 返回全表）
                    try:
                        code_col = "代码" if "代码" in df.columns else None
                        if not code_col:
                            insider_warn.append("insider_code_col_missing")
                            df_sym = df
                        else:
                            df_sym = df[df[code_col].astype(str).str.zfill(6) == s6]
                    except Exception as e:
                        insider_warn.append(f"insider_filter_failed:{type(e).__name__}")
                        df_sym = df

                    df2 = _filter_rows_by_trade_date(
                        df_sym, WatchlistSupplyFetchCfg().insider_date_cols, cutoff, insider_warn
                    )
                    insider_rows = _df_to_rows(df2, limit=max_rows)
                    if not insider_rows:
                        insider_warn.append("no_events")
            except Exception as e:
                insider_status = "ERROR"
                insider_warn.append(f"fetch_failed:ggcg:{type(e).__name__}")

            # 2) 大宗交易明细（供给压力）
            dzjy_rows: List[Dict[str, Any]] = []
            dzjy_status = "OK"  # 事件型数据：空记录是正常状态
            dzjy_warn: List[str] = []
            try:
                df = dzjy_df_all
                if df is None or getattr(df, "empty", False):
                    dzjy_status = "MISSING"
                    dzjy_warn.append("missing:dzjy")
                else:
                    # 按 code 过滤
                    try:
                        code_col = "代码" if "代码" in df.columns else None
                        if not code_col:
                            dzjy_warn.append("dzjy_code_col_missing")
                            df_sym = df
                        else:
                            df_sym = df[df[code_col].astype(str).str.zfill(6) == s6]
                    except Exception as e:
                        dzjy_warn.append(f"dzjy_filter_failed:{type(e).__name__}")
                        df_sym = df

                    df2 = _filter_rows_by_trade_date(
                        df_sym, WatchlistSupplyFetchCfg().dzjy_date_cols, cutoff, dzjy_warn
                    )
                    dzjy_rows = _df_to_rows(df2, limit=max_rows)
                    if not dzjy_rows:
                        dzjy_warn.append("no_events")
            except Exception as e:
                dzjy_status = "ERROR"
                dzjy_warn.append(f"fetch_failed:dzjy:{type(e).__name__}")

            # 综合
            item = {
                "insider": {"data_status": insider_status, "rows": insider_rows, "warnings": insider_warn},
                "block_trade": {"data_status": dzjy_status, "rows": dzjy_rows, "warnings": dzjy_warn},
                "warnings": sym_warn + insider_warn + dzjy_warn,
            }
            out["symbols"][sym] = item

        return out


def stock_ggcg_em(symbol: str = "股东减持") -> pd.DataFrame:
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

