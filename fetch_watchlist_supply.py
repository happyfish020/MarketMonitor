# -*- coding: utf-8 -*-
"""
scripts/fetch_watchlist_supply.py


底层可行性验证脚本（RAW）：
- 调用 WatchlistSupplyDataSource.build_block
- 验证：refresh 写 cache / non-refresh 读 cache / 输出结构稳定

不接入 engine，不影响 Gate/DRS。
"""
from __future__ import annotations

import argparse
import json
import pandas as pd
import requests

from core.adapters.datasources.cn.watchlist_supply_source import WatchlistSupplyDataSource
from akshare.utils.tqdm import get_tqdm

from core.datasources.datasource_base import DataSourceConfig

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
    print(f"total_page:{total_page}")
    
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



def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--cfg", default="config/watchlist_supply.yaml")
    ap.add_argument("--refresh", action="store_true", help="force refresh (fetch & overwrite cache)")
    args = ap.parse_args()

    ds = WatchlistSupplyDataSource( DataSourceConfig(market="cn", ds_name="watchlist_supply"))
    refresh_mode = "refresh" if args.refresh else "none"
    blk = ds.build_block(trade_date=args.trade_date, refresh_mode=refresh_mode, cfg_path=args.cfg, kind="EOD")

    print("schema:", blk.get("schema"))
    print("asof:", blk.get("asof"))
    print("data_status:", blk.get("data_status"))
    ws = blk.get("warnings") or []
    print("warnings:", ws[:10], f"(n={len(ws)})")

    items = blk.get("items") or {}
    print("items:", len(items))
    # print a tiny summary
    for sym, v in list(items.items())[:5]:
        ins = (v or {}).get("insider", {}) or {}
        dz = (v or {}).get("block_trade", {}) or {}
        print(f"- {sym}: insider={ins.get('data_status')} rows={len(ins.get('rows') or [])} | dzjy={dz.get('data_status')} rows={len(dz.get('rows') or [])}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
