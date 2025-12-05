# -*- coding: utf-8 -*-
"""
基于宽基 ETF 构造 A 股北向 NPS 代理的数据源（CN 市场）。

【缓存 & 复用规范（V11.7 标准版）】

1. 符号配置：
   - 来自 config/symbols.yaml：
        cn:
          etf_proxy:
            north_sh: "510300.SS"
            north_sz: "159901.SZ"

2. YF 数据获取：
   - 使用 core.adapters.datasources.cn.yf_client_cn.get_etf_daily(symbol, trade_date)
   - 由 yf_client_cn 内部统一管理：
        - 缓存文件路径：symbol_cache.get_symbol_daily_path("cn", trade_date, symbol, kind="etf")
        - 读写 JSON（比如 data/cache/day_cn/20251205/etf_510300_SS.json）

3. FORCE 刷新语义（非常重要，沿用 v11.6.6）：
   - get_etf_north_proxy(..., force_refresh=True) 时：
       仅在本进程内「第一次调用」会执行：
           - 删除当日两个 ETF 的 JSON 缓存文件
           - 后续再次调用不再删除，避免频繁打 YF
   - 这样：
       - 当天第一次跑模型可以强制拿最新数据
       - 后续所有因子 / datasource 都能复用这两个 ETF 的缓存

4. 返回结构：
   {
       "etf_flow_e9": float,          # 代理北向净流入（亿元）
       "total_turnover_e9": float,    # 这两个 ETF 的成交额合计（亿元）
       "hs300_proxy_pct": float,      # HS300 代理涨跌幅（%）
       "details": [
            {
                "symbol": "510300.SS",
                "pct_change": 0.8,
                "turnover_e9": 123.4,
                "flow_e9": 0.9
            },
            ...
       ]
   }

其它如 index_series_client / global_lead_client / futures_client
应参照本文件的「symbol + symbol_cache + FORCE 刷新」模式实现。
"""

from __future__ import annotations

import os
from datetime import date as Date
from typing import Dict, Any, List, Tuple

from core.utils.config_loader import load_symbols
from core.utils.logger import log
from core.adapters.datasources.cn.yf_client_cn import get_etf_daily
from core.adapters.cache.symbol_cache import get_symbol_daily_path

# 全局符号配置
_symbols = load_symbols()

# 本进程级别的 RefreshOnce 标记：
# 在一次运行内，仅首次 force_refresh 时真正刷新 ETF，之后复用缓存
_ETF_REFRESHED: bool = False


def _get_etf_symbols() -> List[str]:
    """
    从 symbols.yaml 中获取 ETF 代理符号列表。

    规范格式：
        cn:
          etf_proxy:
            north_sh: "510300.SS"
            north_sz: "159901.SZ"
    """
    cn_cfg = _symbols.get("cn", {})
    proxys_raw = cn_cfg.get("etf_proxy", {})

    proxys: Dict[str, str] = proxys_raw if isinstance(proxys_raw, dict) else {}
    symbols: List[str] = []

    for key in ("north_sh", "north_sz"):
        sym = proxys.get(key)
        if sym:
            symbols.append(sym)

    if not symbols:
        log("[ETF Proxy] symbols.yaml 中未配置 cn.etf_proxy.north_sh / north_sz，ETF 代理将返回空。")

    return symbols


def _force_delete_etf_cache(trade_date: Date, symbols: List[str]) -> None:
    """
    FORCE 模式下：仅在本进程内第一次调用时删除当日 ETF 缓存 JSON。

    统一使用 symbol_cache.get_symbol_daily_path 形成规范文件名，例如：
        data/cache/day_cn/20251205/etf_510300_SS.json
    """
    for sym in symbols:
        if not sym:
            continue

        cache_path = get_symbol_daily_path("cn", trade_date, sym, kind="etf")
        abs_path = os.path.abspath(cache_path)

        if os.path.exists(cache_path):
            try:
                os.remove(cache_path)
                log(f"[IO] Remove JSON (force) → {abs_path}")
            except Exception as e:
                log(f"[IO] Remove JSON FAIL → {abs_path}: {e}")


def get_etf_north_proxy(trade_date: Date, force_refresh: bool = False) -> Dict[str, Any]:
    """
    构造基于 ETF 的北向代理（ETF Proxy）。

    - 支持 FORCE 刷新（进程内仅第一次真正删除缓存）
    - 使用统一的 symbol_cache 规范缓存 ETF JSON
    - 返回当日基于 ETF 成交额与涨跌幅估算的 "北向代理流入"
    """
    global _ETF_REFRESHED

    symbols = _get_etf_symbols()

    # === FORCE 模式：仅首次调用时，删除当日 ETF 缓存 JSON ===
    if force_refresh and not _ETF_REFRESHED and symbols:
        log(f"[ETF Proxy] FORCE → refresh ETF proxy for {trade_date}")
        _force_delete_etf_cache(trade_date, symbols)
        _ETF_REFRESHED = True

    # === 拉取 ETF 数据（内部已通过 yf_client_cn 做了缓存读写）===
    items: List[Tuple[str, Dict[str, Any]]] = []
    for sym in symbols:
        etf = get_etf_daily(sym, trade_date)
        if etf:
            items.append((sym, etf))

    # 无数据时返回空代理结构，供因子层兜底
    if not items:
        log(f"[ETF Proxy] {trade_date} 无 ETF 数据，返回空代理。")
        return {
            "etf_flow_e9": 0.0,
            "total_turnover_e9": 0.0,
            "hs300_proxy_pct": 0.0,
            "details": [],
        }

    total_turnover_e9 = 0.0
    total_flow_e9 = 0.0
    hs300_proxy_pct = 0.0
    details: List[Dict[str, Any]] = []

    for sym, etf in items:
        pct = float(etf.get("pct_change", 0.0) or 0.0)
        close = float(etf.get("close", 0.0) or 0.0)
        vol = float(etf.get("volume", 0.0) or 0.0)

        turnover_e9 = (close * vol) / 1e8  # 单位：亿元
        flow_e9 = (pct / 100.0) * turnover_e9

        total_turnover_e9 += turnover_e9
        total_flow_e9 += flow_e9

        if "510300" in sym:
            hs300_proxy_pct = pct

        details.append(
            {
                "symbol": sym,
                "pct_change": pct,
                "turnover_e9": turnover_e9,
                "flow_e9": flow_e9,
            }
        )

    # 若未找到 HS300 ETF → 用均值兜底
    if hs300_proxy_pct == 0.0 and items:
        hs300_proxy_pct = (
            sum(float(etf.get("pct_change", 0.0) or 0.0) for _, etf in items) / len(items)
        )

    log(
        f"[ETF Proxy] {trade_date} etf_flow_e9={total_flow_e9:.2f}, "
        f"turnover_e9={total_turnover_e9:.2f}, hs300_proxy_pct={hs300_proxy_pct:.2f}"
    )

    return {
        "etf_flow_e9": total_flow_e9,
        "total_turnover_e9": total_turnover_e9,
        "hs300_proxy_pct": hs300_proxy_pct,
        "details": details,
    }
