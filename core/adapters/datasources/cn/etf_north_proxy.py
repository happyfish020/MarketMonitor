"""基于宽基 ETF 构造 A 股北向 NPS 代理的数据源（CN 市场）。"""
from __future__ import annotations

from datetime import date as Date
from typing import Dict, Any, List, Tuple

from core.utils.config_loader import load_symbols
from core.utils.logger import log
from core.adapters.datasources.cn.yf_client_cn import get_etf_daily

_symbols = load_symbols()


def get_etf_north_proxy(trade_date: Date) -> Dict[str, Any]:
    """构造基于 ETF 的北向代理。"""
    cn_cfg = _symbols.get("cn", {})
    proxys = cn_cfg.get("etf_proxy", {})
    symbols: List[str] = []

    for key in ("north_sh", "north_sz"):
        sym = proxys.get(key)
        if sym:
            symbols.append(sym)

    items: List[Tuple[str, Dict[str, Any]]] = []
    for sym in symbols:
        etf = get_etf_daily(sym, trade_date)
        if etf:
            items.append((sym, etf))

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
