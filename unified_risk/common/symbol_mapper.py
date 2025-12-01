from __future__ import annotations
  
# unified_risk/common/symbol_mapper.py

SYMBOL_MAP = {
    # --- China A-share indices ---
    "SH":      "000001.SS",   # 上证
    "^SSEC": "000001.SS",
    "CYB":     "399006.SZ",   # 创业板
    "SZCOMP":  "399001.SZ",   # 深成指
    "CSI300":  "000300.SS",   # 沪深300
    "CSI500":  "000905.SS",
    "399006":  "399006.SZ",


    # --- Hong Kong ---
    "HSI":     "^HSI",

   "^FTXIN9": "XIN9.FGI",
    "FTXIN9": "XIN9.FGI",

    # --- US indices ---
    "SPY":     "SPY",
    "NASDAQ":  "^IXIC",
    "DOW":     "^DJI",
    "VIX":     "^VIX",
    "^IXIC": "^IXIC",
    "^GSPC": "^GSPC",
    "^NDX": "^NDX",
    "^VIX": "^VIX",
 
    # --- Europe ---
    "DAX":     "^GDAXI",
    "FTSE":    "^FTSE",

    # --- Commodity proxies ---
    "GOLD":    "GC=F",
    "SILVER":  "SI=F",
    "OIL":     "CL=F",

 

    # --- A50 proxies (YF fallback only) ---
    "A50":     "^XIN9.DE",

    #===

    "510300": "510300.SS",
    "510050": "510050.SS",
    "159902": "159902.SZ",
    "512880": "512880.SS",
    "159915": "159915.SZ",
    "159922": "159922.SZ",
    "159619": "159619.SZ",
    "512000": "512000.SS",
    "159901": "159901.SZ",
}

def map_symbol(symbol: str) -> str:
    """统一映射所有指数 / ETF / 大类资产"""
    return SYMBOL_MAP.get(symbol, symbol)
