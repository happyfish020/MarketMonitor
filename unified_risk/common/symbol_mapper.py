
SYMBOL_MAP = {
    "^FTXIN9": "XIN9.FGI",
    "FTXIN9": "XIN9.FGI",
    "A50": "XIN9.FGI",

    "^SSEC": "000001.SS",
    "^SHCOMP": "000001.SS",
    "^SZCOMP": "399001.SZ",
    "^CSI300": "000300.SS",

    "^IXIC": "^IXIC",
    "^GSPC": "^GSPC",
    "^NDX": "^NDX",
    "^VIX": "^VIX",

    "^HSI": "^HSI",
}

def map_symbol(symbol: str) -> str:
    s = symbol.upper().lstrip("$")
    return SYMBOL_MAP.get(s, symbol)
