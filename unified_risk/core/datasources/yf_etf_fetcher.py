
from unified_risk.common.yf_safe import safe_yf_last_bars
from unified_risk.common.logging_utils import log_info, log_warning
 

def safe_fetch_etf(symbol: str):
    """
    ETF 安全抓取，支持：
    - MultiIndex 列
    - substring 匹配字段
    - 自动 flatten 列名
    返回 DataFrame(date, close, volume) 或 None
    """
    snap = safe_yf_last_bars(
        symbol,
        lookback_days=15,
        interval="1d",
        min_points=5
    )

    # snap invalid
    if not isinstance(snap, dict) or "bars" not in snap:
        log_warning(f"[ETF] {symbol}: invalid snap from yf_last_bars: {snap}")
        return None

    bars = snap["bars"]
    if bars is None or bars.empty:
        log_warning(f"[ETF] {symbol}: empty bars")
        return None

    # ① reset index
    df = bars.reset_index()

    # ② rename via substring matching
    rename_map = {}
    for col in df.columns:
        col_str = str(col).lower()

        if "date" in col_str:
            rename_map[col] = "date"
        elif "close" in col_str and "adj" not in col_str:
            rename_map[col] = "close"
        elif "volume" in col_str:
            rename_map[col] = "volume"

    df = df.rename(columns=rename_map)

    # ③ flatten columns to pure strings
    df.columns = [str(c).lower() for c in df.columns]

    # ④ CHECK REQUIRED FIELDS by substring (not equality)
    required = ("date", "close", "volume")
    for col in required:
        if not any(col in c for c in df.columns):
            log_warning(f"[ETF] {symbol}: missing '{col}' in {list(df.columns)}")
            return None

    # ⑤ select columns using substring
    col_date   = [c for c in df.columns if "date" in c][0]
    col_close  = [c for c in df.columns if "close" in c][0]
    col_volume = [c for c in df.columns if "volume" in c][0]

    df = df[[col_date, col_close, col_volume]]
    df.columns = ["date", "close", "volume"]

    log_info(f"[ETF] {symbol} rows={len(df)} cols={list(df.columns)}")
    return df
