
import yfinance as yf
from unified_risk.common.logging_utils import log_warning, log_info


def safe_yf_last_bars(symbol: str,
                      lookback_days: int = 10,
                      interval: str = "1d",
                      min_points: int = 2):
    """统一 YF fallback：自动回退到最近有效交易日。

    返回:
        dict:
          - last: float
          - prev: float
          - changePct: float
          - bars: DataFrame
        失败返回 None
    """
    try:
        df = yf.download(
            symbol,
            period=f"{lookback_days}d",
            interval=interval,
            progress=False,
            auto_adjust=False,
        )
    except Exception as e:
        log_warning(f"[YF] {symbol}: download error: {e}")
        return None

    if df is None or df.empty:
        log_warning(f"[YF] {symbol}: no data in last {lookback_days}d")
        return None

    df = df.dropna()
    if len(df) < min_points:
        log_warning(f"[YF] {symbol}: not enough bars ({len(df)})")
        return None

    try:
        
        last = df["Close"].iloc[-1].item()
        prev = df["Close"].iloc[-2].item()
    except Exception as e:
        log_warning(f"[YF] {symbol}: failed to read last/prev close: {e}")
        return None

    change_pct = (last - prev) / prev * 100 if prev != 0 else 0.0
    log_info(f"[YF] {symbol}: last={last:.4f}, prev={prev:.4f}, pct={change_pct:.3f}")

    return {
        "last": last,
        "prev": prev,
        "changePct": change_pct,
        "bars": df,
    }
