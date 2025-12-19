# core/utils/yf_utils.py
# UnifiedRisk V12 - Stable YF Client (MultiIndex-safe + StackTrace + FatalExit)

import yfinance as yf
import pandas as pd
import time
import traceback
from typing import Optional

from core.utils.logger import get_logger

LOG = get_logger("Utils.YF")


class YFClientCN:

    def __init__(self, market: str = "glo"):
        self.market = market
        LOG.info(f"[YF] Init YFClientCN(market={market})")

    # ============================================================
    # Internal unified downloader with MultiIndex support
    # ============================================================
    def _download(self, symbol: str, period="6mo", interval="1d") -> Optional[pd.DataFrame]:

        yf_sym = self.normalize(symbol)
        for attempt in range(3):
            try:
                LOG.info(f"[YF] Download attempt={attempt+1} symbol={symbol} safe_symbol={yf_sym}")

                df = yf.download(
                    tickers=yf_sym,
                    period=period,
                    interval=interval,
                    auto_adjust=False,
                    progress=False,
                    timeout=12,
                )
                
                #############




                #######33
                if df is None or df.empty:
                    traceback.print_exc()
                    LOG.error("[YF] Exception stack trace:\n" + traceback.format_exc())
    
                    # -----------------------------
                    # 🔥 Immediately exit system
                    # -----------------------------
                    raise SystemExit(
                        f"[FATAL] YF download failed for symbol={symbol}, attempt={attempt+1}, Something wrong in YF func df is empty"
                    )
                    LOG.warning(f"[YF] Empty data for {symbol}, retrying…")
                    time.sleep(1)

                    continue

                df = df.reset_index()

                # ---------------------------------------------------------
                # Step 1: Rename date column
                # ---------------------------------------------------------
                if "Date" in df.columns:
                    df.rename(columns={"Date": "date"}, inplace=True)
                elif "Datetime" in df.columns:
                    df.rename(columns={"Datetime": "date"}, inplace=True)

                # ---------------------------------------------------------
                # Step 2: Flatten MultiIndex columns (critical fix)
                # ---------------------------------------------------------
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = [str(col[0]).lower() for col in df.columns]
                else:
                    df.columns = [str(c).lower() for c in df.columns]

                # ---------------------------------------------------------
                # Step 3: Ensure dependencies
                # ---------------------------------------------------------
                if "volume" not in df.columns:
                    df["volume"] = 0

                if "close" not in df.columns:
                    LOG.error(f"[YF] No 'close' column for {symbol}")
                    return None

                # ---------------------------------------------------------
                # Step 4: Standardize fields
                # ---------------------------------------------------------
                df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                df["pct"] = df["close"].pct_change().fillna(0.0)

                df = df.sort_values("date")

                return df

            except Exception as e:
                # -----------------------------
                # 🔥 Print full stack trace
                # -----------------------------
                traceback.print_exc()
                LOG.error("[YF] Exception stack trace:\n" + traceback.format_exc())

                # -----------------------------
                # 🔥 Immediately exit system
                # -----------------------------
                raise SystemExit(
                    f"[FATAL] YF download failed for symbol={symbol}, attempt={attempt+1}, error={e}"
                )

        LOG.error(f"[YF] FAILED after 3 attempts symbol={symbol}")
        return None

    # ============================================================
    # External API — Index
    # ============================================================
    def fetch_index_window(self, symbol: str, period="6mo", interval="1d"):
        return self._download(symbol, period=period, interval=interval)

    # ============================================================
    # External API — ETF
    # ============================================================
    def fetch_etf_window(self, symbol: str, period="6mo"):
        return self._download(symbol,   interval="1d")

    @staticmethod
    def normalize(symbol: str) -> str:
        return symbol.replace("_", ".").upper()