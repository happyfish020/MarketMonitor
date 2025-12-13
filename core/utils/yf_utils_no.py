import os
import json
import datetime
import pandas as pd
import yfinance as yf
from core.utils.logger import get_logger

LOG = get_logger("Utils.YF")


class YFClientCN:
    """
    V12 稳定 YF 客户端
    - 固定 16 日窗口
    - 按 symbol 缓存，不按日期缓存（避免路径异常）
    """

    def __init__(self,market:str ):
        self.cache_root = os.path.join("data", market, "cache", "yf")
        
        os.makedirs(self.cache_root, exist_ok=True)
        LOG.info("Here is yf_utils V12 version - 20251209 - 01")

    # --------------------------
    #   符号标准化
    # --------------------------
    @staticmethod
    def normalize(symbol: str) -> str:
        return symbol.replace("_", ".").upper()

    # --------------------------
    #   缓存路径（统一固定）
    # --------------------------
    def _cache_path(self, symbol: str):
        """
        V12 规则：
        每个 symbol 缓存一个 JSON，不分日期。
        """
        safe = symbol.replace(".", "_")
        return os.path.join(self.cache_root, f"{self.trade_date}_{safe}.json")

    def _load_cache(self, symbol: str):
        path = self._cache_path(symbol)
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    LOG.info(f"[YF] LoadCache {path}")
                    #return json.load(f)
                    return pd.read_json(f)
            except Exception:
                return None
        return None

    def _save_cache(self, symbol: str, data: dict):
        path = self._cache_path(symbol)


        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            LOG.info(f"[YF] SaveCache {path}")
        except Exception as e:
            LOG.error(f"[YF] Cache save failed {path}: {e}")

    # --------------------------
    #   核心：获取指数窗口
    # --------------------------
    def fetch_index_window(self, symbol: str, trade_date: str):
        self.trade_date = trade_date
        yf_sym = self.normalize(symbol)

        # 1. 尝试缓存
        cached = self._load_cache(yf_sym)
        if cached:
            return cached

        LOG.info(f"[YF] Fetch symbol={yf_sym} date={trade_date}")

        # 2. YF 下载
        try:
            # todo remove old - self.cache_root = os.path.join("data", market, "cache", "yf")

            df = yf.download(
                yf_sym,  # <-- 这里必须用 yf_sym
                period="6mo",
                interval="1d",
                auto_adjust=False,
            )
            



        except Exception as e:
            LOG.error(f"[YF] Download failed {yf_sym}: {e}")
            return None

        if df is None or df.empty:
            LOG.warning(f"[YF] No data symbol={yf_sym}")
            return None

        df = df.reset_index()
        
        df["Date"] = df["Date"].astype(str)
        # 计算涨跌幅（百分比）
        df["pct"] = df["Close"].pct_change() * 100.0 

        # 3. 找到 <= trade_date 的最近交易日
        df_valid = df[df["Date"] <= trade_date]
        if df_valid.empty:
            LOG.warning(f"[YF] No valid data before {trade_date}, symbol={yf_sym}")
            return None
    
    
        # ⭐⭐ 修复 MultiIndex 列名，将 tuple 展平为单一字符串
        # 例：('Close','000001.SS') → 'Close'
        import pandas as pd
        
        if isinstance(df_valid.columns, pd.MultiIndex):
            new_cols = []
            for col in df_valid.columns:
                if isinstance(col, tuple):
                    # YF 的典型列名格式：('Close', '000001.SS')
                    # 我们只取第一部分即可，因为它才是指标名称
                    primary = col[0]
                    if primary is None or primary == "":
                        # 'Date' 会变成 ('Date', '')，所以直接用 str(col[0])
                        primary = str(col[0])
                    new_cols.append(str(primary))
                else:
                    new_cols.append(str(col))
            df_valid.columns = new_cols
        ####
        last_row = df_valid.iloc[-1]

        # ---- 处理 close（可能是 Series）----
        val_close = last_row["Close"]
        if isinstance(val_close, pd.Series):
            val_close = val_close.dropna()
            val_close = val_close.iloc[0] if len(val_close) > 0 else None

        close_val = float(val_close) if val_close is not None else None

        # ---- 处理 prev_close ----
        if len(df_valid) >= 2:
            prev = df_valid.iloc[-2]["Close"]
            if isinstance(prev, pd.Series):
                prev = prev.dropna()
                prev = prev.iloc[0] if len(prev) > 0 else None
            prev_close = float(prev) if prev is not None else None
        else:
            prev_close = None

        ##
        #  
        #  







        # ---- 构建 block ----
        block = {
            "symbol": yf_sym,
            "date": last_row["Date"],  # 始终是 str，不是 Series
            "close": close_val,
            "pct": last_row["pct"],
            "window": df_valid.to_dict("records"),
        }

        # ---- 保存缓存（不含日期）----
        self._save_cache(yf_sym, block)

        LOG.info(
            f"[YF] WindowOK symbol={yf_sym} rows={len(df_valid)} close={block['close']}"
        )

        #return block
         
        return df_valid

    # --------------------------
    #   获取日行情 + pct
    # --------------------------
    def get_index_daily(self, symbol: str, trade_date: str):
        block = self.fetch_index_window(symbol, trade_date)

        if block is None:
            return {
                "symbol": symbol,
                "trade_date": trade_date,
                "close": None,
                "prev_close": None,
                "pct_change": None,
                "window": [],
            }

        close = block["close"]
        prev = block["prev_close"]

        pct = None
        if close is not None and prev is not None and prev != 0:
            pct = (close - prev) / prev * 100

        return {
            "symbol": block["symbol"],
            "date": block["date"],
            "close": close,
            "prev_close": prev,
            "pct_change": pct,
            "window": block["window"],
        }
