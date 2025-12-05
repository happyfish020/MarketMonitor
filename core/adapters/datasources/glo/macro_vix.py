"""
VIXClient (V11.7 FINAL)
- 不直接使用 yfinance
- 使用 config/symbols.yaml → global.vix 动态符号
- 使用 yf_client_cn.get_macro_daily 统一管理下载、retry、cache、fallback
"""

from datetime import date as Date
from typing import Optional, Dict, Any

from core.utils.logger import log
from core.utils.config_loader import load_symbols

from core.adapters.datasources.cn.yf_client_cn import get_macro_daily


class VIXClient:
    """
    获取 VIX 日级数据（宏观波动率指数）
    输出格式与 get_macro_daily 一致：

        {
            "symbol": "^VIX",
            "date": "YYYY-MM-DD",
            "close": float,
            "prev_close": float,
            "pct_change": float,
        }

    NOTE:
    符号来自 symbols.yaml → global → vix
    """

    def __init__(self):
        # 从 symbols.yaml 获取 vix 对应的 symbol
        symbols_cfg = load_symbols()
        global_cfg = symbols_cfg.get("global", {}) or {}

        # 默认兜底 "^VIX"
        self.symbol = global_cfg.get("vix", "^VIX")

        log(f"[VIX] 初始化: 使用符号 {self.symbol}")

    # ---------------------------------------------------------
    # 主接口：获取 VIX 日级行情
    # ---------------------------------------------------------
    def get_vix_daily(self, trade_date: Date) -> Optional[Dict[str, Any]]:
        log(f"[VIX] 获取 VIX 数据 trade_date={trade_date}")

        row = get_macro_daily(self.symbol, trade_date)

        if row is None:
            log(f"[VIX] 获取 {self.symbol} 于 {trade_date} 的数据失败 → None")
            return None

        return row
