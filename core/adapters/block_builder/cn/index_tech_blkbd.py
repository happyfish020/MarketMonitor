# core/adapters/transformers/cn/index_tech_transformer.py
# UnifiedRisk V12.1 - IndexTechBlockBuilder
# 说明：
# - transformer 层：把 index_core（原始指数序列窗口）转换为客观技术特征
# - 不评分，不判断趋势，只提供 raw features 给 factor
# - 不直接访问外部数据源；仅通过 SymbolSeriesStore（统一序列中心）读取窗口

from __future__ import annotations

from typing import Dict, Any

import numpy as np
import pandas as pd

from core.utils.logger import get_logger
from core.adapters.providers.symbol_series_store import SymbolSeriesStore
from core.adapters.block_builder.block_builder_base import FactBlockBuilderBase
LOG = get_logger("TR.IndexTech")

def _safe_closes(window):
    if not isinstance(window, list) or not window:
        return []
    closes = []
    for row in window:
        if isinstance(row, dict) and isinstance(row.get("close"), (int, float)):
            closes.append(float(row["close"]))
    return closes

def _ret(closes, n):
    if len(closes) <= n:
        return None
    base = closes[-1 - n]
    if base == 0:
        return None
    return closes[-1] / base - 1.0

def _ma(closes, n):
    if len(closes) < n:
        return None
    return sum(closes[-n:]) / n

def _clip100(x):
    if x is None:
        return None
    if x > 100:
        return 100.0
    if x < -100:
        return -100.0
    return float(x)


class IndexTechBlockBuilder(FactBlockBuilderBase):
    """
    输入：
      snapshot["index_core"] = { name: {symbol, close, pct, window...}, ... }

    输出：
      snapshot["index_tech"] = { name: {ma5, ma10, macd..., rsi...}, ... }

    注：只生成客观特征，不做风险判断（风险/趋势结论由 IndexTechFactor 完成）
    """

 
    def __init__(self, window: int = 60):
        super().__init__(name="IndexTech")
        self.window = int(window) if window and window > 0 else 60
        self.store = SymbolSeriesStore.get_instance()



    def build_block(self, snapshot: Dict[str, Any], refresh_mode: str = "none") -> Dict[str, Any]:
        """
        refresh_mode 保留参数是为了与全链路接口对齐（未来可扩展）
        transformer 本身不写 cache，因此目前不会使用 refresh_mode。
        """
        index_core = snapshot.get("index_core_raw", {})

        assert snapshot.get("index_core_raw"), "index_core missing in index_tech blkbd"
        if not isinstance(index_core, dict) or not index_core:
            LOG.warning("[TR.IndexTech] snapshot.index_core empty")
            return {}

         
####
        out: Dict[str, Any] = {}
        sector_score: Dict[str, float] = {}
        
        for name, core in index_core.items():
            if not isinstance(core, dict):
                continue
        
            window = core.get("window", [])
            closes = _safe_closes(window)
        
            pct_1d = core.get("pct")
            pct_1d = float(pct_1d) if isinstance(pct_1d, (int, float)) else None
        
            ret_5d = _ret(closes, 5)
            ret_10d = _ret(closes, 10)
            ret_20d = _ret(closes, 20)
        
            ma5 = _ma(closes, 5)
            ma10 = _ma(closes, 10)
            ma20 = _ma(closes, 20)
        
            close = core.get("close")
            close = float(close) if isinstance(close, (int, float)) else None
        
            # trend_score: 简单可解释版本（-100~100）
            trend = 0.0
            if close is not None and ma20 is not None and ma20 != 0:
                # 价格相对 MA20
                trend += (close / ma20 - 1.0) * 200.0  # 放大到大致 -100~100 的量级
        
            if ma5 is not None and ma10 is not None and ma20 is not None:
                # 均线排列加分/扣分
                if ma5 > ma10 > ma20:
                    trend += 20.0
                elif ma5 < ma10 < ma20:
                    trend -= 20.0
        
            trend_score = _clip100(trend)
        
            # strength: 用收益动量做一个加权（-100~100）
            strength = 0.0
            if pct_1d is not None:
                strength += pct_1d * 2000.0   # 0.5% -> 10 分量级
            if ret_5d is not None:
                strength += ret_5d * 600.0
            if ret_20d is not None:
                strength += ret_20d * 300.0
        
            strength = _clip100(strength)
        
            # score: 综合
            score = None
            if trend_score is not None and strength is not None:
                score = _clip100(0.6 * trend_score + 0.4 * strength)
        
            feats = {
                "symbol": core.get("symbol"),
                "close": close,
                "prev_close": core.get("prev_close"),
                "pct_1d": pct_1d,
                "ret_5d": ret_5d,
                "ret_10d": ret_10d,
                "ret_20d": ret_20d,
                "ma5": ma5,
                "ma10": ma10,
                "ma20": ma20,
                "trend_score": trend_score,
                "strength": strength,
                "score": score,
                "_meta": {
                    "window_len": len(closes),
                    "source": "index_core_raw",
                },
            }
        
            out[name] = feats
            if isinstance(score, (int, float)):
                sector_score[name] = float(score)
    
    
         
    ########     
        # 作为标准出口挂载（不破坏原结构）
        out["_sector_score"] = sector_score

        # === 调试可观测性（新增，不参与计算） ===
        sector_feats = snapshot.get("sector_rotation", {}).get("sector_scores", {})
        


        #
        out["_debug"] = {
            "input_keys": list(snapshot.keys()),
            "sector_feats_present": bool(sector_feats),
            "sector_feats_keys": list(sector_feats.keys()) if isinstance(sector_feats, dict) else None,
            "sector_feats_sample": (
                dict(list(sector_feats.items())[:3])
                if isinstance(sector_feats, dict) else None
            ),
            "reason": "no_sector_feats" if not sector_feats else "sector_feats_present",
        }
        
        ##3
        sector_rotation_block = snapshot.get("sector_rotation")
        
        sector_feats = (
            sector_rotation_block.get("sector_scores", {})
            if isinstance(sector_rotation_block, dict)
            else {}
        )
        
        out["_debug"] = {
            "input_keys": list(snapshot.keys()),
            "sector_rotation_present": isinstance(sector_rotation_block, dict),
            "sector_scores_present": (
                isinstance(sector_rotation_block, dict)
                and "sector_scores" in sector_rotation_block
            ),
            "sector_feats_len": len(sector_feats) if isinstance(sector_feats, dict) else None,
            "sector_feats_keys": list(sector_feats.keys())[:5] if isinstance(sector_feats, dict) else None,
            "sector_feats_source": "snapshot['sector_rotation']['sector_scores']",
            "reason": (
                "sector_rotation_missing"
                if not isinstance(sector_rotation_block, dict)
                else "sector_scores_empty"
                if not sector_feats
                else "sector_scores_present"
            ),
        }        
        
        ### todo
        return out

   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
    # -------------------------------------------------
    def _get_series(self, symbol: str, window: int) -> pd.DataFrame:
        # transformer 只读：由 store 自己决定是否写 history（这是 store 的职责）
        try:
            return self.store.get_series(
                symbol=symbol,
                window=window,
                refresh_mode="none",
                method="index",
                provider="yf",
            )
        except Exception as e:
            LOG.error("[TR.IndexTech] get_series failed symbol=%s err=%s", symbol, e)
            return pd.DataFrame()

    # -------------------------------------------------
    @staticmethod
    def _calc_features(df: pd.DataFrame) -> Dict[str, Any]:
        close = df["close"].astype(float)

        ma5 = close.rolling(5).mean()
        ma10 = close.rolling(10).mean()
        ma20 = close.rolling(20).mean()

        # slope：ma5 近5日变化（客观值）
        ma5_slope = None
        if len(ma5) >= 6 and pd.notna(ma5.iloc[-1]) and pd.notna(ma5.iloc[-6]):
            ma5_slope = (ma5.iloc[-1] - ma5.iloc[-6]) / 5.0

        # MACD
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        dif = ema12 - ema26
        dea = dif.ewm(span=9, adjust=False).mean()
        hist = dif - dea

        # RSI 14
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(14).mean()
        avg_loss = loss.rolling(14).mean()
        rs = avg_gain / avg_loss
        rsi14 = 100 - (100 / (1 + rs))

        last = len(close) - 1

        return {
            "close": _safe(close.iloc[last]),
            "ma5": _safe(ma5.iloc[last]),
            "ma10": _safe(ma10.iloc[last]),
            "ma20": _safe(ma20.iloc[last]),
            "ma5_slope": _safe(ma5_slope),

            "macd_dif": _safe(dif.iloc[last]),
            "macd_dea": _safe(dea.iloc[last]),
            "macd_hist": _safe(hist.iloc[last]),

            "rsi14": _safe(rsi14.iloc[last]),
        }


def _safe(v):
    if v is None:
        return None
    try:
        if isinstance(v, float) and np.isnan(v):
            return None
    except Exception:
        pass
    return float(v)
