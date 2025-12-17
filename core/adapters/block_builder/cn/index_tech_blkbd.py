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

        out: Dict[str, Any] = {}

####
        sector_score: Dict[str, float] = {}
        
        for name, feats in out.items():
            if not isinstance(feats, dict) or not feats:
                continue
        
            # 优先使用显式 score
            if isinstance(feats.get("score"), (int, float)):
                sector_score[name] = float(feats["score"])
                continue
        
            # 兜底：使用常见技术字段
            for key in ("trend_score", "momentum", "strength"):
                if isinstance(feats.get(key), (int, float)):
                    sector_score[name] = float(feats[key])
                    break
        
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
        
        ###
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
