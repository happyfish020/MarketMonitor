# -*- coding: utf-8 -*-
"""
core/adapters/transformers/cn/index_tech_transformer.py
UnifiedRisk V12 - IndexTechBlockBuilder (features-only, volatility-normalized v3)

职责：
- transformer 层：把 index_core_raw（指数序列窗口）转换为“客观技术特征”
- 不输出人话 meaning（语义统一在报告 Block）
- 不产生制度结论（结论由 IndexTechFactor 输出 FactorResult）

输入：
  snapshot["index_core_raw"] = {
      "hs300": {"symbol": "...", "close": ..., "prev_close": ..., "pct": ..., "window":[{"date":..., "close":...}, ...]},
      "zz500": {...},
      "kc50": {...},
  }

输出：
  snapshot["index_tech"] = {
      "hs300": {close, prev_close, pct_1d, ret_5d, ret_10d, ret_20d, ma5, ma10, ma20,
                vol20, trend_score, strength_score, score, _meta},
      "zz500": {...},
      "kc50": {...},
      "_sector_score": {"hs300": score, ...},   # score in [-100,100]
      "_meta": {...}
  }

v3 核心改动（相对 v2）：
- 引入 20D 波动率（vol20）做归一化：让 score 在不同波动环境下更稳定
- 降低“均线排列”离散台阶权重（±10）
- 降低 1D 噪声影响：1D 动量权重显著下调，并按 vol20 归一化
"""

from __future__ import annotations

from typing import Dict, Any, List, Optional
import math

from core.utils.logger import get_logger
from core.adapters.block_builder.block_builder_base import FactBlockBuilderBase

LOG = get_logger("TR.IndexTech")


def _safe_closes(window: Any) -> List[float]:
    if not isinstance(window, list) or not window:
        return []
    closes: List[float] = []
    for row in window:
        if isinstance(row, dict) and isinstance(row.get("close"), (int, float)):
            closes.append(float(row["close"]))
    return closes


def _daily_returns(closes: List[float]) -> List[float]:
    rets: List[float] = []
    if len(closes) < 2:
        return rets
    for i in range(1, len(closes)):
        a = closes[i - 1]
        b = closes[i]
        if a and isinstance(a, float):
            rets.append(b / a - 1.0)
    return rets


def _std(xs: List[float]) -> Optional[float]:
    if not xs:
        return None
    n = len(xs)
    if n < 2:
        return None
    m = sum(xs) / n
    v = sum((x - m) ** 2 for x in xs) / (n - 1)
    if v < 0:
        return None
    return math.sqrt(v)


def _ret(closes: List[float], n: int) -> Optional[float]:
    if len(closes) <= n:
        return None
    base = closes[-1 - n]
    if base == 0:
        return None
    return closes[-1] / base - 1.0


def _ma(closes: List[float], n: int) -> Optional[float]:
    if len(closes) < n:
        return None
    return sum(closes[-n:]) / n


def _clip100(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    if x > 100:
        return 100.0
    if x < -100:
        return -100.0
    return float(x)


def _tanh_clip100(z: Optional[float], scale: float = 1.0) -> Optional[float]:
    """Smooth clipping: map z to (-100,100) using tanh to reduce hard saturation."""
    if z is None:
        return None
    try:
        return 100.0 * math.tanh(float(z) / float(scale))
    except Exception:
        return None


class IndexTechBlockBuilder(FactBlockBuilderBase):
    def __init__(self, window: int = 60):
        super().__init__(name="IndexTech")
        self.window = int(window) if window and window > 0 else 60

    def build_block(self, snapshot: Dict[str, Any], refresh_mode: str = "none") -> Dict[str, Any]:
        index_core = snapshot.get("index_core_raw")
        if not isinstance(index_core, dict) or not index_core:
            LOG.warning("[TR.IndexTech] index_core_raw missing/empty")
            return {}

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

            # 20D 波动率（用最近20个日收益，min(20, len-1)）
            dr = _daily_returns(closes)
            vol20 = None
            if len(dr) >= 10:  # 至少 10 个样本再算
                vol20 = _std(dr[-20:]) if len(dr) >= 20 else _std(dr)

            # fallback vol：若缺失，用非常保守的小值避免分母爆炸
            vol = vol20 if isinstance(vol20, (int, float)) and vol20 > 1e-6 else 0.01

            # -------- Trend (position + MA alignment) --------
            # pos_z：价格相对 MA20 的“波动率单位”偏离
            pos_z = None
            if close is not None and ma20 is not None and ma20 != 0:
                pos_z = (close / ma20 - 1.0) / vol
            # align：均线排列离散项（降低台阶权重）
            align = 0.0
            if ma5 is not None and ma10 is not None and ma20 is not None:
                if ma5 > ma10 > ma20:
                    align = 0.5
                elif ma5 < ma10 < ma20:
                    align = -0.5

            # 将 z 组合后用 tanh 平滑裁剪到 [-100, 100]
            # 经验：pos_z≈2（相对两倍波动）已算“明显偏强/弱”
            trend_z = 1.2 * (pos_z if isinstance(pos_z, float) else 0.0) + 1.0 * align
            trend_score = _tanh_clip100(trend_z, scale=2.5)

            # -------- Strength (momentum) --------
            # 使用 vol 归一化，降低 1D 噪声
            m1 = None
            if pct_1d is not None:
                m1 = pct_1d / vol
            m5 = None
            if ret_5d is not None:
                m5 = ret_5d / (vol * math.sqrt(5.0))
            m10 = None
            if ret_10d is not None:
                m10 = ret_10d / (vol * math.sqrt(10.0))
            m20 = None
            if ret_20d is not None:
                m20 = ret_20d / (vol * math.sqrt(20.0))

            strength_z = 0.0
            # 权重：1D 低权重；5D/10D/20D 为主
            if isinstance(m1, float):
                strength_z += 0.35 * m1
            if isinstance(m5, float):
                strength_z += 0.90 * m5
            if isinstance(m10, float):
                strength_z += 0.70 * m10
            if isinstance(m20, float):
                strength_z += 0.55 * m20

            strength_score = _tanh_clip100(strength_z, scale=2.8)

            # score: 综合（-100~100），趋势更重要
            score = None
            if trend_score is not None and strength_score is not None:
                score = _clip100(0.65 * trend_score + 0.35 * strength_score)

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
                "vol20": vol20,
                "trend_score": trend_score,
                "strength_score": strength_score,
                "score": score,
                "_meta": {
                    "window_len": len(closes),
                    "vol_fallback": (vol20 is None),
                    "source": "index_core_raw",
                    "algo": "v3_vol_norm_tanh",
                },
            }

            out[name] = feats
            if isinstance(score, (int, float)):
                sector_score[name] = float(score)

        out["_sector_score"] = sector_score
        out["_meta"] = {
            "input_key": "index_core_raw",
            "algo": "v3_vol_norm_tanh",
            "indices": [k for k in out.keys() if not k.startswith("_")],
        }
        return out
