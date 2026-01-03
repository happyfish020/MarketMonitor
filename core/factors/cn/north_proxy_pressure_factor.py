# -*- coding: utf-8 -*-
"""
UnifiedRisk V12
Factor: north_proxy_pressure (T-1 Confirmed, price-proxy trend)

目的（冻结版）：
- 用 3 个指数 ETF（HS300/ZZ50(or ZZ500)/KC50 代理）构造“北向代理压力”信号
- 输出一个可进入 GateDecision 的结构性风险因子（score + level + evidence）
- 解决 “north_nps 单日/未算趋势默认 neutral 误导” 的问题：此因子直接用 window 计算趋势证据

重要说明（你在 RoadMap 里冻结的方向）：
- 这是“northbound proxy pressure”，不是北向真实净买卖。
- 预警用途：用于 DRS/REW 与 Gate 的“提前降速/降权限”，而非买卖开关。

输入（只读）：
- input_block["north_nps_raw"]  或  input_block["north_proxy_raw"]
  结构期望（两种兼容）：
  A) dict: proxy_key -> {"symbol": "...", "window":[{"date":"YYYY-MM-DD","close":..,"pct":..},...]}
  B) dict: {"symbol":"...","window":[...]}  (单 ETF)

输出（约定）：
- score：0~100（越高 = 压力越低/结构越好；与现有多数“质量型”因子一致）
- level：LOW/NEUTRAL/HIGH（越高 = 风险越高）
- details：包含每个 proxy 的 ret_5d/10d/20d、slope_5d/10d、ma20_slope、dd10 等证据
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import math

from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult

def _wrap_factor(fr: Any) -> Dict[str, Any]:
    """FactorResult -> dict wrapper (for blocks expecting dict)."""
    if fr is None:
        return {}
    return {
        "score": getattr(fr, "score", None),
        "level": getattr(fr, "level", None),
        "details": getattr(fr, "details", None),
    }
    
class NorthProxyPressureFactor(FactorBase):
    def __init__(self) -> None:
        super().__init__(name="north_proxy_pressure")

        # 你当前工程里已有的 3 ETF proxy（可在 config 里换）
        self._SYMBOLS = {
            "hs300": "510300.SS",
            "large": "510050.SS",   # 你目前用 510050；如后续换 ZZ500 可改这里
            "kc50": "159915.SZ",
        }

        # proxy 聚合权重（冻结：偏重 hs300，兼顾高beta）
        self._W = {"hs300": 0.40, "large": 0.30, "kc50": 0.30}

    # ---------------------------------------------------------
    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        raw = self.pick(input_block, "north_nps_raw", None)

        if raw is None:
            raw = self.pick(input_block, "north_proxy_raw", None)

        if not raw:
            return self._neutral("DATA_NOT_CONNECTED:north_nps_raw/north_proxy_raw")

        proxies = self._normalize_raw(raw)

        # pick required symbols if present
        picked = {}
        for k, sym in self._SYMBOLS.items():
            if sym in proxies:
                picked[k] = proxies[sym]

        if len(picked) < 2:
            # 至少要有 2 个 proxy 才有一致性意义
            return self._neutral(f"INSUFFICIENT_PROXY_COUNT:got={list(proxies.keys())[:10]}")

        # compute per proxy metrics
        per: Dict[str, Dict[str, Any]] = {}
        proxy_scores: Dict[str, float] = {}
        all_reasons: List[str] = []

        for role, blk in picked.items():
            m = self._calc_metrics(blk)
            if m is None:
                continue
            q, reasons = self._quality_score(m)
            per[role] = {**m, "quality": round(q, 2), "reasons": reasons}
            proxy_scores[role] = q
            all_reasons.extend([f"{role}:{r}" for r in reasons])

        if len(proxy_scores) < 2:
            return self._neutral("INSUFFICIENT_HISTORY:window<25 or invalid window format")

        # cross-proxy confirmations (broad pressure + high beta underperform)
        cross_penalty, cross_reasons = self._cross_proxy_penalty(per)
        all_reasons.extend(cross_reasons)

        # weighted average quality
        total_w = 0.0
        q_sum = 0.0
        for role, q in proxy_scores.items():
            w = float(self._W.get(role, 1.0))
            total_w += w
            q_sum += w * q

        base_quality = (q_sum / total_w) if total_w > 0 else sum(proxy_scores.values()) / len(proxy_scores)
        quality = base_quality - cross_penalty

        if quality < 0:
            quality = 0.0
        if quality > 100:
            quality = 100.0

        level = self._level_from_quality(quality)
        pressure_score = 100.0 - quality

        details = {
            "data_status": "OK",
            "score_semantics": "QUALITY_HIGH_IS_LOW_PRESSURE",
            "quality_score": round(quality, 2),
            "pressure_score": round(pressure_score, 2),
            "pressure_level": self._pressure_level(pressure_score),

            "evidence": {
                "quality_score": round(float(quality), 2),
                "pressure_score": round(float(pressure_score), 2),
                "pressure_level": self._pressure_level(pressure_score),
            },

            # per-proxy evidence
            "proxies": per,

            # aggregation metadata
            "proxy_used": list(per.keys()),
            "weights": self._W,
            "cross_penalty": round(float(cross_penalty), 2),
            "reasons": all_reasons[:40],  # 防爆炸（append-only 可改）
        }

        return FactorResult(
            name=self.name,
            score=round(float(quality), 2),
            level=level,
            details=details,
        )

    # ---------------------------------------------------------
    # Scoring helpers
    # ---------------------------------------------------------
    def _quality_score(self, m: Dict[str, Any]) -> Tuple[float, List[str]]:
        """
        输出 quality score（100好 -> 0差），与 etf_index_sync_daily_factor 的方向一致。
        """
        q = 100.0
        reasons: List[str] = []

        r1 = m.get("ret_1d")
        r5 = m.get("ret_5d")
        r10 = m.get("ret_10d")
        r20 = m.get("ret_20d")
        s5 = m.get("slope_5d")
        s10 = m.get("slope_10d")
        ma20_slope = m.get("ma20_slope")
        below_ma20 = m.get("below_ma20")
        dd10 = m.get("dd10")

        # 5D / 10D / 20D 走弱（趋势退潮）
        # 1D shock（兑现/冲击日）：轻惩罚，避免噪声翻转
        if isinstance(r1, (int, float)):
            if r1 <= -0.006:
                q -= 6; reasons.append("ret1<-0.6%")
            elif r1 <= -0.004:
                q -= 3; reasons.append("ret1<-0.4%")


        if isinstance(r5, (int, float)):
            if r5 <= -0.012:
                q -= 15; reasons.append("ret5<-1.2%")
            elif r5 <= -0.008:
                q -= 8; reasons.append("ret5<-0.8%")

        if isinstance(r10, (int, float)):
            if r10 <= -0.025:
                q -= 15; reasons.append("ret10<-2.5%")
            elif r10 <= -0.015:
                q -= 8; reasons.append("ret10<-1.5%")

        if isinstance(r20, (int, float)):
            if r20 <= -0.040:
                q -= 10; reasons.append("ret20<-4%")
            elif r20 <= -0.020:
                q -= 6; reasons.append("ret20<-2%")

        # slope（单位：fraction per day；-0.0015≈-0.15%/day）
        if isinstance(s5, (int, float)) and s5 <= -0.0015:
            q -= 15; reasons.append("slope5<-0.15%/d")
        if isinstance(s10, (int, float)) and s10 <= -0.0010:
            q -= 10; reasons.append("slope10<-0.10%/d")

        # 趋势确认（MA20 下行且价格在其下方）
        if below_ma20 is True and isinstance(ma20_slope, (int, float)) and ma20_slope < 0:
            q -= 10; reasons.append("below_ma20&ma20_down")
        elif isinstance(ma20_slope, (int, float)) and ma20_slope < 0:
            q -= 5; reasons.append("ma20_down")

        # 回撤结构（非崩坏，只做轻惩罚）
        if isinstance(dd10, (int, float)) and dd10 > 0.03:
            q -= 5; reasons.append("dd10>3%")

        if q < 0:
            q = 0.0
        if q > 100:
            q = 100.0

        return q, reasons

    def _cross_proxy_penalty(self, per: Dict[str, Dict[str, Any]]) -> Tuple[float, List[str]]:
        """
        交叉确认：降低误报/漏报。
        - 2/3 proxy 5D 同为负：增加压力（扣分）
        - 高 beta(KC50) 相对 HS300 明显走弱：扣分
        """
        penalty = 0.0
        reasons: List[str] = []

        # Shock confirm (兑现日确认)：>=2 proxies 1D 显著回落 → 额外压力
        shock = 0
        for _, m in per.items():
            r1 = m.get("ret_1d")
            if isinstance(r1, (int, float)) and r1 <= -0.004:
                shock += 1
        if shock >= 2:
            penalty += 5.0
            reasons.append("shock_day_confirm(>=2 proxies ret1<-0.4%)")

# Broad retreat confirm
        neg5 = 0
        for _, m in per.items():
            r5 = m.get("ret_5d")
            if isinstance(r5, (int, float)) and r5 < 0:
                neg5 += 1
        if neg5 >= 2:
            penalty += 10.0
            reasons.append("broad_retreat(>=2 proxies ret5<0)")

        # High-beta underperform
        kc = per.get("kc50")
        hs = per.get("hs300")
        if kc and hs:
            kc5 = kc.get("ret_5d")
            hs5 = hs.get("ret_5d")
            if isinstance(kc5, (int, float)) and isinstance(hs5, (int, float)):
                if (kc5 - hs5) < -0.010:
                    penalty += 10.0
                    reasons.append("kc50_underperform_vs_hs300(>1%)")

        return penalty, reasons

    # ---------------------------------------------------------
    # Metrics
    # ---------------------------------------------------------
    def _calc_metrics(self, blk: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        从 window 计算基础趋势证据。
        需要至少 25 天窗口（保证 ma20_slope 计算稳定）。
        """
        win = blk.get("window")
        if not isinstance(win, list) or len(win) < 25:
            return None

        closes: List[float] = []
        dates: List[str] = []
        for it in win:
            if not isinstance(it, dict):
                continue
            c = it.get("close")
            d = it.get("date")
            if isinstance(c, (int, float)) and isinstance(d, str):
                closes.append(float(c))
                dates.append(d)

        if len(closes) < 25:
            return None

        # helpers
        def _ret(k: int) -> Optional[float]:
            if len(closes) <= k:
                return None
            p0 = closes[-1 - k]
            if not p0:
                return None
            return closes[-1] / p0 - 1.0

        def _slope(m: int) -> Optional[float]:
            if len(closes) < m:
                return None
            y = closes[-m:]
            # demeaned linear regression on x=0..m-1
            x = list(range(m))
            x_mean = (m - 1) / 2.0
            y_mean = sum(y) / m
            num = 0.0
            den = 0.0
            for i in range(m):
                dx = x[i] - x_mean
                dy = y[i] - y_mean
                num += dx * dy
                den += dx * dx
            if den == 0:
                return 0.0
            b = num / den  # price units per day
            last = closes[-1]
            if not last:
                return None
            return b / last  # fraction per day

        def _ma(n: int, offset_from_end: int = 0) -> Optional[float]:
            if len(closes) < n + offset_from_end:
                return None
            end = len(closes) - offset_from_end
            start = end - n
            seg = closes[start:end]
            if not seg:
                return None
            return sum(seg) / len(seg)

        close = closes[-1]
        ret_1d = _ret(1)
        ret_5d = _ret(5)
        ret_10d = _ret(10)
        ret_20d = _ret(20)

        slope_5d = _slope(5)
        slope_10d = _slope(10)

        ma20 = _ma(20, 0)
        ma20_prev = _ma(20, 5)  # five days ago window
        ma20_slope = None
        if isinstance(ma20, (int, float)) and isinstance(ma20_prev, (int, float)) and ma20_prev != 0:
            ma20_slope = (ma20 - ma20_prev) / ma20_prev

        below_ma20 = None
        if isinstance(ma20, (int, float)):
            below_ma20 = close < ma20

        # drawdown over 10 days
        dd10 = None
        if len(closes) >= 10:
            m10 = max(closes[-10:])
            if m10:
                dd10 = (m10 - close) / m10

        return {
            "symbol": blk.get("symbol"),
            "asof": dates[-1],
            "close": round(float(close), 4),
            "ret_1d": self._r(ret_1d),
            "ret_5d": self._r(ret_5d),
            "ret_10d": self._r(ret_10d),
            "ret_20d": self._r(ret_20d),
            "slope_5d": self._r(slope_5d),
            "slope_10d": self._r(slope_10d),
            "ma20_slope": self._r(ma20_slope),
            "below_ma20": below_ma20,
            "dd10": self._r(dd10),
            "window_len": len(closes),
        }

    @staticmethod
    def _r(v: Any) -> Optional[float]:
        if v is None:
            return None
        try:
            if isinstance(v, bool):
                return None
            return round(float(v), 6)
        except Exception:
            return None

    # ---------------------------------------------------------
    # Utilities
    # ---------------------------------------------------------
    @staticmethod
    def _normalize_raw(raw: Any) -> Dict[str, Dict[str, Any]]:
        """
        返回 dict: symbol -> block
        """
        out: Dict[str, Dict[str, Any]] = {}

        if isinstance(raw, dict) and "symbol" in raw and "window" in raw:
            sym = raw.get("symbol")
            if isinstance(sym, str):
                out[sym] = raw
            return out

        if isinstance(raw, dict):
            for _, v in raw.items():
                if isinstance(v, dict) and isinstance(v.get("symbol"), str) and isinstance(v.get("window"), list):
                    out[v["symbol"]] = v
        return out

    @staticmethod
    def _level_from_quality(q: float) -> str:
        # 越高越好 → 风险越低
        if q >= 70:
            return "LOW"
        if q >= 55:
            return "NEUTRAL"
        return "HIGH"

    @staticmethod
    def _pressure_level(pressure_score: float) -> str:
        # 越高越危险
        if pressure_score >= 45:
            return "HIGH"
        if pressure_score >= 25:
            return "NEUTRAL"
        return "LOW"

    def _neutral(self, reason: str) -> FactorResult:
        return FactorResult(
            name=self.name,
            score=50.0,
            level="NEUTRAL",
            details={
                "data_status": "DATA_NOT_CONNECTED",
                "note": reason,
                "score_semantics": "QUALITY_HIGH_IS_LOW_PRESSURE",
                "evidence": {"quality_score": 50.0, "pressure_score": 50.0, "pressure_level": "NEUTRAL"},
            },
        )
