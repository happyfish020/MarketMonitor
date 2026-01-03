# -*- coding: utf-8 -*-
"""
UnifiedRisk V12
Factor: sector_proxy (Sector Proxy · Validation · MVP)

目标（冻结版 / UAT-P0）：
- 用一组“板块/主题 ETF 代理”观察市场参与与结构验证
- 这是结构验证因子：用于解释/辅助 GateDecision/DRS（默认权重=0，先 UAT）

输入（只读）：
- input_block["sector_proxy_raw"] (推荐)
  or input_block["sector_proxy"] (兼容)

期望 raw 结构（由 SectorProxyDataSource 构建）：
{
  "benchmark": {"symbol": "...", "window": [...]} ,
  "sectors": {"ai": {"symbol": "...", "window": [...]}, ...},
  "meta": {...}
}

输出（约定）：
- score：0~100（越高=验证越强 / 结构越健康）
- level：LOW/NEUTRAL/HIGH（越高=风险越高）
- details：包含 per-sector 证据、聚合统计，以及 evidence.* 关键字段（用于报告/校验）
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import math

from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult


class SectorProxyFactor(FactorBase):
    def __init__(self, lookback_days: int = 20) -> None:
        """Sector proxy validation factor.

        This factor is constructed by the YAML-driven factor pipeline.
        The pipeline may pass constructor params from:
        `config/weights.yaml -> factor_pipeline.params.sector_proxy`.

        Args:
            lookback_days: Reserved for future tuning. MVP 仍使用固定的 5/10/20
                日窗口进行 RS/验证统计；该参数先记录不改变当前评分逻辑。
        """
        super().__init__(name="sector_proxy")
        try:
            self.lookback_days = int(lookback_days) if lookback_days else 20
        except Exception:
            self.lookback_days = 20

    # ---------------------------------------------------------
    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        raw = self.pick(input_block, "sector_proxy_raw", None)
        if raw is None:
            raw = self.pick(input_block, "sector_proxy", None)

        if not isinstance(raw, dict) or not raw:
            return self._neutral("DATA_NOT_CONNECTED:sector_proxy_raw/sector_proxy")

        bench = raw.get("benchmark")
        sectors = raw.get("sectors")

        if not isinstance(bench, dict) or not isinstance(sectors, dict) or not sectors:
            return self._neutral("INVALID_RAW_FORMAT:missing benchmark/sectors")

        bench_m = self._calc_metrics(bench)
        if bench_m is None:
            return self._neutral("INSUFFICIENT_HISTORY:benchmark")

        per: Dict[str, Dict[str, Any]] = {}
        rs10_list: List[float] = []
        reasons: List[str] = []

        for sk, blk in sectors.items():
            if not isinstance(sk, str) or not isinstance(blk, dict):
                continue
            # carry metadata (optional) so we can reason about defensive vs risk_on leadership
            grp = blk.get("group")
            alias = blk.get("alias")
            m = self._calc_metrics(blk)
            if m is None:
                continue

            # relative strength vs benchmark (same horizon)
            rs_5 = self._diff(m.get("ret_5d"), bench_m.get("ret_5d"))
            rs_10 = self._diff(m.get("ret_10d"), bench_m.get("ret_10d"))
            rs_20 = self._diff(m.get("ret_20d"), bench_m.get("ret_20d"))
            m["rs_5d"] = rs_5
            m["rs_10d"] = rs_10
            m["rs_20d"] = rs_20

            if isinstance(rs_10, (int, float)):
                rs10_list.append(float(rs_10))

            if isinstance(grp, str):
                m["group"] = grp
            if isinstance(alias, str):
                m["alias"] = alias

            per[sk] = m

        if len(per) < 2 or len(rs10_list) < 2:
            return self._neutral("INSUFFICIENT_SECTOR_COUNT:need>=2")

        leaders = sum(1 for v in rs10_list if v > 0)
        laggards = sum(1 for v in rs10_list if v < 0)
        n = len(rs10_list)
        leaders_ratio = leaders / n if n > 0 else 0.0

        avg_rs10 = sum(rs10_list) / n
        stdev_rs10 = self._stdev(rs10_list)
        span_rs10 = (max(rs10_list) - min(rs10_list)) if rs10_list else 0.0

        quality, q_reasons = self._quality_score(
            bench_ret_20=bench_m.get("ret_20d"),
            bench_ret_10=bench_m.get("ret_10d"),
            leaders_ratio=leaders_ratio,
            avg_rs10=avg_rs10,
            stdev_rs10=stdev_rs10,
            span_rs10=span_rs10,
            per=per,
        )
        reasons.extend(q_reasons)

        level = self._level_from_quality(quality)
        risk_score = 100.0 - quality

        details = {
            "data_status": raw.get("meta", {}).get("data_status", "OK"),
            "score_semantics": "QUALITY_HIGH_IS_STRONG_VALIDATION",

            # evidence: 报告/校验的关键字段（冻结：只增不改）
            "evidence": {
                "validation_score": round(float(quality), 2),
                "risk_score": round(float(risk_score), 2),
                "leaders_ratio_10d": round(float(leaders_ratio), 4),
                "leaders": int(leaders),
                "laggards": int(laggards),
                "sector_count": int(n),
                "avg_rs_10d": round(float(avg_rs10), 6),
                "stdev_rs_10d": round(float(stdev_rs10), 6),
                "span_rs_10d": round(float(span_rs10), 6),
                "bench_ret_10d": bench_m.get("ret_10d"),
                "bench_ret_20d": bench_m.get("ret_20d"),
            },

            "benchmark": bench_m,
            "sectors": per,
            "reasons": reasons[:60],
        }


        # ---------------------------------------------------------
        # Derived structure label (append-only; used by Gate rules)
        # state values (frozen MVP):
        # - risk_on_confirmed / mixed / broad_weak / defensive_lead
        # ---------------------------------------------------------
        try:
            ev = details.get("evidence") if isinstance(details.get("evidence"), dict) else {}
            leaders_ratio_10d = float(ev.get("leaders_ratio_10d")) if isinstance(ev.get("leaders_ratio_10d"), (int, float)) else None
            bench_ret_20d = ev.get("bench_ret_20d")
            bench_ret_20d = float(bench_ret_20d) if isinstance(bench_ret_20d, (int, float)) else None

            # group stats (if provided by datasource via blk.group)
            risk_on_rs = []
            defensive_rs = []
            for _k, _m in per.items():
                if not isinstance(_m, dict):
                    continue
                g = _m.get("group")
                rs10 = _m.get("rs_10d")
                if not isinstance(rs10, (int, float)):
                    continue
                if g == "risk_on":
                    risk_on_rs.append(float(rs10))
                elif g == "defensive":
                    defensive_rs.append(float(rs10))

            avg_risk_on = sum(risk_on_rs) / len(risk_on_rs) if risk_on_rs else None
            avg_defensive = sum(defensive_rs) / len(defensive_rs) if defensive_rs else None

            # default state
            state = "mixed"
            meaning = "板块代理验证为中性/混合：需要结合其他结构证据与执行摩擦判断。"

            # defensive leadership (if groups available)
            if avg_defensive is not None and avg_risk_on is not None and (avg_defensive - avg_risk_on) >= 0.20:
                state = "defensive_lead"
                meaning = "防御类代理相对强于风险偏好代理：更像防御主导/轮动环境，制度倾向谨慎。"
            else:
                # broad weakness: index up but sectors don't confirm (leaders_ratio low)
                if bench_ret_20d is not None and bench_ret_20d > 0.0 and leaders_ratio_10d is not None and leaders_ratio_10d <= 0.45:
                    state = "broad_weak"
                    meaning = "指数可能走强但板块未能广泛确认（扩散不足）：更像结构性分化/轮动，制度倾向谨慎。"
                elif quality >= 70.0 and leaders_ratio_10d is not None and leaders_ratio_10d >= 0.55:
                    state = "risk_on_confirmed"
                    meaning = "板块代理对基准的相对强度较一致：风险偏好验证较强（不等于允许进攻）。"

            # persist derived labels
            details["state"] = state
            details["meaning"] = meaning
            # add group stats (append-only)
            ev.setdefault("group_stats", {})
            if isinstance(ev.get("group_stats"), dict):
                ev["group_stats"].update(
                    {
                        "avg_rs10_risk_on": round(avg_risk_on, 6) if avg_risk_on is not None else None,
                        "avg_rs10_defensive": round(avg_defensive, 6) if avg_defensive is not None else None,
                        "risk_on_count": int(len(risk_on_rs)),
                        "defensive_count": int(len(defensive_rs)),
                    }
                )
            details["evidence"] = ev
        except Exception:
            # if anything fails, still keep a safe default
            details.setdefault("state", "mixed")
            details.setdefault("meaning", "板块代理验证为中性/混合（state 推导失败，已降级兜底）。")

        return FactorResult(
            name=self.name,
            score=round(float(quality), 2),
            level=level,
            details=details,
        )

    # ---------------------------------------------------------
    def _quality_score(
        self,
        bench_ret_20: Any,
        bench_ret_10: Any,
        leaders_ratio: float,
        avg_rs10: float,
        stdev_rs10: float,
        span_rs10: float,
        per: Dict[str, Dict[str, Any]],
    ) -> Tuple[float, List[str]]:
        q = 100.0
        reasons: List[str] = []

        b20 = float(bench_ret_20) if isinstance(bench_ret_20, (int, float)) else None
        b10 = float(bench_ret_10) if isinstance(bench_ret_10, (int, float)) else None

        # 1) 关键逻辑：指数走强但板块不确认（参与不足） -> 验证走弱
        if b20 is not None and b20 > 0:
            if leaders_ratio < 0.34:
                q -= 25; reasons.append("bench_up_but_few_leaders(<34%)")
            elif leaders_ratio < 0.50:
                q -= 10; reasons.append("bench_up_leaders<50%")

            if avg_rs10 < 0:
                q -= 8; reasons.append("avg_rs10<0_under_bench_up")

        # 2) 结构分裂（过度离散）：不一定是风险，但会降低验证一致性
        if span_rs10 > 0.06:
            q -= 8; reasons.append("rs10_span>6%")
        elif span_rs10 > 0.04:
            q -= 5; reasons.append("rs10_span>4%")

        if stdev_rs10 > 0.025:
            q -= 6; reasons.append("rs10_stdev>2.5%")
        elif stdev_rs10 > 0.018:
            q -= 3; reasons.append("rs10_stdev>1.8%")

        # 3) 连续回撤惩罚：多个 sector 同时出现较深 dd10 -> 参与退潮/高波动
        dd_bad = 0
        for sk, m in per.items():
            dd10 = m.get("dd10")
            if isinstance(dd10, (int, float)) and float(dd10) <= -0.06:
                dd_bad += 1
        if dd_bad >= 2:
            q -= 8; reasons.append(f"dd10_bad>=2({dd_bad})")
        elif dd_bad == 1:
            q -= 3; reasons.append("dd10_bad=1")

        # 4) 弱市/横盘时的“轮动韧性”轻奖励：至少一半板块相对强
        if b20 is not None and b20 <= 0 and leaders_ratio >= 0.50:
            q += 3; reasons.append("bench_weak_but_rotation_resilient")

        # 5) b10 走弱时，如果 leaders_ratio 仍极低，轻惩罚（广泛退潮）
        if b10 is not None and b10 < 0 and leaders_ratio < 0.34:
            q -= 5; reasons.append("bench_down_and_few_leaders")

        # clamp
        if q < 0:
            q = 0.0
        if q > 100:
            q = 100.0
        return q, reasons

    # ---------------------------------------------------------
    @staticmethod
    def _diff(a: Any, b: Any) -> Optional[float]:
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            return float(a) - float(b)
        return None

    @staticmethod
    def _stdev(xs: List[float]) -> float:
        if not xs:
            return 0.0
        m = sum(xs) / len(xs)
        v = sum((x - m) ** 2 for x in xs) / len(xs)
        return math.sqrt(v)

    # ---------------------------------------------------------
    def _calc_metrics(self, blk: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """从单 ETF raw block 计算指标。要求 window 至少 25 条。"""
        win = blk.get("window")
        if not isinstance(win, list) or len(win) < 25:
            return None

        closes: List[float] = []
        for r in win:
            if not isinstance(r, dict):
                continue
            c = r.get("close")
            if isinstance(c, (int, float)):
                closes.append(float(c))

        if len(closes) < 25:
            return None

        ret_1d = self._ret(closes, 1)
        ret_5d = self._ret(closes, 5)
        ret_10d = self._ret(closes, 10)
        ret_20d = self._ret(closes, 20)

        slope_5d = self._slope(closes, 5)
        slope_10d = self._slope(closes, 10)

        dd10 = self._dd(closes, 10)

        out = {
            "symbol": blk.get("symbol"),
            "ret_1d": ret_1d,
            "ret_5d": ret_5d,
            "ret_10d": ret_10d,
            "ret_20d": ret_20d,
            "slope_5d": slope_5d,
            "slope_10d": slope_10d,
            "dd10": dd10,
        }
        return out

    @staticmethod
    def _ret(closes: List[float], horizon: int) -> Optional[float]:
        if len(closes) < horizon + 1:
            return None
        p0 = closes[-(horizon + 1)]
        p1 = closes[-1]
        if p0 <= 0:
            return None
        return (p1 / p0) - 1.0

    @staticmethod
    def _slope(closes: List[float], horizon: int) -> Optional[float]:
        # 简化：horizon 窗口内，平均日变化率（fraction per day）
        if len(closes) < horizon:
            return None
        p0 = closes[-horizon]
        p1 = closes[-1]
        if p0 <= 0:
            return None
        days = max(horizon - 1, 1)
        return ((p1 / p0) - 1.0) / days

    @staticmethod
    def _dd(closes: List[float], horizon: int) -> Optional[float]:
        if len(closes) < horizon:
            return None
        seg = closes[-horizon:]
        peak = seg[0]
        dd = 0.0
        for x in seg:
            if x > peak:
                peak = x
            if peak > 0:
                dd = min(dd, (x / peak) - 1.0)
        return dd

    # ---------------------------------------------------------
    @staticmethod
    def _level_from_quality(q: float) -> str:
        # q 越高越好，level 越低越安全
        if q >= 70:
            return "LOW"
        if q >= 55:
            return "NEUTRAL"
        return "HIGH"

    def _neutral(self, reason: str) -> FactorResult:
        return FactorResult(
            name=self.name,
            score=50.0,
            level="NEUTRAL",
            details={
                "data_status": reason,
                "score_semantics": "QUALITY_HIGH_IS_STRONG_VALIDATION",
                "evidence": {"validation_score": 50.0, "risk_score": 50.0},
                "reasons": [reason],
            },
        )
