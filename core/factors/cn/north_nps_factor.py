# core/factors/cn/north_nps_factor.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Dict, Any, List

from core.factors.base import BaseFactor
from core.models.factor_result import FactorResult
from core.utils.logger import get_logger

LOG = get_logger("Factor.NorthNPS")


class NorthNPSFactor(BaseFactor):
    """
    V12 北向代理因子（松耦合版）

    - 只读 snapshot["north_nps"]，不再直接访问数据源
    - 使用 NorthNpsDataSource 预处理好的强度/趋势
    """

    name = "north_nps"

 

    def compute(self, snapshot: Dict[str, Any]) -> FactorResult:
        data = snapshot.get("north_nps") or {}

        strength = float(data.get("strength_today") or 0.0)
        turnover = float(data.get("turnover_today_e9") or 0.0)
        trend_3d = float(data.get("trend_3d") or 0.0)
        trend_5d = float(data.get("trend_5d") or 0.0)
        zone = (data.get("zone") or "中性").strip()

        etf_symbols = data.get("etf_symbols") or {}
        strength_series: List[Dict[str, Any]] = data.get("strength_series") or []

        LOG.info(
            "[NorthNPSFactor] input: strength=%.2f turnover=%.2f trend3=%.2f trend5=%.2f zone=%s",
            strength,
            turnover,
            trend_3d,
            trend_5d,
            zone,
        )

        if not strength_series:
            desc = "北向代理数据缺失或无效（按中性处理）"
            detail = (
                "当日北向代理强度：0.00\n"
                "估算 ETF 总成交额：0.00 亿\n"
                "3 日 / 5 日趋势：0.00 / 0.00（中性）\n"
                "北向强弱区间：中性\n"
                "数据监控：无有效 ETF 序列"
            )
            return FactorResult(score=50.0, desc=desc, detail=detail)

        # ------------------ 打分逻辑 ------------------
        # 1) 基础分：强度区间
        abs_strength = abs(strength)
        if abs_strength >= 200:
            base_score = 80.0
        elif abs_strength >= 80:
            base_score = 65.0
        elif abs_strength >= 20:
            base_score = 55.0
        else:
            base_score = 50.0

        # 方向偏多 / 偏空
        if strength < 0:
            base_score = 100.0 - base_score  # 强度越大（负），分数越低（偏空）

        # 2) 趋势分（3 日 / 5 日）
        def _norm(v: float, cap: float) -> float:
            if cap <= 0:
                return 0.0
            if v > cap:
                v = cap
            if v < -cap:
                v = -cap
            return v / cap

        n3 = _norm(trend_3d, 200.0)
        n5 = _norm(trend_5d, 300.0)

        trend_score = 10.0 * n3 + 6.0 * n5

        score = base_score + trend_score
        score = max(0.0, min(100.0, score))

        # ------------------ 文案生成 ------------------
        if score >= 70:
            desc = "北向代理资金偏多"
        elif score <= 35:
            desc = "北向代理资金偏空"
        else:
            desc = "北向代理资金中性"

        def _trend_text(v: float, name: str) -> str:
            if v > 200:
                return f"{name}：{v:.2f}（明显偏多）"
            elif v > 80:
                return f"{name}：{v:.2f}（偏多）"
            elif v > 10:
                return f"{name}：{v:.2f}（略偏多）"
            elif v < -200:
                return f"{name}：{v:.2f}（明显偏空）"
            elif v < -80:
                return f"{name}：{v:.2f}（偏空）"
            elif v < -10:
                return f"{name}：{v:.2f}（略偏空）"
            else:
                return f"{name}：{v:.2f}（中性）"

        trend3_text = _trend_text(trend_3d, "3 日趋势")
        trend5_text = _trend_text(trend_5d, "5 日趋势")

        if "强势" in zone:
            zone_text = f"{zone}（北向处于历史偏高水平）"
        elif "弱势" in zone or "偏空" in zone:
            zone_text = f"{zone}（北向处于历史偏低水平）"
        else:
            zone_text = f"{zone}（北向处于中性区间）"

        detail_lines = [
            f"当日北向代理强度：{strength:.2f}",
            f"估算 ETF 总成交额：{turnover:.2f} 亿",
            trend3_text,
            trend5_text,
            f"北向强弱区间：{zone_text}",
        ]

        if etf_symbols:
            sh = etf_symbols.get("north_sh")
            sz = etf_symbols.get("north_sz")
            detail_lines.append(f"ETF 代理标的：SH={sh}，SZ={sz}")

        detail_lines.append("数据监控：未见明显异常（仅基于 ETF 代理推算）")

        detail = "\n".join(detail_lines)
         
        fr = FactorResult()
        fr.score = score
        fr.desc = desc
        fr.detail = detail
        return fr
        
