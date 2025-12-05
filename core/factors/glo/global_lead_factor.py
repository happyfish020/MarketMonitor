# -*- coding: utf-8 -*-
"""
GlobalLeadFactor (UnifiedRisk V11.7 FINAL)
------------------------------------------
海外市场对 A 股次日（T+1）走势具有显著引导作用。
本因子读取 snapshot["global_lead"]，计算 T+1 风险方向。

因子结构：
- score 0~100
- level（强、中性、弱）
- details（明细）
"""

from __future__ import annotations
from typing import Dict, Any

from core.utils.logger import log


class GlobalLeadFactor:
    NAME = "global_lead"

    #def __init__(self, snapshot: Dict[str, Any]):
    #    self.data = snapshot.get("global_lead", {}) or {}

    def __init__(self, snapshot: Dict[str, Any]) -> None:
        self.snapshot = snapshot

    # ----------------------------------
    # 主函数：输出因子结构
    # ----------------------------------
    def compute(self) -> Dict[str, Any]:
        self.data = self.snapshot.get("global_lead", {}) or {}
        if not self.data:
            return self._null_result()

        log("[GlobalLeadFactor] 开始计算海外引导因子")

        contributions, weighted = self._calc_weighted_contribution()
        score = self._score(weighted)
        level = self._level(score)

        return {
            "name": self.NAME,
            "score": score,
            "level": level,
            "details": {
                "pct": self.data,
                "weighted": weighted,
                "contributions": contributions,
            }
        }

    # ----------------------------------
    # 计算权重贡献
    # ----------------------------------
    def _calc_weighted_contribution(self):
        d = self.data

        # 防错
        spx = float(d.get("spx") or 0.0)
        ndx = float(d.get("ndx") or 0.0)
        hsi = float(d.get("hsi") or 0.0)
        a50 = float(d.get("a50") or 0.0)
        usdcnh = float(d.get("usdcnh") or 0.0)
        vix = float(d.get("vix") or 0.0)

        # 统一权重（可加入 YAML 未来动态化）
        w = {
            "spx": 0.35,
            "ndx": 0.20,
            "hsi": 0.20,
            "a50": 0.15,
            "vix": -0.10,     # VIX 上升为负贡献
            "usdcnh": 0.10    # 人民币升值（usdcnh 下跌）为正贡献
        }

        contributions = {
            "spx": spx * w["spx"],
            "ndx": ndx * w["ndx"],
            "hsi": hsi * w["hsi"],
            "a50": a50 * w["a50"],
            "vix": vix * w["vix"],
            "usdcnh": (-usdcnh) * w["usdcnh"],  # usdcnh 下跌利好
        }

        weighted_sum = round(sum(contributions.values()), 6)

        return contributions, weighted_sum

    # ----------------------------------
    # 分数模型：将 weighted_sum 映射到 0~100
    # ----------------------------------
    def _score(self, weighted: float) -> float:
        # 常规分布：±1% 区间内大部分数据
        # 将 weighted（典型范围 -0.02 → +0.02）映射到 0~100
        raw = (weighted * 2500) + 50   # 放大 + 平移

        # 裁剪
        raw = max(0, min(100, raw))
        return round(raw, 2)

    # ----------------------------------
    # 文本 level
    # ----------------------------------
    def _level(self, score: float) -> str:
        if score >= 75:
            return "强"
        if score >= 55:
            return "中性偏强"
        if score >= 45:
            return "中性"
        if score >= 25:
            return "偏弱"
        return "弱"

    # ----------------------------------
    # 若无数据
    # ----------------------------------
    def _null_result(self):
        return {
            "name": self.NAME,
            "score": 50.0,
            "level": "中性",
            "details": {"msg": "global_lead 数据缺失"}
        }
