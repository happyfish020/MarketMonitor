# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any
from core.factors.base import FactorResult


class IndexGlobalFactor:
    """
    海外指数强弱因子（A50 夜盘、VIX、SPX）
    V11.8：从 snapshot['index_global'] 读取数据，不访问 client。
    """

    name = "index_global"

    def compute_from_snapshot(self, snapshot: Dict[str, Any]) -> FactorResult:
        ig = snapshot.get("index_global") or {}

        a50 = ig.get("a50_future", {}) or {}
        vix = ig.get("vix", {}) or {}
        spx = ig.get("spx", {}) or {}

        score_a50, desc_a50 = self._score_a50(a50)
        score_vix, desc_vix = self._score_vix(vix)
        score_spx, desc_spx = self._score_spx(spx)

        score = round((score_a50 + score_vix + score_spx) / 3.0, 2)

        if score >= 65:
            level = "偏多"
        elif score >= 55:
            level = "略偏多"
        elif score >= 45:
            level = "中性"
        elif score >= 35:
            level = "略偏空"
        else:
            level = "偏空"

        details = {
            "a50_score": score_a50, "a50_desc": desc_a50,
            "vix_score": score_vix, "vix_desc": desc_vix,
            "spx_score": score_spx, "spx_desc": desc_spx,
        }

        return FactorResult(
            name=self.name,
            score=score,
            level=level,
            details=details,
            factor_obj=self,
        )

    # 子因子评分
    def _score_a50(self, a50):
        pct = a50.get("pct_change", 0.0)
        if pct >= 1.0:
            return 80, f"A50 夜盘强势 +{pct:.2%}"
        if pct >= 0.5:
            return 65, f"A50 夜盘偏强 +{pct:.2%}"
        if pct > -0.5:
            return 50, f"A50 夜盘中性 {pct:.2%}"
        if pct > -1.0:
            return 40, f"A50 夜盘偏弱 {pct:.2%}"
        return 30, f"A50 夜盘走弱 {pct:.2%}"

    def _score_vix(self, vix):
        pct = vix.get("pct_change", 0.0)
        if pct <= -3:
            return 70, f"VIX 大幅回落（低波动，偏多） {pct:.2%}"
        if -1 <= pct <= 1:
            return 50, f"VIX 稳定（中性） {pct:.2%}"
        if pct <= 5:
            return 40, f"VIX 升高（风险上升） {pct:.2%}"
        return 25, f"VIX 飙升（风险偏高） {pct:.2%}"

    def _score_spx(self, spx):
        pct = spx.get("pct_change", 0.0)
        if pct >= 1:
            return 70, f"标普上涨（偏多） +{pct:.2%}"
        if pct >= 0.5:
            return 60, f"标普偏强 +{pct:.2%}"
        if pct > -0.5:
            return 50, f"标普震荡 {pct:.2%}"
        if pct > -1.0:
            return 40, f"标普偏弱 {pct:.2%}"
        return 30, f"标普走弱 {pct:.2%}"
