# core/factors/global_lead_factor.py
# -*- coding: utf-8 -*-

from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from typing import Dict, Any, Optional

from core.utils.logger import log
from core.adapters.datasources.glo.global_lead_client import get_global_lead

from core.models.factor_result import FactorResult


class GlobalLeadFactor:
    name = "global_lead"

    # ===== 数据源 =====
    SYMBOL_TNX = "^TNX"        # 美债 10Y
    SYMBOL_FVX = "^FVX"        # 美债 5Y
    SYMBOL_DXY = "DX-Y.NYB"    # 美元指数
    SYMBOL_NASDAQ = "^IXIC"    # 纳指

    # ===== 权重 =====
    W_BOND = 0.40
    W_DXY = 0.20
    W_NAS = 0.40

    # ===== 判定区间 =====
    BOND_BULL = -0.005     # -0.5%
    BOND_BEAR = +0.005     # +0.5%
    DXY_BULL = -0.004
    DXY_BEAR = +0.004
    NAS_BULL = +0.015
    NAS_BEAR = -0.015

    # ===================================================================
    def compute_from_daily(self, processed: Dict[str, Any], trade_date: date, force_refresh: bool = False) -> FactorResult:
        """
        兼容 FactorResult V11.7 版本（需要 report_block）
        """

        # ===== 获取数据 =====
        d10 = get_global_lead(self.SYMBOL_TNX, trade_date, force_refresh)
        d05 = get_global_lead(self.SYMBOL_FVX, trade_date, force_refresh)
        ddxy = get_global_lead(self.SYMBOL_DXY, trade_date, force_refresh)
        dnas = get_global_lead(self.SYMBOL_NASDAQ, trade_date, force_refresh)

        fields = {
            "bond10": d10,
            "bond05": d05,
            "dxy": ddxy,
            "nasdaq": dnas,
        }

        # 数据缺失 → 中性
        if not d10 or not d05 or not ddxy or not dnas:
            score = 50.0
            level = "中性"
            signal = "海外数据缺失，视为中性"
            report_block = (
                "  - global_lead: 50.00（中性）\n"
                "      · 海外市场数据缺失，无法评估全球风险引导\n"
            )
            return FactorResult(
                name=self.name, score=score, level=level,
                signal=signal, details=fields, raw=fields,
                report_block=report_block
            )

        # ===== 解析 pct =====
        pct10 = float(d10.get("pct_change") or 0.0)
        pct05 = float(d05.get("pct_change") or 0.0)
        pct_dxy = float(ddxy.get("pct_change") or 0.0)
        pct_nas = float(dnas.get("pct_change") or 0.0)

        # ===== 美债平均变化 =====
        avg_bond = (pct10 + pct05) / 2

        # ===== 各项细分评分 =====
        score_bond, label_bond = self._score_bond(avg_bond)
        score_dxy, label_dxy = self._score_dxy(pct_dxy)
        score_nas, label_nas = self._score_nas(pct_nas)

        # ===== 权重加权总分 =====
        contrib_bond = score_bond * self.W_BOND
        contrib_dxy = score_dxy * self.W_DXY
        contrib_nas = score_nas * self.W_NAS

        score = contrib_bond + contrib_dxy + contrib_nas
        score = round(score, 2)

        # ===== 总体判断 =====
        if score >= 70:
            zone = "海外偏多"
        elif score >= 55:
            zone = "中性偏多"
        elif score >= 45:
            zone = "中性"
        elif score >= 30:
            zone = "中性偏空"
        else:
            zone = "海外偏空"

        level = zone
        signal = f"美债={label_bond}，美元={label_dxy}，纳指={label_nas}"

        # ===================================================================
        # 🧾 详细量化报告（符合你要求的“B 版模板”）
        # ===================================================================
        report_block = (
            f"  - global_lead: {score:.2f}（{level}）\n"
            f"      · 美债利率：10Y={pct10*100:.2f}%；5Y={pct05*100:.2f}%；平均={avg_bond*100:.2f}%（{label_bond}）\n"
            f"      · 美元指数 DXY：{pct_dxy*100:.2f}%（{label_dxy}）\n"
            f"      · 纳斯达克 NDX：{pct_nas*100:.2f}%（{label_nas}）\n"
            f"      · 权重贡献：美债={contrib_bond:.2f}；美元={contrib_dxy:.2f}；纳指={contrib_nas:.2f}\n"
            f"      · 海外市场综合判断：{zone}\n"
        )

        # ===================================================================
        # 返回结果
        # ===================================================================
        details = {
            "pct10": pct10, "pct05": pct05, "avg_bond": avg_bond,
            "pct_dxy": pct_dxy, "pct_nas": pct_nas,
            "score_bond": score_bond, "score_dxy": score_dxy, "score_nas": score_nas,
            "contrib_bond": contrib_bond, "contrib_dxy": contrib_dxy, "contrib_nas": contrib_nas,
            "zone": zone,
        }

        return FactorResult(
            name=self.name,
            score=score,
            level=level,
            signal=signal,
            details=details,
            raw=fields,
            report_block=report_block,
        )

    # ===================================================================
    # 评分细则函数
    # ===================================================================
    def _score_bond(self, avg_pct: float):
        if avg_pct <= self.BOND_BULL:
            return 80, "利率下行（偏多）"
        if avg_pct >= self.BOND_BEAR:
            return 30, "利率上行（偏空）"
        return 50, "利率震荡（中性）"

    def _score_dxy(self, pct: float):
        if pct <= self.DXY_BULL:
            return 65, "美元走弱（偏多）"
        if pct >= self.DXY_BEAR:
            return 35, "美元走强（偏空）"
        return 50, "美元震荡（中性）"

    def _score_nas(self, pct: float):
        if pct >= self.NAS_BULL:
            return 80, "纳指强势（偏多）"
        if pct <= self.NAS_BEAR:
            return 30, "纳指走弱（偏空）"
        return 55, "纳指震荡（中性）"
