# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Any
from  core.factors.base import FactorResult


class GlobalLeadFactor:
    """
    全球宏观引导因子（利率 / 美元 / 成长性）
    V11.8：从 daily_snapshot["global_lead"] 读取数据，不访问 client。
    """

    name = "global_lead"

    # 权重
    WEIGHT_BOND = 0.40
    WEIGHT_DXY = 0.20
    WEIGHT_NASDAQ = 0.40

    # 阈值（pct_change 单位为小数，如 0.01 = 1%）
    BOND_BULL = -0.005
    BOND_BEAR = +0.005
    DXY_BULL = -0.004
    DXY_BEAR = +0.004
    NAS_BULL = +0.015
    NAS_BEAR = -0.015

    # ==========================================================
    # 主入口：compute_from_snapshot
    # ==========================================================
    def compute_from_snapshot(self, snapshot: Dict[str, Any]) -> FactorResult:
        gl = snapshot.get("global_lead") or {}

        bond10 = gl.get("^TNX") or {}
        bond05 = gl.get("^FVX") or {}
        dxy = gl.get("DX-Y.NYB") or {}
        nas = gl.get("^IXIC") or {}

        # 子因子计算
        score_bond, desc_bond = self._score_bond(bond10, bond05)
        score_dxy, desc_dxy = self._score_dxy(dxy)
        score_nas, desc_nas = self._score_nas(nas)

        # 加权总分
        score = (
            score_bond * self.WEIGHT_BOND
            + score_dxy * self.WEIGHT_DXY
            + score_nas * self.WEIGHT_NASDAQ
        )
        score = round(score, 2)

        # level
        if score >= 65:
            level = "偏多（外部环境友好）"
        elif score >= 55:
            level = "略偏多"
        elif score > 45:
            level = "中性"
        elif score > 35:
            level = "略偏空"
        else:
            level = "偏空（外部环境偏紧）"

        details = {
            "bond_score": score_bond,
            "bond_desc": desc_bond,
            "dxy_score": score_dxy,
            "dxy_desc": desc_dxy,
            "nas_score": score_nas,
            "nas_desc": desc_nas,
        }

        return FactorResult(
            name=self.name,
            score=score,
            level=level,
            details=details,
            factor_obj=self,
        )

    # ==========================================================
    # 各子因子打分
    # ==========================================================
    def _score_bond(self, b10, b05):
        if not b10 or not b05:
            return 50, "利率：无数据（中性）"

        p10 = b10.get("pct_change", 0.0)
        p05 = b05.get("pct_change", 0.0)
        avg = (p10 + p05) / 2.0

        if avg <= self.BOND_BULL:
            return 80, f"利率下行（偏多），avg={avg:.4f}"
        elif avg >= self.BOND_BEAR:
            return 20, f"利率上行（偏空），avg={avg:.4f}"
        return 50, f"利率震荡（中性），avg={avg:.4f}"

    def _score_dxy(self, dxy):
        if not dxy:
            return 50, "美元指数：无数据（中性）"
        pct = dxy.get("pct_change", 0.0)

        if pct <= self.DXY_BULL:
            return 80, f"美元走弱（偏多），pct={pct:.4f}"
        elif pct >= self.DXY_BEAR:
            return 20, f"美元走强（偏空），pct={pct:.4f}"
        return 50, f"美元震荡（中性），pct={pct:.4f}"

    def _score_nas(self, nas):
        if not nas:
            return 50, "纳指：无数据（中性）"
        pct = nas.get("pct_change", 0.0)

        if pct >= self.NAS_BULL:
            return 80, f"纳指上涨（偏多），pct={pct:.4f}"
        elif pct <= self.NAS_BEAR:
            return 20, f"纳指回落（偏空），pct={pct:.4f}"
        return 50, f"纳指震荡（中性），pct={pct:.4f}"
