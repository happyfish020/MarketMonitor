# -*- coding: utf-8 -*-
"""
EmotionDataSource (V12 松耦合版)
负责从 snapshot 中提取指数、量能、北向、主力、衍生品等指标，
封装为统一 emotion block。
"""

from __future__ import annotations
from typing import Dict, Any

from core.adapters.datasources.base import BaseDataSource
from core.utils.logger import get_logger

LOG = get_logger("DS.Emotion")


class EmotionDataSource(BaseDataSource):
    def __init__(self):
        super().__init__( name="emotion")
        self.market="cn"


    # -----------------------------
    # V12 统一接口: get_block()
    # -----------------------------
    def get_block(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        LOG.info("[DS.Emotion] 构建 emotion block ...")

        idx = snapshot.get("index", {})
        north = snapshot.get("north_nps", {})
        turnover = snapshot.get("turnover", {})
        sentiment = snapshot.get("sentiment", {})

        # -------- Index 部分 --------
        index_pct = idx.get("pct_chg", 0.0)
        index_label = self._label_index(index_pct)

        # -------- Volume 部分（成交额）--------
        total_vol = turnover.get("total", 0.0)
        vol_label = self._label_volume(total_vol)

        # -------- Breadth 部分（从 sentiment 来）--------
        adv = sentiment.get("adv", 0)
        dec = sentiment.get("dec", 0)
        breadth_label = self._label_breadth(adv, dec)

        # -------- 北向方向（nps 因子已有 proxy）--------
        nb_strength = north.get("strength_today", 0.0)
        north_label = self._label_north(nb_strength)

        # -------- 主力强度（未来可扩展）--------
        main_force = turnover.get("main_force", 0.0)
        main_label = self._label_main_force(main_force)

        # -------- 衍生品（暂时 Neutral）--------
        der_label = "Neutral"

        block = {
            "index_pct": index_pct,
            "index_label": index_label,
            "volume_e9": total_vol,
            "volume_label": vol_label,
            "adv": adv,
            "dec": dec,
            "breadth_label": breadth_label,
            "north_strength": nb_strength,
            "north_label": north_label,
            "main_force": main_force,
            "main_force_label": main_label,
            "derivative_label": der_label
        }

        LOG.info("[DS.Emotion] block=%s", block)
        return block

    # -----------------------------
    # 以下为分类函数（V11 保留逻辑）
    # -----------------------------
    def _label_index(self, pct):
        if pct >= 1.2:
            return "Strong Bull"
        if pct >= 0.3:
            return "Bullish"
        if pct <= -1.2:
            return "Strong Bear"
        if pct <= -0.3:
            return "Bearish"
        return "Neutral"

    def _label_volume(self, v):
        if v >= 12000:
            return "High Volume"
        if v >= 8000:
            return "Normal Volume"
        return "Low Volume"

    def _label_breadth(self, adv, dec):
        if adv + dec == 0:
            return "Neutral"
        ratio = adv / max(dec, 1)
        if ratio >= 2.0:
            return "Strong Breadth"
        if ratio >= 1.2:
            return "Positive Breadth"
        if ratio <= 0.5:
            return "Weak Breadth"
        return "Neutral Breadth"

    def _label_north(self, strength):
        if strength >= 60:
            return "Strong Inflow"
        if strength >= 30:
            return "Inflow"
        if strength <= -60:
            return "Strong Outflow"
        if strength <= -30:
            return "Outflow"
        return "Neutral"

    def _label_main_force(self, mf):
        if mf >= 10:
            return "Strong MF In"
        if mf >= 3:
            return "MF In"
        if mf <= -10:
            return "Strong MF Out"
        if mf <= -3:
            return "MF Out"
        return "Neutral"
