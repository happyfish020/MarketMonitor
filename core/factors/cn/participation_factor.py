# core/factors/cn/participation_factor.py
# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - ParticipationFactor (CN A-Share)

职责：
- 输入：snapshot["participation"]（由 ParticipationDataSource 提供）
- 输出：Participation State（Broad Up / Neutral / Narrow Leadership / Hidden Weakness / Broad Down）
- 作为“结构裁决层”的核心证据之一（不做交易信号）

注意：
- 不依赖 sector/行业数据
- 不访问 DB（DB 在 DS 层）
"""

from __future__ import annotations

from typing import Any, Dict, List

from core.utils.logger import get_logger
from core.factors.factor_base import FactorBase, RiskLevel, FactorResult


LOG = get_logger("Factor.Participation")


class ParticipationFactor(FactorBase):

    def __init__(self):
        super().__init__(name="participation")
        #self.name = "participation"

    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        block = input_block.get("participation_raw") or {}
        
        assert block, "snapshot participation_raw is empty!"
        adv_ratio = float(block.get("adv_ratio", 0.0) or 0.0)
        median_ret = float(block.get("median_return", 0.0) or 0.0)
        index_ret = float(block.get("index_return", 0.0) or 0.0)

        state, score, level, interp = self._map_state(adv_ratio, median_ret, index_ret)

        LOG.info(
            "[ParticipationFactor] score=%.2f level=%s state=%s adv_ratio=%.4f median=%.4f index=%.4f",
            score,
            level,
            state,
            adv_ratio,
            median_ret,
            index_ret,
        )

        return self.build_result(
            score=score,
            level=level,
            details={
                "state": state,
                "adv_ratio": adv_ratio,
                "median_return": median_ret,
                "index_return": index_ret,
                "interpretation": interp,
                "data_status": "OK",
                "_raw_data": block,   # 用于审计/调试；是否展示由 reporter 控制
            },
        )

    @staticmethod
    def _map_state(adv_ratio: float, median_ret: float, index_ret: float):
        """
        冻结版最小映射（先跑通生产日报，再做阈值微调）：

        - Broad Up:
            adv_ratio >= 0.55 且 median_ret >= index_ret
        - Broad Down:
            adv_ratio < 0.45 且 index_ret < 0
        - Hidden Weakness:
            adv_ratio < 0.45 且 index_ret >= 0
        - Narrow Leadership:
            adv_ratio < 0.50 且 index_ret > 0
        - Neutral:
            其它
        """
        interp: List[str] = []

        if adv_ratio >= 0.55 and median_ret >= index_ret:
            state = "Broad Up"
            score = 70.0
            level: RiskLevel = "LOW"
            interp.append("上涨具备广泛参与，结构健康")
            return state, score, level, interp

        if adv_ratio < 0.45 and index_ret < 0:
            state = "Broad Down"
            score = 20.0
            level = "HIGH"
            interp.append("多数个股下跌且指数下跌，结构走弱明显")
            return state, score, level, interp

        if adv_ratio < 0.45 and index_ret >= 0:
            state = "Hidden Weakness"
            score = 30.0
            level = "HIGH"
            interp.append("指数尚可但多数个股下跌，存在“假强”风险")
            return state, score, level, interp

        if adv_ratio < 0.50 and index_ret > 0:
            state = "Narrow Leadership"
            score = 40.0
            level = "NEUTRAL"
            interp.append("指数上涨但参与度不足，上涨集中于少数权重/龙头")
            return state, score, level, interp

        state = "Neutral"
        score = 50.0
        level = "NEUTRAL"
        interp.append("参与度中性，未出现明显结构背离")
        return state, score, level, interp
