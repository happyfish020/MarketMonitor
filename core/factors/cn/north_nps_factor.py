# -*- coding: utf-8 -*-
"""
UnifiedRisk V12
Factor: north_nps

Pre-Stable version (PASS-THROUGH)

职责：
- 校验 north_nps_raw 是否存在
- 输出最小、稳定、可审计的结构性信息
- 不做任何分析、不做趋势、不参与预测

冻结规则：
- 不计算 score
- 不计算 level
- 不引入 trend / strength
"""

from typing import Dict, Any, List

from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult



class NorthNPSFactor(FactorBase):
    def __init__(self):
        # factor 名称必须与 raw key 区分
        super().__init__("north_nps")

    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        """
        输入：
        - input_block["north_nps_raw"]

        输出（details）：
        - data_status
        - proxy_count
        - window_len_min
        - window_len_max
        """

        data = self.pick(input_block, "north_nps_raw", {})

        # -------------------------------
        # 1. 数据缺失处理（允许）
        # -------------------------------
        if not data:
            return FactorResult(
                name=self.name,
                score=None,
                level=None,
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "proxy_count": 0,
                },
            )

        # -------------------------------
        # 2. 基础结构统计（不做分析）
        # -------------------------------
        proxy_keys: List[str] = list(data.keys())

        window_lengths: List[int] = []
        for proxy_key in proxy_keys:
            proxy = data.get(proxy_key, {})
            window = proxy.get("window", [])
            if isinstance(window, list):
                window_lengths.append(len(window))

        # window_lengths 为空的极端情况（理论上不该发生）
        if not window_lengths:
            return FactorResult(
                name=self.name,
                score=None,
                level=None,
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "proxy_count": len(proxy_keys),
                },
            )

        # -------------------------------
        # 3. 输出冻结字段
        # -------------------------------
        details = {
            "data_status": "OK",
            "proxy_count": len(proxy_keys),
            "window_len_min": min(window_lengths),
            "window_len_max": max(window_lengths),
            "_raw_data": "OK"
        }

        return FactorResult(
            name=self.name,
            score=50.0,
            level="NEUTRAL",
            details=details,
        )
