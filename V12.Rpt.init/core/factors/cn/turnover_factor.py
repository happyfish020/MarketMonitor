# core/factors/cn/turnover_factor.py
# -*- coding: utf-8 -*-

from __future__ import annotations
from typing import Dict, Any

import json
from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult

from core.utils.logger import get_logger

LOG = get_logger("Factor.Turnover")


class TurnoverFactor(FactorBase):
    """
    成交额流动性因子（V12 专业版）
    衡量市场成交额高低对风险偏好的影响
    """
    def __init__(self):
        
        super().__init__("turnover_raw")
         

    def compute(self, snapshot: Dict[str, Any]) -> FactorResult:
        data = snapshot.get("turnover_raw", {}) or {}
        assert data, "turnover_raw is missing"

        if not data:
            return FactorResult(
                name=self.name,
                score=50.0,
                level="NEUTRAL",
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "reason": "turnover data missing",
                },
            )

        sh = data.get("sh")
        sz = data.get("sz")
        total = data.get("total")
        # 若 total 未提供，则尝试计算总成交额
        if total is None and (sh is not None or sz is not None):
            total = (sh or 0.0) + (sz or 0.0)

        # 数据缺失处理
        if total is None:
            return FactorResult(
                name=self.name,
                value=None,
                score=50.0,
                level="中",
                summary="成交额数据缺失（按中性处理）",
                details={},
                meta={}
            )

        # 计算成交额因子得分
        # 阈值设定（单位：亿元）
        extreme_high = 10000.0  # 成交额极高阈值（例如 >=10000亿）
        high_thresh = 5000.0    # 成交额较高阈值（例如 >=5000亿）
        low_thresh = 2000.0     # 成交额较低阈值（例如 <=2000亿）
        extreme_low = 500.0     # 成交额极低阈值（例如 <=500亿）

        if total >= extreme_high:
            score = 90.0
        elif total >= high_thresh:
            score = 70.0
        elif total <= extreme_low:
            score = 10.0
        elif total <= low_thresh:
            score = 30.0
        else:
            score = 50.0
        score = round(score, 2)

        # 等级划分
        if score >= 60:
            level = "HIGH"    
        elif score <= 40:
            level = "NEUTRAL"
        else:
            level = "LOW"

        # 描述生成
        if level == "HIGH":
            summary = "市场成交额放大，流动性充裕"
        elif level == "LOW":
            summary = "市场成交额低迷，流动性不足"
        else:
            summary = "市场成交额平稳，流动性正常"

        # 详细数据
        details: Dict[str, Any] = {}
        if sh is not None:
            details["sh_turnover"] = sh
        if sz is not None:
            details["sz_turnover"] = sz
        details["total_turnover"] = total
        details["data_status"] =  "OK"
        details["_raw_data"] = json.dumps(data)[:160] + "..."
        LOG.info(f"[TurnoverFactor] sh={sh} sz={sz} total={total:.0f} score={score:.2f} level={level}")
        
        return self.build_result(
            score=score,
            level=level,
            details= details 
            
        )


 