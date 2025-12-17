# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - CN AShare Daily Engine

职责（V12 终态）：
- 作为系统 orchestration 层
- 组织 Fetcher → Snapshot → Factors → Prediction → Reporter
- 不解析业务结果
- 不输出人类可读文本
- 不向 main.py 返回业务数据
"""

from __future__ import annotations

from typing import Dict, Any
from datetime import datetime

from core.utils.logger import get_logger
from core.adapters.fetchers.cn.ashare_fetcher import AshareDataFetcher

from core.factors.cn.unified_emotion_factor import UnifiedEmotionFactor
from core.factors.cn.margin_factor import MarginFactor
from core.factors.cn.north_nps_factor import NorthNPSFactor
from core.factors.cn.turnover_factor import TurnoverFactor
from core.factors.cn.sector_rotation_factor import SectorRotationFactor
from core.factors.cn.index_tech_factor import IndexTechFactor
from core.factors.cn.participation_factor import ParticipationFactor

from core.factors.glo.global_macro_factor import GlobalMacroFactor
from core.factors.glo.global_lead_factor import GlobalLeadFactor
from core.factors.glo.index_global_factor import IndexGlobalFactor
from core.factors.cn.breadth_factor import BreadthFactor

from core.factors.factor_result import FactorResult
from core.adapters.policy_slot_binders.cn.ashares_policy_slot_binder import ASharesPolicySlotBinder
from core.predictors.prediction_engine import PredictionEngine


from core.predictors.prediction_engine import PredictionEngine
from core.reporters.cn.ashare_daily_reporter import build_daily_report_text, save_daily_report


LOG = get_logger("Engine.AshareDaily")


def _normalize_trade_date(trade_date: str | None) -> str:
    if trade_date:
        s = str(trade_date).strip()
        if len(s) == 8 and s.isdigit():
            return f"{s[:4]}-{s[4:6]}-{s[6:]}"
        return s
    return datetime.now().strftime("%Y-%m-%d")


def run_cn_ashare_daily(trade_date: str | None = None, refresh_mode: str = "auto") -> None:
    trade_date_str = _normalize_trade_date(trade_date)

    LOG.info(
        "Run CN AShare Daily | trade_date=%s refresh=%s",
        trade_date_str,
        refresh_mode,
    )

    # 1️⃣ Fetch snapshot
    fetcher = AshareDataFetcher(trade_date=trade_date_str, refresh_mode=refresh_mode)
    snapshot: Dict[str, Any] = fetcher.prepare_daily_market_snapshot()

    # 2️⃣ Factors
    factor_list = [
        UnifiedEmotionFactor(),
        ParticipationFactor(),
        GlobalMacroFactor(),
        IndexGlobalFactor(),
        GlobalLeadFactor(),
        NorthNPSFactor(),
        TurnoverFactor(),
        MarginFactor(),
        SectorRotationFactor(),
        IndexTechFactor(),
        BreadthFactor(),
    ]

    factors: Dict[str, Any] = {}

     # 1️⃣ 计算所有 Factor（raw）
    factors: dict[str, FactorResult] = {}
    
    for factor in factor_list:
        try:
            fr = factor.compute(snapshot)
            factors[factor.name] = fr
            print(factor.name)
            assert factors[factor.name] , f"{factor.name} is missing" 
            LOG.info("[Factor.%s] score=%.2f level=%s", factor.name, fr.score, fr.level)
        except Exception as e:
            LOG.error("[Factor.%s] compute failed: %s", factor.name, e, exc_info=True)
    
    # 2️⃣ PolicySlotBinder（raw → 制度槽位）
    binder = ASharesPolicySlotBinder()
    factors_bound = binder.bind(factors)
    
    # 3️⃣ Prediction（只吃制度槽位）
    prediction_engine = PredictionEngine()
    prediction = prediction_engine.predict(factors_bound)

    # meta
    meta = {
        "market": "cn",
        "trade_date": trade_date_str,
    }

    # 4️⃣ Reporter
    report_text = build_daily_report_text(
        meta=meta,
        factors=factors,
        prediction=prediction,
        snapshot=snapshot,
    )

    if not report_text:
        LOG.error("[Engine.AshareDaily] report_text is EMPTY, skip saving")
        LOG.info("CN AShare Daily finished successfully.")
        return

    save_daily_report(
        trade_date=trade_date_str,
        text=report_text,
    )

    LOG.info("CN AShare Daily finished successfully.")
