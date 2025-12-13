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
from datetime import datetime, timezone, timedelta

from core.utils.logger import get_logger
from core.adapters.fetchers.cn.ashare_fetcher import AshareDataFetcher
from core.predictors.prediction_engine import PredictionEngine

# factors
from core.factors.cn.unified_emotion_factor import UnifiedEmotionFactor
from core.factors.cn.margin_factor import MarginFactor
from core.factors.cn.north_nps_factor import NorthNPSFactor
from core.factors.cn.sector_rotation_factor import SectorRotationFactor
from core.factors.cn.index_tech_factor import IndexTechFactor

# global / macro factors（真实目录是 glo）
from core.factors.glo.global_macro_factor import GlobalMacroFactor
from core.factors.glo.global_lead_factor import GlobalLeadFactor
from core.factors.glo.index_global_factor import IndexGlobalFactor

# reporter（唯一输出层）
from core.reporters.cn.ashare_daily_reporter import (
    build_daily_report_text,
    save_daily_report,
)

LOG = get_logger("Engine.AshareDaily")


# ----------------------------------------------------------------------
# 工具：trade_date 统一
# ----------------------------------------------------------------------
def _normalize_trade_date(trade_date: str | None) -> str:
    if trade_date:
        return str(trade_date)

    bj_tz = timezone(timedelta(hours=8))
    return datetime.now(bj_tz).strftime("%Y-%m-%d")


# ----------------------------------------------------------------------
# 主入口（被 main.py 调用）
# ----------------------------------------------------------------------
def run_cn_ashare_daily(
    *,
    trade_date: str | None = None,
    refresh_mode: str = "readonly",
) -> None:
    """
    CN AShare 日度运行入口（V12）

    - 不返回任何业务数据
    - 不抛出非致命异常
    """

    trade_date_str = _normalize_trade_date(trade_date)

    LOG.info(
        "Run CN AShare Daily | trade_date=%s refresh=%s",
        trade_date_str,
        refresh_mode,
    )

    # ------------------------------------------------------------------
    # 1️⃣ Snapshot（Fetcher）
    # ------------------------------------------------------------------
    fetcher = AshareDataFetcher(trade_date_str, refresh_mode=refresh_mode)
    snapshot: Dict[str, Any] = fetcher.prepare_daily_market_snapshot()

    meta = snapshot.get("meta", {})
    meta.setdefault("trade_date", trade_date_str)

    # ------------------------------------------------------------------
    # 2️⃣ Factors（严格 V12：compute(input_block)）
    # ------------------------------------------------------------------
    factor_list = [
        UnifiedEmotionFactor(),
        GlobalMacroFactor(),
        IndexGlobalFactor(),
        GlobalLeadFactor(),
        NorthNPSFactor(),
        #TurnoverFactor(),
        MarginFactor(),
        SectorRotationFactor(),
        IndexTechFactor(),
    ]

    factors: Dict[str, Any] = {}

    for factor in factor_list:
        try:
            fr = factor.compute(snapshot)
            factors[fr.name] = fr

            # Engine 日志：不假设 level 类型
            level = (
                fr.level.value
                if hasattr(fr.level, "value")
                else str(fr.level)
            )

            LOG.info(
                "[Factor.%s] score=%.2f level=%s",
                fr.name,
                fr.score,
                level,
            )

        except Exception:
            LOG.exception(
                "Factor compute failed: %s",
                getattr(factor, "name", factor.__class__.__name__),
            )

    # ------------------------------------------------------------------
    # 3️⃣ Prediction（不解构）
    # ------------------------------------------------------------------
    predictor = PredictionEngine()
    prediction = predictor.predict(factors)

    LOG.info(
        "Prediction generated: %s",
        type(prediction).__name__,
    )

    # ------------------------------------------------------------------
    # 4️⃣ Reporter（唯一输出层）
    # ------------------------------------------------------------------
    report_text = build_daily_report_text(
        meta=meta,
        factors=factors,
        prediction=prediction,
    )

    save_daily_report(
        market="cn",
        trade_date=trade_date_str,
        text=report_text,
    )

    LOG.info("CN AShare Daily finished successfully.")
