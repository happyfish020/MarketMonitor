# core/engines/cn/ashare_daily_engine.py

"""
UnifiedRisk V12 - A股日度引擎（核心调度模块）

流程：
  1) 调用 AshareFetcher 获取 raw snapshot
  2) SnapshotBuilderV12 统一结构化
  3) 依次计算各因子（带详细日志）
  4) unify_scores 汇总评分
  5) PredictionEngine 生成预测
  6) Reporter 输出报告
"""

from datetime import datetime
from typing import Dict, Any

from core.utils.logger import get_logger
from core.utils.time_utils import BJ_TZ

# Fetcher
from core.adapters.fetchers.cn.ashare_fetcher import AshareDataFetcher

# Snapshot Builder
from core.snapshot.ashare_snapshot import SnapshotBuilder

# 因子
from core.factors.cn.unified_emotion_factor import UnifiedEmotionFactor
from core.factors.cn.turnover_factor import TurnoverFactor
#from core.factors.cn.market_sentiment_factor import MarketSentimentFactor
from core.factors.cn.margin_factor import MarginFactor
from core.factors.cn.north_nps_factor import NorthNPSFactor
from core.factors.cn.index_tech_factor import IndexTechFactor
from core.factors.cn.emotion_factor import EmotionFactor

# Score Engine
from core.factors.score_unified import unify_scores
from core.utils.time_utils import now_bj
# Prediction
from core.predictors.prediction_engine import PredictionEngine
#from core.adapters.fetchers.cn.refresh_controller_cn import RefreshControllerCN 
# Reporter
from core.reporters.cn.ashare_daily_reporter import build_daily_report_text
from core.utils.trade_calendar import get_last_trade_date

LOG = get_logger("Engine.AShareDaily")

 
# ----------------------------------------------------------------------
def run_cn_ashare_daily(refresh_mode: str = "readonly") -> Dict[str, Any]:
    """
    refresh_mode ∈ {"readonly", "snapshot", "full"}
    """
    LOG.info("======== A股日度风险流程启动 | refresh_mode=%s ========", refresh_mode)

    bj_now = now_bj()
    
    trade_date = get_last_trade_date(bj_now)
    trade_date_str = trade_date.strftime("%Y-%m-%d")

 



    # ------------------------------------------------------------------
    # Step 1: 时间
    ## ------------------------------------------------------------------
    #bj_now = datetime.now(BJ_TZ)
    LOG.info("当前北京时间: %s", bj_now.strftime("%Y-%m-%d %H:%M:%S"))

    # ------------------------------------------------------------------
    # Step 2: Snapshot（Fetcher → SnapshotBuilder）
    # ------------------------------------------------------------------
    LOG.info("[Step 2] 构建 Snapshot (Fetcher → Builder) ...")

    fetcher = AshareDataFetcher(trade_date_str, refresh_mode=refresh_mode )
    raw_snapshot = fetcher.build_daily_snapshot()

    builder = SnapshotBuilder()
    snapshot = builder.build(raw_snapshot)

    #trade_date = snapshot["meta"].get("trade_date")
    LOG.info("Snapshot 构建完成 trade_date=%s", trade_date_str)

    # ------------------------------------------------------------------
    # Step 3: 计算因子（按顺序）
    # ------------------------------------------------------------------
    LOG.info("[Step 3] 开始计算因子 ...")

    factors = {}

    def compute_factor(name: str, factor_obj):
        LOG.info("[Factor.%s] ComputeStart", name)
        try:
            result = factor_obj.compute(snapshot)
            LOG.info("[Factor.%s] ComputeEnd: score=%.2f", name, result.score)
            return result
        except Exception as e:
            LOG.error("[Factor.%s] ComputeError: %s", name, e)
            raise


    factors["north_nps"] = compute_factor("north_nps", NorthNPSFactor( ))
    factors["turnover"] = compute_factor("turnover", TurnoverFactor())
    factors["margin"] = compute_factor("margin", MarginFactor( ))
    factors["unified_emotion"] = compute_factor("unified_emotion", UnifiedEmotionFactor())
  
    #factors["sentiment"] = compute_factor("sentiment", MarketSentimentFactor())
    
    #factors["emotion"] = compute_factor("emotion", EmotionFactor())
 

    #factors["index_tech"] = compute_factor("index_tech", IndexTechFactor())

    # ------------------------------------------------------------------
    # Step 4: 综合评分 unify_scores
    # ------------------------------------------------------------------
    LOG.info("[Step 4] 综合评分 unify_scores 开始 ...")

    unified = unify_scores(
        
        turnover=factors["turnover"].score,
        #sentiment=factors["sentiment"].score,
        margin=factors["margin"].score,
        north_nps=factors["north_nps"].score,
        #emotion=factors["emotion"].score,
        unified_emotion=factors["unified_emotion"].score,
        #index_tech=factors["index_tech"].score,
    )

    LOG.info("[Step 4] 综合评分结束 | TotalScore=%.2f", unified.total)

    # ------------------------------------------------------------------
    # Step 5: 预测
    # ------------------------------------------------------------------
    LOG.info("[Step 5] 预测模型启动 PredictionEngine ...")

    predictor = PredictionEngine()
    prediction = predictor.predict(snapshot, factors)

    LOG.info("[Step 5] 预测完成 | summary=%s", prediction.get("summary"))

    # ------------------------------------------------------------------
    # Step 6: Reporter（构建文本，不保存）
    # ------------------------------------------------------------------
    LOG.info("[Step 6] 构建报告文本 build_daily_report_text ...")

    report_text = build_daily_report_text(snapshot, factors,  prediction)

    LOG.info("[Step 6] 报告文本构建完成 | 字符数=%s", len(report_text))

    LOG.info("======== A股日度风险流程完成 ========")

    # ------------------------------------------------------------------
    return {
        "meta": snapshot["meta"],
        "snapshot": snapshot,
        "factors": factors,
        "unified": unified,
        "prediction": prediction,
        "report_text": report_text,
    }
