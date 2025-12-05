# -*- coding: utf-8 -*-
"""UnifiedRisk V11.6.6
A股日级引擎（松耦合因子 + 情绪报告整合版）

核心设计：
- Fetcher 负责组装 snapshot（etf_proxy / turnover / breadth / margin 等）
- 本 Engine 负责：
    1）根据 snapshot 构造各因子需要的 features
    2）调用各 Factor 计算，得到 FactorResult
    3）调用 EmotionEngine 生成情绪结果，并适配为 FactorResult
    4）使用 UnifiedScoreBuilder 做统一评分（summary）
    5）使用 ashare_report_cn.build_daily_report_text 生成“因子报告”
    6）使用 emotion_report_writer 生成“情绪报告”
    7）两者合并为单一文本，落地到 reports 目录

特点：
- Engine 不关心各因子内部的字段细节
- 报告模块只依赖 FactorResult.ensure_report_block()
- 新增因子时，只需要在这里增加一行调用 + 在因子内部封装 report_block
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from core.utils.time_utils import now_bj
from core.utils.logger import log
from core.utils.config_loader import load_paths

from core.engines.cn.refresh_controller_cn import RefreshControllerCN, RefreshPlanCN


from core.factors.glo.global_lead_factor import GlobalLeadFactor
#from core.predictors.predict_t1_t5 import PredictionEngine
from core.reporters.cn.ashare_daily_reporter import build_daily_report_text, save_daily_report
#from core.predictors.predict_t1_t5 import PredictorT1T5
# Fetcher 层
from core.adapters.fetchers.cn.ashare_fetcher import (
    AshareFetcher,
    get_daily_cache_path,
    get_intraday_cache_path,
)

# 因子
from core.models.factor_result import FactorResult
from core.factors.score_unified import UnifiedScoreBuilder
from core.factors.cn.north_nps_factor import NorthNPSFactor
from core.factors.cn.turnover_factor import TurnoverFactor
from core.factors.cn.market_sentiment_factor import MarketSentimentFactor
from core.factors.cn.margin_factor import MarginFactor
from core.factors.cn.emotion_engine import compute_cn_emotion_from_snapshot

# 情绪报告格式化
from core.engines.cn.emotion_report_writer import format_cn_ashare_emotion_report

# 因子报告（松耦合）
from core.reporters.cn.ashare_daily_reporter import (
    build_daily_report_text,
    save_daily_report,
)


# =====================================================================
# Cache 辅助
# =====================================================================

_paths = load_paths()
DATA_CACHE_DIR = _paths.get("cache_dir", "data/cache/")


def _daily_cache_exists(trade_date) -> bool:
    return Path(get_daily_cache_path(trade_date)).exists()


def _intraday_cache_exists() -> bool:
    return Path(get_intraday_cache_path()).exists()


# =====================================================================
# 主入口：run_cn_ashare_daily
# =====================================================================

def run_cn_ashare_daily(force_daily_refresh: bool = False) -> Dict[str, Any]:
    """A股日级执行入口（V11.7增强版：加入 GlobalLead + T+1/T+5 预测）。"""

    # ============================================================
    # 1) 确定交易日 & 刷新计划
    # ============================================================
 
    bj_now = now_bj()
    controller = RefreshControllerCN(bj_now)
    trade_date = controller.trade_date

    has_daily_cache = _daily_cache_exists(trade_date)
    plan: RefreshPlanCN = controller.build_refresh_plan(
        force_daily=force_daily_refresh,
        has_daily_cache=has_daily_cache,
    )


    log(f"[AShareDaily] Start run at {bj_now.isoformat()}")
    log(f"[AShareDaily] trade_date={trade_date}, force={force_daily_refresh}, has_cache={has_daily_cache}")

    # ============================================================
    # 2) 获取 snapshot
    # ============================================================
    fetcher = AshareFetcher()
    daily_snapshot = fetcher.get_daily_snapshot(
        trade_date=trade_date,
        force_refresh=plan.should_refresh_daily,
    )

    # ============================================================
    # 3) 构造 features → 因子输入（纯 snapshot 转换）
    # ============================================================
    processed = _build_processed_for_factors(daily_snapshot)

    # ============================================================
    # 4) 计算所有因子（结果为 FactorResult，松耦合）
    # ============================================================
    factors: Dict[str, FactorResult] = {}

    # 北向（NPS）
    north_factor = NorthNPSFactor()
    factors[north_factor.name] = north_factor.compute_from_daily(processed)

    # 成交额（Turnover）
    turn_factor = TurnoverFactor()
    factors[turn_factor.name] = turn_factor.compute_from_daily(processed)

    # 市场情绪（涨跌家数）
    ms_factor = MarketSentimentFactor()
    factors[ms_factor.name] = ms_factor.compute_from_daily(processed)

    # 两融（Margin）
    margin_factor = MarginFactor()
    factors[margin_factor.name] = margin_factor.compute_from_daily(processed)

    # ============================================================
    # 5) A 股情绪因子（v11 FULL）
    # ============================================================
    emotion_input = _build_emotion_input_from_snapshot(daily_snapshot)
    emotion_dict = compute_cn_emotion_from_snapshot(emotion_input)
    emotion_fr = _emotion_dict_to_factor_result(emotion_dict)
    factors[emotion_fr.name] = emotion_fr

    # ============================================================
    # 6) 新增：全球引导因子（GlobalLeadFactor）
    # ============================================================
    gl_factor = GlobalLeadFactor(daily_snapshot).compute()
    factors["global_lead"] = FactorResult(
    name="global_lead",
    score=gl_factor["score"],
    details={
        "level": gl_factor["level"],
        **gl_factor["details"],
        }
    )
    # ============================================================
    # 7) 统一评分（用于风险等级 summary，不用于报告内容排版）
    # ============================================================
    usb = UnifiedScoreBuilder()
    summary = usb.unify(factors)

    # ============================================================
    # 8) 新增：T+1 / T+5 预测（PredictionEngine）
    # ============================================================
    
    from core.predictors.prediction_engine import PredictorT1T5 

    predictor = PredictorT1T5()
    
    #prediction_block = predictor.format_report(prediction_result)
    log("[Prediction] Start prediction")
    log(f"[Prediction] Input factor keys: {list(factors.keys())}")

    #from core.predictors.prediction_engine import PredictorT1T5

    predictor = PredictorT1T5()
    pred_raw = predictor.predict(factors)  # {'T+1': {...}, 'T+5': {...}}
    
    # --- 适配 reporter 期望的 key 命名 ---
    prediction_block = {
        "t1": pred_raw.get("T+1", {}),
        "t5": pred_raw.get("T+5", {}),
    }
    
    # Debug 日志（可选，但强烈推荐）：
    log(f"[Prediction] T+1 score={prediction_block['t1'].get('score')}, direction={prediction_block['t1'].get('direction')}")
    log(f"[Prediction] T+5 score={prediction_block['t5'].get('score')}, direction={prediction_block['t5'].get('direction')}")

    # ============================================================
    # 9) 构建 meta
    # ============================================================
    
    trade_date_str = trade_date.strftime("%Y-%m-%d")     # 报告内部显示用
    file_date_str = bj_now.strftime("%Y%m%d")            # ★ 文件名用（20251205）

    meta = {
        "market": "CN",
        "trade_date": trade_date_str,
        "version": "UnifiedRisk_v11.7",
        "generated_at": bj_now.strftime("%Y-%m-%d %H:%M:%S"),
        "total_score": summary.get("total_score"),
        "risk_level": summary.get("risk_level"),
    }

    # ============================================================
    # 10) 生成 A 股风险报告（含预测模块）
    # ============================================================
    risk_report_text = build_daily_report_text(
        meta=meta,
        factors=factors,
        prediction=prediction_block,   # ★ 新增：报告加入预测
    )

    # ============================================================
    # 11) 生成情绪报告（保持原版）
    # ============================================================
    emotion_report_text = format_cn_ashare_emotion_report({
        "generated_at": bj_now,
        "trade_date": trade_date,
        "emotion": {
            **emotion_dict,
            "raw": emotion_input,
        },
    })

    
    full_report = (
        risk_report_text
        + "\n"
        + "=" * 80 + "\n"
        + "====================  A股情绪监控报告（V11 FULL）  ====================\n"
        + "=" * 80 + "\n\n"
        + emotion_report_text
    )

    # ============================================================
    # 12) 写入报告文件
    # ============================================================
    
    # 用 file_date_str 生成文件名
    final_path = save_daily_report("cn", trade_date_str, full_report)
    log(f"[AShareDaily] Report saved: {final_path}")
    

    # ============================================================
    # 13) 返回数据结构
    # ============================================================
    return {
        "meta": meta,
        "snapshot": daily_snapshot,
        "processed": processed,
        "factors": factors,
        "summary": summary,
        "emotion": emotion_dict,
        "prediction": prediction_block,
        "report_path": final_path,
        "report_text": full_report,
    }


# =====================================================================
# 辅助：snapshot → processed（供因子使用）
# =====================================================================

def _build_processed_for_factors(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """根据 snapshot 组装因子层需要的 features。

    目标：
      1）保持与 snapshot 同构：etf_proxy / turnover / breadth / margin 等原样保留
      2）附加一层 features：给后续复杂模型 / 预测使用
    """

    snapshot = snapshot or {}

    etf = snapshot.get("etf_proxy", {}) or {}
    turnover = snapshot.get("turnover", {}) or {}
    breadth = snapshot.get("breadth", {}) or {}
    margin = snapshot.get("margin", {}) or {}

    # 这里的成交额字段，以你 datasource 实际输出为准：
    # 若 MarketDataReaderCN 已经返回 {shanghai, shenzhen, total}，则可以不再转换；
    # 若仍然是 sh_turnover_e9 等，可以在这里做一次兼容映射。
    sh = float(
        turnover.get("shanghai")
        or turnover.get("sh_turnover_e9")
        or 0.0
    )
    sz = float(
        turnover.get("shenzhen")
        or turnover.get("sz_turnover_e9")
        or 0.0
    )
    total_t = float(
        turnover.get("total")
        or turnover.get("total_turnover_e9")
        or (sh + sz)
    )

    # 一个简单的流动性占比 proxy：相对于 1200 亿基准
    base_liq = 1200.0
    liq_ratio = (total_t / base_liq) if base_liq > 0 else 1.0

    features = {
        # 北向 NPS 代理
        "etf_flow_e9": float(etf.get("etf_flow_e9", 0.0) or 0.0),
        "etf_turnover_e9": float(etf.get("total_turnover_e9", 0.0) or 0.0),
        "hs300_proxy_pct": float(etf.get("hs300_proxy_pct", 0.0) or 0.0),

        # 成交额 / 流动性
        "sh_turnover_e9": sh,
        "sz_turnover_e9": sz,
        "total_turnover_e9": total_t,
        "turnover_liquidity_ratio": liq_ratio,

        # 市场宽度 / 涨跌停
        "adv": int(breadth.get("adv", 0) or 0),
        "dec": int(breadth.get("dec", 0) or 0),
        "total_stocks": int(breadth.get("total", 0) or 0),
        "limit_up": int(breadth.get("limit_up", 0) or 0),
        "limit_down": int(breadth.get("limit_down", 0) or 0),
    }

    # processed = snapshot 的增强版
    processed: Dict[str, Any] = {
        # 保留所有原始字段
        **snapshot,

        # 按因子习惯挂载的字段（可选，增强清晰度）
        "etf_proxy": etf,
        "turnover": {
            "shanghai": sh,
            "shenzhen": sz,
            "total": total_t,
        },
        "breadth": breadth,
        "margin": margin,

        # 附加 feature 向量
        "features": features,
    }

    # 可选：给 MarketSentimentFactor 直接用
    processed["hs300_pct"] = features["hs300_proxy_pct"]

    return processed


# =====================================================================
# 辅助：EmotionEngine 输入 & 适配
# =====================================================================
def _build_emotion_input_from_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """将日级 snapshot 转成 EmotionEngine 所需的 snap 字段。

    统一约定：
    - index_pct：HS300 代理涨跌（%）
    - volume_change_pct：以 500 亿 为“中性基准”的放量/缩量百分比（简化版）
    - north_net_flow：北向代理净流入（单位：亿元）
    - main_force_net_flow：主力净流（预留，单位：亿元）
    """

    etf = snapshot.get("etf_proxy", {}) or {}
    breadth = snapshot.get("breadth", {}) or {}
    turnover = snapshot.get("turnover", {}) or {}

    index_pct = float(etf.get("hs300_proxy_pct", 0.0) or 0.0)

    # === 成交量变化：用总成交额相对“500 亿”的简易放缩 ===
    total_turnover = float(turnover.get("total_turnover_e9", 0.0) or 0.0)  # 已是“亿”
    base_turnover = 500.0  # 你可以之后根据实际市场调整这个基准
    if base_turnover > 0:
        volume_change_pct = (total_turnover - base_turnover) / base_turnover * 100.0
    else:
        volume_change_pct = 0.0

    adv = int(breadth.get("adv", 0) or 0)
    total = int(breadth.get("total", 1) or 1)

    north_net_flow = float(etf.get("etf_flow_e9", 0.0) or 0.0)  # 统一为“亿”
    main_force_net_flow = 0.0  # 未来接入主力资金后，保持“亿”单位

    futures_basis_pct = 0.0
    ivx_change_pct = 0.0

    limit_up_count = int(breadth.get("limit_up", 0) or 0)
    limit_down_count = int(breadth.get("limit_down", 0) or 0)

    return {
        "index_pct": index_pct,
        "volume_change_pct": volume_change_pct,
        "breadth_adv": adv,
        "breadth_total": total if total > 0 else 1,
        "north_net_flow": north_net_flow,          # 单位：亿
        "main_force_net_flow": main_force_net_flow,  # 单位：亿（预留）
        "futures_basis_pct": futures_basis_pct,
        "ivx_change_pct": ivx_change_pct,
        "limit_up_count": limit_up_count,
        "limit_down_count": limit_down_count,
    }

def _emotion_dict_to_factor_result(emotion_dict: Dict[str, Any]) -> FactorResult:
    """
    将情绪因子 emotion_dict 转换为 FactorResult（V11.7 结构化版本）
    emotion_dict 格式示例：
        {
            "score": 52.3,
            "level": "中性偏弱",
            "signal": "情绪降温",
            "details": {...},   # 可以没有
            "raw": {...}
        }
    """

    score = float(emotion_dict.get("score", 50.0))
    level = emotion_dict.get("level", "中性")
    signal = emotion_dict.get("signal", "")
    raw = emotion_dict.get("raw", {})

    # 若 emotion_engine 没有 details，则用 raw 兜底
    details = emotion_dict.get("details") or {
        "level": level,
        **(raw or {})
    }

    # 自动生成报告块（可自定义）
    report_block = (
        f"  - emotion: {score:.2f}（{level}）\n"
        f"      · 情绪信号：{signal}\n"
    )

    # 若 raw 中含更多字段，可自动展开
    for k, v in raw.items():
        report_block += f"      · {k}: {v}\n"

    return FactorResult(
        name="emotion",
        score=score,
        details=details,
        level=level,
        signal=signal,
        raw=raw,
        report_block=report_block,
    )
 