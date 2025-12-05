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
from core.report.cn.ashare_report_cn import (
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
    """A股日级执行入口（V11.6.6 松耦合版）。"""

    # 1) 确定交易日 & 刷新计划
    bj_now = now_bj()
    controller = RefreshControllerCN(bj_now)
    trade_date = controller.trade_date

    has_daily_cache = _daily_cache_exists(trade_date)
    plan: RefreshPlanCN = controller.build_refresh_plan(
        force_daily=force_daily_refresh,
        has_daily_cache=has_daily_cache,
    )

    log(f"[AShareDaily] Start run at {bj_now.isoformat()}")
    log(f"[AShareDaily] trade_date = {trade_date}, force={force_daily_refresh}, has_cache={has_daily_cache}")

    # 2) 获取 snapshot
    fetcher = AshareFetcher()
    daily_snapshot = fetcher.get_daily_snapshot(
        trade_date=trade_date,
        force_refresh=plan.should_refresh_daily,
    )

    # 3) 构造因子 features（完全基于 snapshot）
    processed = _build_processed_for_factors(daily_snapshot)

    # 4) 计算各因子（全部返回 FactorResult，内含 report_block）
    factors: Dict[str, FactorResult] = {}

    north_factor = NorthNPSFactor()
    factors[north_factor.name] = north_factor.compute_from_daily(processed)

    turn_factor = TurnoverFactor()
    factors[turn_factor.name] = turn_factor.compute_from_daily(processed)

    ms_factor = MarketSentimentFactor()
    factors[ms_factor.name] = ms_factor.compute_from_daily(processed)

    margin_factor = MarginFactor()
    factors[margin_factor.name] = margin_factor.compute_from_daily(processed)

    # 5) 情绪因子：调用 EmotionEngine → 适配为 FactorResult
    emotion_input = _build_emotion_input_from_snapshot(daily_snapshot)
    emotion_dict = compute_cn_emotion_from_snapshot(emotion_input)

    emotion_fr = _emotion_dict_to_factor_result(emotion_dict)
    factors[emotion_fr.name] = emotion_fr

    # 6) 统一评分（仅用于返回结构 / 上层决策，不参与报告排版）
    usb = UnifiedScoreBuilder()
    summary = usb.unify(factors)

    trade_date_str = trade_date.isoformat()

    meta = {
        "market": "CN",
        "trade_date": trade_date_str,
        "version": "UnifiedRisk_v11.6.6",
        "generated_at": bj_now.strftime("%Y-%m-%d %H:%M:%S"),
        "total_score": summary.get("total_score"),
        "risk_level": summary.get("risk_level"),
    }

    # 7) 构建“因子报告”（松耦合）
    risk_report_text = build_daily_report_text(meta, factors)

    # 8) 构建“情绪报告”
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

    # 9) 写入报告文件
    final_path = save_daily_report("cn", trade_date_str, full_report)
    log(f"[AShareDaily] Report saved: {final_path}")

    return {
        "meta": meta,
        "snapshot": daily_snapshot,
        "processed": processed,
        "factors": factors,
        "summary": summary,
        "emotion": emotion_dict,
        "report_path": final_path,
        "report_text": full_report,
    }


# =====================================================================
# 辅助：snapshot → processed（供因子使用）
# =====================================================================

def _build_processed_for_factors(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """根据 snapshot 组装因子层需要的 features。

    目前支持：
    - north_nps: 依赖 etf_proxy.etf_flow_e9 / total_turnover_e9 / hs300_proxy_pct
    - turnover: 依赖 turnover.{sh_turnover_e9, sz_turnover_e9, total_turnover_e9}
    - market_sentiment: 依赖 breadth.{adv, dec, total, limit_up, limit_down} + hs300_proxy_pct
    - margin: 直接在因子内部调用 EastmoneyMarginClientCN（与 processed 无强耦合）
    """
    etf = snapshot.get("etf_proxy", {}) or {}
    turnover = snapshot.get("turnover", {}) or {}
    breadth = snapshot.get("breadth", {}) or {}

    sh = float(turnover.get("sh_turnover_e9", 0.0) or 0.0)
    sz = float(turnover.get("sz_turnover_e9", 0.0) or 0.0)
    total_t = float(turnover.get("total_turnover_e9", sh + sz) or (sh + sz))

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

    return {
        "raw": snapshot,
        "features": features,
    }


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


def _emotion_dict_to_factor_result(emo: Dict[str, Any]) -> FactorResult:
    """将 EmotionEngine 的 dict 结果适配成标准 FactorResult。"""

    score = float(emo.get("EmotionScore", 50.0) or 50.0)
    level = str(emo.get("EmotionLevel", "Neutral") or "Neutral")

    idx_lbl = emo.get("IndexLabel", "")
    vol_lbl = emo.get("VolumeLabel", "")
    brd_lbl = emo.get("BreadthLabel", "")
    nf_lbl = emo.get("NorthLabel", "")
    mf_lbl = emo.get("MainForceLabel", "")
    der_lbl = emo.get("DerivativeLabel", "")
    lim_lbl = emo.get("LimitLabel", "")

    signal = f"情绪：{level}（score={score:.1f}）"

    report_block = f"""  - emotion: {score:.2f}（{level}）
        · 指数：{idx_lbl}
        · 成交量：{vol_lbl}
        · 市场宽度：{brd_lbl}
        · 北向资金：{nf_lbl}
        · 主力资金：{mf_lbl}
        · 衍生品：{der_lbl}
        · 涨跌停：{lim_lbl}
"""

    return FactorResult(
        name="emotion",
        score=score,
        signal=signal,
        raw=emo,
        report_block=report_block,
    )
