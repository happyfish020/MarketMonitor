#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GlobalMultiRisk v5.6.full - unified entry
支持模式：
  --mode=ashare_daily
  --mode=us_daily
  --mode=preopen
  --mode=auction
  --mode=morning
  --mode=noon
  --mode=after
  --mode=auto        (自动识别时间段)
  --mode=all         (全部模块)
"""

import argparse
import logging
from datetime import datetime
from global_risk.cache.auction_cache import has_auction_cache, write_auction_cache
#from global_risk.cache.auction_cache import write_auction_cache
from global_risk.utils.time_utils import now_bj

# ============================================================
# ① 关键修复：清空旧 handler（否则不会写入日志文件）
# ============================================================
logging.getLogger("GlobalMultiRisk").handlers.clear()
logging.getLogger("GlobalMultiRisk.main").handlers.clear()
logging.getLogger().handlers.clear()

# ============================================================
# ② 必须在 logger 初始化前确保 logs 目录存在
# ============================================================
from global_risk.config import (
    LOG_DIR, REPORT_DIR, VERSION
)
LOG_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# ③ 初始化日志系统
# ============================================================
from global_risk.utils.logging_utils import setup_logger
logger = setup_logger("GlobalMultiRisk.main")

# ============================================================
# ④ 导入主引擎 / 模式调度
# ============================================================
from global_risk import GlobalRiskEngine, get_daily_global_risk
from global_risk.modes import (
    run_us_daily_mode,
    run_preopen_mode,
    run_tomorrow_mode,
)

# 报告模块（你刚才上传的）
from global_risk.report_writer import (
    write_preopen_report,
    write_auction_report,
    write_morning_report,
    write_noon_report,
    write_afterclose_report
)

# 时间工具
from global_risk.utils.time_utils import now_bj


# ============================================================
# 打印头部信息
# ============================================================

def _print_header(mode: str):
    bj = now_bj().strftime("%Y-%m-%d %H:%M:%S")
    today = now_bj().strftime("%Y-%m-%d")

    logger.info("=== GlobalMultiRisk %s 启动 ===", VERSION)
    logger.info("Beijing time : %s", bj)
    logger.info("Run mode     : %s", mode)
    logger.info("Daily log    : %s", LOG_DIR / f"global_risk_{today}.log")
    logger.info("Latest log   : %s", LOG_DIR / "global_risk_latest.log")
    logger.info("==========================================")


# ============================================================
# 模式入口
# ============================================================

def run_ashare_daily():
    _print_header("ashare_daily")
    logger.info("Running A-share Daily (含宏观 + T-5)…")

    payload = get_daily_global_risk(as_dict=True)

    logger.info("A-share Daily finished: score=%.1f, level=%s",
                payload["ashare_daily"]["total_score"],
                payload["ashare_daily"]["level"])

    # 自动生成盘后报告（无 tomorrow_view）
    write_afterclose_report(payload, now_bj(), tomorrow_view=None)

    return payload


def run_us_daily():
    _print_header("us_daily")
    return run_us_daily_mode(logger)


def run_preopen():
    _print_header("preopen")

    payload = get_daily_global_risk(as_dict=True)
    write_preopen_report(payload, now_bj())

    return payload


def run_auction():
    _print_header("auction")

    # 1) 计算日度风险（含 macro + ashare）
    payload = get_daily_global_risk(as_dict=True)

    # 2) 写入竞价缓存（仅 09:15–09:25 会生效）
    bj_time = now_bj()
    raw_snapshot = payload.get("raw", {})

    try:
        if not has_auction_cache(bj_time):
            write_auction_cache(payload["raw"], bj_time)
       #write_auction_cache(raw_snapshot=raw_snapshot, bj_time=bj_time)
    except Exception as e:
        print("[WARN] write_auction_cache failed:", e)

    # 3) 输出竞价报告
    write_auction_report(payload, bj_time)

    return payload

def run_morning():
    _print_header("morning")

    payload = get_daily_global_risk(as_dict=True)
    write_morning_report(payload, now_bj())

    return payload


def run_noon():
    _print_header("noon")

    payload = get_daily_global_risk(as_dict=True)
    write_noon_report(payload, now_bj())

    return payload


def run_after():
    _print_header("after")

    payload = get_daily_global_risk(as_dict=True)
    tomorrow = run_tomorrow_mode(logger)

    write_afterclose_report(payload, now_bj(), tomorrow_view=tomorrow)

    return payload


def run_all():
    _print_header("all")
    logger.info("== [1] A-share Daily ==")
    ash = run_ashare_daily()

    logger.info("== [2] US Daily ==")
    us = run_us_daily_mode(logger)

    logger.info("== [3] Preopen ==")
    pre = run_preopen()

    logger.info("== [4] Auction ==")
    auc = run_auction()

    logger.info("== [5] Morning ==")
    mor = run_morning()

    logger.info("== [6] Noon ==")
    noo = run_noon()

    logger.info("== [7] Afterclose ==")
    aft = run_after()

    logger.info("All modules executed.")
    return {
        "ashare": ash,
        "us": us,
        "pre": pre,
        "auction": auc,
        "morning": mor,
        "noon": noo,
        "after": aft
    }


# ============================================================
# ⑤ 自动识别模式（MorningView 风格）
# ============================================================

def run_auto():
    bj = now_bj()
    h, m = bj.hour, bj.minute

    # 盘前
    if h < 9 or (h == 9 and m <= 15):
        return run_preopen()

    # 竞价
    if h == 9 and 15 < m < 25:
        return run_auction()

    # 早盘
    if (h == 9 and m >= 25) or (h == 10 and m <= 30):
        return run_morning()

    # 午盘
    if h == 11 or (h == 12 and m <= 30):
        return run_noon()

    # 盘后（默认 15:00–18:00）
    if h >= 15 and h <= 18:
        return run_after()

    # 夜间 → 明日预测
    return run_tomorrow_mode(logger)


# ============================================================
# main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="GlobalMultiRisk v5.6.full")
    parser.add_argument(
        "--mode",
        required=True,
        type=str,
        choices=[
            "ashare_daily", "us_daily",
            "preopen", "auction", "morning", "noon", "after",
            "global_macro", "tomorrow", "auto", "all"
        ],
    )
    args = parser.parse_args()

    if args.mode == "ashare_daily": run_ashare_daily()
    elif args.mode == "us_daily": run_us_daily()
    elif args.mode == "preopen": run_preopen()
    elif args.mode == "auction": run_auction()
    elif args.mode == "morning": run_morning()
    elif args.mode == "noon": run_noon()
    elif args.mode == "after": run_after()
    elif args.mode == "global_macro": run_global_macro()
    elif args.mode == "tomorrow": run_tomorrow_mode(logger)
    elif args.mode == "auto": run_auto()
    elif args.mode == "all": run_all()


def run_global_macro():
    _print_header("global_macro")
    eng = GlobalRiskEngine()
    res = eng.run_daily()
    logger.info("Macro: score=%.1f, level=%s", res.macro_score, res.macro_level)
    return res


if __name__ == "__main__":
    main()
