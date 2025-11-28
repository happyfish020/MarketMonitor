# global_risk/report_writer.py
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any

from .config import REPORT_DIR
from .utils.logging_utils import setup_logger
from .utils.time_utils import now_bj

# === 新增模块 ===
from .cache.auction_cache import read_auction_cache, has_auction_cache
from .scoring.auction_scorer import score_auction_sentiment
from .scoring.commodity_scoring import build_commodity_view
from .scoring.gold_six_factor import build_gold_six_view


logger = setup_logger("GlobalMultiRisk.writer")


# -----------------------------------------------------------------------------
#   通用：文件写入辅助
# -----------------------------------------------------------------------------
def _write_report(path: Path, lines: list[str]) -> Path:
    try:
        with path.open("w", encoding="utf-8") as f:
            f.writelines(lines)
        logger.info("Report written: %s", path)
        return path
    except Exception as e:
        logger.error("Failed to write report %s: %s", path, e)
        raise


# -----------------------------------------------------------------------------
#   PREOPEN（盘前）
# -----------------------------------------------------------------------------
def write_preopen_report(payload: Dict[str, Any], bj_time: datetime) -> Path:
    """
    A-RiskReport_YYYYMMDD_pre.txt
    """
    date_str = bj_time.strftime("%Y%m%d")
    path = REPORT_DIR / f"A-RiskReport_{date_str}_pre.txt"

    raw_macro = payload.get("raw", {}).get("macro", {})
    raw_ash = payload.get("raw", {}).get("ashares", {})

    # 大宗商品
    com_view = build_commodity_view(raw_macro)

    # 昨日竞价
    yesterday = (bj_time - timedelta(days=1)).date()
    if has_auction_cache(yesterday):
        y_raw = read_auction_cache(yesterday)
        y_view = score_auction_sentiment(y_raw)
        y_text = [
            "【昨日竞价回顾】\n",
            f"- 等级：{y_view.level}\n",
            f"- 说明：{y_view.desc}\n\n",
        ]
    else:
        y_text = [
            "【昨日竞价回顾】\n",
            "- 昨日无竞价缓存（可能昨日程序未运行 / 非竞价时间段）\n\n",
        ]

    lines = [
        "================== 盘前多因子预警 ==================\n",
        f"日期：{bj_time.strftime('%Y-%m-%d %H:%M:%S')}\n\n",

        "【宏观环境】\n",
        f"- 美债 10Y：{raw_macro.get('treasury_10y')}%\n",
        f"- 美债 5Y ：{raw_macro.get('treasury_5y')}%\n",
        f"- 收益率曲线：{raw_macro.get('ycurve_bps')} bps\n",
        f"- 纳指：{raw_macro.get('nasdaq_pct')}%\n",
        f"- SPY ：{raw_macro.get('spy_pct')}%\n",
        f"- VIX ：{raw_macro.get('vix_last')}\n",
        f"- A50 夜盘：{raw_macro.get('a50_night_pct')}%\n\n",

        "【大宗商品信号】\n",
        f"- 黄金：{com_view.gold_view}；{com_view.gold_comment}\n",
        f"- 原油：{com_view.oil_view}；{com_view.oil_comment}\n",
        f"- 期铜：{com_view.copper_view}；{com_view.copper_comment}\n",
        f"- 美元指数：{com_view.dxy_comment}\n",
        f"→ 综合判断：{com_view.overall_comment}\n\n",
    ]

    # 加入昨日竞价回顾
    lines.extend(y_text)

    # A 股情绪
    lines.extend([
        "【A 股市场情绪（前日）】\n",
        f"- 上证涨跌：{raw_ash.get('sh_change_pct')}\n",
        f"- 创业板：{raw_ash.get('cyb_change_pct')}\n",
        f"- 涨家数：{raw_ash.get('adv')}  跌家数：{raw_ash.get('dec')}\n",
        f"- 流动性信号：{raw_ash.get('liquidity', {}).get('signal_desc', '')}\n\n"
    ])

    lines.append("======================================================\n")

    return _write_report(path, lines)


# -----------------------------------------------------------------------------
#   AUCTION（竞价）
# -----------------------------------------------------------------------------
def write_auction_report(payload: Dict[str, Any], bj_time: datetime) -> Path:
    """
    A-RiskReport_YYYYMMDD_auction.txt
    """
    date_str = bj_time.strftime("%Y%m%d")
    path = REPORT_DIR / f"A-RiskReport_{date_str}_auction.txt"

    # 注意：缓存已经在 run_auction() 里写入，我们这里只写报告文本
    raw_macro = payload.get("raw", {}).get("macro", {})
    raw_ash = payload.get("raw", {}).get("ashares", {})

    # 当前竞价评分
    a_view = score_auction_sentiment(payload.get("raw", {}))

    lines = [
        "================== 竞价阶段报告 ==================\n",
        f"时间：{bj_time.strftime('%Y-%m-%d %H:%M:%S')}\n\n",

        "【竞价情绪】\n",
        f"- 等级：{a_view.level}\n",
        f"- 说明：{a_view.desc}\n\n",

        "【主要指数竞价情况】\n",
        f"- 上证指数竞价涨跌：{raw_ash.get('sh_change_pct')}\n",
        f"- 创业板竞价涨跌：{raw_ash.get('cyb_change_pct')}\n\n",

        "【市场广度（竞价）】\n",
        f"- 涨家数：{raw_ash.get('adv')} / 跌家数：{raw_ash.get('dec')}\n\n",
    ]

    lines.append("======================================================\n")

    return _write_report(path, lines)


# -----------------------------------------------------------------------------
#   NOON（午盘跳水预警）
# -----------------------------------------------------------------------------
def write_noon_report(payload: Dict[str, Any], bj_time: datetime) -> Path:
    """
    A-RiskReport_YYYYMMDD_noon.txt
    """
    date_str = bj_time.strftime("%Y%m%d")
    path = REPORT_DIR / f"A-RiskReport_{date_str}_noon.txt"

    raw_macro = payload.get("raw", {}).get("macro", {})
    raw_ash = payload.get("raw", {}).get("ashares", {})

    # 今日竞价回看
    if has_auction_cache(bj_time):
        today_raw = read_auction_cache(bj_time)
        a_view = score_auction_sentiment(today_raw)
        auction_text = [
            "【今日竞价回顾】\n",
            f"- 竞价等级：{a_view.level}\n",
            f"- 说明：{a_view.desc}\n\n",
        ]
    else:
        auction_text = [
            "【今日竞价回顾】\n",
            "- 今日无竞价缓存\n\n",
        ]

    lines = [
        "================== 午盘跳水预警 ==================\n",
        f"时间：{bj_time.strftime('%Y-%m-%d %H:%M:%S')}\n\n",

        "【上午行情】\n",
        f"- 上证：{raw_ash.get('sh_change_pct')}\n",
        f"- 创业板：{raw_ash.get('cyb_change_pct')}\n\n",
    ]

    lines.extend(auction_text)
    lines.append("======================================================\n")

    return _write_report(path, lines)


# -----------------------------------------------------------------------------
#   AFTER (盘后复盘)
# -----------------------------------------------------------------------------
def write_afterclose_report(payload: Dict[str, Any], bj_time: datetime) -> Path:
    """
    A-RiskReport_YYYYMMDD_after.txt
    """
    date_str = bj_time.strftime("%Y%m%d")
    path = REPORT_DIR / f"A-RiskReport_{date_str}_after.txt"

    raw_macro = payload.get("raw", {}).get("macro", {})
    raw_ash = payload.get("raw", {}).get("ashares", {})

    # 大宗商品
    com_view = build_commodity_view(raw_macro)

    # 今日竞价 → 全天复盘
    if has_auction_cache(bj_time):
        a_raw = read_auction_cache(bj_time)
        a_view = score_auction_sentiment(a_raw)
        a_text = [
            "【竞价 → 全天走势复盘】\n",
            f"- 竞价等级：{a_view.level}\n",
            f"- 说明：{a_view.desc}\n",
            "- 作用：竞价偏强 → 冲高概率增；竞价偏弱 → 午盘跳水概率上升\n\n",
        ]
    else:
        a_text = [
            "【竞价 → 全天走势复盘】\n",
            "- 无竞价数据（未在 09:15–09:25 运行）\n\n",
        ]

    lines = [
        "================== 盘后复盘 ==================\n",
        f"时间：{bj_time.strftime('%Y-%m-%d %H:%M:%S')}\n\n",

        "【指数情况】\n",
        f"- 上证：{raw_ash.get('sh_change_pct')}\n",
        f"- 创业板：{raw_ash.get('cyb_change_pct')}\n\n",

        "【大宗商品】\n",
        f"- 黄金：{com_view.gold_view}；{com_view.gold_comment}\n",
        f"- 原油：{com_view.oil_view}；{com_view.oil_comment}\n",
        f"- 期铜：{com_view.copper_view}；{com_view.copper_comment}\n",
        f"- 美元指数：{com_view.dxy_comment}\n",
        f"→ 综合：{com_view.overall_comment}\n\n",
    ]

    lines.extend(a_text)

    lines.append("======================================================\n")

    return _write_report(path, lines)
