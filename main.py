# -*- coding: utf-8 -*-
"""
UnifiedRisk v11.6.6 FULL
兼容旧版 CLI：--market cn --mode ashare_daily --force
整合新版松耦合因子框架 + 情绪报告系统
"""

import argparse
from core.utils.logger import init_run_logger, log

# 新松耦合日级引擎
from core.engines.cn.ashare_daily_engine import run_cn_ashare_daily

# 松耦合因子报告
from core.reporters.cn.ashare_daily_reporter import build_daily_report_text, save_daily_report


def main() -> None:
    parser = argparse.ArgumentParser(description="UnifiedRisk V11.6.6 FULL CN Engine")
    parser.add_argument(
        "--market",
        choices=["cn"],
        default="cn",
        help="市场：目前仅支持 A股 cn",
    )
    parser.add_argument(
        "--mode",
        choices=["ashare_daily"],
        default="ashare_daily",
        help="运行模式：ashare_daily",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="强制刷新当日日级缓存",
    )

    args = parser.parse_args()

    if args.market != "cn":
        raise ValueError("当前版本仅支持 market=cn")

    # 初始化日志
    init_run_logger(args.market, args.mode)
    log(f"Running UnifiedRisk V11.6.6 ... market={args.market}, mode={args.mode}")

    # 启动 A股日级任务（松耦合）
    result = run_cn_ashare_daily(force_daily_refresh=args.force)

    # 收集结果
    meta = result["meta"]
    factors = result["factors"]
    full_report_text = result["report_text"]      # 已经包含 因子报告 + 情绪报告
    trade_date = meta["trade_date"]

    # 落地保存报告
    report_path = save_daily_report("cn", trade_date, full_report_text)

    # 输出到控制台
    print(full_report_text)
    log(f"[Main] 报告文件: {report_path}")


if __name__ == "__main__":
    main()
