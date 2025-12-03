import argparse

from core.utils.logger import init_run_logger, log

from core.engines.cn.ashare_daily_engine import (
    run_cn_ashare_daily,
    run_cn_ashare_intraday,
)

# === 新 V11 FULL 报告接口 ===
from core.report.cn.ashare_report_cn import (
    build_daily_report_text,
    save_daily_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="UnifiedRisk V11 FULL CN Engine")
    parser.add_argument(
        "--market",
        choices=["cn"],
        default="cn",
        help="市场: 目前仅支持 cn(A股)",
    )
    parser.add_argument(
        "--mode",
        choices=["ashare_daily", "ashare_intraday"],
        default="ashare_daily",
        help="运行模式：A股日级 / A股盘中",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="强制刷新对应模式的数据缓存",
    )

    args = parser.parse_args()

    if args.market != "cn":
        raise ValueError("当前版本仅支持 market=cn")

    # === 日志初始化 ===
    init_run_logger(args.market, args.mode)
    log(f"Running UnifiedRisk V11 FULL ... market={args.market}, mode={args.mode}")

    # =========================================================
    #  A 股日级模式
    # =========================================================
    if args.mode == "ashare_daily":
        result = run_cn_ashare_daily(force_daily_refresh=args.force)

        # 新格式化 + 保存到 /reports
        trade_date_str = result["meta"]["trade_date"]
        report_text = build_daily_report_text(
            trade_date=trade_date_str,
            summary=result["summary"],
        )
        report_path = save_daily_report("cn", trade_date_str, report_text)

    # =========================================================
    #  A 股盘中模式
    # =========================================================
    elif args.mode == "ashare_intraday":
        result = run_cn_ashare_intraday(force_intraday_refresh=args.force)

        # 盘中也走统一报告接口（你未来可以扩展模板）
        trade_date_str = result["meta"]["trade_date"]
        report_text = build_daily_report_text(
            trade_date=trade_date_str,
            summary=result["unified"],
        )
        report_path = save_daily_report("cn_intraday", trade_date_str, report_text)

    else:
        raise ValueError("未知模式: {}".format(args.mode))

    # 输出结果到 console
    print(report_text)
    log(f"[Main] 报告文件: {report_path}")


if __name__ == "__main__":
    main()
