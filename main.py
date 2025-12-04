import argparse

from core.utils.logger import init_run_logger, log

# 你的工程真实存在的唯一 A股日级引擎
from core.engines.cn.ashare_daily_engine import run_cn_ashare_daily

# 报告接口（新版三参数）
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
        help="市场（当前仅支持 A股：cn）",
    )
    parser.add_argument(
        "--mode",
        choices=["ashare_daily"],
        default="ashare_daily",
        help="运行模式（当前仅支持 ashare_daily）",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="强制刷新日级缓存",
    )
    args = parser.parse_args()

    # 初始化日志
    init_run_logger(args.market, args.mode)
    log(f"Running UnifiedRisk V11 FULL ... market={args.market}, mode={args.mode}")

    # =======================================================
    #  A 股日级模式（当前版本唯一支持）
    # =======================================================
    result = run_cn_ashare_daily(force_daily_refresh=args.force)

    # 结果结构：
    # result["unified"] : summary
    # result["factors"] : 所有因子 FactorResult
    trade_date_str = result["unified"]["trade_date"]

    # --- 生成报告文本（新版三参数） ---
    report_text = build_daily_report_text(
        trade_date=trade_date_str,
        summary=result["unified"],
        factors=result["factors"],
    )

    # --- 写报告 ---
    report_path = save_daily_report("cn", trade_date_str, report_text)

    print(report_text)
    log(f"[Main] 报告文件: {report_path}")


if __name__ == "__main__":
    main()
