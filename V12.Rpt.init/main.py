# main.py - UnifiedRisk V12 启动入口

import argparse

from core.utils.logger import setup_logging, get_logger
from core.engines.cn.ashare_daily_engine import run_cn_ashare_daily


def parse_args():
    parser = argparse.ArgumentParser(description="UnifiedRisk V12 Runner")

    parser.add_argument("--market", type=str, default="cn")
    parser.add_argument("--mode", type=str, default="ashare_daily")
    parser.add_argument("--full-refresh", action="store_true")
    parser.add_argument("--ss-refresh", action="store_true")

    return parser.parse_args()


def get_refresh_mode(args):
    if args.full_refresh:
        return "full"
    if args.ss_refresh:
        return "snapshot"
    return "readonly"


def main():
    args = parse_args()
    refresh_mode = get_refresh_mode(args)

    # 初始化日志（入口层唯一职责之一）
    setup_logging(market=args.market, mode=args.mode)
    LOG = get_logger("Main")

    LOG.info(
        "启动 UnifiedRisk V12 | market=%s mode=%s refresh=%s",
        args.market,
        args.mode,
        refresh_mode,
    )

    if args.market == "cn" and args.mode == "ashare_daily":
        # ✅ V12：只调用，不接收，不解析
        run_cn_ashare_daily(refresh_mode=refresh_mode)
    else:
        LOG.error("不支持的参数: market=%s mode=%s", args.market, args.mode)


if __name__ == "__main__":
    main()
