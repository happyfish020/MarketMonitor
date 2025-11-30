#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""UnifiedRisk v9 master - 统一版入口

当前支持模式：
  --mode=ashare_daily   生成 A股日级风险快照（含 情绪 / 短期 / 中期 / 北向(NPS) / 综合评分）
"""

import argparse
from pprint import pprint

from unified_risk.core.engines.ashare_daily_engine import run_ashare_daily
from unified_risk.common.logging_utils import log_info
from unified_risk.core.report.report_writer import write_ashare_daily_report
from unified_risk.common.time_utils import now_bj, get_ashare_trade_date
from unified_risk.core.ashare.factor_history import append_factor_history

def run_ashare_daily_mode():
    log_info("[UnifiedRisk] Running ashare_daily (unified_risk.core)…")
    bj_now = now_bj()
    result = run_ashare_daily()
    append_factor_history(result, bj_now)  # 🔥 加这行
   
    print("\n=== A股日级风险快照 (UnifiedRisk v9 master) ===")
    # 写入报告文件
    write_ashare_daily_report(result, bj_now)

    print(result["summary"])
    print(result["summary"])

    print("\n--- 关键数值 ---")
    uni = result["unified"]
    print(f"综合风险得分: {uni.total:.1f} / 100  ({uni.level})")
    print("组件得分:")
    for k, v in uni.components.items():
        print(f"  - {k}: {v:.2f}")

    print("\n--- 北向因子 (NPS) ---")
    north = result.get("north")
    if north:
        print(north.description)
        print("raw:")
        pprint(north.raw)

    return result


def main():
    parser = argparse.ArgumentParser(description="UnifiedRisk v9 master")
    parser.add_argument(
        "--mode",
        type=str,
        required=True,
        choices=["ashare_daily"],
        help="目前只支持 ashare_daily 模式",
    )
    args = parser.parse_args()

    if args.mode == "ashare_daily":
        run_ashare_daily_mode()


if __name__ == "__main__":
    main()
