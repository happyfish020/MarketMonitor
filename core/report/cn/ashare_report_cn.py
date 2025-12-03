import os
from datetime import datetime
from typing import Dict, Any


REPORT_ROOT = "reports"
os.makedirs(REPORT_ROOT, exist_ok=True)


def build_daily_report_text(trade_date: str, summary: Dict[str, Any]) -> str:
    """
    V11 FULL 风格简洁报告模板。
    后续你想加趋势、市场快照、因子原始值，我随时可扩展。
    """

    title = f"=== A股日级风险报告（V11 FULL） ===\n"
    time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    header = (
        f"生成时间：{time_str}\n"
        f"交易日：{trade_date}\n"
        f"综合得分：{summary.get('total_score', 0):.2f}\n"
        f"风险等级：{summary.get('risk_level', 'N/A')}\n"
        f"\n因子得分：\n"
    )

    factor_lines = []
    factor_scores = summary.get("factor_scores", {})
    for name, score in factor_scores.items():
        factor_lines.append(f"  - {name}: {score:.2f}")

    return title + header + "\n".join(factor_lines) + "\n"


def save_daily_report(market: str, trade_date: str, text: str) -> str:
    """
    将报告写入 root/reports 目录。
    """
    filename = f"{market}_ashare_daily_{trade_date}.txt"
    path = os.path.join(REPORT_ROOT, filename)

    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

    return path
