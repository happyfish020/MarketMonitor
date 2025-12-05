# -*- coding: utf-8 -*-
"""A股日级风险报告生成器（V11.7，支持 T+1 / T+5 预测模块）。

设计原则：
- 完全兼容 V11.6.6 的因子松耦合输出机制
- 不改变 FactorResult 的接口
- 在报告末尾加入 prediction_block（T+1 / T+5）
"""

import os
from datetime import datetime
from typing import Dict, Any
from core.models.factor_result import FactorResult
from core.utils.config_loader import reports_path

# 报告输出根目录
REPORT_ROOT = reports_path()


# ============================================================
# 辅助函数：格式化预测模块的文本
# ============================================================
def _build_prediction_block(pred):
    """
    兼容 V11.8 新版预测结构（score + direction + explain）
    同时兼容旧版结构（score + direction + details）
    """

    if not pred:
        return "\n【预测】暂无数据\n"

    t1 = pred.get("t1", {}) or {}
    t5 = pred.get("t5", {}) or {}

    def build_one(label: str, obj: Dict[str, Any]):
        """
        自动兼容三种结构：
        1) 旧版：包含 details
        2) 新版：包含 explain
        3) 极简版：只有 score + direction
        """
        lines = []
        score = obj.get("score", "-")
        direction = obj.get("direction", "-")

        lines.append(f"【{label}】方向：{direction}  |  综合分：{score}")

        # --- 情况1：旧版结构，有 details ---
        details = obj.get("details")
        if isinstance(details, dict):
            lines.append("  - 多空结构分析：")
            for fname, detail in details.items():
                fs = detail.get("factor_score")
                lines.append(f"        · {fname}: {fs}")

            lines.append("  - 贡献分布：")
            for fname, detail in details.items():
                fs = detail.get("factor_score")
                w = detail.get("weight")
                c = detail.get("contribution")
                lines.append(f"        · {fname}: {fs} × {w} = {c}")

        # --- 情况2：新版结构，有 explain ---
        explain = obj.get("explain")
        if explain:
            lines.append("  - 综合解读：")
            for part in explain.split("\n"):
                lines.append(f"        {part}")

        # --- 情况3：极简结构 ---
        if not details and not explain:
            lines.append("  - 综合解读：")
            lines.append("        综合因子评估后，保持震荡结构。")

        return "\n".join(lines)

    # 组合 T+1 / T+5 文本
    return (
        "\n\n【预测模块】\n"
        + build_one("T+1", t1)
        + "\n\n"
        + build_one("T+5", t5)
        + "\n"
    )

 
# ============================================================
# 主函数：构建完整报告
# ============================================================
def build_daily_report_text(
    meta: Dict[str, Any],
    factors: Dict[str, FactorResult],
    prediction: Dict[str, Any] = None,
) -> str:
    """构建 A 股日级风险报告全文（含预测模块）"""

    market = meta.get("market", "CN")
    trade_date = meta.get("trade_date", "")
    version = meta.get("version", "UnifiedRisk_v11.8")
    ts = meta.get("generated_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ------------------ 标题 ------------------
    title = f"UnifiedRisk A股日级风险报告 ({market})\n"
    header = (
        f"交易日：{trade_date}    生成时间：{ts}    版本：{version}\n"
        + "-" * 72 + "\n"
        + "因子得分：\n"
    )

    # ------------------ 因子输出 ------------------
    preferred_order = ["north_nps", "turnover", "market_sentiment", "margin"]
    lines = []

    used_keys = set()
    for key in preferred_order:
        res = factors.get(key)
        if not res:
            continue

        # 优先 factor_obj.report_block(fr)
        block = None
        if hasattr(res, "factor_obj") and res.factor_obj and hasattr(res.factor_obj, "report_block"):
            block = res.factor_obj.report_block(res)

        # 其次尝试 res.report_block
        if not block:
            block = getattr(res, "report_block", None)

        # fallback
        if not block:
            score = getattr(res, "score", 50.0)
            level = getattr(res, "level", "中性")
            block = f"  - {key}: {score:.2f}（{level}）"

        lines.append(block.rstrip() + "\n")
        used_keys.add(key)

    # ------------------ 其它因子按字母序附加 ------------------
    for name in sorted(k for k in factors.keys() if k not in used_keys):
        res = factors[name]

        block = None
        if hasattr(res, "factor_obj") and res.factor_obj and hasattr(res.factor_obj, "report_block"):
            block = res.factor_obj.report_block(res)

        if not block:
            block = getattr(res, "report_block", None)

        if not block:
            score = getattr(res, "score", 50.0)
            level = getattr(res, "level", "中性")
            block = f"  - {name}: {score:.2f}（{level}）"

        lines.append(block.rstrip() + "\n")

    # ------------------ 新增：预测模块 ------------------
    prediction_text = _build_prediction_block(prediction)

    return title + header + "\n".join(lines) + prediction_text + "\n"


# ============================================================
# 保存报告文件
# ============================================================

def save_daily_report(market: str, date_str: str, text: str) -> str:
    """
    报告写入: reports/{market}/ashare_daily_{date_str}.txt
    例如: reports/cn/ashare_daily_20251205.txt
    """
 

    root = reports_path()  # 或用 load_paths()["reports_dir"]
    folder = os.path.join(root, market)
    os.makedirs(folder, exist_ok=True)

    filename = f"ashare_daily_{date_str}.txt"
    path = os.path.join(folder, filename)

    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

    return path