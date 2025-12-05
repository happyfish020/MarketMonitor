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
    if not pred:
        return "【预测】暂无数据\n"

    t1 = pred.get("t1", {})
    t5 = pred.get("t5", {})

    def _explain_factor(name, score):
        if score is None:
            return "-"
        if score >= 75: return f"{score}（强势多头）"
        if score >= 65: return f"{score}（偏多）"
        if score >= 55: return f"{score}（略偏多）"
        if score >= 45: return f"{score}（中性）"
        if score >= 35: return f"{score}（偏空）"
        return f"{score}（强势空头）"

    def build_one(tag, obj):
        out = []
        out.append(f"【{tag}】方向：{obj.get('direction','-')}  |  综合分：{obj.get('score','-')}")
        out.append("  - 多空结构分析：")
        for fname, detail in obj["details"].items():
            fs = detail["factor_score"]
            out.append(f"        · {fname}：{_explain_factor(fname, fs)}")
        out.append("  - 贡献分布：")
        for fname, detail in obj["details"].items():
            fs = detail["factor_score"]
            if fs is None:
                continue
            out.append(
                f"        · {fname}: {fs:.2f} × {detail['weight']:.2f} = {detail['contribution']:.2f}"
            )
        # 综合解读
        out.append("  - 综合解读：")
        if obj["direction"].startswith("震荡偏多"):
            out.append("        市场多空力量对冲，但在短期情绪带动下略有偏多。")
        elif obj["direction"].startswith("震荡偏空"):
            out.append("        杠杆与成交结构偏弱，中期压力形成偏空结构。")
        elif obj["direction"].startswith("偏多"):
            out.append("        多因子共振向上，短期趋势偏多。")
        elif obj["direction"].startswith("偏空"):
            out.append("        多项因子显示空头占优，风险需要关注。")
        else:
            out.append("        多空力量平衡，预计维持震荡结构。")
        return "\n".join(out)

    return (
        build_one("T+1", t1)
        + "\n\n"
        + build_one("T+5", t5)
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
    version = meta.get("version", "UnifiedRisk_v11.7")
    ts = meta.get("generated_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ------------------ 标题 ------------------
    title = f"UnifiedRisk A股日级风险报告 ({market})\n"
    header = (
        f"交易日：{trade_date}    生成时间：{ts}    版本：{version}\n"
        f"{'-'*72}\n"
        f"因子得分：\n"
    )

    # ------------------ 原因子输出逻辑 不变 ------------------
    preferred_order = ["north_nps", "turnover", "market_sentiment", "margin"]
    lines = []

    used_keys = set()
    for key in preferred_order:
        res = factors.get(key)
        if not res:
            continue
        lines.append(res.ensure_report_block().rstrip() + "\n")
        used_keys.add(key)

    # 其它因子按字母序附加
    for name in sorted(k for k in factors.keys() if k not in used_keys):
        res = factors[name]
        lines.append(res.ensure_report_block().rstrip() + "\n")

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