from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, List, Optional


@dataclass
class UnifiedScore:
    """统一风险评分结果.

    total: 0-100
    level: 文本级别描述
    components: 各子因子原始得分（0-20 为主）
    explanation: 汇总说明
    """
    total: float
    level: str
    components: Dict[str, float]
    explanation: str


def _risk_level(score: float) -> str:
    """根据总分给出风险等级描述."""
    if score >= 80:
        return "极低风险 / 偏多机会"
    if score >= 60:
        return "低风险 / 温和偏多"
    if score >= 40:
        return "中性震荡"
    if score >= 20:
        return "偏高风险 / 谨慎"
    return "高风险 / 控制仓位"


def _block(scores: List[Optional[float]], max_block: float) -> float:
    """将若干 0-20 因子压缩到 [0, max_block].

    - 忽略 None
    - 若全部缺失，返回中性水平 (max_block / 2)
    """
    valid = [s for s in scores if s is not None]
    if not valid:
        return max_block / 2.0

    avg = sum(valid) / len(valid)  # 理论范围 0-20
    blk = avg / 20.0 * max_block
    return max(0.0, min(max_block, blk))


def unify_scores(
    *,
    # ---- A 股部分（每项一般 0-20）----
    a_emotion: float,
    a_short: float,
    a_mid: float,
    a_north: Optional[float] = None,
    a_sector: Optional[float] = None,
    a_margin: Optional[float] = None,
    # ---- 美股 / 全球部分（0-20）----
    us_daily: float = 10.0,
    us_short: float = 10.0,
    us_mid: float = 10.0,
) -> UnifiedScore:
    """统一汇总 A 股 + 美股 多因子得分为 0-100.

    约定：
    - 所有子因子分值区间约为 [0, 20]
    - A 股总块 max_block = 60
    - 美股总块 max_block = 40
    - 最终总分 = 0.6 * A + 0.4 * US
    """
    components: Dict[str, float] = {
        "a_emotion": a_emotion,
        "a_short": a_short,
        "a_mid": a_mid,
        "us_daily": us_daily,
        "us_short": us_short,
        "us_mid": us_mid,
    }
    if a_north is not None:
        components["a_north"] = a_north
    if a_sector is not None:
        components["a_sector"] = a_sector
    if a_margin is not None:
        components["a_margin"] = a_margin

    a_scores: List[Optional[float]] = [a_emotion, a_short, a_mid]
    if a_north is not None:
        a_scores.append(a_north)
    if a_sector is not None:
        a_scores.append(a_sector)
    if a_margin is not None:
        a_scores.append(a_margin)

    a_block = _block(a_scores, 60.0)
    us_block = _block([us_daily, us_short, us_mid], 40.0)

    total = max(0.0, min(100.0, a_block * 0.6 + us_block * 0.4))
    level = _risk_level(total)
    explanation = (
        f"综合风险得分 {total:.1f} / 100，{level}。"
        f"A股分块 {a_block:.1f} / 60，美股分块 {us_block:.1f} / 40。"
    )
    return UnifiedScore(total=total, level=level, components=components, explanation=explanation)
