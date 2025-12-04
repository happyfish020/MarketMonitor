from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
import yaml


# ---------------------------------------------------------
# 加载因子权重
# ---------------------------------------------------------
def _load_weights_config() -> Dict[str, Any]:
    """
    加载 A 股因子权重配置 config/weights_cn.yaml。
    """
    base = Path(__file__).resolve().parents[2]
    cfg_path = base / "config" / "weights_cn.yaml"

    if not cfg_path.exists():
        return {}

    try:
        with cfg_path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return data.get("weights", {})
    except Exception:
        return {}


# ---------------------------------------------------------
# 主统一评分函数（内部）
# ---------------------------------------------------------
def unify(factor_scores: Dict[str, float]) -> Dict[str, Any]:
    """
    unified scoring engine
    输入：
        {
            "north_nps": score,
            "turnover": score,
            "market_sentiment": score,
            "margin": score
        }
    返回：
        {
            "total_score": xx,
            "risk_level": "...",
            "factor_scores": {...}
        }
    """
    weights_cfg = _load_weights_config()

    # 默认权重（若 YAML 没配置）
    default_weights = {
        "north_nps": 0.30,
        "turnover": 0.25,
        "market_sentiment": 0.25,
        "margin": 0.20,
    }

    # 以 YAML 配置覆盖默认
    weights = default_weights.copy()
    weights.update(weights_cfg)

    # 加权得分
    total_score = 0.0
    for name, score in factor_scores.items():
        w = weights.get(name, 0)
        total_score += score * w

    # 风险等级
    if total_score >= 70:
        risk = "偏强 / 多头占优"
    elif total_score >= 55:
        risk = "中性偏强"
    elif total_score >= 45:
        risk = "中性"
    elif total_score >= 30:
        risk = "偏弱"
    else:
        risk = "风险偏高 / 空头主导"

    return {
        "total_score": total_score,
        "risk_level": risk,
        "factor_scores": factor_scores,
    }


# ---------------------------------------------------------
# 提供给外部的正式接口 unify_scores()
# ---------------------------------------------------------
def unify_scores(*, north_nps: float, turnover: float,
                 market_sentiment: float, margin: float) -> Dict[str, Any]:
    """
    对外统一接口，engine 可直接调用。
    """
    return unify({
        "north_nps": north_nps,
        "turnover": turnover,
        "market_sentiment": market_sentiment,
        "margin": margin,
    })
