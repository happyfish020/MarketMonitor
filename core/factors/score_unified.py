from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
import yaml


def _load_weights_config() -> Dict[str, Any]:
    """加载 A 股因子权重配置 config/weights_cn.yaml。"""
    base = Path(__file__).resolve().parents[2]
    cfg_path = base / "config" / "weights_cn.yaml"
    if not cfg_path.exists():
        return {}
    try:
        with cfg_path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            return {}
        return data
    except Exception:
        return {}


class UnifiedScoreBuilder:
    """统一因子得分器：根据因子结果 + 权重，计算综合得分和风险等级。"""

    def __init__(self) -> None:
        cfg = _load_weights_config()
        self.weights: Dict[str, float] = cfg.get("weights", {})

    def unify(self, factor_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        参数:
            factor_results: {factor_name: FactorResult}

        返回:
            {
              "total_score": float,
              "risk_level": str,
              "factor_scores": {name: score}
            }
        """
        total_score = 0.0
        factor_scores: Dict[str, float] = {}

        for name, fr in factor_results.items():
            score = getattr(fr, "score", None)
            if score is None:
                continue
            score_f = float(score)
            factor_scores[name] = score_f
            w = float(self.weights.get(name, 0.0))
            total_score += w * score_f

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