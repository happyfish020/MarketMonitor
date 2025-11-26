"""
UnifiedRisk v1.9 - Common Scoring Utilities
-----------------------------------------
This module provides:

1) Risk level classification
2) Multi-horizon aggregation
3) YAML-driven threshold & weight config
4) Unified debug logging

It is used by:
    - MidtermEngine
    - ShorttermEngine
    - GlobalDailyRiskEngine
    - AShareDailyEngine
    - Future: US/EU/Commodity Risk Engines
"""

from __future__ import annotations
from typing import Dict, Any
from pathlib import Path
import yaml

from unifiedrisk.common.logger import get_logger


LOG = get_logger("UnifiedRisk.Scoring", debug=False)


# ============================================================
# 1. Risk Level Classification
# ============================================================

def classify_level(score: float, thresholds: Dict[str, Any]):
    """
    Classify risk level based on user-defined thresholds.

    thresholds.yaml example:
    ------------------------
    bull: 1.0
    neutral: 0.0
    bear: -1.0

    Returned:
        (key, label)
    """

    bull = thresholds.get("bull", 1.0)
    neutral = thresholds.get("neutral", 0.0)
    bear = thresholds.get("bear", -1.0)

    if score >= bull:
        return "bull", "做多 / Bullish"
    elif score >= neutral:
        return "neutral", "中性 / Neutral"
    else:
        return "bear", "偏空 / Bearish"


# ============================================================
# 2. Multi-Horizon Aggregation (Core Logic)
# ============================================================

def aggregate_horizons(horizon_results: Dict[str, Dict[str, Any]]):
    """
    Aggregate risk across multiple time horizons.

    Input example:
    ----------------
    {
        "midterm": {
            "total_score": 0.35,
            "risk_level": "bull",
            ...
        },
        "shortterm": {
            "total_score": -0.10,
            "risk_level": "bear",
            ...
        },
        "ashare_daily": {
            "total_score": 0.05,
            "risk_level": "neutral",
            ...
        }
    }

    weights.yaml:
    -------------
    horizons:
        midterm: 0.30
        shortterm: 0.25
        global_daily: 0.25
        ashare_daily: 0.20

    Output:
    -------
    {
        "total_score": float,
        "risk_level": "neutral",
        "risk_label": "中性 / Neutral",
        "details": {...}
    }
    """

    # -------- Load horizon weights --------
    weight_path = Path("config/weights.yaml")
    if weight_path.exists():
        cfg = yaml.safe_load(weight_path.read_text(encoding="utf-8"))
        weights = cfg.get("horizons", {})
    else:
        # Default weights
        weights = {
            "midterm": 0.30,
            "shortterm": 0.25,
            "global_daily": 0.25,
            "ashare_daily": 0.20,
        }

    LOG.info("=== Aggregation Start ===")

    final_score = 0.0

    for horizon_name, result in horizon_results.items():
        raw_score = float(result.get("total_score", 0.0))
        w = float(weights.get(horizon_name, 0.0))

        LOG.info(
            "Horizon %-15s | score=%6.3f | weight=%.2f | weighted=%.3f",
            horizon_name,
            raw_score,
            w,
            raw_score * w,
        )

        final_score += raw_score * w

    # -------- Load thresholds --------
    thr_path = Path("config/thresholds.yaml")
    if thr_path.exists():
        thr = yaml.safe_load(thr_path.read_text(encoding="utf-8"))
    else:
        thr = {"bull": 1.0, "neutral": 0.0, "bear": -1.0}

    risk_key, risk_label = classify_level(final_score, thr)

    LOG.info("Final Aggregated Score = %.3f → %s", final_score, risk_key)

    return {
        "total_score": final_score,
        "risk_level": risk_key,
        "risk_label": risk_label,
        "details": horizon_results,
    }
