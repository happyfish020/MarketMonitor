# core/actions/action_hint_builder.py

from typing import Dict, Any


def build_action_hint(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """
    Phase-3 Action Governance

    Input:
        snapshot["gate"] = {
            "level": "NORMAL | CAUTION",
            "reasons": [...],
            "evidence": {...},
        }

    Output:
        ActionHint (frozen schema)
    """

    gate = snapshot.get("gate")
    if not gate:
        # Phase-3 不负责兜底计算
        return {
            "action": "FREEZE",
            "reason": "Missing gate decision",
            "allowed": {"etf": False, "single_stock": False},
            "forbidden": ["ALL"],
            "limits": {},
            "conditions": [],
        }

    level = gate.get("level")
    reasons = gate.get("reasons", [])

    # -------------------------------
    # CAUTION : 非对称风险，管住手
    # -------------------------------
    if level == "CAUTION":
        return {
            "action": "HOLD",
            "reason": "Gate=CAUTION：结构性反证存在，观望是系统合法行为",
            "allowed": {
                "etf": True,          # 仅条件型
                "single_stock": False
            },
            "forbidden": [
                "ADD_SINGLE_STOCK",
                "AGGRESSIVE_BUY",
                "CHASE_UP",
            ],
            "limits": {
                "max_units_today": 1,
                "max_units_total": 1,
            },
            "conditions": [
                "仅限ETF",
                "仅在回踩或确认支撑时",
                "不可追高",
            ],
        }

    # -------------------------------
    # NORMAL : 结构允许进攻（保守）
    # -------------------------------
    if level == "NORMAL":
        return {
            "action": "ETF_LADDER",
            "reason": "Gate=NORMAL：结构允许，按阶梯执行",
            "allowed": {
                "etf": True,
                "single_stock": False,   # Phase-3 暂不放开个股
            },
            "forbidden": [
                "ALL_IN",
                "CHASE_UP",
            ],
            "limits": {
                "max_units_today": 2,
                "max_units_total": 3,
            },
            "conditions": [
                "仅ETF",
                "分批执行",
                "单日不超过上限",
            ],
        }

    # -------------------------------
    # 兜底（理论上不应到达）
    # -------------------------------
    return {
        "action": "FREEZE",
        "reason": f"Unsupported gate level: {level}",
        "allowed": {"etf": False, "single_stock": False},
        "forbidden": ["ALL"],
        "limits": {},
        "conditions": [],
    }
