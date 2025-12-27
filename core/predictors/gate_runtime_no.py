# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 FULL - Phase-2 Gate Runtime
------------------------------------------
职责（冻结）：
- 仅做 Phase-2 Gate 的 runtime 裁决（Normal/Caution/PlanB/Freeze）
- 严格 append-only：只写入 snapshot['gate']
- 不访问 DS / Provider / DB
- 不做预测（Phase-3 禁止抢跑）
- 纪律优先：降级快、恢复慢、反横跳（anti-zigzag）
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from core.adapters.cache.file_cache import load_json, save_json
from core.utils.logger import get_logger

LOG = get_logger("Phase2.Gate")

_ALLOWED_LEVELS = ("Normal", "Caution", "PlanB", "Freeze")


def _as_date(x: Any) -> Optional[date]:
    if x is None:
        return None
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, str):
        try:
            return datetime.strptime(x[:10], "%Y-%m-%d").date()
        except Exception:
            return None
    return None


def _pick_trade_date(snapshot: Dict[str, Any]) -> Optional[date]:
    gate = snapshot.get("gate") if isinstance(snapshot.get("gate"), dict) else None
    if gate:
        sig = gate.get("signals")
        if isinstance(sig, dict):
            d = _as_date(sig.get("trade_date"))
            if d:
                return d

    ms = snapshot.get("market_sentiment")
    if isinstance(ms, dict):
        d = _as_date(ms.get("trade_date"))
        if d:
            return d

    to = snapshot.get("turnover")
    if isinstance(to, dict):
        d = _as_date(to.get("trade_date"))
        if d:
            return d

    return None


def _get(obj: Any, key: str, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _max_level(a: str, b: str) -> str:
    order = {"Normal": 0, "Caution": 1, "PlanB": 2, "Freeze": 3}
    return a if order.get(a, 0) >= order.get(b, 0) else b


@dataclass(frozen=True, slots=True)
class GateDecision:
    level: str
    reasons: List[str]
    signals: Dict[str, Any]
    zigzag_detected: bool
    violations: List[str]


class ASharesGateDecider:
    """
    Phase-2 Gate runtime 状态机（最小可跑版本）
    """

    def __init__(
        self,
        cache_path: str = "data/cn/cache/gate/gate_state.json",
        cooldown_days: int = 3,
    ) -> None:
        self.cache_path = cache_path
        self.cooldown_days = max(1, int(cooldown_days))

    def append(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(snapshot, dict):
            return snapshot

        decision = self._decide(snapshot)

        snapshot["gate"] = {
            "level": decision.level,
            "reasons": decision.reasons,
            "signals": decision.signals,
            "discipline": {
                "zigzag_detected": decision.zigzag_detected,
                "violations": decision.violations,
                "cooldown_days": self.cooldown_days,
            },
            "version": "phase2_gate_v1",
        }

        self._save_state(decision, snapshot)
        return snapshot

    # ------------------------------------------------------------
    # Core decision
    # ------------------------------------------------------------
    def _decide(self, snapshot: Dict[str, Any]) -> GateDecision:
        trade_date = _pick_trade_date(snapshot)

        ms = snapshot.get("market_sentiment") or {}
        to = snapshot.get("turnover") or {}
        sc = snapshot.get("structural_context") or {}

        breadth_damage = snapshot.get("breadth_damage")
        participation = snapshot.get("participation")
        idx_sector_corr = snapshot.get("index_sector_corr")

        # 👉 新增：ETF × Index × Spot 同步因子（结构反证）
        etf_sync = snapshot.get("etf_index_sync")

        reasons: List[str] = []
        violations: List[str] = []
        zigzag = False

        signals: Dict[str, Any] = {
            "trade_date": trade_date.isoformat() if trade_date else None,
            "adv_ratio": ms.get("adv_ratio"),
            "turnover_total": to.get("total") or to.get("total_turnover"),
            "structural_health": sc.get("health"),
        }

        level = "Normal"

        # -------- 基础保守规则 --------
        if breadth_damage is None:
            level = _max_level(level, "Caution")
            reasons.append("breadth_missing")

        if participation is None:
            level = _max_level(level, "Caution")
            reasons.append("participation_missing")

        if idx_sector_corr is None:
            level = _max_level(level, "Caution")
            reasons.append("index_sector_corr_missing")

        if isinstance(sc, dict) and sc.get("health") == "FAIL":
            level = _max_level(level, "PlanB")
            reasons.append("structural_health_fail")

        # -------- 新增规则：ETFIndexSync（只降级） --------
        if isinstance(etf_sync, dict):
            lvl = etf_sync.get("level")
            if lvl == "LOW":
                level = _max_level(level, "Caution")
                reasons.append("etf_index_sync_disconfirm")

        # -------- 反横跳 / 冷却 --------
        last = self._load_state()
        if last:
            last_level = last.get("level")
            last_changed = _as_date(last.get("last_changed_date"))

            if last_level == "PlanB" and level == "Normal":
                zigzag = True
                violations.append("forbidden_jump_planb_to_normal")
                level = "Caution"
                reasons.append("enforced_gradual_recovery")

        if level not in _ALLOWED_LEVELS:
            level = "Caution"
            reasons.append("invalid_level_fallback")

        return GateDecision(
            level=level,
            reasons=reasons,
            signals=signals,
            zigzag_detected=zigzag,
            violations=violations,
        )

    # ------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------
    def _load_state(self) -> Optional[Dict[str, Any]]:
        try:
            return load_json(self.cache_path)
        except Exception:
            return None

    def _save_state(self, decision: GateDecision, snapshot: Dict[str, Any]) -> None:
        trade_date = _pick_trade_date(snapshot)
        state = {
            "level": decision.level,
            "trade_date": trade_date.isoformat() if trade_date else None,
            "last_changed_date": trade_date.isoformat() if trade_date else None,
            "version": "phase2_gate_state_v1",
        }
        save_json(self.cache_path, state)
