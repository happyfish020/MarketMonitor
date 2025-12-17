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

说明：
- Breadth Damage / Participation / Index-Sector Corr 等 pillar 可能尚未接入；
  但 GateRuntime 必须在缺失输入时“保守运行”，绝不输出虚假的 Normal 通行证。
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

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
        # Accept YYYY-MM-DD
        try:
            return datetime.strptime(x[:10], "%Y-%m-%d").date()
        except Exception:
            return None
    return None


def _pick_trade_date(snapshot: Dict[str, Any]) -> Optional[date]:
    # 统一取 trade_date：Gate signals -> market_sentiment -> turnover
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
    """Read field from dict or attribute from object."""
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


class Phase2GateRuntime:
    """
    Phase-2 Gate runtime 状态机（最小可跑版本）
    - 读取 snapshot 中已有块
    - 生成 snapshot['gate']（append-only）
    - 用本地 cache 保存 last gate state（用于 anti-zigzag 与恢复纪律）
    """

    def __init__(
        self,
        cache_path: str = "data/cn/cache/gate/gate_state.json",
        cooldown_days: int = 3,
    ) -> None:
        self.cache_path = cache_path
        self.cooldown_days = max(1, int(cooldown_days))

    # ------------------------------------------------------------
    # Public
    # ------------------------------------------------------------
    def append(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(snapshot, dict):
            return snapshot

        decision = self._decide(snapshot)

        gate_block = {
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

        # append-only
        snapshot["gate"] = gate_block

        # persist state (best-effort; failures should be visible)
        try:
            self._save_state(decision, snapshot)
        except Exception as e:
            LOG.error("[Phase2GateRuntime] save_state failed: %s", e)
            raise

        return snapshot

    # ------------------------------------------------------------
    # Core decision
    # ------------------------------------------------------------
    def _decide(self, snapshot: Dict[str, Any]) -> GateDecision:
        trade_date = _pick_trade_date(snapshot)

        ms = snapshot.get("market_sentiment") if isinstance(snapshot.get("market_sentiment"), dict) else {}
        to = snapshot.get("turnover") if isinstance(snapshot.get("turnover"), dict) else {}
        sc = snapshot.get("structural_context") if isinstance(snapshot.get("structural_context"), dict) else {}

        breadth_damage = snapshot.get("breadth_damage")
        participation = snapshot.get("participation")
        idx_sector_corr = snapshot.get("index_sector_corr")

        reasons: List[str] = []
        violations: List[str] = []
        zigzag = False

        # ----------------------------
        # 1) base signals snapshot
        # ----------------------------
        signals: Dict[str, Any] = {
            "trade_date": trade_date.isoformat() if trade_date else None,
            "adv_ratio": ms.get("adv_ratio"),
            "limit_down": ms.get("limit_down"),
            "turnover_total": to.get("total") or to.get("total_turnover") or to.get("turnover_total"),
            "structural_health": sc.get("health"),
        }

        # ----------------------------
        # 2) minimum conservative rules
        # ----------------------------
        level = "Normal"

        # Any structural pillar missing => never allow Normal (Phase-2 conservative runtime)
        if breadth_damage is None:
            level = _max_level(level, "Caution")
            reasons.append("breadth_missing")
        if participation is None:
            level = _max_level(level, "Caution")
            reasons.append("participation_missing")
        if idx_sector_corr is None:
            level = _max_level(level, "Caution")
            reasons.append("index_sector_corr_missing")

        # Structural Context FAIL => at least PlanB (system not trustworthy)
        if isinstance(sc, dict) and sc.get("health") == "FAIL":
            level = _max_level(level, "PlanB")
            reasons.append("structural_health_fail")

        # Optional soft filters (if present, do not invent)
        # Participation hidden weakness hint: if provided as dict and has a boolean field we recognize.
        hw = _get(participation, "hidden_weakness", None)
        if hw is True:
            level = _max_level(level, "Caution")
            reasons.append("hidden_weakness")

        # Breadth confirmed damage (if pillar provides it)
        if _get(breadth_damage, "confirmed_damage", False) is True:
            level = _max_level(level, "PlanB")
            reasons.append("breadth_confirmed_damage")
        if _get(breadth_damage, "extreme", False) is True:
            level = _max_level(level, "Freeze")
            reasons.append("breadth_extreme")

        # Breadth raw stats into signals (append-only)
        br_ratio = _get(breadth_damage, "new_lows_ratio", None)
        if br_ratio is not None:
            signals["new_lows_ratio_50d"] = br_ratio
            signals["new_lows_50d"] = _get(breadth_damage, "new_lows_50d", None)
            signals["breadth_universe"] = _get(breadth_damage, "universe", None)

        # Index-sector correlation regime (if present)
        tag = _get(idx_sector_corr, "regime_tag", None)
        if isinstance(tag, str) and tag:
            signals["regime_tag"] = tag

        # ----------------------------
        # 3) discipline: anti-zigzag / recovery cooldown
        # ----------------------------
        last = self._load_state()
        if last:
            last_level = last.get("level")
            last_date = _as_date(last.get("trade_date"))
            last_changed = _as_date(last.get("last_changed_date")) or last_date

            if isinstance(last_level, str) and last_level in _ALLOWED_LEVELS:
                # forbidden direct jumps
                if last_level == "PlanB" and level == "Normal":
                    zigzag = True
                    violations.append("forbidden_jump_planb_to_normal")
                    # enforce gradual recovery
                    level = "Caution"
                    reasons.append("enforced_gradual_recovery")
                if last_level == "Freeze" and level in ("Normal", "Caution"):
                    zigzag = True
                    violations.append("forbidden_jump_freeze_to_caution_or_normal")
                    level = "PlanB"
                    reasons.append("enforced_gradual_recovery")

                # cooldown: do not allow upgrading too quickly (calendar-day based fallback)
                if trade_date and last_changed and level == "Normal":
                    delta_days = (trade_date - last_changed).days
                    if delta_days < self.cooldown_days:
                        zigzag = True
                        violations.append(f"cooldown_not_met({delta_days}<{self.cooldown_days})")
                        level = "Caution"
                        reasons.append("cooldown_enforced")

        # final sanity
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
            data = load_json(self.cache_path)
            return data if isinstance(data, dict) else None
        except Exception as e:
            LOG.error("[Phase2GateRuntime] load_state error: %s", e)
            # Do not block decision; but make it visible.
            return None

    def _save_state(self, decision: GateDecision, snapshot: Dict[str, Any]) -> None:
        trade_date = _pick_trade_date(snapshot)
        now_iso = datetime.utcnow().isoformat()

        last = self._load_state() or {}
        last_level = last.get("level")
        last_changed = _as_date(last.get("last_changed_date")) or _as_date(last.get("trade_date"))

        # update last_changed_date only if level changed
        changed_date = last_changed
        if isinstance(last_level, str) and last_level == decision.level and last_changed:
            changed_date = last_changed
        else:
            changed_date = trade_date or _as_date(last.get("trade_date")) or date.today()

        state = {
            "level": decision.level,
            "trade_date": trade_date.isoformat() if trade_date else None,
            "last_changed_date": changed_date.isoformat() if changed_date else None,
            "updated_at_utc": now_iso,
            "version": "phase2_gate_state_v1",
        }

        save_json(self.cache_path, state)
