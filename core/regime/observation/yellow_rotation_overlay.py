#-*- coding: utf-8 -*-
"""UnifiedRisk V12 - YELLOW_ROTATION Overlay (P0)

目标：解决“指数上涨但系统长期输出防守”的明显缺陷，在不放松 DRS=RED 硬防守的前提下，
为“板块轮动结构出现 + 市场扩散/流动性不差”的阶段提供一个可读的中间态：YELLOW_ROTATION。

原则（冻结）：
- 这是 DRS 的 *解释性覆盖*（overlay），不改变 Gate 的判定。
- 当 DRS=RED 时，永不升级为 YELLOW_ROTATION（RED 一票否决）。
- 仅依赖 slots 已有的只读事实：rotation_market_signal + market/structure 关键字段。
- 连续性用本地 state json 文件保存（同 DRSContinuity 的兜底方式）。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from core.regime.observation.drs_continuity import JsonStateStore


@dataclass(frozen=True)
class YellowRotationConfig:
    # 最低门槛：必须出现 L2 的“轮动事实”候选（由 B-step 打分产生）
    min_boost_level: int = 2
    # 评分门槛（与 rotation_market_signal 现有默认一致：top candidates 通常 >75）
    min_signal_score: float = 75.0
    # 市场扩散/流动性确认：宽松阈值，只做“不要太差”的过滤
    min_adv_ratio: float = 0.50
    min_amount_ma20_ratio: float = 0.95
    max_proxy_top20_ratio: float = 0.75


class YellowRotationOverlay:
    """Apply overlay to drs_obs + slots governance."""

    STATE_KEY = "yellow_rotation_persistence"

    @classmethod
    def apply(
        cls,
        *,
        drs_obs: Dict[str, Any],
        slots: Dict[str, Any],
        asof: str,
        cfg: Optional[YellowRotationConfig] = None,
        state_path: str = "state/yellow_rotation_persistence.json",
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        cfg = cfg or YellowRotationConfig()

        # Defensive: structure not ready
        obs = drs_obs.get("observation")
        if not isinstance(obs, dict):
            return drs_obs, slots

        base_signal = str(obs.get("signal") or "").upper()
        if base_signal == "RED":
            # RED is a hard veto; still record persistence reset for audit clarity
            cls._update_persistence(slots=slots, asof=asof, active=False, state_path=state_path)
            return drs_obs, slots

        # ---- pull rotation_market_signal ----
        rm = slots.get("rotation_market_signal")
        if not isinstance(rm, dict) or not rm:
            cls._update_persistence(slots=slots, asof=asof, active=False, state_path=state_path)
            return drs_obs, slots

        meta = rm.get("meta") if isinstance(rm.get("meta"), dict) else {}
        data_status = str((meta or {}).get("data_status") or "").upper()
        if data_status and data_status not in ("OK", "PARTIAL"):
            cls._update_persistence(slots=slots, asof=asof, active=False, state_path=state_path)
            return drs_obs, slots

        candidates = rm.get("candidates") if isinstance(rm.get("candidates"), list) else []
        if not candidates:
            cls._update_persistence(slots=slots, asof=asof, active=False, state_path=state_path)
            return drs_obs, slots

        # ---- market confirmation (best-effort) ----
        adv_ratio = _safe_float(
            (slots.get("crowding_concentration") or {}).get("adv_ratio")
            if isinstance(slots.get("crowding_concentration"), dict)
            else (slots.get("attack_window") or {}).get("evidence", {}).get("adv_ratio")
        )

        amount_ma20_ratio = _safe_float(
            (slots.get("liquidity") or {}).get("amount_ma20_ratio")
            if isinstance(slots.get("liquidity"), dict)
            else (slots.get("attack_window") or {}).get("evidence", {}).get("amount_ma20_ratio")
        )

        proxy_top20_amount_ratio = _safe_float(
            (slots.get("crowding_concentration") or {}).get("proxy_top20_amount_ratio")
            if isinstance(slots.get("crowding_concentration"), dict)
            else (slots.get("attack_window") or {}).get("evidence", {}).get("proxy_top20_amount_ratio")
        )

        # Allow missing -> treat as "unknown" and don't block
        if adv_ratio is not None and adv_ratio < cfg.min_adv_ratio:
            cls._update_persistence(slots=slots, asof=asof, active=False, state_path=state_path)
            return drs_obs, slots
        if amount_ma20_ratio is not None and amount_ma20_ratio < cfg.min_amount_ma20_ratio:
            cls._update_persistence(slots=slots, asof=asof, active=False, state_path=state_path)
            return drs_obs, slots
        if proxy_top20_amount_ratio is not None and proxy_top20_amount_ratio > cfg.max_proxy_top20_ratio:
            cls._update_persistence(slots=slots, asof=asof, active=False, state_path=state_path)
            return drs_obs, slots

        # ---- find best candidate ----
        best = None
        best_score = -1.0
        for r in candidates:
            if not isinstance(r, dict):
                continue
            action = str(r.get("action") or r.get("ACTION") or "").upper()
            if action not in ("ENTER", "WATCH"):
                continue
            boost = int(_safe_float(r.get("boost_level") or 0) or 0)
            sc = _safe_float(r.get("signal_score") or r.get("SIGNAL_SCORE"))
            if sc is None:
                continue
            if boost >= cfg.min_boost_level and sc >= cfg.min_signal_score and sc > best_score:
                best = r
                best_score = sc

        if not best:
            cls._update_persistence(slots=slots, asof=asof, active=False, state_path=state_path)
            return drs_obs, slots

        # ---- upgrade signal ----
        obs["signal"] = "YELLOW_ROTATION"
        # Keep meaning short and human
        obs["meaning"] = "大盘总体仍偏谨慎，但出现可参与的结构性轮动机会：仅限小步/分批/不追价。"
        drivers = obs.get("drivers") if isinstance(obs.get("drivers"), list) else []
        drivers.append("rotation_market:boost_L2")
        obs["drivers"] = drivers

        # Persist count and attach into slots for report
        count = cls._update_persistence(slots=slots, asof=asof, active=True, state_path=state_path)
        # attach to rotation_market_signal meta for report rendering convenience
        try:
            meta2 = rm.get("meta") if isinstance(rm.get("meta"), dict) else {}
            meta2["yellow_rotation_persistence_days"] = count
            rm["meta"] = meta2
            slots["rotation_market_signal"] = rm
        except Exception:
            pass

        # Also mirror into governance.drs for downstream ActionHint / summary
        gov = slots.get("governance")
        if not isinstance(gov, dict):
            gov = {}
            slots["governance"] = gov
        d = gov.get("drs")
        if not isinstance(d, dict):
            d = {}
        d["signal"] = "YELLOW_ROTATION"
        gov["drs"] = d

        return drs_obs, slots

    @classmethod
    def _update_persistence(cls, *, slots: Dict[str, Any], asof: str, active: bool, state_path: str) -> int:
        store = JsonStateStore(state_path)
        state = store.load() if hasattr(store, "load") else {}
        if not isinstance(state, dict):
            state = {}
        prev = state.get(cls.STATE_KEY, {})
        if not isinstance(prev, dict):
            prev = {}

        prev_signal = str(prev.get("signal") or "")
        prev_count = prev.get("count", 0)
        if not isinstance(prev_count, int) or prev_count < 0:
            prev_count = 0

        if active and prev_signal == "YELLOW_ROTATION":
            count = prev_count + 1
        elif active:
            count = 1
        else:
            count = 0

        state[cls.STATE_KEY] = {"asof": asof, "signal": "YELLOW_ROTATION" if active else "NONE", "count": count}
        try:
            store.save(state)
        except Exception:
            pass

        # expose for report (optional)
        gov = slots.get("governance")
        if not isinstance(gov, dict):
            gov = {}
            slots["governance"] = gov
        gov.setdefault("yellow_rotation", {})
        if isinstance(gov.get("yellow_rotation"), dict):
            gov["yellow_rotation"].update({"asof": asof, "active": bool(active), "persistence_days": count})

        return count


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return float(int(x))
        return float(x)
    except Exception:
        return None
