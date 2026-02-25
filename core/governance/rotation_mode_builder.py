#-*- coding: utf-8 -*-
"""UnifiedRisk V12 · RotationMode Evaluator (Phase-1 · Observe Only)

目标：为“轮动市”提供一个制度级中间态（OFF/WATCH/TRY/ON），用于观测与解释，
不直接改变 FinalDecision / ActionHint（Phase-1：只计算 + 只展示）。

Frozen Principles:
- Hard veto: DRS=RED 时不启用（mode=OFF）
- 仅依赖 slots 的只读事实：rotation_market_signal + market/structure 关键字段
- 连续性使用本地 state json（与 drs_continuity 同风格）
- 输出写入 slots["governance"]["rotation_mode"]，供 report block 展示与后续决策层读取
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from core.regime.observation.drs_continuity import JsonStateStore
from core.utils.logger import get_logger


log = get_logger(__name__)


@dataclass(frozen=True)
class RotationModeConfig:
    # rotation_market_signal 中候选的最低门槛
    min_boost_level: int = 2
    min_signal_score: float = 75.0
    # 市场确认（只做“不太差”过滤）
    min_adv_ratio: float = 0.35
    min_amount_ma20_ratio: float = 0.85
    max_proxy_top20_ratio: float = 0.80
    # 连续性映射
    try_days: int = 2
    on_days: int = 3


class RotationModeBuilder:
    """Compute rotation participation mode (observe only)."""

    STATE_KEY = "rotation_mode_persistence"

    @classmethod
    def build(
        cls,
        *,
        slots: Dict[str, Any],
        asof: str,
        cfg: Optional[RotationModeConfig] = None,
        state_path: str = "state/rotation_mode_persistence.json",
    ) -> Dict[str, Any]:
        cfg = cfg or RotationModeConfig()

        gov = slots.get("governance") if isinstance(slots.get("governance"), dict) else {}
        drs_level = _pick_drs_level(gov)
        if str(drs_level or "").upper() == "RED":
            # hard veto
            cnt = cls._update_persistence(asof=asof, active=False, state_path=state_path)
            return {
                "asof": asof,
                "mode": "OFF",
                "active": False,
                "persistence_days": cnt,
                "reason": "DRS=RED 硬否决：轮动参与关闭（观察层）。",
                "inputs": {"drs": drs_level},
                "data_status": "OK",
                "schema_version": "ROTATION_MODE_V1",
            }

        rm = slots.get("rotation_market_signal")
        if not isinstance(rm, dict) or not rm:
            cnt = cls._update_persistence(asof=asof, active=False, state_path=state_path)
            return {
                "asof": asof,
                "mode": "OFF",
                "active": False,
                "persistence_days": cnt,
                "reason": "无 rotation_market_signal：轮动参与关闭（观察层）。",
                "inputs": {"drs": drs_level},
                "data_status": "OK",
                "schema_version": "ROTATION_MODE_V1",
            }

        meta = rm.get("meta") if isinstance(rm.get("meta"), dict) else {}
        data_status = str(meta.get("data_status") or "").upper()
        if data_status and data_status not in ("OK", "PARTIAL"):
            cnt = cls._update_persistence(asof=asof, active=False, state_path=state_path)
            return {
                "asof": asof,
                "mode": "OFF",
                "active": False,
                "persistence_days": cnt,
                "reason": f"rotation_market_signal data_status={data_status}：轮动参与关闭（观察层）。",
                "inputs": {"drs": drs_level, "data_status": data_status},
                "data_status": "PARTIAL",
                "schema_version": "ROTATION_MODE_V1",
            }

        candidates = rm.get("candidates") if isinstance(rm.get("candidates"), list) else []
        best = _pick_best_candidate(candidates=candidates, cfg=cfg)
        if not best:
            # 有轮动事实但不足以参与
            cnt = cls._update_persistence(asof=asof, active=False, state_path=state_path)
            return {
                "asof": asof,
                "mode": "WATCH",
                "active": False,
                "persistence_days": cnt,
                "reason": "轮动候选不足（未达 L2/score 门槛）：仅观察。",
                "inputs": {"drs": drs_level, "best": None},
                "data_status": "OK",
                "schema_version": "ROTATION_MODE_V1",
            }

        market_ok, market_inputs, market_reason = _market_confirmation(slots=slots, cfg=cfg)
        if not market_ok:
            cnt = cls._update_persistence(asof=asof, active=False, state_path=state_path)
            return {
                "asof": asof,
                "mode": "WATCH",
                "active": False,
                "persistence_days": cnt,
                "reason": f"市场确认不足：{market_reason}（仅观察）。",
                "inputs": {"drs": drs_level, **market_inputs, "best": _brief(best)},
                "data_status": "OK",
                "schema_version": "ROTATION_MODE_V1",
            }

        # Active day
        cnt = cls._update_persistence(asof=asof, active=True, state_path=state_path)
        if cnt >= cfg.on_days:
            mode = "ON"
            reason = f"轮动结构连续 {cnt} 日（>= {cfg.on_days}）：可参与（观察层）。"
        elif cnt >= cfg.try_days:
            mode = "TRY"
            reason = f"轮动结构连续 {cnt} 日（>= {cfg.try_days}）：试参与（观察层）。"
        else:
            mode = "WATCH"
            reason = "轮动结构出现但连续性不足：先观察。"

        out = {
            "asof": asof,
            "mode": mode,
            "active": True,
            "persistence_days": cnt,
            "reason": reason,
            "inputs": {"drs": drs_level, **market_inputs, "best": _brief(best)},
            "thresholds": {
                "min_boost_level": cfg.min_boost_level,
                "min_signal_score": cfg.min_signal_score,
                "min_adv_ratio": cfg.min_adv_ratio,
                "min_amount_ma20_ratio": cfg.min_amount_ma20_ratio,
                "max_proxy_top20_ratio": cfg.max_proxy_top20_ratio,
                "try_days": cfg.try_days,
                "on_days": cfg.on_days,
            },
            "data_status": "OK",
            "schema_version": "ROTATION_MODE_V1",
        }

        # structured log
        try:
            log.info(
                "[RotationMode] asof=%s drs=%s mode=%s active=%s days=%s best=%s market=%s",
                asof,
                drs_level,
                mode,
                True,
                cnt,
                _brief(best),
                {k: market_inputs.get(k) for k in ("adv_ratio", "amount_ma20_ratio", "proxy_top20_amount_ratio", "failure_rate", "trend_state")},
            )
        except Exception:
            pass

        return out

    @classmethod
    def _update_persistence(cls, *, asof: str, active: bool, state_path: str) -> int:
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

        if active and prev_signal == "ACTIVE":
            count = prev_count + 1
        elif active:
            count = 1
        else:
            count = 0

        state[cls.STATE_KEY] = {"asof": asof, "signal": "ACTIVE" if active else "NONE", "count": count}
        try:
            store.save(state)
        except Exception:
            pass
        return count


def _pick_drs_level(gov: Dict[str, Any]) -> Optional[str]:
    if not isinstance(gov, dict):
        return None
    d = gov.get("drs")
    if isinstance(d, dict):
        v = d.get("level") or d.get("drs_level") or d.get("signal")
        if isinstance(v, str):
            # signal may be YELLOW_ROTATION etc; keep level first
            return v
    v2 = gov.get("drs_level")
    return v2 if isinstance(v2, str) else None


def _pick_best_candidate(*, candidates: List[Any], cfg: RotationModeConfig) -> Optional[Dict[str, Any]]:
    best: Optional[Dict[str, Any]] = None
    best_score: float = -1.0
    for r in candidates:
        if not isinstance(r, dict):
            continue
        action = str(r.get("action") or r.get("ACTION") or "").upper()
        if action not in ("ENTER", "WATCH"):
            continue
        boost = int(_safe_float(r.get("boost_level") or r.get("BOOST_LEVEL") or 0) or 0)
        sc = _safe_float(r.get("signal_score") or r.get("SIGNAL_SCORE"))
        if sc is None:
            continue
        if boost >= cfg.min_boost_level and sc >= cfg.min_signal_score and sc > best_score:
            best = r
            best_score = sc
    return best


def _market_confirmation(*, slots: Dict[str, Any], cfg: RotationModeConfig) -> Tuple[bool, Dict[str, Any], str]:
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
    proxy_top20 = _safe_float(
        (slots.get("crowding_concentration") or {}).get("proxy_top20_amount_ratio")
        if isinstance(slots.get("crowding_concentration"), dict)
        else (slots.get("attack_window") or {}).get("evidence", {}).get("proxy_top20_amount_ratio")
    )
    fr = slots.get("failure_rate") if isinstance(slots.get("failure_rate"), dict) else {}
    failure_level = fr.get("level") or fr.get("state")
    trend = slots.get("trend_in_force") if isinstance(slots.get("trend_in_force"), dict) else {}
    trend_state = trend.get("state") or trend.get("status")

    inputs = {
        "adv_ratio": adv_ratio,
        "amount_ma20_ratio": amount_ma20_ratio,
        "proxy_top20_amount_ratio": proxy_top20,
        "failure_rate": failure_level,
        "trend_state": trend_state,
    }

    # Allow missing -> do not block (observe-only)
    if adv_ratio is not None and adv_ratio < cfg.min_adv_ratio:
        return False, inputs, f"adv_ratio<{cfg.min_adv_ratio}"
    if amount_ma20_ratio is not None and amount_ma20_ratio < cfg.min_amount_ma20_ratio:
        return False, inputs, f"amount_ma20_ratio<{cfg.min_amount_ma20_ratio}"
    if proxy_top20 is not None and proxy_top20 > cfg.max_proxy_top20_ratio:
        return False, inputs, f"proxy_top20_ratio>{cfg.max_proxy_top20_ratio}"
    if isinstance(failure_level, str) and failure_level.upper() == "HIGH":
        return False, inputs, "failure_rate=HIGH"
    if isinstance(trend_state, str) and trend_state.upper().startswith("BROKEN"):
        return False, inputs, f"trend={trend_state}"

    return True, inputs, "OK"


def _brief(r: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(r, dict):
        return None
    return {
        "sector_id": r.get("sector_id") or r.get("SECTOR_ID"),
        "sector_name": r.get("sector_name") or r.get("SECTOR_NAME"),
        "action": r.get("action") or r.get("ACTION"),
        "signal_score": r.get("signal_score") if "signal_score" in r else r.get("SIGNAL_SCORE"),
        "boost_level": r.get("boost_level") if "boost_level" in r else r.get("BOOST_LEVEL"),
        "stability_score": r.get("stability_score") if "stability_score" in r else r.get("STABILITY_SCORE"),
        "score_ma3": r.get("score_ma3") if "score_ma3" in r else r.get("SCORE_MA3"),
    }


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return float(int(x))
        return float(x)
    except Exception:
        return None
