# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - P0-2 DRS 连续性（Anti-Chop / Persistence）

冻结原则：
- 不改变 DRS 单日信号判定（signal/meaning 由上游提供）
- 只做“连续性确认”（count/required/confirmed）
- 不参与 Gate，不生成交易信号
- 可审计：evidence 必须包含 yesterday/today/count/required/confirmed
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Literal


DRSSignal = Literal["GREEN", "YELLOW", "RED"]


@dataclass(frozen=True)
class DRSContinuityConfig:
    """
    冻结默认阈值：
    - RED 连续 2 日确认
    - YELLOW 连续 2 日确认
    - GREEN 连续 3 日确认（放松更慢）
    """
    red_required: int = 2
    yellow_required: int = 2
    green_required: int = 3

    def required(self, signal: str) -> int:
        if signal == "RED":
            return self.red_required
        if signal == "YELLOW":
            return self.yellow_required
        # default: GREEN 或未知 → 按 GREEN
        return self.green_required


class JsonStateStore:
    """
    兜底实现：用本地 json 文件保存状态（仅用于你暂时未接入 CacheManager 时）
    - 你后续可用 CacheManager/FileCache 注入替换（不改本文件逻辑）
    """

    def __init__(self, path: str):
        self.path = path

    def load(self) -> Dict[str, Any]:
        if not os.path.exists(self.path):
            return {}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                obj = json.load(f)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    def save(self, data: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)


class DRSContinuity:
    """
    将“单日 DRS”升级为“带连续确认的 DRS”。

    输入：
      - drs_obs: dict，必须包含 observation.signal / observation.meaning（你现有结构）
      - asof: str，交易日（YYYY-MM-DD）
      - state_store: 可选，需实现 load()/save()

    输出：
      - 返回更新后的 drs_obs（原结构不变，只新增 observation.persistence 和 evidence.continuity）
    """

    STATE_KEY = "drs_persistence"

    @classmethod
    def apply(
        cls,
        *,
        drs_obs: Dict[str, Any],
        asof: str,
        config: Optional[DRSContinuityConfig] = None,
        state_store: Optional[Any] = None,
        fallback_state_path: str = "state/drs_persistence.json",
    ) -> Dict[str, Any]:
        config = config or DRSContinuityConfig()

        # ---- 读取 today signal ----
        obs = drs_obs.get("observation")
        if not isinstance(obs, dict):
            # 冻结：结构不对则原样返回，但加审计提示
            drs_obs.setdefault("evidence", {})
            drs_obs["evidence"]["continuity"] = {
                "asof": asof,
                "error": "missing:observation",
            }
            return drs_obs

        today_signal = obs.get("signal")
        if not isinstance(today_signal, str) or not today_signal:
            drs_obs.setdefault("evidence", {})
            drs_obs["evidence"]["continuity"] = {
                "asof": asof,
                "error": "missing:observation.signal",
            }
            return drs_obs

        required = config.required(today_signal)

        # ---- 获取状态存储 ----
        if state_store is None:
            state_store = JsonStateStore(fallback_state_path)

        state = state_store.load() if hasattr(state_store, "load") else {}
        if not isinstance(state, dict):
            state = {}

        prev = state.get(cls.STATE_KEY, {})
        if not isinstance(prev, dict):
            prev = {}

        prev_asof = prev.get("asof")
        prev_signal = prev.get("signal")
        prev_count = prev.get("count", 0)
        if not isinstance(prev_count, int) or prev_count < 0:
            prev_count = 0

        # ---- 连续计数逻辑（冻结） ----
        # 仅比较 signal 是否相同；日期仅用于审计，不做跳日修正（后续可接入交易日历）
        if prev_signal == today_signal:
            count = prev_count + 1
        else:
            count = 1

        confirmed = bool(count >= required)

        # ---- 写回 observation.persistence（不改原字段）----
        obs["persistence"] = {
            "count": count,
            "required": required,
            "confirmed": confirmed,
        }

        # ---- 审计链：写入 evidence.continuity ----
        drs_obs.setdefault("evidence", {})
        drs_obs["evidence"]["continuity"] = {
            "asof": asof,
            "today_signal": today_signal,
            "prev_asof": prev_asof,
            "prev_signal": prev_signal,
            "prev_count": prev_count,
            "count": count,
            "required": required,
            "confirmed": confirmed,
        }

        # ---- 更新 state ----
        state[cls.STATE_KEY] = {
            "asof": asof,
            "signal": today_signal,
            "count": count,
        }
        if hasattr(state_store, "save"):
            state_store.save(state)

        return drs_obs
