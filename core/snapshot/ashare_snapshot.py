# -*- coding: utf-8 -*-
"""
UnifiedRisk V12
CN A-Share Snapshot Builder

职责：
- 规范 CN A-Share snapshot 结构
- 统一字段命名
- 映射 unified_emotion → market_sentiment / emotion
"""

from typing import Dict, Any

from core.snapshot.snapshot_builder_base import SnapshotBuilderBase
from core.utils.logger import get_logger

LOG = get_logger("Snapshot.Ashare")


class AshareSnapshotBuilder(SnapshotBuilderBase):
    """
    CN A-Share SnapshotBuilder（V12 定稿）
    """

    # ------------------------------------------------------------------
    def build(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(snapshot, dict):
            raise ValueError("snapshot must be a dict")

        # 1️⃣ 基础结构兜底（只补 key，不覆盖）
        self._ensure_basic_blocks(snapshot)

        # 2️⃣ unified_emotion → 标准字段映射（关键）
        self._fix_unified_emotion(snapshot)

        return snapshot

    # ==================================================================
    # 内部方法
    # ==================================================================

    def _ensure_basic_blocks(self, snapshot: Dict[str, Any]) -> None:
        """
        确保 CN A-Share snapshot 至少包含这些标准块
        注意：只 setdefault，绝不覆盖
        """
        snapshot.setdefault("market_sentiment", {})
        snapshot.setdefault("emotion", {})
        snapshot.setdefault("amount", {})
        snapshot.setdefault("margin", {})
        snapshot.setdefault("north_nps_raw", {})
        snapshot.setdefault("index_tech", {})
        snapshot.setdefault("index_global", {})
        snapshot.setdefault("global_lead", {})
        snapshot.setdefault("global_macro", {})
        snapshot.setdefault("sector_rotation", {})

        # Phase-2 structural pillars (append-only defaults)
        snapshot.setdefault("breadth_damage", None)
        snapshot.setdefault("participation", None)
        #snapshot.setdefault("index_sector_corr_raw", {})
        snapshot.setdefault("index_sector_corr", None)

    def _fix_unified_emotion(self, snapshot: Dict[str, Any]) -> None:
        """
        V12 定稿规则：
        unified_emotion → market_sentiment / emotion

        规则：
        - unified_emotion 是上游聚合结果（完整保留）
        - 下游标准字段只在为空时填充
        - 不覆盖 DS / BlockBuilder 已有结果
        """

        ue = snapshot.get("unified_emotion")
        if not isinstance(ue, dict):
            return

        # -------- market_sentiment（市场情绪 / 广度）--------
        if not snapshot.get("market_sentiment"):
            mi = ue.get("market_internal") or {}
            snapshot["market_sentiment"] = {
                "adv": mi.get("adv"),
                "dec": mi.get("dec"),
                "flat": mi.get("flat"),
                "adv_ratio": mi.get("adv_ratio"),
                "limit_up": mi.get("limit_up"),
                "limit_down": mi.get("limit_down"),
                "extreme_ratio": mi.get("extreme_ratio"),
            }

            LOG.info(
                "[Snapshot] market_sentiment filled from unified_emotion "
                "(keys=%s)",
                list(snapshot["market_sentiment"].keys()),
            )

        # -------- emotion（行为 / 资金 / 流动性）--------
        if not snapshot.get("emotion"):
            bh = ue.get("behavior") or {}
            snapshot["emotion"] = {
                "total": bh.get("total"),
                "sh": bh.get("sh"),
                "sz": bh.get("sz"),
                "bj": bh.get("bj"),
                "concentration": bh.get("concentration"),
            }

            LOG.info(
                "[Snapshot] emotion filled from unified_emotion "
                "(keys=%s)",
                list(snapshot["emotion"].keys()),
            )
