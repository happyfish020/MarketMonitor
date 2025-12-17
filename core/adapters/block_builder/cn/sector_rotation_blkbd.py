# -*- coding: utf-8 -*-
"""
UnifiedRisk V12
Sector Rotation BlockBuilder (CN A-Share)

最小可用版（B方案）：
- 使用 snapshot 中已有的板块/指数强弱信息
- 计算“强势组 vs 弱势组”的相对差
- 输出结构稳定，便于后续升级
"""

from typing import Dict, Any, Optional
from core.adapters.block_builder.block_builder_base import FactBlockBuilderBase
from core.utils.logger import get_logger
import json
LOG = get_logger("BlockBuilder.SectorRotation")


class SectorRotationBlockBuilder(FactBlockBuilderBase):
    """
    板块轮动 BlockBuilder（V12）

    输入（来自 snapshot，必须已有）：
    - snapshot["index_tech"]       # 技术强弱（如科技/成长）
    - snapshot["market_sentiment"] # 市场情绪（可辅助）
    - snapshot["index_global"]     # 可选（不强依赖）

    输出：
    - snapshot["sector_rotation"] = {
          rotation_diff: float,
          leader: str | None,
          lagger: str | None,
          detail: dict
      }
    """

    # ------------------------------------------------------------------
    def transform(
        self,
        snapshot: Dict[str, Any],
        refresh_mode: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        计算板块轮动结构

        refresh_mode:
            - 仅作为透传参数
            - 不用于 IO 决策
        """

        index_tech = snapshot.get("index_tech") or {}
        market_sentiment = snapshot.get("market_sentiment") or {}

        # --------------------------------------------------------------
        # 1️⃣ 从 index_tech 中提取“候选板块强弱”
        # --------------------------------------------------------------
        # 约定：
        # index_tech 中可能包含：
        #   {
        #       "tech": 0.8,
        #       "ai": 0.9,
        #       "consumption": 0.3,
        #       "finance": 0.4,
        #       ...
        #   }

        sector_scores = {
            k: v
            for k, v in index_tech.items()
            if isinstance(v, (int, float))
        }

        if len(sector_scores) < 2:
            # 数据不足，返回“可识别但中性”的结果
            LOG.info(
                "[SectorRotation] insufficient sector data: keys=%s",
                list(sector_scores.keys()),
            )
            return {
                "rotation_diff": 0.0,
                "leader": None,
                "lagger": None,
                "detail": {
                    "reason": "insufficient_sector_scores",
                    "sector_scores": sector_scores,
                    "_raw_data": json.dumps(market_sentiment)[:160] + "..."
                },
            }

        # --------------------------------------------------------------
        # 2️⃣ 排序，找 leader / lagger
        # --------------------------------------------------------------
        sorted_sectors = sorted(
            sector_scores.items(),
            key=lambda x: x[1],
            reverse=True,
        )

        leader, leader_score = sorted_sectors[0]
        lagger, lagger_score = sorted_sectors[-1]

        rotation_diff = float(leader_score - lagger_score)

        # --------------------------------------------------------------
        # 3️⃣ 使用 market_sentiment 做轻度修正（可解释）
        # --------------------------------------------------------------
        adv_ratio = market_sentiment.get("adv_ratio")

        sentiment_adj = 0.0
        try:
            if adv_ratio is not None:
                adv_ratio = float(adv_ratio)
                # 市场整体偏强 → 放大轮动
                if adv_ratio > 0.6:
                    sentiment_adj = +0.05
                # 市场整体偏弱 → 收敛轮动
                elif adv_ratio < 0.4:
                    sentiment_adj = -0.05
        except Exception:
            pass

        rotation_diff_adj = rotation_diff * (1.0 + sentiment_adj)

        LOG.info(
            "[SectorRotation] leader=%s(%.3f) lagger=%s(%.3f) "
            "diff=%.3f adj=%.3f",
            leader,
            leader_score,
            lagger,
            lagger_score,
            rotation_diff,
            rotation_diff_adj,
        )

        # --------------------------------------------------------------
        # 4️⃣ 返回结构（写入 snapshot）
        # --------------------------------------------------------------
        return {
            "rotation_diff": rotation_diff_adj,
            "leader": leader,
            "lagger": lagger,
            "detail": {
                "raw_diff": rotation_diff,
                "sentiment_adj": sentiment_adj,
                "sector_scores": sector_scores,
                "_raw_data": json.dumps(market_sentiment)[:160] + "..."
            },
        }
