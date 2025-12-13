import os
import json
import numpy as np
import pandas as pd
from typing import Dict, Any, Tuple

from core.utils.logger import get_logger
from core.datasources.datasource_base import DataSourceConfig,BaseDataSource
from core.adapters.datasources.cn.market_sentiment_source import MarketSentimentDataSource

LOG = get_logger("DS.UnifiedEmotion")


class UnifiedEmotionDataSource:
    """
    V12 情绪数据源（统一情绪 DS）
    负责输出：
       1) snapshot["sentiment"]  —— 市场宽度
       2) snapshot["emotion"]    —— 行为情绪结构
    """

    def __init__(self, config: DataSourceConfig):
        self.market = config.market
        self.ds_name = config.ds_name

        self.cache_root = config.cache_root
        self.history_root = config.history_root

        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        # 内部调用 market_sentiment DS
        self.sentiment_ds = MarketSentimentDataSource(
            DataSourceConfig(
                market=config.market,
                ds_name="sentiment",
            )
        )

        LOG.info(f"[DS.UnifiedEmotion] Init: cache={self.cache_root}")

    # ------------------------------------------------------------
    # 主接口（被 fetcher 调用）
    # ------------------------------------------------------------
    def get_blocks(
        self,
        snapshot: Dict[str, Any],
        trade_date: str,
        refresh_mode: str,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        返回 (sentiment_block, emotion_block)
        """
        LOG.info("[DS.UnifiedEmotion] Build emotion blocks .")

        # ❗V12 版本：不再依赖 snapshot['spot'] DataFrame，
        # MarketSentimentDataSource 自己负责从底层数据构建宽度。
        spot_df = None

        # ---------- 1) 获取市场宽度 sentiment ----------
        # 这里仍然保留 spot_df 参数（传 None），以兼容现有
        # MarketSentimentDataSource 接口，不修改其底层机制。
        sentiment_block = self.sentiment_ds.get_sentiment_block(
            spot_df=spot_df,
            trade_date=trade_date,
            refresh_mode=refresh_mode,
        )

        # ---------- 2) 计算行为情绪结构 ----------
        # 行为情绪只基于：
        #   - index_core
        #   - turnover
        #   - sentiment_block (adv/dec)
        #   - north_nps
        emotion_block = self._build_behavior_emotion(
            spot_df=spot_df,
            sentiment_block=sentiment_block,
            snapshot=snapshot,
        )

        LOG.info(f"[DS.UnifiedEmotion] sentiment={sentiment_block}")
        LOG.info(f"[DS.UnifiedEmotion] emotion={emotion_block}")

        return sentiment_block, emotion_block

    # ------------------------------------------------------------
    # 行为情绪结构（基于宽度 + 成交额 + 北向）
    # ------------------------------------------------------------
    # ------------------------------------------------------------
    # 行为情绪结构（基于宽度 + 成交额 + 北向）
    # ------------------------------------------------------------
    def _build_behavior_emotion(
        self,
        spot_df,
        sentiment_block: Dict[str, Any],
        snapshot: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        V12：行为情绪构建版本（FULL VERSION）
        - 不再依赖 spot_df
        - 使用 index_core / turnover / sentiment_block / north_nps 计算行为情绪
        - 所有字段统一安全 float 转换，解决 None / np.float64 / str 问题
        """

        # ============================================================
        # 1) 指数方向（HS300）
        # ============================================================
        index_core = snapshot.get("index_core", {})
        hs300 = index_core.get("hs300", {})

        raw_pct = hs300.get("pct", 0.0)
        try:
            index_pct = float(raw_pct)
        except Exception:
            index_pct = 0.0

        if index_pct > 0:
            index_label = "Positive"
        elif index_pct < 0:
            index_label = "Negative"
        else:
            index_label = "Neutral"

        # ============================================================
        # 2) 成交额（亿元）
        # ============================================================
        turn = snapshot.get("turnover", {}) or {}
        raw_total = turn.get("total", 0.0)

        try:
            volume_f = float(raw_total)
        except Exception:
            volume_f = 0.0

        if volume_f > 15000:
            volume_label = "High Volume"
        elif volume_f < 8000:
            volume_label = "Low Volume"
        else:
            volume_label = "Normal Volume"

        # ============================================================
        # 3) 市场宽度 adv/dec（来自 MarketSentimentDataSource）
        # ============================================================
        adv = sentiment_block.get("adv", 0) or 0
        dec = sentiment_block.get("dec", 0) or 0

        try:
            adv_f = float(adv)
            dec_f = float(dec)
        except Exception:
            adv_f = 0.0
            dec_f = 0.0

        if adv_f > dec_f * 1.2:
            breadth_label = "Positive Breadth"
        elif dec_f > adv_f * 1.2:
            breadth_label = "Negative Breadth"
        else:
            breadth_label = "Neutral Breadth"

        # ============================================================
        # 4) 北向资金强弱
        # ============================================================
        north = snapshot.get("north_nps", {}) or {}

        raw_strength = (
            north.get("strength")
            if "strength" in north
            else north.get("strength_today", 0.0)
        )

        try:
            north_strength = float(raw_strength)
        except Exception:
            north_strength = 0.0

        if north_strength > 0.5:
            north_label = "Inflow"
        elif north_strength < -0.5:
            north_label = "Outflow"
        else:
            north_label = "Neutral"

        # ============================================================
        # 5) 主力资金（未来支持，这里保持中性）
        # ============================================================
        main_force = 0.0
        main_force_label = "Neutral"

        # ============================================================
        # 6) 衍生品情绪（期权/波动率，目前保持中性）
        # ============================================================
        derivative_label = "Neutral"

        # ============================================================
        # 7) 合成 emotion block（最终返回）
        # ============================================================
        block = {
            "index_pct": index_pct,
            "index_label": index_label,

            "volume_e9": volume_f,
            "volume_label": volume_label,

            "adv": adv_f,
            "dec": dec_f,
            "breadth_label": breadth_label,

            "north_strength": north_strength,
            "north_label": north_label,

            "main_force": main_force,
            "main_force_label": main_force_label,

            "derivative_label": derivative_label,
        }

        return block
 
    # ------------------------------------------------------------
    # 中性 emotion
    # ------------------------------------------------------------
    def _neutral_emotion(self):
        return {
            "index_pct": 0.0,
            "index_label": "Neutral",
            "volume_e9": 0.0,
            "volume_label": "Neutral Volume",
            "adv": 0,
            "dec": 0,
            "breadth_label": "Neutral",
            "north_strength": 0.0,
            "north_label": "Neutral",
            "main_force": 0.0,
            "main_force_label": "Neutral",
            "derivative_label": "Neutral",
        }
