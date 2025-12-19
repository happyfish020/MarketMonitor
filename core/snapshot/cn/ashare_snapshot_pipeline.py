# -*- coding: utf-8 -*-
"""
UnifiedRisk V12
CN A-Share Snapshot Pipeline

职责（冻结）：
- 作为 Snapshot Facade / Pipeline
- 负责：
    Fetcher → raw snapshot
    SnapshotBuilder → 结构修复
- 对 Engine 暴露统一接口
"""

from typing import Dict, Any, Optional

from core.adapters.fetchers.cn.ashare_fetcher import AshareDataFetcher
from core.snapshot.ashare_snapshot import AshareSnapshotBuilder


class AshareSnapshotPipeline:
    """
    CN A-Share Snapshot Pipeline（V12）
    """

    def __init__(self) -> None:
        self._builder = AshareSnapshotBuilder()

    def build(
        self,
        *,
        trade_date: str,
        refresh_mode: str,
        market: str = "CN_A",
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Engine 统一调用入口（冻结）
        """

        # 1️⃣ Fetch raw snapshot
        fetcher = AshareDataFetcher(
            trade_date=trade_date,
            refresh_mode=refresh_mode,
        )
        raw_snapshot = fetcher.prepare_daily_market_snapshot()

        # 2️⃣ Build / fix structure
        snapshot = self._builder.build(raw_snapshot)

        return snapshot
