# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - FetcherBase

职责（冻结）：
- 统一 Fetcher 的最小公共属性
- 提供 market / trade_date / refresh_controller
- 不引入任何业务逻辑
- 不接 Router / Snapshot / Factor
"""

from __future__ import annotations

from typing import Optional

from core.utils.logger import get_logger

LOG = get_logger("Fetcher.Base")


class FetcherBase:
    """
    Base class for all market fetchers.

    设计原则（V12 铁律）：
    - 只做“数据装配入口”
    - 不做数据处理
    - 不做结构判断
    """

    def __init__(
        self,
        market: str,
        trade_date: str,
        refresh_mode: str,
    ):
        """
        Parameters
        ----------
        market : str
            市场标识，例如 "cn" / "glo"
        trade_date : str
            交易日（YYYY-MM-DD）
        refresh_controller : RefreshController
            刷新控制器（V12 已冻结）
        """
        self.market = market
        self.trade_date = trade_date
        self.refresh_mode=refresh_mode

        LOG.info(
            "[FetcherBase] init market=%s trade_date=%s refresh_mode=%s",
            market,
            trade_date,
            getattr(self.refresh_mode, "refresh_mode", "none"),
        )

    # ------------------------------------------------------
    # 子类必须实现
    # ------------------------------------------------------
    def prepare_daily_market_snapshot(self):
        """
        子类必须实现：
        - 返回 Dict[str, Any]
        - 内容必须是 DS raw
        """
        raise NotImplementedError("prepare_daily_market_snapshot must be implemented")
