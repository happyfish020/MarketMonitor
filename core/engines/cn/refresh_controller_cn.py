from dataclasses import dataclass
from datetime import datetime, date

from core.utils.time_utils import is_intraday_trading_time
from core.utils.trade_calendar import get_trade_date_daily, is_trading_day
from core.utils.logger import log


@dataclass
class RefreshPlanCN:
    trade_date: date
    is_trading_day_now: bool
    should_refresh_daily: bool
    should_refresh_intraday: bool


class RefreshControllerCN:
    def __init__(self, bj_now: datetime) -> None:
        self.bj_now = bj_now
        # 日级 trade_date 使用方案 B 逻辑
        self.trade_date = get_trade_date_daily(bj_now)

    def build_refresh_plan(
        self,
        force_daily: bool = False,
        force_intraday: bool = False,
        has_daily_cache: bool = False,
        has_intraday_cache: bool = False,
    ) -> RefreshPlanCN:
        today = self.bj_now.date()
        is_trading_today = is_trading_day(today)
        trading_time = is_intraday_trading_time(self.bj_now)

        # 日级刷新策略：
        # - 有 --force 就一定刷新
        # - 否则：只要没有日级缓存就刷新一次
        should_refresh_daily = False
        if force_daily:
            should_refresh_daily = True
        else:
            if not has_daily_cache:
                should_refresh_daily = True

        # 盘中刷新策略（Q2：在交易时间用 today，否则用缓存/T）
        should_refresh_intraday = False
        if force_intraday:
            should_refresh_intraday = True
        else:
            if is_trading_today and trading_time and (not has_intraday_cache):
                should_refresh_intraday = True

        plan = RefreshPlanCN(
            trade_date=self.trade_date,
            is_trading_day_now=is_trading_today,
            should_refresh_daily=should_refresh_daily,
            should_refresh_intraday=should_refresh_intraday,
        )
        log(f"[CN RefreshPlan] now={self.bj_now}, plan={plan}")
        return plan
