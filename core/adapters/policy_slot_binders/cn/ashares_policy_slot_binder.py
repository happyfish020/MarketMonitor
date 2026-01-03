from __future__ import annotations

import logging
from typing import Dict

from core.adapters.policy_slot_binders.binder_base import PolicySlotBinderBase
from core.factors.factor_result import FactorResult
from core.regime.observation.watchlist.watchlist_ob_builder import WatchlistObservationBuilder

LOG = logging.getLogger(__name__)


class ASharesPolicySlotBinder(PolicySlotBinderBase):
    """
    """