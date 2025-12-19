"""
Structural Context

Role:
    Structural Context is a single, final structural background container.
    It assembles multiple structural pillars into one context block
    for upper layers (e.g., Phase-3).

Design Freeze:
    - SINGLE implementation (no interface, no factory, no subclassing)
    - Assembly-only (no calculation, no scoring)
    - Health is a veto signal, NOT a decision signal
    - Must NOT influence Phase-2 Gate behavior

Explicitly Forbidden:
    - Multiple implementations or strategy variants
    - Generating or modifying Gate / Regime
    - Predictive logic or trend inference
    - Reading price, return, or Phase-2 outputs
    - Weighting, voting, or scoring mechanisms
"""

from typing import Dict, Any


class StructuralContext:
    """
    StructuralContext (FINAL)

    Single implementation by design.
    Extensions are allowed ONLY via:
        - additional pillars (input side), or
        - upper-layer interpretation (Phase-3+).
    """

    def __init__(self) -> None:
        # No configuration, no state, no side effects.
        pass

    def assemble(self, pillars: Dict[str, Any]) -> Dict[str, Any]:
        """
        Assemble structural context from given pillars.

        Parameters
        ----------
        pillars : Dict[str, Any]
            Mapping of pillar name -> pillar result object.
            Must be treated as read-only.

        Returns
        -------
        Dict[str, Any]
            Structural context dictionary with health implemented (C-2).
        """

        # -------- C-1: Assembly (unchanged) --------
        def _extract(pillar: Any) -> Dict[str, Any]:
            if pillar is None:
                return {
                    "state": None,
                    "since": None,
                    "confidence": None,
                    "notes": [],
                }
            return {
                "state": getattr(pillar, "state", None),
                "since": getattr(pillar, "since", None),
                "confidence": getattr(pillar, "confidence", None),
                "notes": [],
            }

        ctx = {
            "breadth_damage": _extract(pillars.get("breadth_damage")),
            "participation": _extract(pillars.get("participation")),
            "index_sector_corr": _extract(pillars.get("index_sector_corr")),
            "health": None,
            "tags": [],
        }

        # -------- C-2: Health (veto-only) --------
        # Helper getters (read-only, tolerant to missing attrs)
        def _get_attr(pillar: Any, name: str, default=None):
            return getattr(pillar, name, default) if pillar is not None else default

        breadth = pillars.get("breadth_damage")
        participation = pillars.get("participation")
        corr = pillars.get("index_sector_corr")

        # FAIL-1: Inherited failure
        if (
            _get_attr(breadth, "health") == "FAIL"
            or _get_attr(participation, "health") == "FAIL"
            or _get_attr(corr, "health") == "FAIL"
        ):
            ctx["health"] = "FAIL"
            return ctx

        # FAIL-2: Structural flapping (delegated to pillars)
        if (
            _get_attr(breadth, "is_flapping", False)
            or _get_attr(participation, "is_flapping", False)
            or _get_attr(corr, "is_flapping", False)
        ):
            ctx["health"] = "FAIL"
            return ctx

        # FAIL-3: Consistency fail (white-listed, long-term)
        # Condition (must be provided by upstream as a sustained fact):
        #   Breadth.state == CONFIRMED_DAMAGED
        #   Participation.state == STRONG
        #   Duration >= N  (duration qualification handled upstream)
        if (
            _get_attr(breadth, "state") == "CONFIRMED_DAMAGED"
            and _get_attr(participation, "state") == "STRONG"
            and _get_attr(breadth, "sustained", False) is True
        ):
            ctx["health"] = "FAIL"
            return ctx

        # HEALTHY fallback (structural stability, not market health)
        # Requirements:
        #   - all three pillars exist
        #   - state present
        #   - since present (monotonicity validated upstream)
        if (
            breadth is not None
            and participation is not None
            and corr is not None
            and ctx["breadth_damage"]["state"] is not None
            and ctx["participation"]["state"] is not None
            and ctx["index_sector_corr"]["state"] is not None
            and ctx["breadth_damage"]["since"] is not None
            and ctx["participation"]["since"] is not None
            and ctx["index_sector_corr"]["since"] is not None
        ):
            ctx["health"] = "HEALTHY"
        else:
            # Defensive fallback (should rarely happen)
            ctx["health"] = "FAIL"

        return ctx
