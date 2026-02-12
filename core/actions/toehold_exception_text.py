from __future__ import annotations
from typing import Any, Dict, List, Tuple

def apply_toehold_exception_text(*, gate: str, governance: Dict[str, Any] | None, allowed: List[str], forbidden: List[str], limits: str, conditions: str) -> Tuple[List[str], List[str], str, str]:
    """Inject Toe-hold exception wording into ActionHint without changing Gate semantics.
    Only adds explicit, auditable exception text; does not remove existing bans.
    """
    if not isinstance(governance, dict):
        return allowed, forbidden, limits, conditions
    th = governance.get("toehold_exception")
    if not isinstance(th, dict):
        return allowed, forbidden, limits, conditions
    if str(th.get("permit") or "").upper() != "YES":
        return allowed, forbidden, limits, conditions

    wl = th.get("whitelist") or []
    names = []
    for x in wl:
        if isinstance(x, dict):
            alias = x.get("alias")
            sym = x.get("symbol")
            if alias and sym:
                names.append(f"{alias}({sym})")
            elif alias:
                names.append(str(alias))
            elif sym:
                names.append(str(sym))
    max_lots = th.get("max_lots", 1)
    # Add to allowed as an explicit exception
    msg = f"允许刹车仓脚尖仓占位（仅白名单、最多{max_lots}手、不可加仓/追涨/轮动）"
    if msg not in allowed:
        allowed.append(msg)

    # Keep forbidden list intact, but add clarity
    if "脚尖仓不得加仓（只允许1手占位）" not in forbidden:
        forbidden.append("脚尖仓不得加仓（只允许1手占位）")

    # Enhance limits/conditions
    limits = limits + f" ｜脚尖仓例外：{', '.join(names) if names else 'whitelist'}"
    conditions = conditions + " ｜TOEHOLD=YES"
    return allowed, forbidden, limits, conditions
