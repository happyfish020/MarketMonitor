def render_toehold_line(governance: dict) -> str:
    th = (governance or {}).get("toehold_exception")
    if not isinstance(th, dict):
        return ""
    permit = str(th.get("permit") or "").upper()
    if permit != "YES":
        return ""
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
    return f"- 脚尖仓例外：允许=YES（仅白名单：{', '.join(names) if names else 'whitelist'}；最多{max_lots}手；不可加仓/追涨/轮动）"
