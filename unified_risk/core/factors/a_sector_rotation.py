def compute_sector_rotation(snapshot: dict):
    index = snapshot.get("index", {})
    sh = float(index.get("sh_pct", 0.0) or 0.0)
    cyb = float(index.get("cyb_pct", 0.0) or 0.0)

    diff = cyb - sh        # 越大越偏成长，越小越偏价值

    # 归一化为 0-20
    score = 10 + diff * 40
    score = max(0, min(score, 20))

    if diff > 0.5:
        desc = "成长风格明显占优"
    elif diff > 0.0:
        desc = "成长风格略占优"
    elif diff > -0.5:
        desc = "价值风格略占优"
    else:
        desc = "价值风格明显占优"

    return score, desc
