
from __future__ import annotations
from typing import Dict, Any, Tuple

from unified_risk.common.logging_utils import log_info
from unified_risk.core.datasources.sector_fetcher import fetch_sector_flow_topN


def compute_sector_rotation(snapshot: Dict[str, Any]) -> Tuple[float, str]:
    boards = fetch_sector_flow_topN(80)
    if not boards:
        desc = "板块轮动数据暂不可用，按中性处理（10/20）。"
        return 10.0, desc

    total = len(boards)
    pos = [b for b in boards if b["main_flow"] > 0]
    pos_ratio = len(pos) / total if total else 0.0

    pos_sorted = sorted(pos, key=lambda x: x["main_flow"], reverse=True)
    total_in = sum(b["main_flow"] for b in pos_sorted)
    top3_in = sum(b["main_flow"] for b in pos_sorted[:3])
    concentration = top3_in / total_in if total_in else 0.0

    breadth = max(0.0, 1.0 - abs(pos_ratio - 0.5) / 0.3)
    conc = max(0.0, 1.0 - abs(concentration - 0.5) / 0.3)

    score = (breadth * 0.6 + conc * 0.4) * 20.0
    score = max(min(score, 20.0), 0.0)

    desc = (
        f"今日 {len(pos)}/{total} 板块主力净流入为正（多头比 {pos_ratio*100:.1f}%），"
        f"前三板块集中度 {concentration*100:.1f}% → 板块轮动得分 {score:.1f}/20。"
    )

    log_info(f"[SECTOR] score={score:.2f}")
    return score, desc
