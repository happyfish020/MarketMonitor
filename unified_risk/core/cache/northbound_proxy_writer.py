from __future__ import annotations
from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, Any

from unified_risk.core.cache.cache_writer import smart_write_ashare_northbound

@dataclass
class NPSConfig:
    w_f62: float = 0.70
    w_hs300: float = 0.30
    strong_pos: float = 0.40
    weak_pos: float = 0.15
    weak_neg: float = -0.15
    strong_neg: float = -0.40

def compute_f62_ratio(f62_sh: float, f62_sz: float) -> float:
    total = float(f62_sh) + float(f62_sz)
    scale = 2e9
    if total == 0:
        return 0.0
    return total / (abs(total) + scale)

def compute_nps(f62_sh: float, f62_sz: float, hs300_ret: float, cfg: NPSConfig | None = None) -> float:
    if cfg is None:
        cfg = NPSConfig()
    f62_ratio = compute_f62_ratio(f62_sh, f62_sz)
    return cfg.w_f62 * f62_ratio + cfg.w_hs300 * float(hs300_ret)

def score_nps(nps: float, cfg: NPSConfig | None = None) -> int:
    if cfg is None:
        cfg = NPSConfig()
    if nps >= cfg.strong_pos:
        return 2
    if nps >= cfg.weak_pos:
        return 1
    if nps <= cfg.strong_neg:
        return -2
    if nps <= cfg.weak_neg:
        return -1
    return 0

class NorthboundProxyWriter:
    @staticmethod
    def build_payload(trade_date: date, f62_sh: float, f62_sz: float, hs300_ret: float, cfg: NPSConfig | None = None) -> Dict[str, Any]:
        if cfg is None:
            cfg = NPSConfig()
        f62_ratio = compute_f62_ratio(f62_sh, f62_sz)
        nps = compute_nps(f62_sh, f62_sz, hs300_ret, cfg)
        nps_score = score_nps(nps, cfg)
        return {
            "date": trade_date.strftime("%Y-%m-%d"),
            "source": "proxy",
            "f62_sh": float(f62_sh),
            "f62_sz": float(f62_sz),
            "f62_total": float(f62_sh) + float(f62_sz),
            "f62_ratio": float(f62_ratio),
            "hs300_ret": float(hs300_ret),
            "nps": float(nps),
            "nps_score": int(nps_score),
        }

    @staticmethod
    def write(trade_date: date, now_bj: datetime, f62_sh: float, f62_sz: float, hs300_ret: float, cfg: NPSConfig | None = None):
        payload = NorthboundProxyWriter.build_payload(trade_date, f62_sh, f62_sz, hs300_ret, cfg)
        return smart_write_ashare_northbound(trade_date, payload, now_bj)
