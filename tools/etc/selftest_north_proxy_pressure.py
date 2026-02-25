# -*- coding: utf-8 -*-
"""
Self-test for NorthProxyPressureFactor using local cached JSON samples.

Run:
  python selftest_north_proxy_pressure.py
"""
import json
from pathlib import Path

from core.factors.cn.north_proxy_pressure_factor import NorthProxyPressureFactor

def load(p):
    return json.loads(Path(p).read_text(encoding="utf-8"))

def main():
    # simulate input_block["north_nps_raw"] structure: dict of proxies
    raw = {
        "kc50": load("159915_SZ_2025-12-29.json"),
        "large": load("510050_SS_2025-12-29.json"),
        "hs300": load("510300_SS_2025-12-29.json"),
    }
    f = NorthProxyPressureFactor()
    fr = f.compute({"north_nps_raw": raw})
    print("name:", fr.name)
    print("score:", fr.score, "level:", fr.level)
    print("details keys:", list(fr.details.keys()))
    print("quality:", fr.details.get("quality_score"), "pressure:", fr.details.get("pressure_score"), "p_level:", fr.details.get("pressure_level"))
    print("reasons:", fr.details.get("reasons"))

if __name__ == "__main__":
    main()
