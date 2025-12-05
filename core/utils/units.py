"""
统一单位换算模块（UnifiedRisk v11 全系统标准）

所有因子内部一律使用 “亿元（e9）” 为基础单位。
外部数据进入系统时必须做单位转换。
"""

def yuan_to_e9(v: float) -> float:
    """元 → 亿元"""
    return float(v) / 1e8


def wan_to_e9(v: float) -> float:
    """万元 → 亿元"""
    return float(v) / 1e4


def million_to_e9(v: float) -> float:
    """百万 → 亿元"""
    return float(v) / 100.0


def raw_to_e9(v: float, unit: str) -> float:
    """
    按指定单位转换到“亿元”
    unit ∈ {"yuan", "wan", "million", "e9"}
    """
    if unit == "yuan":
        return yuan_to_e9(v)
    if unit == "wan":
        return wan_to_e9(v)
    if unit == "million":
        return million_to_e9(v)
    return float(v)  # 认为已是 e9
