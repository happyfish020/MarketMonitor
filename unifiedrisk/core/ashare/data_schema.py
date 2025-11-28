"""
UnifiedRisk AShare Daily Snapshot Schema (v5.0.2)

作用：
- 约定 DataFetcher / Engine / RiskScorer 之间传递的日级快照(raw)结构
- 为后续因子扩展、回测、Web 输出提供稳定的数据契约
"""

from typing import Dict, Any


# 顶层 raw["meta"] 结构
META_SCHEMA = {
    "version": str,        # 例如 "UnifiedRisk_v5.0.1"
    "date": str,           # "YYYY-MM-DD"
    "bj_time": str,        # 北京时间 ISO 字符串
}


# 日级快照的统一 Schema（raw 本体，而非最终 payload）
DAILY_SNAPSHOT_SCHEMA = {
    "index": dict,         # 指数数据（上证 / 深证 / 创业板）
    "sse": dict,           # 上交所成交额 / 成交量
    "szse": dict,          # 深交所成交额 / 成交量
    "breadth": dict,       # 市场宽度（涨跌家数 / 平均涨跌幅）
    "north": dict,         # 北向资金（整体 + 近几日趋势）
    "margin": dict,        # 两融数据（融资融券余额等）
    "mainflow": dict,      # 全市场主力资金数据
    "sector": dict,        # 行业主力资金数据
}


def ensure_meta(meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    填补/规范 raw["meta"] 字段：
    - 缺失字段使用安全默认值
    - 类型不匹配时做轻微纠正（例如数字转 str）
    """
    out: Dict[str, Any] = {}

    version = meta.get("version") if isinstance(meta, dict) else None
    if not isinstance(version, str):
        version = "UnifiedRisk_v5.0.1"
    out["version"] = version

    date = meta.get("date") if isinstance(meta, dict) else None
    if not isinstance(date, str):
        # 留空字符串而不是 None，避免下游 string 操作异常
        date = ""
    out["date"] = date

    bj_time = meta.get("bj_time") if isinstance(meta, dict) else None
    if not isinstance(bj_time, str):
        bj_time = ""
    out["bj_time"] = bj_time

    return out


def ensure_daily_snapshot(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    确保 raw 至少包含 DAILY_SNAPSHOT_SCHEMA 中的所有 key。
    对缺失 key 自动补一个空 dict，保证 RiskScorer / ReportWriter 不会因 KeyError 崩溃。
    """
    if not isinstance(raw, dict):
        raw = {}

    out: Dict[str, Any] = dict(raw)  # 浅拷贝

    for key, typ in DAILY_SNAPSHOT_SCHEMA.items():
        value = out.get(key)
        if not isinstance(value, typ):
            # 默认给一个空 dict，方便后续 .get() 链式调用
            out[key] = typ()  # dict() / list() 等

    return out
