ETF / YF 缓存规范（UnifiedRisk V11.7.2）
-----------------------------------------
🎯 文档目的

本规范用于定义 UnifiedRisk（CN 市场）在使用 Yahoo Finance（YF）数据源时的统一缓存机制。
ETF 数据是北向资金代理、海外引导、指数序列等多个因子的基础，因此缓存必须 稳定、可复用、可控、支持 FORCE 刷新。

本规范同时适用于：

etf_north_proxy.py

index_series_client.py

global_lead_client.py

futures_client.py

所有后续基于 symbol 的 datasource（如 macro、vix、sector 等）

1. 缓存整体设计原则
(1) 可复用（Reuse-First）

所有使用 YF 的 datasource 都必须：

先查当日缓存（JSON）

缓存存在 → 直接返回

缓存不存在 → 下载数据 → 写入缓存 → 返回

避免多因子重复打 YF（速度快、稳定、减 API 风险）。

(2) FORCE 刷新语义（Refresh-Once）

在 --force 或 force_refresh=True 模式下：

仅在当日第一次调用时删除缓存 JSON

后续调用不再重复删除（避免多次下载）

示例行为：

运行次数	FORCE 模式	ETF JSON 删除	YF 下载	后续因子复用
第一次	True	是	是	是
第二次	True	否	否（复用缓存）	是
第三次	False	否	否	是

进程级别的 _REFRESHED = False → True 负责保证“只删一次”。

(3) 缓存路径统一规范

所有 symbol 缓存路径由：

core.adapters.cache.symbol_cache.get_symbol_daily_path(market, trade_date, symbol, kind)


生成：

示例（当日 = 2025-12-05）：

data/cache/day_cn/20251205/etf_510300_SS.json
data/cache/day_cn/20251205/etf_159901_SZ.json
data/cache/day_cn/20251205/index_SH000300.json
data/cache/day_cn/20251205/global_US_SPX.json


命名规则：

{kind}_{symbol_normalized}.json


如：
510300.SS → 510300_SS
^GSPC → GSPC
HSI.HK → HSI_HK

(4) 缓存写入 JSON 格式

ETF 数据示例：

{
  "date": "2025-12-05",
  "close": 4.32,
  "pct_change": 0.88,
  "volume": 123456789,
  "turnover_e9": 35.75
}


其它数据源应采用同样结构化格式：

有 date

有 close/pct_change

有基于业务需要的结构化字段（如 turnover、volume、yield 等）

(5) 错误容错机制

YF 超时 → 返回空结构（交由因子 → 50 分 neutral）

数据缺失 → 写入 {"error": "...", "msg": "..."}

FORCE + 下载失败 → 缓存可能为空，但不会影响系统继续运行

2. etf_north_proxy 的标准流程
输入：
    trade_date
    force_refresh

步骤：
    1. 如 force_refresh 且本进程未刷新过 → 删除当日 ETF 缓存 JSON
    2. symbols = 从 symbols.yaml 加载北向 ETF proxy 列表
    3. 对每个 symbol：
           调用 get_etf_daily(symbol, trade_date)
           → 自动使用 symbol_cache 路径
    4. 整合：
           etf_flow_e9
           total_turnover_e9
           hs300_proxy_pct
           details
    5. 返回结构化 dict


输出示例：

{
  "etf_flow_e9": 0.32,
  "total_turnover_e9": 35.75,
  "hs300_proxy_pct": 0.88,
  "details": [
    {"symbol": "510300.SS", "pct_change": 0.88, "turnover_e9": 20.1, "flow_e9": 0.12},
    {"symbol": "159901.SZ", "pct_change": 0.75, "turnover_e9": 15.6, "flow_e9": 0.20}
  ]
}

3. 其它 datasource 必须复用此规范
包括：
datasource	是否使用 symbol_cache	FORCE 刷新	说明
index_series_client	必须	可选	指数序列缓存
global_lead_client	必须	可选	SPX/NDX/VIX/USDCNH
futures_client	必须	可选	IF/IM/IH 等基差
macro_vix_client	必须	可选	宏观风向因子