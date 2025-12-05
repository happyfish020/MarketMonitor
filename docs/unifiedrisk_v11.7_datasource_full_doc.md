UnifiedRisk V11.7 — Datasource Layer Full Documentation

（可保存为：docs/unifiedrisk_v11.7_datasource_full_doc.md）

更新时间：2025-12-07
作者：Fisher + ChatGPT（V11.7 标准共同制定）

# 📑 TOC — 全目录结构

Introduction（简介）

Architecture Overview（架构总览）

Role of Datasource Layer（核心职责）

SymbolCache Mechanism（符号缓存机制）

Snapshot Interaction（与 Snapshot 的关系）

Unified Data Pipeline（数据总流程图）

Datasource Modules Specification（逐模块说明）
 7.1 ETF North Proxy
 7.2 Margin (RZRQ)
 7.3 Index Series
 7.4 Global Lead Series
 7.5 Futures Series（V11.7 修正版）
 7.6 zh_spot / MarketDB
 7.7 yf_client_cn

Folder Structure（目录结构规范）

Interfaces & I/O Contract（输入输出统一协议）

Logging Standard（日志规范）

Retry/Timeout Standards（重试/超时规范）

Forbidden Behaviors（禁止行为）

Compliance Checklist（合规检查清单）

Future Extensions（未来扩展建议）

# 1. Introduction（简介）

Datasource 层是 UnifiedRisk V11.x 的底层数据抽象层，
负责：

调用 API（YF / Eastmoney / Index / Futures）

写入 symbolcache（单标缓存）

返回标准化结构

绝不写 snapshot

它是整个系统的基础数据提供者。

因子（Factor）、评分（Scorer）、预测（T+1/T+5）、报告（Report）
全部依赖此层提供一致、可复现、可缓存的数据。

# 2. Architecture Overview（架构总览）

Datasource 层 → Fetcher → Snapshot → Factor → Scorer → Output

Datasource 层的关键价值：

统一 API 层隔离

缓存（symbolcache）统一管理

市场数据可复现（deterministic）

与 snapshot、因子解耦（松耦合）

# 3. Role of Datasource Layer（核心职责）

Datasource 的职责只有三项：

职责	描述
1. 获取数据	从 YF、Eastmoney、内部数据源读取
2. 写入 symbolcache	单 symbol 缓存文件（如 macro_VIX.json）
3. 返回 dict/list	让 fetcher 整合到 snapshot

Datasource 不负责：

❌ 写 snapshot

❌ 写 datasource 级缓存文件（如 global_lead.json）

❌ 直接使用 yfinance（必须走 yf_client_cn）

❌ 做多日分析（属于 factor 层）

# 4. SymbolCache Mechanism（符号缓存机制）

符号缓存路径（标准化）：

data/cache/day_cn/YYYYMMDD/{kind}_{normalized_symbol}.json


示例：

etf_510300_SS.json
macro_^GSPC.json
macro_^VIX.json
index_sh000300.json
futures_IF00_CFE.json


symbol 标准化规则（symbol_cache.py）：

原 symbol	转换后
510300.SS	510300_SS
^VIX	VIX
GC=F	GC_F
IF00.CFE	IF00_CFE

symbolcache 是 datasource 唯一允许写入的缓存层。

# 5. Snapshot Interaction（与 Snapshot 的关系）

Snapshot：

ashare_daily_snapshot.json


由 Fetcher 写入，Datasource 不能写。

Snapshot 是：

因子层的唯一入口

数据汇总（index + breadth + turnover + margin + global lead）

Datasource 负责“叶子节点数据” → symbolcache
Fetcher 负责“组合数据树” → snapshot

# 6. Unified Data Pipeline（数据总流程图）
flowchart TD

    %% YF Client 层
    subgraph YC["YF Client（唯一 yfinance 入口）"]
        YC1[get_etf_daily]
        YC2[get_macro_daily]
    end

    %% Symbol Cache 层
    subgraph SC["Symbol Cache（单标缓存）"]
        SC1[etf_510300_SS.json]
        SC2[macro_^GSPC.json]
        SC3[index_sh000300.json]
        SC4[futures_IF00_CFE.json]
    end

    YC1 --> SC
    YC2 --> SC

    %% Datasource 层
    subgraph DS["Datasource（不写 snapshot）"]
        D1[ETF North Proxy]
        D2[Global Lead]
        D3[Index Series]
        D4[Margin Series]
        D5[Futures Series]
    end

    SC --> D1
    SC --> D2
    SC --> D3
    SC --> D4
    SC --> D5

    %% Breadth 专属
    subgraph BR["Breadth（来自 zh_spot，不属于 datasource）"]
        BR1[get_breadth_summary]
    end

    %% Fetcher 层
    subgraph F["Fetcher（唯一写 snapshot）"]
        F1[assemble snapshot]
    end

    D1 --> F1
    D2 --> F1
    D3 --> F1
    D4 --> F1
    D5 --> F1
    BR1 --> F1

    F1 --> SNAPSHOT[ashare_daily_snapshot.json]

    %% Factor 层
    subgraph FT["Factor（只读 snapshot）"]
        FT1[Turnover]
        FT2[Margin]
        FT3[IndexTrend]
        FT4[GlobalLead]
        FT5[FuturesBasis]
    end

    SNAPSHOT --> FT1
    SNAPSHOT --> FT2
    SNAPSHOT --> FT3
    SNAPSHOT --> FT4
    SNAPSHOT --> FT5

# 7. Datasource Modules Specification（逐模块说明）
## 7.1 ETF North Proxy（已完全符合）

用途：

获取 ETF（510300.SS, 159901.SZ）

北向代理流入指标

写入 symbolcache（etf_*.json）

禁止：

❌ 不写 etf_proxy.json

✔ fetcher 写 snapshot["etf_proxy"]

## 7.2 Margin (RZRQ)（合规）

EastmoneyMarginClientCN：

retry=3, sleep=10, timeout=20

不写 datasource JSON

返回融资、融券总额的序列（单位 e9）

fetcher 写 snapshot["margin"]

可选：

是否将每日日级写入 symbolcache（不强制）

## 7.3 Index Series（已修复为合规）

IndexSeriesClient：

使用 get_macro_daily

写 symbolcache

不写 index_series.json

返回：

{
  "sh": {"pct_change": ...},
  "sz": {...},
  "hs300": {...}
}

## 7.4 Global Lead（已修复为合规）

GlobalLeadClient：

使用 get_macro_daily

写 symbolcache

不写 global_lead.json

返回：

{"spx": ..., "ndx": ..., "hsi": ...}

## 7.5 Futures Series（重大修复，现已完全合规）

之前版本严重违反规范，现已修复。

V11.7 Final 行为：

使用 yfinance（非 akshare）

retry / fallback / timeout

写入 symbolcache（futures_IF00_CFE.json）

不写 Futures-index.json

返回：

{
 "if": {future_pct, index_pct, basis_pct},
 "ih": {...},
 "im": {...}
}


由 fetcher 统一写入 snapshot["futures"]。

## 7.6 zh_spot / MarketDB（合规）

提供：

turnover（成交额）

breadth（adv/dec）

Breadth 必须来自 zh_spot → snapshot，不经 datasource。

## 7.7 yf_client_cn（合规）

功能：

统一 YF 调用层

自动写 symbolcache

retry / fallback / timeout

被所有 datasource 依赖（必须使用）

这是整个 V11 的数据稳定基础。

# 8. Folder Structure（目录规范）
core/
  adapters/
    datasources/
      cn/
        etf_north_proxy.py
        em_margin_client.py
        index_series_client.py
        global_lead_client.py
        futures_client.py    ← 修复后
        market_db_client.py
        yf_client_cn.py
        zh_spot_utils.py


废弃文件：

❌ breadth_series_client.py（已删除）

❌ index_series.json / global_lead.json（不允许存在）

# 9. Interfaces & I/O Contract（数据输入输出协议）

所有 datasource 必须满足：

输入：
fetch(trade_date: Date)

输出 dict：
{
  "key": value,
  ...
}


或序列：

[
  {...},
  {...}
]

禁止输出：

❌ DataFrame

❌ 原始 JSON

❌ 文件路径

❌ 多层 snapshot-like 结构

# 10. Logging Standard（日志标准）

必须使用：

log("[MODULE] message")


禁止：

❌ print

❌ logging.getLogger

❌ 大量 dump（例如 DataFrame）

# 11. Retry/Timeout Standards（统一重试规范）

所有外部 API（YF / Eastmoney）必须遵守：

retry = 3

sleep = 10s

timeout = 20s

fallback（最近剔除缺失的交易日）

# 12. Forbidden Behaviors（禁止行为）
行为	后果
写 datasource JSON（global_lead.json）	❌ 破坏缓存一致性
datasource 写 snapshot	❌ 破坏分层
因子读取 symbolcache	❌ 破坏抽象层
datasource 使用 yfinance（不走 yf_client_cn）	❌ 数据不可控
使用 akshare 获取关键行情	❌ 不稳定 / 无 retry
写 breadth_series.json	❌ breadth 不属于 datasource
# 13. Compliance Checklist（合规检查表）
检查项	是否符合
所有 YF 调用走 yf_client_cn	✔
datasource 不写 snapshot	✔
datasource 不写自有 JSON	✔
futures 使用 yfinance + symbolcache	✔
breadth 来自 snapshot（非 datasource）	✔
fetcher 写唯一 snapshot 文件	✔
retry/sleep/timeout 正确	✔
# 14. Future Extensions（未来扩展）

你现在的 datasource 层已经是 V11.7 完整版本，可以安全扩展：

Crypto Index Client（BTC / ETH / CIBR 因子）

Commodity Client（Copper / Gold / Oil）

Bond Yield Client（US10Y / CN10Y）

FX Client（USDJPY, CNH）

Sector ETF Client（行业轮动因子）

所有新 datasource 必须遵守：

使用 yf_client_cn / 内部 API

写 symbolcache

不写 snapshot

返回 dict / list

fetcher 再写 snapshot