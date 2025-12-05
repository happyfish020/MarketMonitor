UnifiedRisk V11.7 — Datasource 层统一规范（正式版）
1）Datasource 的职责（必须遵守）
项目	说明
🎯 主要职责	调用原始数据接口（YF / Eastmoney / AKShare / 内部数据源）并生成“原始数据块”
🎯 返回内容	dict / list，不写任何 snapshot 文件
✔ 可写缓存	仅允许写 symbolcache（单标数据）
❌ 禁止写缓存	不允许写 datasource 自己的 json（例如 global_lead.json）
❌ 禁止写 snapshot	snapshot 必须由 fetcher 写入
✔ 必须支持 retry	统一 retry=3 + sleep(10) + timeout=20
2）Datasource 必须通过 symbolcache 落盘

形如：

data/cache/day_cn/YYYYMMDD/{kind}_{symbol}.json


例如：

etf_510300_SS.json

macro_^GSPC.json

macro_MARGIN_RZRQ.json

breadth_BASIC.json

3）Datasource 禁止的行为

❌ 调用 yfinance.download（必须走 yf_client_cn）

❌ 写 day_cn/xxx.json（除 symbolcache 外）

❌ 在 datasource 中构建 snapshot

❌ 依赖 snapshot 内容（datasource 必须是底层）

❌ 使用 logger.info（必须用 log()）

4）fetcher 才能写 snapshot

snapshot 形式：

ashare_daily_snapshot.json


包含：

index_series

breadth_series

margin

etf_proxy

global_lead

5）Datasource 的返回格式规范

每个 datasource 的返回必须是容易嵌入 snapshot 的：

示例：
global_lead_client
{
  "spx": 0.0043,
  "ndx": -0.0022,
  "hsi": 0.0061,
  "a50": -0.0011
}

index_series_client
{
  "sh": { "symbol": "sh000001", "pct_change": -0.0033 },
  "sz": { "symbol": "sz399001", "pct_change": 0.0021 },
  "hs300": { "symbol": "sh000300", "pct_change": -0.0018 }
}

margin_client
[
  {"date":"2025-12-01", "rz": 102.1, "rq": 2.34, "rzrq":104.4},
  ...
]

6）Mermaid 数据流图（Datasource 层）
flowchart TD

    subgraph SymbolCache["Symbol Cache（单标缓存）"]
        C1[etf_510300_SS.json]
        C2[macro_^GSPC.json]
        C3[macro_MARGIN_RZRQ.json]
        C4[breadth_BASIC.json]
    end

    subgraph DS["Datasource 层（不写 snapshot）"]
        D1[ETF Proxy]
        D2[Global Lead]
        D3[Index Series]
        D4[Margin Series]
        D5[Breadth Series]
    end

    SymbolCache --> D1
    SymbolCache --> D2
    SymbolCache --> D3

    D1 --> F
    D2 --> F
    D3 --> F
    D4 --> F
    D5 --> F

    subgraph Fetcher["Fetcher（唯一写 snapshot）"]
        F[assemble snapshot]
    end

    F --> S[ashare_daily_snapshot.json]

🎉 以上 A + B + D 全部完成！

你现在有：

fully symbolcache 规范化的 margin_client

breadth_series_client（retry + symbolcache）

datasource 统一规范文档（正式版 MD）

下一步建议（任选）：
➤ C）统一 index_series_client 的“多日序列版”（用于 T+5 预测）
➤ 开始写 price_action_factor（最强预测因子）
➤ 写 global_lead_factor（T+1 核心）
➤ 构建一个 DATASOURCE LINT 工具（自动检查是否违规写 cache/snapshot）

你想继续哪一个？