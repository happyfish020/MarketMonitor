----------------------------------------------------
# UnifiedRisk V11.7 — 数据流图（Dataflow Diagram）
----------------------------------------------------

版本：V11.7
更新时间：2025-12-06

本文件展示 UnifiedRisk 系统从 Datasource → Fetcher → Snapshot → Factors → Engine → Reporter
的完整数据流流程（Dataflow）。

此图用于：

因子开发

Debug 和 Trace

系统演进（如 V12 模块化）

文档化、团队协作

# 1. 数据流图（Mermaid 格式）

将以下内容直接复制到你的 .md 文件，可在 GitHub、Obsidian、Typora、MkDocs 中渲染：

flowchart TD

    %% --------------------------
    %% Datasource Layer
    %% --------------------------
    subgraph Datasources["Datasource Layer (Raw Data Fetchers)"]
        A1[IndexSeriesClient\nC1 指数序列] -->|OHLCV| F
        A2[BreadthSeriesClient\nC2 宽度序列] -->|adv/dec| F
        A3[GlobalLeadClient\nC3 海外引导] -->|spx/ndx/a50/usdcnh| F
        
        A4[ETF North Proxy\netf_north_proxy.py] -->|北向代理| F
        A5[MarketDataReaderCN\nmarket_db_client.py] -->|成交额/宽度| F
        A6[MarginClient\nem_margin_client.py] -->|两融序列| F
    end

    %% --------------------------
    %% Fetcher Layer
    %% --------------------------
    subgraph Fetcher["Fetcher Layer (Snapshot Assembly)"]
        F[AshareFetcher\n组合 snapshot] --> S[snapshot.json\n日级快照]
    end

    %% --------------------------
    %% Factors
    %% --------------------------
    subgraph Factors["Factor Layer (Risk & Signal Models)"]
        S --> F1[north_nps_factor]
        S --> F2[turnover_factor]
        S --> F3[market_sentiment_factor]
        S --> F4[margin_factor]

        S --> F5[price_action_factor\n(C1关键因子)]
        S --> F6[breadth_trend_factor\n(C2增强因子)]
        S --> F7[global_lead_factor\n(C3预测因子)]
    end

    %% --------------------------
    %% Engine Layer
    %% --------------------------
    subgraph Engine["Engine Layer (Score & Decisions)"]
        F1 --> E[ashare_daily_engine]
        F2 --> E
        F3 --> E
        F4 --> E
        F5 --> E
        F6 --> E
        F7 --> E
    end

    %% --------------------------
    %% Reporter Layer
    %% --------------------------
    subgraph Reporter["Reporter Layer (Final Output)"]
        E --> R[ashare_daily_reporter\n生成风险报告/方向判断]
    end

# 2. 数据流说明（Summary）
数据流顺序：

Datasource

获取原始数据（akshare / yfinance / eastmoney）

每个 datasource 都只负责自己的缓存

Fetcher（AshareFetcher）

调用所有 datasource

将各模块数据组装为统一 snapshot

写入日级缓存

Factors（因子层）

只读取 snapshot（不得访问 datasource）

输出可用的风险评分 / 方向信号

Engine（引擎层）

汇总各因子

做权重整合 + 方向判断

生成统一风险评分

Reporter（报表层）

最终输出：

风险报告

方向预测（T+1 / T+5）

图表 / 文本

# 3. Datasource → Snapshot 字段映射
Datasource	输出字段	Snapshot 字段	说明
IndexSeriesClient	OHLCV	index_series	C1，价格结构序列
BreadthSeriesClient	adv/dec	breadth_series	C2，宽度增强
GlobalLeadClient	美股/港股/汇率	global_lead	C3，T+1 引导
MarketDataReaderCN	成交额/基础宽度	turnover, breadth	主指标
etf_north_proxy	流入代理	etf_proxy	北向替代指标
MarginClient	两融序列	margin	杠杆风险

这个表可以贴进你的开发规范，作为因子开发的参考。

# 4. 适用场景（Uses）

此文档用于：

编写因子时查看 snapshot 字段来源

Debug 数据问题（哪层出错？ datasource / fetcher / factor）

新 datasource 接入

新因子设计（清楚输入与数据限制）

系统升级（如 V12 版本：拆分全球/本地模块）


flowchart TD

    %% ---------- YF Client 层 ----------
    subgraph YFClient["YF Client（唯一 yfinance 调用处）"]
        A1[get_etf_daily]
        A2[get_macro_daily]
    end

    %% ---------- Symbol Cache ----------
    subgraph SymbolCache["Symbol Cache（每个 symbol 一个文件）"]
        C1[etf_510300_SS.json]
        C2[etf_159901_SZ.json]
        C3[macro_^GSPC.json]
        C4[macro_^VIX.json]
        C5[index_sh000001.json]
    end

    A1 --> SymbolCache
    A2 --> SymbolCache

    %% ---------- Datasources ----------
    subgraph DS["Datasource（不写 snapshot）"]
        D1[etf_north_proxy]
        D2[global_lead_client]
        D3[index_series_client]
        D4[VIXClient]
    end

    SymbolCache --> D1
    SymbolCache --> D2
    SymbolCache --> D3
    SymbolCache --> D4

    %% ---------- Fetcher ----------
    subgraph Fetcher["Fetcher（唯一写 snapshot）"]
        F[assemble snapshot]
    end

    D1 --> F
    D2 --> F
    D3 --> F
    D4 --> F

    F --> S[ashare_daily_snapshot.json]
