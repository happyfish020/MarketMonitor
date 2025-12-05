                index_sh000300.json
📌 Breadth 在 snapshot 内，不生成独立文件。
# 8. 数据流图（Mermaid 完整版）
mermaid
Copy code
flowchart TD

    %% ---------- YFClient ----------
    subgraph YFClient["YF Client（唯一 yfinance 调用）"]
        A1[get_etf_daily]
        A2[get_macro_daily]
    end

    %% ---------- SymbolCache ----------
    subgraph SymbolCache["Symbol Cache（单标缓存）"]
        C1[etf_510300_SS.json]
        C2[etf_159901_SZ.json]
        C3[macro_^GSPC.json]
        C4[index_sh000300.json]
    end

    A1 --> SymbolCache
    A2 --> SymbolCache

    %% ---------- Datasources ----------
    subgraph DS["Datasource 层（不写 snapshot）"]
        D1[ETF Proxy]
        D2[Global Lead]
        D3[Index Series]
        D4[Margin Series]
    end

    SymbolCache --> D1
    SymbolCache --> D2
    SymbolCache --> D3
    D4 --> DS

    %% ---------- Breadth ----------
    subgraph BreadthBlock["Breadth（来自 MarketDataReaderCN）"]
        B1[Adv / Dec / LU / LD / Total]
    end

    %% ---------- Fetcher ----------
    subgraph Fetcher["Fetcher（唯一写 snapshot）"]
        F[assemble snapshot]
    end

    D1 --> F
    D2 --> F
    D3 --> F
    D4 --> F
    B1 --> F

    F --> S[ashare_daily_snapshot.json]
# 9. 常见错误与禁止行为
错误行为	原因
❌ datasource 写 global_lead.json	snapshot 必须唯一写入
❌ breadth_series_client.py	breadth 已在 snapshot 中，无需 datasource
❌ datasource 直接使用 yfinance	必须走 yf_client_cn
❌ breadth 使用 symbolcache	breadth 无 symbol 特征
❌ factor 读取 symbolcache	factor 只能读取 snapshot