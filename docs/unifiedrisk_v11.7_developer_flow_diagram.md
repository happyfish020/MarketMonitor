# UnifiedRisk V11.7
# 开发者全流程图（包含因子层）
flowchart TD

    %% ========== YF Client ==========
    subgraph YF["YF Client（唯一 yfinance 入口）"]
        Y1[get_etf_daily]
        Y2[get_macro_daily]
    end

    %% ========== Symbol Cache ==========
    subgraph SC["Symbol Cache（单标缓存）"]
        SC1[etf_510300_SS.json]
        SC2[macro_^GSPC.json]
        SC3[macro_^VIX.json]
        SC4[index_sh000300.json]
    end

    Y1 --> SC
    Y2 --> SC

    %% ========== Datasource 层 ==========
    subgraph DS["Datasource 层（不写 snapshot）"]
        D1[ETF Proxy]
        D2[Global Lead]
        D3[Index Series]
        D4[Margin Series]
    end

    SC --> D1
    SC --> D2
    SC --> D3
    SC --> D4

    %% ========== Breadth（特殊） ==========
    subgraph BR["Breadth（来自 Reader，不属于 datasource）"]
        B1[get_breadth_summary]
    end

    %% ========== Fetcher ==========
    subgraph F["Fetcher（唯一写 snapshot）"]
        F1[assemble snapshot]
    end

    D1 --> F1
    D2 --> F1
    D3 --> F1
    D4 --> F1
    B1 --> F1

    F1 --> SNAPSHOT[ashare_daily_snapshot.json]

    %% ========== Factor 层 ==========
    subgraph FT["Factor 层（只读 snapshot）"]
        FT1[TurnoverFactor]
        FT2[MarginFactor]
        FT3[NPSFactor]
        FT4[GlobalLeadFactor]
        FT5[SentimentFactor]
    end

    SNAPSHOT --> FT1
    SNAPSHOT --> FT2
    SNAPSHOT --> FT3
    SNAPSHOT --> FT4
    SNAPSHOT --> FT5

    FT1 --> SCORE[T+1 / T+5 Risk Score]
    FT2 --> SCORE
    FT3 --> SCORE
    FT4 --> SCORE
    FT5 --> SCORE