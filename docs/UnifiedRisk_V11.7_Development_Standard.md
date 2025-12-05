----------------------------------------------------
# UnifiedRisk V11.7 — 开发规范（Development Standard）
----------------------------------------------------
作者：Fisher + ChatGPT
版本：V11.7
更新时间：2025-12-06
# 目录（TOC）

1. 框架目录结构（Framework Directory View）

2. 命名规范（Naming Convention）

3. 日志规范（Logging Standard）

4. 缓存规范（Caching Standard）

5. 分层规范：Datasource vs Fetcher

6. 因子规范（Factor Standard）

7. Snapshot 结构规范（Daily & Intraday）

8. 代码风格规范（Coding Style）

9. 数据流图（Mermaid）

# 1. 框架目录结构
project_root/
│
├── core/
│   ├── adapters/
│   │   ├── cache/
│   │   │   ├── file_cache.py
│   │   │   └── cache_manager.py
│   │   │
│   │   ├── datasources/
│   │   │   ├── cn/
│   │   │   │   ├── etf_north_proxy.py
│   │   │   │   ├── market_db_client.py
│   │   │   │   ├── zh_spot_utils.py
│   │   │   │   ├── em_margin_client.py
│   │   │   │   ├── index_series_client.py        # C1
│   │   │   │   ├── breadth_series_client.py      # C2
│   │   │   │   └── global_lead_client.py         # C3
│   │   │   │
│   │   │   └── global/
│   │   │
│   │   ├── fetchers/
│   │   │   ├── cn/
│   │   │   │   └── ashare_fetcher.py
│   │   │   └── global/
│   │   │
│   │   └── reporters/
│   │       ├── cn/
│   │       │   └── ashare_daily_reporter.py
│   │       └── global/
│   │
│   ├── factors/
│   │   ├── north_nps_factor.py
│   │   ├── turnover_factor.py
│   │   ├── market_sentiment_factor.py
│   │   ├── margin_factor.py
│   │   ├── price_action_factor.py         # 新
│   │   ├── breadth_trend_factor.py        # 新
│   │   └── global_lead_factor.py          # 新
│   │
│   ├── engines/
│   │   ├── cn/
│   │   │   └── ashare_daily_engine.py
│   │   └── global/
│   │
│   ├── utils/
│   │   ├── logger.py
│   │   ├── time_utils.py
│   │   ├── config_loader.py
│   │   └── math_tools.py
│   │
│   └── scoring/
│       ├── score_short_term.py
│       └── score_unified.py
│
├── data/
│   ├── ashare/
│   └── cache/
│       ├── day_cn/
│       │   └── YYYYMMDD/
│       │       ├── ashare_daily_snapshot.json
│       │       ├── index_series.json
│       │       ├── breadth_series.json
│       │       └── global_lead.json
│       └── intraday_cn/
│           └── ashare_intraday_snapshot.json
│
├── logs/
│
└── main.py

# 2. 命名规范（Naming Convention）
2.1 文件命名
层级	后缀	例子
Datasource	*_client.py	index_series_client.py
Fetcher	*_fetcher.py	ashare_fetcher.py
Factor	*_factor.py	price_action_factor.py
Reporter	*_reporter.py	ashare_daily_reporter.py
Engine	*_engine.py	ashare_daily_engine.py
2.2 类命名（PascalCase）
IndexSeriesClient
BreadthSeriesClient
GlobalLeadClient
AshareFetcher
PriceActionFactor

# 3. 日志规范（Logging Standard）
3.1 datasource 层必须使用：
from core.utils.logger import get_logger
LOG = get_logger("CN.DS.IndexSeries")
LOG("开始刷新指数序列")


禁止：

LOG.info

print()

logging.info

3.2 fetcher / engine 层必须使用：
from core.utils.logger import log
log("[CN Fetcher] 刷新日级数据")

# 4. 缓存规范（Caching Standard）

所有读写必须使用：

from core.adapters.cache.file_cache import load_json, save_json

日级缓存路径：
data/cache/day_cn/{YYYYMMDD}/xxx.json

盘中缓存路径：
data/cache/intraday_cn/ashare_intraday_snapshot.json

datasource 缓存文件：
index_series.json
breadth_series.json
global_lead.json


禁止：

open() 自己写文件

非 JSON 格式缓存

datasource 访问 snapshot 缓存

# 5. 分层规范：Datasource vs Fetcher
5.1 Datasource 只能做：

调用 akshare/yfinance

数据预处理

写 datasource 缓存

返回原始结构数据（dict/list）

禁止：

拼 snapshot

写 snapshot 缓存

做因子计算

5.2 Fetcher 只能做：

调 datasource

合并数据、组装 snapshot

写 snapshot 缓存

返回 snapshot dict

禁止：

调 akshare/yf

修改 datasource 缓存

# 6. 因子规范（Factor Standard）
6.1 因子输出结构
FactorResult(
  name="north_nps",
  score=45.00,
  level="中性偏弱",
  desc="北向代理资金略偏空",
  details={
      "value": -4.32,
      "trend3": -1.11,
      "acc": 0.32,
  }
)

6.2 因子只接受 snapshot，不访问 datasource
6.3 因子不能做文件 IO
6.4 因子必须松耦合，具有可组合性
# 7. Snapshot 结构规范（Daily & Intraday）
7.1 Daily snapshot
snapshot {
    meta: {...},
    etf_proxy: {...},
    turnover: {...},
    breadth: {...},
    margin: {...},
    index_series: {...},      # C1
    breadth_series: {...},    # C2
    global_lead: {...}        # C3
}

7.2 Intraday snapshot
{
   "timestamp": "...",
   "index": {
       "sh_change": 0.0,
       "cyb_change": 0.0
   },
   "meta": { "source": "UnifiedRisk_V11.7_cn_intraday" }
}

# 8. 代码风格规范（Coding Style）
✔ Imports 分组

Python 内置

第三方库

core.adapters

core.utils

本地模块

✔ 禁止 print
✔ 禁止 logging.*
✔ 禁止动态 import
✔ 禁止在因子中访问网络
# 9. 数据流图（Mermaid）

放入你的 Markdown 会自动渲染。

flowchart TD

    subgraph Datasources
        A1[IndexSeriesClient] -->|raw OHLCV| F
        A2[BreadthSeriesClient] -->|adv/dec| F
        A3[GlobalLeadClient] -->|spx/ndx/a50| F
        A4[ETF North Proxy] --> F
        A5[MarketDataReaderCN] --> F
        A6[MarginClient] --> F
    end

    subgraph Fetcher
        F[AshareFetcher<br/>assemble snapshot] --> S[snapshot.json]
    end

    subgraph Factors
        S --> F1[north_nps_factor]
        S --> F2[turnover_factor]
        S --> F3[market_sentiment_factor]
        S --> F4[margin_factor]
        S --> F5[price_action_factor]
        S --> F6[breadth_trend_factor]
        S --> F7[global_lead_factor]
    end

    subgraph Engine
        F1 --> E[ashare_daily_engine]
        F2 --> E
        F3 --> E
        F4 --> E
        F5 --> E
        F6 --> E
        F7 --> E
    end

    E --> R[Reporter<br/>ashare_daily_reporter]