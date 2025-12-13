A 股系统数据流文档（Snapshot → 因子 → 预测 → 报告）
-----------------------------------------
🎯 总览

统一风险系统（UnifiedRisk CN）每日运行的完整流程如下：

                ┌───────────────┐
                │ Remote Datasource│
                │ (YF/东财/新浪等) │
                └───────────────┘
                          │
                          ▼
               ┌───────────────────┐
               │  AShareFetcher      │
               │  get_daily_snapshot │
               └───────────────────┘
                          │
                          ▼
          Snapshot(JSON)：etf_proxy / turnover / margin / index_series / global_lead / breadth
                          │
                          ▼
         ┌────────────────────────────────┐
         │ _build_processed_for_factors() │
         └────────────────────────────────┘
                          │
                          ▼
                processed dict（因子输入）
                          │
    ┌───────────────┬───────────────┬───────────────┬──────────────┐
    ▼               ▼               ▼               ▼              ▼
NorthNPSFactor   TurnoverFactor  MarginFactor   SentimentFactor   GlobalLeadFactor
    │               │               │              │               │
    └───────────────┴───────────────┴──────────────┴──────────────┘
                          │
                          ▼
               FactorResult (V11.7 结构化)
                          │
                          ▼
                UnifiedScoreBuilder
                          │
                          ▼
                 PredictionEngine
                (T+1 / T+5 方向预测)
                          │
                          ▼
                    ReportBuilder
                          │
                          ▼
                完整日报 TXT（包含因子 + 情绪 + 预测）

1. Snapshot 层（Fetcher）

Snapshot 必须包含：

snapshot = {
  "meta": {...},
  "etf_proxy": {...},
  "turnover": {...},
  "breadth": {...},
  "margin": {...},
  "index_series": {...},
  "global_lead": {...}
}


所有因子必须基于 snapshot，不允许自行请求网络。

2. processed（因子统一输入结构）

processed = snapshot 增强版：

processed = {
  "etf_proxy": {...},
  "turnover": {...},
  "breadth": {...},
  "margin": {...},
  "global_lead": {...},
  "index_series": {...},
  "features": {...},     # 数字化特征集合
  "raw": snapshot
}

3. Factor 层

所有因子必须返回：

FactorResult(
    name,
    score,
    details={
        "level": "...",
        "raw_fields": ...
        ...
    },
    level="...",
    signal="...",
    raw={...},
    report_block="..."
)


因子不得做网络请求
必须使用 processed 中的字段。

4. UnifiedScore 层

统一分数 = 多因子分数的加权汇总。

summary = {
  "total_score": float,
  "risk_level": "强/中性/弱",
  "factor_scores": {...}
}

5. Prediction 层（T+1/T+5）

输入：

{
  "north_nps": 62.5,
  "turnover": 70.0,
  "margin": 45.0,
  "sentiment": 58.0,
  "global_lead": 67.0
}


输出：

T+1: {
   "score": 61.2,
   "direction": "偏多"
}
T+5: {
   "score": 53.4,
   "direction": "震荡偏强"
}

6. Report 层（日报）

包含：

Meta 信息

各因子分数

情绪部分

预测结果

分隔线结构化排版


《UnifiedRisk V12 → V13 进化路线图：结合 ChatGPT 带来飞跃式增强》

包括：

自适应因子权重

超参数优化

事件驱动评分

预测引擎升级

多市场联动判断

行为金融因子

仓位动态控制模块（position sizing engine）

统一风险仪表盘（HTML or Excel）

你要不要我给你设计这张路线图？

要的“提前预测砸盘”，本质是什么？

你说的“资金砸盘”其实一般有几类：

日内闪崩 / 快速杀跌

特征：分时放量、bid 被连续打穿、权重/龙头一起下砸

需要：高频 tick / 分时数据、盘口结构变化、期指/ETF 联动

日线级别的连续杀跌（几天内加速出货）

特征：

北向/主力资金持续净流出

成交额结构改变（缩量阴跌 / 放量长阴）

情绪因子急转（涨跌家数、跌停家数、adv/dec 突变）

指数关键位技术形态被破坏（颈线、均线、前低）

你真正要的是：

在砸盘真正发生的当天 / 前一两天，
听到系统在耳边说一句：
“今天 / 明天要小心了，风险结构明显变坏了。”

这个东西，本质上是一个：

✅ “统计 + 规则 + 多因子 + 回测验证”的预警引擎

它必须：

读你自己的 snapshot

用你定义好的因子结果（north_nps / turnover / margin / emotion / global_lead …）

按你能理解的规则给出 确定性信号（比如 R1 / R2 / R3 / R4 等）

可以用历史数据去验证：“以前这么评分时，后面三天跌多少的概率有多大？”

❗️这些，只有你本地程序能做；我做不了。

2️⃣ 我能做什么？我明确说下边界

我能做的：

帮你设计 哪些因子有利于提前识别砸盘

帮你设计 预警规则（比如：N 日内 north_nps 连续低位 + margin 断崖式下降 + global_lead 转空）

帮你解释：某天的因子组合意味着什么风险结构

帮你优化：规则怎么改、权重怎么调、如何降低误报和漏报

但我做不到：

接你本地 akshare / 东财 的实时数据

8:00 自动跑一遍全市场 snapshot

每 5 分钟监控一次分时、盘口、ETF/期指联动

把结果写进 json/parquet 并保存 300 天做回测

按统一规则打日志、出报告、出 Excel

这些都是 “执行层 + 运维层能力”，AI 聊天模型是没有的。