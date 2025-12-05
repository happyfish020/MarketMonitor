GlobalLeadFactor + PredictionEngine 接口规范（V11.7.2）
-----------------------------------------
🎯 GlobalLead 因子的定位

GlobalLeadFactor 是 外盘对 A 股的领先影响因子，主要来自：

SPX

NDX

HSI

SGX A50

VIX

USDCNH

全球风险溢价指标

基于这些数据计算：

- weighted global score
- level
- contributions

1. 数据输入（来自 snapshot）

Fetcher 写入 snapshot：

snapshot["global_lead"] = {
   "spx_pct": 0.21,
   "ndx_pct": 0.32,
   "hsi_pct": 0.11,
   "a50_pct": 0.30,
   "vix": 14.2,
   "usdcnh": 7.12,
   ...
}

2. GlobalLeadFactor.compute()

返回字典：

{
   "score": float,        # 0~100
   "level": "偏多/偏空/中性",
   "details": {
       "weighted": float,
       "contributions": {
            "spx": 0.14,
            "ndx": 0.18,
            "hsi": 0.02,
            "a50": 0.06,
            ...
       }
   }
}


再包装为 FactorResult：

FactorResult(
   name="global_lead",
   score=score,
   details=details,
   level=level,
   signal=signal,
   raw=raw,
   report_block=report_block
)

3. PredictionEngine 输入接口

PredictionEngine 输入必须是：

{
  "north_nps": FactorResult,
  "turnover": FactorResult,
  "margin": FactorResult,
  "market_sentiment": FactorResult,
  "global_lead": FactorResult
}


内部会抽取：

x = {
  "north_nps": fr.score,
  "turnover": fr.score,
  "margin": fr.score,
  "sentiment": fr.score,
  "global_lead": fr.score
}

4. 输出格式（必须是结构化 dict）
{
  "t1": {
      "score": float,
      "direction": "偏多/偏空/震荡"
  },
  "t5": {
      "score": float,
      "direction": "偏多/偏空/震荡"
  }
}


方向规则：

score > 60 → 偏多
score < 40 → 偏空
否则 → 震荡

5. 整体系统的可扩展性

GlobalLeadFactor 和 PredictionEngine 均为 松耦合组件：

可以替换数据源

可以增加模型权重

可以添加 ML 预测模型

可以扩展更多全球风险因子（DXY、TNX、美债期限结构)