# CASE-CN-20251229-EOD-INDEX_STABLE_STOCKS_WEAK-D1-CAUTION

## 背景（一句话）
指数看起来稳，但涨少跌多 + 放量分歧/轮动 + 执行 D1（追价胜率低），制度输出 N（Gate=CAUTION）。

## 真实盘面锚点（人类事实/体感）
- 体感：指数没怎么跌，但多数股票在跌；更像调仓轮动/兑现，而非全面风险偏好抬升。
- etf_spot_sync（你系统当日字段）：
  - adv_ratio = 0.4079（涨少跌多）
  - top20_amount_ratio = 0.772（成交极度集中/窄领涨/拥挤）
  - interpretation: crowding=high, direction=diverged, participation=weak, dispersion=moderate, divergence=low
  - same_direction = false
  - dispersion = 2.4326
  - divergence_index: intraday≈0.0083, eod≈0.0067

## 期望制度输出（回归断言）
- ActionHint.code = N
- Gate = CAUTION（允许 HOLD/DEFENSE；禁止 ADD-RISK）
- Execution.band = D1（必须为单值；禁止 "A/D1"）
- Structure Facts states：
  - breadth.state = healthy（必须带“仅表示未坏/不代表可进攻”的语义收敛提示；且提示不得重复）
  - amount.state = expanding（文案必须偏“分歧/轮动/调仓”，不得偏乐观）
  - index_tech.state = neutral
  - failure_rate.state = stable
  - trend_in_force.state = in_force（强调成功率下降/避免过度利用趋势）
  - north_proxy_pressure.state = pressure_low（强调仍需结合广度/成功率）
- 报告不得出现 debug/审计告警行：
  - 禁止包含 "semantic_caution_state:"
  - 禁止包含 "note:execution_code_diff"
