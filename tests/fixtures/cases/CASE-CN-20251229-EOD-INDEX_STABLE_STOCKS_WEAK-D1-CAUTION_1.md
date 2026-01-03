CASE-CN-20251229-EOD-INDEX_STABLE_STOCKS_WEAK-D1-CAUTION

一句话：指数看起来稳，但涨少跌多 + 放量分歧/轮动 + 执行 D1（追价胜率低），制度输出 N（Gate=CAUTION）。

真实市场现实快照（由你提供的“事实口径”，作为 case 的外部锚点）

注：这里不要求系统当下就能自动抓全量数据；这是 “人类事实” 对照锚点，用于检验语义是否贴合体感。

盘面体感：指数没怎么跌，但多数股票在跌；更像调仓轮动/兑现，而非全面风险偏好抬升。

结构性内核（来自你系统当日可得字段）：

adv_ratio=0.4079（涨少跌多）

top20_amount_ratio=0.772（成交极度集中，窄领涨/拥挤）

dispersion=2.4326（分化中等）

direction=diverged / same_direction=false（不同步）

participation=weak（参与度弱）

intraday divergence_index≈0.0083；EOD divergence_index≈0.0067

snapshot_type=EOD，amount_stage=FULL

V12 当日制度输出（作为“应当保持”的目标输出）

你最新输出（已达标），在这个 case 里固定为“Expected”：

ActionHint：N

Gate：CAUTION（允许 HOLD/DEFENSE；禁止 ADD-RISK）

Execution band：D1（且必须为单值，不得出现 “A/D1”）

Structure Facts（关键 state）

breadth = healthy（但必须带“仅表示未坏/不代表可进攻”的语义收敛提示）

amount = expanding（但文案必须偏“分歧/轮动/调仓”，不能偏乐观）

index_tech = neutral

failure_rate = stable

trend_in_force = in_force（但要强调成功率下降/避免过度利用趋势）

north_proxy_pressure = pressure_low（并强调“仍需结合广度/成功率”）

体感句（至少保留这一类表达，不一定逐字）
“指数可能较稳，但多数股票偏弱（涨少跌多），盘面更像调仓轮动，而非全面风险偏好抬升。”

Regression / Acceptance Criteria（后续改动必须满足）

把这些当作“冻结测试断言”：

A. 报告格式一致性

Execution band 必须是单值：只允许 A|N|D|D1|D2，禁止 A/D1 这种组合。

报告中不得出现 debug/审计型提示：

禁止 semantic_caution_state:*

禁止 note:execution_code_diff

B. 语义一致性（避免“看起来偏乐观”）

当 Gate=CAUTION 时：

breadth/amount 这类“正向词”必须被语义收敛：明确不等于允许进攻（但提示只出现一次，不能重复叠加）。

amount=expanding 的 meaning 必须是：

更可能分歧/调仓/轮动，而不是“进攻动能增强”。

breadth=healthy 的 meaning 必须表达：

未见系统性破坏 + 扩散不足；若 adv_ratio≤0.42，允许/建议出现“涨少跌多”体感提示。

C. 因子替代策略（north_proxy_pressure 接管）

当 north_proxy_pressure 存在时：

结构块里 不再展示 north_nps（deprecated/RAW_ONLY 一律隐藏），避免 neutral 误导。

建议落库形式（你直接 commit 到 repo）

你可以用“一份 md + 一份 json fixture”，后续所有改动跑 selftest/pytest 过这条就算兼容。

1) 文档（人读）

建议路径：
docs/cases/CASE-CN-20251229-EOD-INDEX_STABLE_STOCKS_WEAK-D1-CAUTION.md

内容就用我上面这份（可以直接复制）。

2) Fixture（机读，给测试用）

建议路径：
tests/fixtures/cases/CASE-CN-20251229-EOD.json

示例骨架（你可以把当日 slots/factors 的关键字段填进去）：

{
  "case_id": "CASE-CN-20251229-EOD-INDEX_STABLE_STOCKS_WEAK-D1-CAUTION",
  "trade_date": "2025-12-29",
  "kind": "EOD",
  "inputs": {
    "etf_spot_sync": {
      "adv_ratio": 0.4079,
      "top20_amount_ratio": 0.772,
      "dispersion": 2.4326,
      "same_direction": false,
      "interpretation": {
        "crowding": "high",
        "direction": "diverged",
        "participation": "weak",
        "dispersion": "moderate",
        "divergence": "low"
      }
    },
    "expected_structure_states": {
      "breadth": "healthy",
      "amount": "expanding",
      "index_tech": "neutral",
      "failure_rate": "stable",
      "trend_in_force": "in_force",
      "north_proxy_pressure": "pressure_low"
    }
  },
  "expected": {
    "actionhint_code": "N",
    "gate": "CAUTION",
    "execution_band": "D1",
    "must_not_contain": [
      "semantic_caution_state:",
      "note:execution_code_diff",
      "A/D1"
    ],
    "must_contain_any": [
      "涨少跌多",
      "调仓",
      "轮动"
    ],
    "caution_hint_no_duplicate": true
  }
}

这个 case 对后续修改的约束点

你后面要加的 MarketOverviewBlock（收盘事实）、或者调整 mapper/builder 文案，都必须满足：

制度结论不变：N / CAUTION / D1

语义不漂移：不能把这种盘面说得像“风险偏好抬升”

输出不脏：不出现 debug warning 行；提示不重复

如果你愿意，我也可以把这个 case 再升级一步：给你写一个最小 selftest_case_runner.py，读这个 JSON fixture，跑一遍 report 生成，然后做上述断言（就是“兼容今天 case”的自动化闸门）。你只要把“生成报告的入口函数/类”那个文件路径发我就行。