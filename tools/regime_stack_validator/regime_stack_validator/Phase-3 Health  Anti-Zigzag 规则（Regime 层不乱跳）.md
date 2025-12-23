Phase-3 Health / Anti-Zigzag 规则（Regime 层不乱跳）

目标：Regime 变化频率必须显著低于 Phase-2 Gate，并且“降级快、恢复慢、禁止横跳”的纪律在 Regime 层同样成立（但 Regime 不产生动作，只提供背景语义）。

A. Regime 状态机的“允许转移图”（离散制度）

只允许以下转移（其余一律视为 ZIGZAG/FAIL）：

Risk-On → Neutral → (Distribution 或 Risk-Off) ✅

Neutral → Risk-On ✅（但需更严格恢复条件，见 C）

Neutral → Distribution / Risk-Off ✅（降级快）

Distribution → Risk-Off ✅（可直接恶化）

Distribution → Neutral ✅（恢复慢）

Risk-Off → Neutral ✅（恢复慢）

Risk-Off → Risk-On ❌（禁止“跨级恢复”，必须先回 Neutral）

强约束：任何 “Risk-On ↔ Risk-Off” 直接互跳一律 FAIL。
任何 “Distribution ↔ Risk-On” 直接互跳一律 FAIL（必须经 Neutral）。

B. 最小驻留期（Minimum Hold）

Regime 一旦切换，必须满足 最小驻留期 才允许再次切换（否则视为 Zigzag）。

进入 Risk-Off / Distribution：min_hold = 5 个交易日

进入 Neutral：min_hold = 3 个交易日

进入 Risk-On：min_hold = 5 个交易日（防止“假修复”）

解释：Regime 是“背景层”，不允许跟随日波动而抖动。

C. 双门槛：降级快 / 恢复慢（Hysteresis）

Regime 的切换必须满足“确认次数”门槛（不涉及指标，只是制度形式）：

降级门槛（快）：满足条件 连续 2 次 即可触发

Neutral → Distribution/Risk-Off

Risk-On → Neutral

恢复门槛（慢）：满足条件 连续 5 次 才允许触发

Risk-Off → Neutral

Distribution → Neutral

Neutral → Risk-On

这条是 Phase-2 “恢复粘性”在 Regime 层的等价制度化：恢复比降级更难。

D. Regime 置信度与“锁定”规则（Confidence Lock）

Phase-3 输出包含 confidence ∈ {LOW, MID, HIGH}，它只用于 抑制 Regime 自己横跳：

若当前 confidence = LOW：

禁止切换到更乐观的 Regime（只能维持或更保守）

允许降级，但降级后必须满足 min_hold

若当前 confidence = HIGH：

允许按规则恢复，但仍受 min_hold 与恢复门槛约束

关键：confidence 不用于放大 Phase-2 Gate 行为，只用于让 Regime 更稳定。

E. Phase-3 Health 判定（制度级“红线”）

Phase-3 的 Health 只关注两类失败：纪律失败 与 逻辑自洽失败。

E1. Zigzag Fail（纪律失败）
任一触发即 FAIL：

任意 非法转移

在 min_hold 内发生切换

5 个交易日内切换次数 ≥ 2（可配置，但必须存在硬阈值）

E2. Inconsistency Fail（自洽失败）
任一触发即 FAIL（注意：不是推翻 Phase-2，而是 Phase-3 自己判失败）：

出现“结构性破坏已确认”的背景下（来自 Phase-3 自己的结构语义），却输出 Risk-On

Phase-2 Gate 已处于 PlanB/Freeze，而 Phase-3 持续输出 Risk-On 且 HIGH（连续 ≥ 3 次）

这不是要让 Phase-3 改 Gate，而是防止 Phase-3 给出“反语义”。

3️⃣ 把 Phase-3 映射回 UnifiedRisk V12 的 Factor / Predictor 接口规范（不写代码）

你要求：严格分层、不假设已有类/字段。下面是接口契约级映射：放到哪个层、产出什么字段、谁消费它。

A. Phase-3 在 V12 架构中的位置

Phase-3 是“背景语义层”，最合适的承载方式是：

✅ Factor 层：输出 RegimeResult（离散语义 + 解释文本 + 置信度）

✅ Predictor 层：读取 Phase-2 Gate + Phase-3 Regime，生成“解释块”（不改变 Gate）

明确禁止：

Phase-3 不作为 DataSource（不新增数据抓取）

Phase-3 不作为 Gate 决策器（不覆盖 Phase-2）

B. Snapshot 字段（只声明容器，不声明实现）

为了让 Predictor 与 Reporter 读取统一口径，Snapshot 建议新增一个顶层字段（容器式）：

snapshot["regime_confirmation"] = {
  "regime": "RISK_ON | NEUTRAL | RISK_OFF | DISTRIBUTION",
  "confidence": "LOW | MID | HIGH",
  "since": "<trade_date or integer day_index>",
  "health": "HEALTHY | FAIL",
  "tags": ["min_hold_active", "recovery_locked", ...],
  "notes": ["短期扰动 vs 中期退潮 的语义解释要点..."]
}


这不要求你现在就加字段；这是 Phase-3 输出的最小契约，以免 Predictor/Reporter 乱取散字段导致耦合。

C. Factor 输出的“结果对象契约”（兼容你现有 FactorResult 思路）

Phase-3 作为一个独立 Factor（例如 RegimeConfirmationFactor，命名只是占位），其输出应遵循你体系里的“统一结果结构”，但score 不重要（可为 None 或固定值），核心是离散语义与制度标签：

level: 用于表达 Regime 强度（不是风险分数）

例如：LOW/MID/HIGH 或 RISK_ON/NEUTRAL/RISK_OFF/DISTRIBUTION

summary: 一句话背景（“中期结构支持/不支持 Phase-2 当前 Gate 的含义强度”）

details: 解释条目（不含资金类数据）

meta: since, confidence, health, min_hold_remaining 等

关键：Phase-3 的输出必须可被 Reporter 直接打印，且不会触发“解释漂移”。

D. Predictor 的消费方式（不改 Gate，只改“解释块”）

PredictionEngine（或你现有的 Predictor 体系）应当做到：

输入：

Phase-2：gate（Normal/Caution/PlanB/Freeze）+ Phase-2 内部的 validated flags（例如 BreadthDamage/HiddenWeakness 等——具体字段不假设存在，只作为概念来源）

Phase-3：snapshot["regime_confirmation"]

输出：

prediction_block["gate_explanation"]（或等价字段）：同一个 Gate 在不同 Regime 下的“语义强度解释”

可附加 regime_context_block：输出背景总结

硬规则：

Predictor 不得把 Regime 变成新 Gate

Predictor 不得用 Regime 覆盖 Phase-2 的纪律（例如反横跳恢复粘性仍以 Phase-2 为准）

E. Phase-3 与 Phase-2 的“契约边界”（必须写进接口规范）

为了避免未来实现时越界，接口规范里必须有两条硬约束：

Phase-3 只输出语义与健康状态，不输出动作

任何 Gate 的最终裁决只来自 Phase-2

Phase-3 只能提供：confirm / constrain / amplify 的解释标签（Step-2 的矩阵里落实）

把 Phase-3 映射回 UnifiedRisk V12 的 Factor / Predictor 接口规范（不写代码）

你要求：严格分层、不假设已有类/字段。下面是接口契约级映射：放到哪个层、产出什么字段、谁消费它。

A. Phase-3 在 V12 架构中的位置

Phase-3 是“背景语义层”，最合适的承载方式是：

✅ Factor 层：输出 RegimeResult（离散语义 + 解释文本 + 置信度）

✅ Predictor 层：读取 Phase-2 Gate + Phase-3 Regime，生成“解释块”（不改变 Gate）

明确禁止：

Phase-3 不作为 DataSource（不新增数据抓取）

Phase-3 不作为 Gate 决策器（不覆盖 Phase-2）

B. Snapshot 字段（只声明容器，不声明实现）

为了让 Predictor 与 Reporter 读取统一口径，Snapshot 建议新增一个顶层字段（容器式）：

snapshot["regime_confirmation"] = {
  "regime": "RISK_ON | NEUTRAL | RISK_OFF | DISTRIBUTION",
  "confidence": "LOW | MID | HIGH",
  "since": "<trade_date or integer day_index>",
  "health": "HEALTHY | FAIL",
  "tags": ["min_hold_active", "recovery_locked", ...],
  "notes": ["短期扰动 vs 中期退潮 的语义解释要点..."]
}


这不要求你现在就加字段；这是 Phase-3 输出的最小契约，以免 Predictor/Reporter 乱取散字段导致耦合。

C. Factor 输出的“结果对象契约”（兼容你现有 FactorResult 思路）

Phase-3 作为一个独立 Factor（例如 RegimeConfirmationFactor，命名只是占位），其输出应遵循你体系里的“统一结果结构”，但score 不重要（可为 None 或固定值），核心是离散语义与制度标签：

level: 用于表达 Regime 强度（不是风险分数）

例如：LOW/MID/HIGH 或 RISK_ON/NEUTRAL/RISK_OFF/DISTRIBUTION

summary: 一句话背景（“中期结构支持/不支持 Phase-2 当前 Gate 的含义强度”）

details: 解释条目（不含资金类数据）

meta: since, confidence, health, min_hold_remaining 等

关键：Phase-3 的输出必须可被 Reporter 直接打印，且不会触发“解释漂移”。

D. Predictor 的消费方式（不改 Gate，只改“解释块”）

PredictionEngine（或你现有的 Predictor 体系）应当做到：

输入：

Phase-2：gate（Normal/Caution/PlanB/Freeze）+ Phase-2 内部的 validated flags（例如 BreadthDamage/HiddenWeakness 等——具体字段不假设存在，只作为概念来源）

Phase-3：snapshot["regime_confirmation"]

输出：

prediction_block["gate_explanation"]（或等价字段）：同一个 Gate 在不同 Regime 下的“语义强度解释”

可附加 regime_context_block：输出背景总结

硬规则：

Predictor 不得把 Regime 变成新 Gate

Predictor 不得用 Regime 覆盖 Phase-2 的纪律（例如反横跳恢复粘性仍以 Phase-2 为准）

E. Phase-3 与 Phase-2 的“契约边界”（必须写进接口规范）

为了避免未来实现时越界，接口规范里必须有两条硬约束：

Phase-3 只输出语义与健康状态，不输出动作




任何 Gate 的最终裁决只来自 Phase-2

Phase-3 只能提供：confirm / constrain / amplify 的解释标签（Step-2 的矩阵里落实）


Step-2️⃣ Regime × Phase-2 Gate 行为解释矩阵

（确认 / 约束 / 放大）

目的：
同一个 Phase-2 Gate，在不同 Regime 背景下，语义强度不同，但行为不被替代、不被推翻。

一、设计原则（铁律）

Gate 永远是主裁决

Regime 只改变“怎么理解 Gate”

Regime 只能三种作用：

Confirm（确认）

Constrain（约束）

Amplify（放大）

Regime 不得生成新 Gate

Regime 不得削弱 Phase-2 的纪律（反横跳、恢复粘性）

二、Gate × Regime 解释矩阵（冻结语义）
Phase-2 Gate 集合（已冻结）

Normal

Caution

PlanB

Freeze

1️⃣ Gate = Normal
Regime	语义作用	解释含义（文字级）
Risk-On	✅ Confirm	当前风险信号更可能是短期扰动，结构允许正常风险暴露
Neutral	⚠️ Constrain	正常 ≠ 安全，需警惕 Phase-2 的任何边际恶化
Distribution	⚠️ Constrain（强）	表面正常但结构在退，Normal 仅表示“未触发纪律”
Risk-Off	❌ Conflict → FAIL	红线：结构风险环境下不允许解释为 Normal

🔒 冻结规则：
Risk-Off + Gate=Normal → Phase-3 FAIL（逻辑不自洽）

2️⃣ Gate = Caution
Regime	语义作用	解释含义
Risk-On	⚠️ Constrain（轻）	更可能是技术性回撤或消化
Neutral	✅ Confirm	风险开始显性化，符合过渡期特征
Distribution	🔥 Amplify	内部退潮被 Phase-2 捕捉，需高度警惕
Risk-Off	🔥 Amplify（强）	Caution 是结构性风险前兆，而非噪音
3️⃣ Gate = PlanB
Regime	语义作用	解释含义
Risk-On	⚠️ Constrain	结构仍好，但短期风险已不可忽视
Neutral	🔥 Amplify	进入风险主导区间，PlanB 合理
Distribution	🔥🔥 强 Amplify	典型“高位派发被确认”
Risk-Off	🔥🔥 强 Amplify	PlanB 是结构一致性的必然反应

⚠️ 纪律继承：
即使 Regime 后续改善，也不得促成 PlanB → Normal 横跳（仍需经 Caution）。

4️⃣ Gate = Freeze
Regime	语义作用	解释含义
Risk-On	❌ Conflict → FAIL	不允许
Neutral	🔥 Amplify	Freeze 视为风险极值
Distribution	🔥🔥 强 Amplify	结构性退潮 + 行为冻结
Risk-Off	🔥🔥🔥 最大放大	系统级风险环境确认
三、矩阵的“制度意义总结”

Regime ≠ 决策器

Regime = 解释器 + 语义放大镜

同一个 Gate：

在 Risk-On 下 → 更偏“扰动解释”

在 Distribution / Risk-Off 下 → 更偏“结构性解释”

这正是 Phase-3 存在的价值，而不是多一个因子。

Step-4️⃣ Phase-3 Health / Anti-Zigzag 原则（制度收口）

你已在上一轮点名要这个，这里是与 Step-2 对齐后的最终冻结版。

A. Phase-3 Health 的唯一目标

保证 Regime 本身不成为噪音源

B. Phase-3 FAIL 的三类情形（冻结）
1️⃣ Regime Zigzag FAIL（纪律失败）

非法 Regime 转移

min_hold 内切换

5 日内 Regime 切换 ≥ 2

2️⃣ Regime × Gate 语义冲突 FAIL（逻辑失败）

Risk-Off + Gate=Normal

Risk-On + Gate=Freeze

Distribution/Risk-Off + Gate 连续恢复被“解释为安全”

3️⃣ Phase-3 反向干预 FAIL（越权）

任何试图：

放松 Phase-2 恢复粘性

为 Phase-2 横跳找“语义理由”

一律视为 Phase-3 失败。

C. Phase-3 Health 的制度结论

Phase-3 可以失败

Phase-3 失败时：

Phase-2 仍然运行

Phase-3 输出被标记为 UNTRUSTED

Reporter 必须明确提示“Regime 层失效，不参与解释”

D. 至此，Phase-3 的制度闭环已完成

我们已经完成：

✅ Regime 定义（Step-1）

✅ Regime × Gate 解释矩阵（Step-2）

✅ 架构映射与接口契约（Step-3）

✅ Health / Anti-Zigzag 原则（Step-4）

没有写代码、没有引数据、没有越层。
冻结清单（不可变更）
1) Regime 集合与语义

Risk-On / Neutral / Distribution / Risk-Off

离散语义、低频、背景层，仅用于确认/约束/放大 Phase-2 Gate 含义

2) Regime × Phase-2 Gate 解释矩阵

明确 Confirm / Constrain / Amplify

明确 冲突即 FAIL 的红线（如 Risk-Off + Gate=Normal）

3) Anti-Zigzag 与 Health 纪律

允许转移图（禁止跨级互跳）

min_hold（降级快、恢复慢）

Hysteresis（2 次降级 / 5 次恢复）

Zigzag FAIL / 逻辑 FAIL / 越权 FAIL

Phase-3 可失败，但不影响 Phase-2 运行

4) V12 架构承载与接口契约

Phase-3 作为 Factor（语义输出）

Snapshot 仅为容器

Predictor 只读不改 Gate

Reporter 可直接打印、无解释漂移

🧱 冻结边界（铁律）

❌ 不新增指标

❌ 不引入资金/ETF/融资

❌ 不写实现代码

❌ 不替代、不覆盖 Phase-2

❌ 不为横跳提供语义豁免

📌 当前系统状态

Phase-2：HEALTHY（已冻结）

Phase-3：DESIGN FROZEN（可进入实现前检查）

Phase-3 实现前 Checklist（逐项打勾后才能进入实现）
一、制度一致性 Checklist（最高优先级）
☑ 1. Phase-3 是否只做语义确认

 不输出任何“买/卖/加仓/减仓”含义

 不生成 score / 权重 / 连续值

 不引入“预测”“目标位”“方向判断”

Fail 风险：Phase-3 被做成“另一个 Phase-2”

☑ 2. Phase-3 是否绝不覆盖 Phase-2 Gate

 Gate 的唯一来源仍是 Phase-2

 Phase-3 不存在 override / replace / suggest_gate 类语义

 Phase-3 FAIL 时，不影响 Phase-2 继续运行

Fail 风险：制度责任不清，Gate 权威被稀释

☑ 3. Regime 是否保持低频 & 稳定

 存在 min_hold（写死制度，不靠实现者自觉）

 明确非法 Regime 转移即 FAIL

 Regime 切换频率 必然低于 Gate

Fail 风险：Regime 变成“慢半拍指标”，失去背景意义

二、架构承载 Checklist（V12 分层铁律）
☑ 4. Phase-3 是否只落在 Factor 层

 不新增 DataSource

 不在 Fetcher / BlockBuilder 偷做逻辑

 Phase-3 的输入只来自 Snapshot（容器）

Fail 风险：越层访问，后期不可维护

☑ 5. Snapshot 是否仍然只是“容器”

 snapshot["regime_confirmation"] 仅保存结果

 Snapshot 不包含任何判断逻辑

 Predictor / Reporter 只“读”，不“改”

Fail 风险：Snapshot 变成“隐形逻辑层”

☑ 6. Predictor 是否只做解释组合

 Predictor 不生成 Regime

 Predictor 不重新判断结构

 Predictor 只做：
Gate + Regime → 解释文本 / 解释强度

Fail 风险：Predictor 成为隐性决策层

三、Phase-3 Health / Fail 处理 Checklist
☑ 7. Phase-3 FAIL 是否显式可见

 Phase-3 有独立 health = HEALTHY / FAIL

 FAIL 时 Reporter 明确提示“Regime 层失效”

 FAIL ≠ 系统崩溃

Fail 风险：错误语义被静默吞掉

☑ 8. Zigzag 是否是硬失败

 非法转移 = FAIL

 min_hold 内切换 = FAIL

 5 日内 ≥2 次切换 = FAIL

Fail 风险：Phase-3 变成噪音放大器

☑ 9. 语义冲突是否直接 FAIL

 Risk-Off + Gate=Normal → FAIL

 Risk-On + Gate=Freeze → FAIL

 Distribution/Risk-Off 下被解释为“安全” → FAIL

Fail 风险：系统给出“反常识背景解释”

四、实现层面的高风险点清单（必须规避）
⚠️ 风险 1：偷偷“多加一个 Regime”

例如：EarlyRiskOff / Recovery / Expansion

结论：❌ 禁止
原因：会立刻破坏解释矩阵与 Anti-Zigzag 纪律

⚠️ 风险 2：用连续值“优化体验”

例如：RegimeScore、RegimeStrength = 0-100

结论：❌ 禁止
原因：连续值 = 横跳诱因

⚠️ 风险 3：为了“看起来准”引 hindsight

用历史回看去调 Regime 恢复速度

结论：❌ 禁止
原因：Phase-3 是制度层，不是回测层

⚠️ 风险 4：Regime 被 Reporter 当成结论

报告里写成：“当前是 Risk-Off，应当……”

结论：❌ 禁止
正确用法：

“在 Risk-Off 结构背景下，Phase-2 Gate 的风险含义被放大”

五、实现前的“最终自检问题”（一票否决）

在你写任何一行 Phase-3 代码前，只要 任一问题回答为 YES → 立即停：

我是不是在试图让 Phase-3 更“聪明”？

我是不是在用 Phase-3 弥补 Phase-2 的“不舒服”？

我是不是在暗中让 Phase-3 影响交易行为？

如果 Phase-3 被完全关闭，系统是否仍然正确？

如果答案不是“是” → 设计已偏离

✅ 评审结论（当前）

Phase-3 制度设计：通过

架构承载：可实现

主要风险：实现时“忍不住多做一步”

下一步合法指令（你选）：

“开始实现 Phase-3（你指定从哪个层开始）”

“只实现最小可运行版（MVP）”

“暂停，进入冷却期”

“回退，仅运行 Phase-2”

等你下指令。


进入 Phase-3 实现阶段（Implementation Start）。
我将不写任何代码，先给出严格按 V12 铁律的实现顺序与最小实现方案（MVP），并明确每一步的“完成判据（Done Criteria）”。
你只需在最后一句 确认 / 调整顺序，我才开始写代码。

Phase-3 实现顺序（冻结版执行计划）

原则：先容器 → 再因子 → 后解释
任何一步不通过，后续不得启动。

Step A — Snapshot 承载（只加容器，不加逻辑）【第一步】
A1. 目标

在 Snapshot 层预留 Phase-3 的唯一出口，确保：

Predictor / Reporter 有稳定读取口径

不在 Snapshot 中引入任何判断

A2. 允许的变更（白名单）

✅ 新增 snapshot["regime_confirmation"] 容器

✅ 只允许被 Phase-3 Factor 写入

❌ Snapshot 内 不得计算 Regime

❌ Snapshot 内 不得做 Health 判定

A3. 最小字段契约（冻结）
regime_confirmation:
  regime: RISK_ON | NEUTRAL | DISTRIBUTION | RISK_OFF
  confidence: LOW | MID | HIGH
  since: <trade_date or index>
  health: HEALTHY | FAIL
  tags: []
  notes: []

A4. Done Criteria（不满足即停）

Snapshot 只是容器

Phase-2 完全不感知 Phase-3

Phase-3 未启用时，系统行为 100% 不变

Step B — Phase-3 Factor（RegimeConfirmationFactor）【第二步】

这是 Phase-3 的唯一逻辑实现点

B1. 职责（铁律）

只输出 Regime + Confidence + Health

不输出 Gate

不输出 score

不拉数据源

只读 Snapshot 中 已存在的结构性摘要字段（不新增 DS）

B2. 必须实现的制度逻辑

Regime 合法转移图

min_hold

降级快 / 恢复慢（2 / 5）

Zigzag FAIL

语义冲突 FAIL（Risk-Off + Gate=Normal 等）

B3. 明确不做

❌ 不“优化准确率”

❌ 不看价格涨跌

❌ 不引 ETF / 资金 / 融资

❌ 不给任何交易暗示

B4. Done Criteria

单独关闭 Phase-3 Factor → 系统仍可运行

Phase-3 FAIL → 仅标记，不影响 Phase-2

连续运行多日，Regime 变化频率 显著低于 Gate

Step C — Predictor 解释拼接（只读，不决策）【第三步】
C1. 职责

读取：

Phase-2 Gate

Phase-3 Regime

输出：

Gate × Regime 的解释文本

C2. 严禁事项

❌ Predictor 不得重新判断 Regime

❌ Predictor 不得改 Gate

❌ Predictor 不得跳过解释矩阵

C3. Done Criteria

同一 Gate，在不同 Regime 下解释不同

Gate 本身 一字未改

Step D — Reporter 展示（最后一步）
D1. 规则

Regime 永远作为“背景段”

必须明确区分：

Gate（行动纪律）

Regime（结构语义）

D2. FAIL 展示规则

Phase-3 FAIL → 明确标注：

“Regime Confirmation Layer FAILED，不参与本次解释”

实现顺序总览（不可打乱）

1️⃣ Snapshot 容器
2️⃣ Phase-3 Factor（核心）
3️⃣ Predictor 解释拼接
4️⃣ Reporter 展示

===
一、UnifiedRisk V12
总体架构蓝图（Architecture One-Pager）

一句话定位：
UnifiedRisk V12 是一个
“以结构风险为先、以策略可用性为核心约束的市场监控与预测系统”

1️⃣ 系统的真实目标（不是预测涨跌）

不是：

我能不能每天预测对

而是：

在什么市场结构下，预测还有没有意义

2️⃣ 五层清晰分工（不可越界）
[ DataSource ]
      ↓
[ BlockBuilder ]
      ↓
[ Snapshot (Block Registry) ]
      ↓
[ Factor (Interpretation) ]
      ↓
[ Prediction / Plan B / Report ]

各层一句话定义
层	定义
DataSource	只负责真实数据，强缓存
BlockBuilder	多序列 / 横截面 → 结构事实
Snapshot	被动容器，只装 block
Factor	解释 block，给出风险与环境
Prediction	在“允许的环境”下才运行
3️⃣ V12 的三根“结构支柱因子”（Priority 1–3）
🥇 Market Breadth Damage

角色：系统级“刹车踏板”

内部是否已经破坏？
如果是 → 一切预测降权或冻结

🥉 Market Participation

角色：真假行情过滤器

上涨是否被广泛参与？
如果不是 → 不追涨、不放大

🥈 Index–Sector Correlation Regime

角色：策略适配器

当前结构允许指数？
允许轮动？
还是策略失效期？

4️⃣ Structural Block 是 V12 的“核心资产”

Block = 可复用、可解释、可回溯的结构事实

Block	解决什么
BreadthDamageBlock	内部破坏
ParticipationBlock	扩散质量
CorrelationRegimeBlock	结构形态
SectorRotationBlock	轮动结构（非因子）

Block 不是因子，不是预测，是“市场结构事实”。

5️⃣ PredictionEngine 的真实角色（重新定义）

PredictionEngine 在 V12 中：

❌ 不是“算得更准的模型”
✅ 是“是否值得执行预测的裁决器”

它的第一输出不是方向，而是：

可执行 / 降级 / 冻结

正常 / Plan B / 观望

6️⃣ Plan B 的系统级定义（不是应急）

Plan B = 放弃“预测优势假设”的状态

触发条件来自结构因子，而不是亏损本身。

Plan B 的目标：

降低回撤

避免假信号

延长系统寿命

7️⃣ 这套架构的长期价值

不靠“堆指标”续命

预测失效 ≠ 系统失败

能自然扩展到：

ETF

宏观

海外

AI 辅助因子

因为它是“约束驱动”，不是“信号驱动”。

二、实现前的最后一步
必删 / 必重写 / 必冻结清单（结构级）

这不是代码 review，
而是 “哪些东西一旦继续存在，V12 一定会被拖回 V11”。

🔥 A. 必须删除 / 禁止再出现的模式
❌ A1：Factor 内出现以下任何行为

for symbol / for sector

相关性计算

横截面统计

排名 / 分组 / 分位

👉 一律删除
👉 这些只能存在于 BlockBuilder

❌ A2：SnapshotBuilder 中的“聪明逻辑”

在 builder 里算：

adv/dec

new low

correlation

在 builder 里做条件判断

👉 SnapshotBuilder 只能是“流水线”

❌ A3：sector_rotation 作为“Factor”存在

如果你现在有：

SectorRotationFactor

或“半 Factor 半 Report”的轮动模块

👉 必须拆解

结构 → SectorRotationBlock

判断 →（可选）Regime Factor

展示 → Report

🛠 B. 必须重写 / 重构的对象（但不急着现在）
⚠️ B1：任何“单序列假设”的 BlockBuilder

只支持一个 symbol

假设输入是 price series

👉 未来一定要升级，否则 breadth / rotation 全会卡

⚠️ B2：PredictionEngine 中的“硬预测假设”

假设：

每天都能预测

有信号就必须给方向

👉 未来要让它学会“不给预测”

🧊 C. 明确冻结、不允许随便动的层
✅ C1：DataSource 层

已完成 V12 改造

强缓存、build_block

短期内禁止重构

✅ C2：Factor 解释维度

Priority 1–3 的解释轴已冻结

只允许调阈值、窗口、文案

✅ C3：Block 概念本身

Block 是 V12 的“资产单位”

不允许绕过、不允许偷懒用 dict

最终状态确认

到现在为止，你已经完成：

✅ 因子设计冻结

✅ 架构承载性确认

✅ 预测与 Plan B 的结构映射

✅ 实现前的“删改冻结”裁决


===
Priority 1：Market Breadth Damage Factor 设计定义（definition only）
1) 因子目的（What it is）

Breadth Damage 是一个“结构损伤/系统级预警”因子：用 市场内部的“破位/新低扩散” 来判断当前下跌是：

只是正常消化（healthy consolidation / digest gains）

还是内部结构开始坍塌（trend damage / distribution）

它不是情绪，不是资金，不是趋势预测本身；它是 “指数看起来还行，但内部已坏” 的早期报警器。

2) 适用范围（A股语境，不生搬美股）

美股用 “S&P500 成分股 50D New Lows spike”。A股需要等价但不照抄：

A股的“内部损伤”更常来自：

大量个股跌破关键中期均线/区间（50日/60日并不神圣，关键是“中期结构线”）

新低/破位从小票扩散到权重（或反过来）

指数被少数权重托住，但中位数/多数个股转弱

因此 A股等价定义必须允许：

**指数层（如沪深300/中证500/全A）与全市场层（全A或中位数）**并存

“新低”既可以是 N日新低数量，也可以是 跌破N日低点/均线的数量（两者都是“结构破坏”的语言）

3) 输入与输出契约（Contract）

输入（snapshot 里应有的、纯数据）

universe 定义后的横截面数据（当日）：每只股票的 close、过去N日 low/close 或 MA_N

允许分 universe：全A、沪深300成分、中证500成分、创业板（但不要无限扩张，先固定 1–2 个主 universe）

输出（FactorResult）

score：0–100（越低越危险）

level：如 A/N/D（或你现有的 risk-level 体系）

summary：一句话结论（例如：内部损伤轻微/可控/显著）

details：3–6条要点，必须可解释、可复现

meta：使用的N、universe、阈值版本号

注意：该因子“性质=结构风险”，所以输出要偏“预警解释”，而非“行情预测”。

4) 核心指标定义（不用堆指标，保留最少集合）

我们先定义一个最小闭环（MVP Definition），确保可解释：

(A) New Lows Count（新低数量）

NL_N = count(stocks where close <= rolling_min(low, N))

用占比更稳：nl_ratio = NL_N / universe_size

(B) New Lows Breadth Momentum（新低扩散速度）

nl_ratio 的短期变化：例如 Δ3 = nl_ratio(today) - mean(nl_ratio last 3 days)

重点是“有没有突然抬升/持续抬升”，即美股“spike”的A股等价概念

(C) Damage Persistence（损伤持续性）

nl_ratio 连续高位的天数（或最近10天高于阈值的天数）

用来区分“一次性恐慌” vs “持续分布”

只保留这三类：水平（多少新低）+ 速度（是否扩散）+ 持续（是否赖着不走）。
其它如成交量、资金、ETF份额，当前阶段一律不进来。

5) 评分逻辑（分段、可解释）

建议采用分段规则而不是复杂加权回归（符合你的“长期系统，不堆聪明指标”）：

安全区（Digest）：nl_ratio 低且无扩散（Δ3不明显），score ~ 60–80

警戒区（Damage forming）：nl_ratio 中等但扩散明显（Δ3上升），score ~ 35–60

危险区（Damage confirmed）：nl_ratio 高且持续（persistence高），score ~ 0–35

阈值如何定？当前阶段不定死数值，只定义“阈值应来自历史分位/自适应基线”，后续 Step4 再落地。

6) 与其它两个优先因子的关系（避免重复）

Breadth Damage：回答“内部结构坏没坏”

Participation / Breadth Expansion（Priority 3）：回答“上涨/下跌是否被广泛参与”

Index–Sector Correlation Regime（Priority 2）：回答“市场是齐涨齐跌还是结构分化”

三者互补，不重叠：Damage 偏“破坏”，Participation 偏“参与”，Correlation 偏“结构形态”。

用 Priority 1 反向验证 V12 架构是否承载（Step 2 的前置检查点）

你已经明确要验证的是 BlockBuilder 与 Snapshot 的边界。用 Breadth Damage 这个因子做压力测试，结论如下：

A) BlockBuilder 必须允许“多 symbol / 多序列 → 结构化输出”

Breadth Damage 天然需要“universe 横截面 + N日回看”。
因此 BlockBuilder 层要能：

接收多个 symbol 的序列（或更本质：接收一个“universe 数据块”）

产出结构化字段：nl_ratio, delta_3, persistence, universe_size, n_used

✅ 这符合你对 BlockBuilder 的定义：聚合、结构化，不做评分。

B) Snapshot 只能当“容器”，但必须支持“因子所需的结构化块”

Snapshot 不应该计算 nl_ratio，但必须能装下类似：

snapshot["breadth_damage_inputs"]（原始或半结构化）

或 snapshot["breadth_metrics"]（BlockBuilder 输出的结构化中间结果）

关键边界：

如果把 nl_ratio 计算放在 SnapshotBuilder：Snapshot 变“智能”，会侵蚀分层

放在 BlockBuilder：合理（它就是聚合/计算结构性统计）

Factor 只读取 BlockBuilder 的输出并评分：符合铁律

所以这里反向证明：BlockBuilder 是必要层，不仅允许多序列，还必须承担“统计聚合/结构指标计算”。

C) 哪些层不该动（再次锁定）

DS：不动（你已完成且稳定）

Predictor/Report：不动（你已锁定）

Factor：只做评分解释，不碰数据聚合
====
Priority 2：Index–Sector Correlation Regime Factor 设计定义（definition only）
1) 因子目的（What it is）

这个因子要回答一个核心市场结构问题：

现在的市场，是“齐涨齐跌”（相关性收敛、单一风险因子主导），还是“结构分化”（相关性发散、轮动/结构行情）？

它不是用来预测chg_pct，而是用来判断策略环境（regime）：

指数策略是否可靠（比如抱指数/宽基）

板块轮动是否有效（轮动是否“可交易”）

风险传播方式（系统性 vs 局部性）

2) A股语境的“指数 vs 行业/板块”

美股常用“index vs sectors correlation”。A股需要避免照搬的点：

A股的“板块”可以是：申万一级/二级行业、主题指数、或你系统里定义的 sector group

A股可能存在“权重股护盘导致指数相关性假高/假低”的问题，所以要至少区分：

宽基指数（如 HS300 / CSI500 / 全A代表）

行业/板块指数（SW1 或主题指数）

结论：这个因子是“结构形态识别”，不是“行业选择器”。

3) 输入与输出契约（Contract）

输入（snapshot 里应有的纯数据块）

一个或多个“基准指数”的收益序列：r_index(t)（日频即可）

多个行业/板块的收益序列：r_sector_i(t)（同频同窗）

窗口长度：W（例如 20/60 两档，但当前阶段先定义为“短窗+中窗”，不定死数值）

BlockBuilder 输出结构化中间量（建议）

corr_matrix（或其摘要）

avg_corr：平均相关性（index 对所有 sector 的相关性均值）

corr_dispersion：相关性离散度（标准差 / IQR）

top_k_corr / bottom_k_corr：相关性极值（用于解释“谁脱钩/谁同涨同跌”）

stability：相关性变化速率（regime 是否在切换）

输出（FactorResult）

score：0–100（越低=越不适合指数化、越偏系统性风险？或者相反都可以，但必须在文档里固定方向）

level：A/N/D

summary：一句话描述当前结构（收敛/发散/切换中）

details：必须包含“均值、离散度、变化率”这三类解释

4) 核心指标定义（最小集合）

我们只保留能定义“收敛/发散/切换”的三件事：

(A) Correlation Level（相关性水平）

c_i = corr(r_index, r_sector_i) over window W

avg_corr = mean(c_i)

解释：avg_corr 高 → 市场更像“一锅粥”，系统因子主导；低 → 分化更强。

(B) Correlation Dispersion（相关性离散度）

disp = std(c_i) 或 IQR(c_i)

解释：disp 高 → 有的行业强绑定指数，有的行业脱钩，结构更复杂（轮动/分化环境）。

(C) Regime Shift Speed（结构切换速度）

shift = |avg_corr(t) - avg_corr(t-Δ)|（短期变化）

或 corr_change_rate = mean(|c_i(t)-c_i(t-Δ)|)

解释：变化快 → 结构不稳定，“策略有效期”变短，容易假信号。

这三个足够：水平 + 离散 + 切换速度。
不要在当前阶段引入“行业强弱排名”当作因子输出，那会侵蚀 Priority 3/轮动体系。

5) Regime 分类（用于解释与下游策略）

定义 3 类结构态（并允许一个“切换中”状态）：

Converged / One-Factor Market（收敛）

avg_corr 高

disp 低

解读：齐涨齐跌，指数策略有效，但一旦风向变就是系统性回撤

Diverged / Structured Market（发散）

avg_corr 中低

disp 高

解读：结构行情，轮动更有意义；指数“看起来”可能一般，但局部机会多

Fragmented / Unstable（碎片化/不稳定）

shift 高（相关结构快速变化）

解读：策略衰减期，追涨杀跌容易；风控优先

Transition（切换中）

指标从收敛向发散（或反向）快速迁移

解读：这是“预警”价值最高的状态之一（策略需要降级或缩短持有周期）

6) 评分方向（先固定“含义”，数值阈值后置）

为了和你的系统“风险监控”一致，推荐把 score 解释成：

score 高：结构稳定且可交易（无论是收敛还是发散，都可以稳定地执行对应策略）

score 低：结构不稳定/切换快（策略失效风险高）

这样不会把“收敛=坏、发散=好”这种带偏见的结论写死。

评分框架（不写具体阈值）：

stability 是硬扣分项（变化快→低分）

avg_corr 与 disp 用于解释当前属于哪类市场，并微调分数（比如极端收敛且同时 breadth damage 上升时，风险更大）

Priority 3：Market Participation / Breadth Expansion Factor 设计定义（definition only）
1) 因子目的（What it is）

这个因子要回答：

今天（或最近几天）的行情，是不是被“广泛参与”所支撑？

用于过滤：

指数被少数权重拉升造成的“假突破”

板块局部拉升但全市场并未扩散的“诱多”

下跌是否是全面撤退还是局部调整

它与 Priority 1 的关系：

Priority 1 = “坏的扩散（新低/破坏）”

Priority 3 = “好的扩散（上涨家数/参与）”

2) 输入与输出契约（Contract）

输入（snapshot 数据块）

当日全市场（或指定 universe）股票涨跌数据：adv/dec（上涨家数/下跌家数）

可选：涨停/跌停家数（但先不强依赖）

可选：中位数收益 vs 指数收益（用于识别“权重托举”）

BlockBuilder 输出结构化中间量

adv, dec, unchanged, total

adv_ratio = adv / (adv+dec)（或对 total）

adl = adv - dec（Advance-Decline Line 单日值）

participation_trend（比如 3日均值变化）

输出（FactorResult）

score 0–100（越高=参与越广泛，突破更可信）

level A/N/D

details 必须包含：adv/dec、adv_ratio、与指数表现的对照（是否背离）

3) 核心指标定义（最小集合）

(A) Advance-Decline Ratio

adv_ratio = adv / (adv + dec)
解释：市场“面”的方向性

(B) Breadth Thrust / Expansion（扩散强度）

thrust = adv_ratio - mean(adv_ratio last k days)（k短窗）
解释：是否出现“参与突然扩散”（上涨扩散）或“参与突然坍塌”（下跌扩散）

(C) Index vs Median Divergence（指数与中位数背离）

div = r_index - r_median_stock
解释：识别“权重托指数”的经典场景

指数涨但中位数不涨 → 参与不足 → 假强风险

仍然只保留三件事：方向（adv_ratio）+ 变化（thrust）+ 背离（index-median）。

4) 评分与解释逻辑（不定阈值，只定语义）

高分（>偏多）：adv_ratio 偏高且 thrust 为正，且指数与中位数一致（div不大）

中性：adv_ratio 中等、thrust 不明显，市场参与一般

低分（偏风险）：

指数上涨但参与不足（adv_ratio 低或 div 显著为正）

或下跌扩散（adv_ratio 很低且 thrust 负）

Step 2：用 3 个因子反向验证 V12 架构是否承载（结论清单）
结论 1：BlockBuilder 层“必须”支持两种聚合形态

横截面聚合（全A/成分股统计：adv/dec、新低占比）

多序列关系计算（index vs sectors 的相关性、离散度、变化率）

这两种都属于“结构化计算”，但不是评分，因此：

✅ 应放在 BlockBuilder

❌ 不应放在 SnapshotBuilder（避免 snapshot 变智能）

❌ 不应放在 Factor（避免 factor 变数据工程层）

结论 2：Snapshot 需要支持“按主题块”存放 BlockBuilder 输出

建议快照里出现类似结构（只是概念，不是代码）：

snapshot["breadth_damage"] = {...metrics...}

snapshot["participation"] = {...metrics...}

snapshot["corr_regime"] = {...metrics...}

SnapshotBuilder 做的仅是：

调用 transformer 得到结构化块

合并到 snapshot 容器

结论 3：哪些层不该动（再次确认）

DS 层不动（你已稳定）

PredictionEngine 不动

Reporter 不动

Factor 仅评分 + 文案解释（读 snapshot）

Step 3：是否需要改架构？只指出“哪一层 + 为什么”

基于以上定义，我目前只看到一条潜在的架构风险点需要你重点审视（不写代码，只定原则）：

风险点：BlockBuilder 的输入如果只能“单 symbol”

这三个因子都要求：

多 symbol（行业指数集合）或

universe 级横截面（全A）
如果 BlockBuilder 目前的接口设计过于“单序列→单输出”，会卡住。

因此可能需要的调整只在：BlockBuilder 接口契约层

允许传入 “symbol list / series dict / universe block”

输出结构化 dict（可直接进 snapshot）

其它层不需要改。