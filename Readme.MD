# UAT-P0 · Gate Overlay + Dynamic Report Blocks (Patch Pack)

This pack contains new modules + templates to support:
1) Gate overlay rules from `config/gate_rules.yaml` (RuleSpec)
2) GateDecisionBlock renders rule titles/semantics from the same YAML (no more hard-coded meaning text)
3) ReportEngine block order/selection configured by `config/report_blocks.yaml`

## What is included

- `core/governance/gate_rule_overlay.py`
  - `apply_gate_overlay(raw_gate, slots, spec)` => {final_gate, hits, warnings, spec_ref}
- `core/reporters/report_block_registry.py`
  - allowlist mapping from YAML `id` to block class
- `core/reporters/report_block_loader.py`
  - loads `config/report_blocks.yaml` and returns `block_builders` dict in YAML order
- `core/reporters/report_blocks/gate_decision_blk.py`
  - upgraded to load semantics/title/description from `config/gate_rules.yaml` (best-effort)
- `config/report_blocks.yaml`
  - template for block selection/order

## Integration steps (minimal changes)

### A) ReportEngine uses YAML blocks
In `core/engines/cn/ashare_daily_engine.py` (where ReportEngine is built), replace the hard-coded dict:

```py
block_builders={
  "structure.facts": StructureFactsBlock().render,
  "summary": SummaryANDBlock().render,
  ...
}
```

with:

```py
from core.reporters.report_block_loader import load_block_builders

report_blocks_path = weights_cfg.get("report_blocks_path") or "config/report_blocks.yaml"
block_builders = load_block_builders(self.report_kind, report_blocks_path)

engine = ReportEngine(
  market="CN",
  actionhint_service=ActionHintService(),
  block_builders=block_builders,
)
```

And add to `weights.yaml` (optional):
```yaml
report_blocks_path: "config/report_blocks.yaml"
```

### B) Gate overlay (RuleSpec)
In `_make_gate_decision()`:
- compute base gate as today (`ASharesGateDecider().decide(...)`)
- load spec from `gate_rules_path` (you already added in weights.yaml)
- apply overlay with `apply_gate_overlay(...)`
- write back to `slots["governance"]["gate"]`:
  `{raw_gate, final_gate, mode, hits, warnings, spec_ref, rule_spec_path}`

### C) GateDecisionBlock semantics
No extra step if you replaced `gate_decision_blk.py` with the version in this pack.
It will try to load `config/gate_rules.yaml` automatically.

## Smoke test
1) Ensure `config/gate_rules.yaml` contains a "smoke hit" rule (exists condition), so you see hits in report.
2) Run engine and confirm:
   - Gate block shows rule hits with titles
   - Block order matches `config/report_blocks.yaml`
