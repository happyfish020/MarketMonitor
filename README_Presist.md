# UnifiedRisk V12 Persistence (L1/L2) - v1

This package implements the **Pre-Stable** persistence foundation:

- **L2 (Institutional, S1 strong consistency)**:
  - Report Artifact (immutable text + hash)
  - Decision Evidence Snapshot (DES) (immutable json + hash)
  - Atomic `publish()` transaction: report + des + link + audit

- **L1 (Run → Persist, engineering trace)**:
  - run_meta / snapshot_raw / factor_result / gate_decision
  - Best-effort, append-only (except run_meta status update)
  - No run meta_json stored (v1 choice A); keep meta in logs only.

## Quick self-test

```bash
python selftest.py
```

It will create `./unifiedrisk_persistence_demo.db` and run:
- L2: happy path publish + duplicate publish + verify
- L1: start_run + record_snapshot/factor/gate + finish_run
```

