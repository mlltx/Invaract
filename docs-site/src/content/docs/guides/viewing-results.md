---
title: View Verification Results
description: Inspect a run's report — in the web UI or as raw JSON.
sidebar:
  order: 4
---

import { Steps } from '@astrojs/starlight/components';

Every `./dev/test` run produces a report describing what happened: the build, the
transformation's translated plan, and the contract verification outcome. You can read it
as JSON or through the mobile-friendly web UI.

## Web UI

<Steps>

1. Generate a report if you haven't already:

   ```bash
   ./dev/test
   ```

2. Start the viewer:

   ```bash
   ./dev/report
   ```

3. Open `http://localhost:3000`. In Codespaces, forward port 3000 and open it on your
   phone — the UI is responsive down to 375px screens and polls for a new report every 2
   seconds while open.

</Steps>

The page shows:

- **Status badge** — ✓ PASS or ✕ FAIL, large and mobile-visible
- **Build information** — plugin, Spark, and Java versions, run duration
- **Test results** — unit and integration test pass/fail counts
- **Input/output data** — row count, schema, sample rows
- **Transformation IR** — the real translated plan, rendered
- **Contract verification** — PASSED/FAILED, with violation detail
- **Plugin events** — the transformation's execution timeline and diagnostics
- **Errors** — full error messages, if execution failed

## Raw JSON

```bash
cat demo/output/report.json | jq .
```

Relevant top-level fields:

| Field | Meaning |
|---|---|
| `status` | `"PASS"` or `"FAIL"` for the overall job run |
| `contractVerification.status` | `"PASSED"` or `"FAILED"` — did the output satisfy the contract |
| `contractVerification.violations` | List of violations, if any — see [Violation Types](/Invariant/reference/violation-types/) |
| `transformationIR.plan` | The rendered Spark plan, translated to Invariant's IR |
| `transformationIR.lineage` | Column-level lineage traced from that plan |
| `plugin.events` | The transformation's own execution timeline |
| `error` | Present if the job itself failed to execute |

`status` and `contractVerification.status` are deliberately separate fields — "did the
Spark job execute" and "does its output satisfy the contract" are different questions,
and conflating them would hide which one actually failed.

This format is internal to the example harness (`report.json` isn't versioned or
published as an API) — if you need a machine-readable result *from the engine itself* in
your own job, that's `VerificationResult` (see
[Reference → Contract Format](/Invariant/reference/contract-format/)), not this file.
