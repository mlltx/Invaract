---
title: Quick Start
description: Build every module, run a real Spark job with the enforcement engine installed, and view the result.
sidebar:
  order: 2
---

import { Steps, Aside } from '@astrojs/starlight/components';

This walks through the fastest path from a fresh clone to a real, contract-verified Spark
write — using the example job this repository ships with. It assumes you've completed
[Installation](/Invariant/getting-started/installation/).

<Steps>

1. ### Run the test harness

   From the repository root:

   ```bash
   ./dev/test
   ```

   This single command:

   1. Builds all 5 modules (`contract`, `ir`, `plugin` concurrently, then `spark-adapter`,
      then `runner`), in dependency order.
   2. Runs a real Spark job — `DemoJobHarness` — via actual `spark-submit`, with
      Invariant's `ContractEnforcementRule` installed in the `SparkSession`.
   3. The job reads `demo/input/sample.csv`, runs a small example transformation
      (`InvariantPlugin`, which adds a `value_squared` column), and writes the result to
      `demo/output/result.parquet` — a write that Invariant verifies against
      `demo/contracts/invariant_output.yaml` before it's allowed to happen.
   4. Generates and validates `demo/output/report.json`.

   Expect output ending in something like:

   ```
   Step 7/7: Validating execution report...
     Status: PASS
     Plugin Version: 0.1.0
     Duration: 2345ms

   ✓ All validation passed
   ✓ Execution report: demo/output/report.json
   ```

   Exit code `0` means the engine actually verified a real Spark write — not just that
   the code compiled.

2. ### View the result

   Start the results UI:

   ```bash
   ./dev/report
   ```

   Open `http://localhost:3000`. In Codespaces, the port is auto-forwarded — open the
   forwarded URL, including on your phone, since the UI is responsive down to 375px
   screens.

   You'll see:

   - A **PASS** status badge
   - Build info (plugin/Spark/Java versions, duration)
   - Input/output schema and sample rows
   - The translated **Transformation IR** — the actual Spark plan Invariant checked
   - **Contract verification**: `PASSED (invariant_demo_output@1.0.0)`, no violations

3. ### See it actually block a bad write

   Everything so far proves a *passing* write goes through. To see Invariant reject a
   violating one:

   ```bash
   ./dev/regression
   ```

   This runs two real jobs: one against the contract above (passes, output file is
   created), and one against a deliberately broken contract
   (`demo/contracts/invariant_output_broken_example.yaml`, which requires a
   `customer_name` column the demo transformation never produces). The second job exits
   non-zero, and its output file is **never created** — the write was aborted before
   Spark touched disk. See
   [Prove Enforcement with the Regression Pack](/Invariant/guides/running-the-regression-pack/)
   for the full walkthrough and output.

</Steps>

<Aside type="tip">
Don't have a local Spark/sbt setup handy? `./dev/regression-docker` runs the same
pass/fail proof inside a self-contained Docker image — nothing but Docker required.
</Aside>

## What's next

- [Your First Contract](/Invariant/getting-started/first-contract/) — change the contract
  yourself and watch verification react.
- [Write a Contract](/Invariant/guides/writing-a-contract/) — the full format, from
  scratch.
- [Install the Enforcement Rule](/Invariant/guides/installing-the-enforcement-rule/) — wire
  Invariant into your own Spark job, not just the demo.
