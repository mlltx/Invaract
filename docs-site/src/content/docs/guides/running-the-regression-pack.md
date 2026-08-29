---
title: Prove Enforcement with the Regression Pack
description: Run the pass/fail pair that proves Invariant actually blocks a bad write.
sidebar:
  order: 5
---

import { Tabs, TabItem, Aside } from '@astrojs/starlight/components';

`ContractEnforcementRule` gates every Spark write against a contract before it executes: a
write that violates its contract is aborted, and no output is ever created. The
regression pack re-runs that guarantee as a script instead of a one-off manual check, with
real `spark-submit` invocations and real assertions — not mocks.

## Run it

<Tabs>
<TabItem label="Local Spark/sbt">

```bash
./dev/regression
```

Requires a working `./dev/test` environment (Codespaces already provides one).

</TabItem>
<TabItem label="Docker">

```bash
./dev/regression-docker
```

Builds a self-contained image and runs the same pack inside it — useful for verifying the
guarantee on a machine that hasn't set up JDK/sbt/Spark at all. Only Docker is required.

</TabItem>
</Tabs>

## What it checks

Two real cases, exiting non-zero if either behaves unexpectedly:

1. **Contract satisfied** (`demo/contracts/invariant_output.yaml`) — the job exits `0`,
   the report says `PASS`, and the output file exists.
2. **Contract violated** (`demo/contracts/invariant_output_broken_example.yaml`, which
   requires a `customer_name` column the demo transformation never produces) — the job
   exits non-zero, the report says `FAIL` with a `MISSING_OUTPUT_FIELD` violation, and —
   the core guarantee — **the output file is never created**.

<Aside type="tip">
This same pack runs in CI on every push (`.github/workflows/test.yml`'s
`docker-regression` job), so the abort path is checked automatically on every change, not
just the happy path.
</Aside>

## Why this exists as a separate script

Unit tests can assert that `ContractEnforcementRule.verifyOrThrow` throws for a given
input, but that doesn't prove the *real* Spark write never happens end-to-end, via a real
`spark-submit` process, the way a production job would run. The regression pack closes
that gap: it's the same proof [Your First Contract](/Invariant/getting-started/first-contract/)
walked through manually, automated and asserted on every run.
