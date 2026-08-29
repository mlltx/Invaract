---
title: Dev Commands
description: Every command for building, testing, and demonstrating Invariant from a clone of the repository.
sidebar:
  order: 4
---

These scripts, under `dev/`, are how you build, exercise, and demonstrate Invariant from
a cloned repository today — there's no published package yet (see
[Installation](/getting-started/installation/)).

| Command | Purpose |
|---|---|
| `./dev/test` | Build every module, run the demo job on a real Spark session, generate and validate a report. The primary "does everything still work" command. |
| `./dev/build` | Build every module's jar, in dependency order, without running the demo job. |
| `./dev/regression` | Contract regression pack — proves a satisfied contract executes and a violated one is aborted before any output is written. See [Prove Enforcement with the Regression Pack](/guides/running-the-regression-pack/). |
| `./dev/regression-docker` | Same regression pack, in a self-contained Docker image — no local JDK/sbt/Spark needed. |
| `./dev/report` | Start the results web UI on `http://localhost:3000`. See [View Verification Results](/guides/viewing-results/). |

## Per-module commands

Run from inside a module's directory (`contract`, `ir`, `spark-adapter`, or `plugin`):

| Command | Purpose |
|---|---|
| `sbt test` | Run that module's unit test suite in isolation. |
| `sbt stryker` | Run mutation testing (`ir`/`spark-adapter` only) — a code-quality check for contributors, not something a user of the engine needs to run. |

## Direct `spark-submit` invocation

`./dev/test` and `./dev/regression` wrap this; running it directly is useful for trying a
contract against the demo job without the full harness (as in
[Your First Contract](/getting-started/first-contract/)):

```bash
spark-submit \
  --class com.example.runner.DemoJobHarness \
  --master local[*] \
  --jars plugin/target/scala-2.12/invariant-spark-plugin-0.1.0.jar \
  runner/target/scala-2.12/invariant-spark-runner.jar \
  <input-path> <output-path> <report-path> <contract-path>
```

All four positional arguments are optional and default to the demo's own paths — you can
override only the ones you need, e.g. just the contract path to try a different contract
against the same demo data.

## Exit codes

Every script here follows the same convention: exit code `0` means success — for
`./dev/test`, that the engine actually verified a real Spark write, not just that code
compiled. A non-zero exit means something failed; check the script's console output and
`demo/output/report.json` first (see
[Troubleshooting](/troubleshooting/common-problems/)).
