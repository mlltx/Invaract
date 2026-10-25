---
title: Dev Commands
description: Every command for building, testing, and demonstrating Invaract from a clone of the repository.
sidebar:
  order: 4
---

These scripts, under `dev/`, are how you build, exercise, and demonstrate Invaract from
a cloned repository (see [Installation](/getting-started/installation/)).

| Command | Purpose |
|---|---|
| `./dev/test` | Build every module, run the demo job on a real Spark session, generate and validate a report. The primary "does everything still work" command. |
| `./dev/build` | Build every module's jar, in dependency order, without running the demo job. |
| `./dev/dry-run` | Run the demo job with no contract at all, and print one inferred from its actual inputs/outputs. See [Infer a Starting Contract with Dry-Run Mode](/guides/dry-run-mode/). |
| `./dev/location-provider-demo` | Run the demo job against a contract whose input location is a `ref://` reference, resolved at startup from a location map. See [Resolve Dataset Locations at Runtime](/guides/resolving-dataset-locations/). |
| `./dev/catalog-registration-demo` | A narrated walkthrough of catalog registration: three real Spark sessions against an embedded Hive metastore, one per scenario, printing every event published. See [Require Catalog Registration](/guides/requiring-catalog-registration/). |
| `./dev/registry-demo` | Start a real invaract-registry server, register a contract into it, and run the demo job against a `registry://<id>@<version>` reference resolved from that server. See [Use a Contract Registry](/guides/using-a-contract-registry/). |
| `./dev/regression` | Contract regression pack — proves a satisfied contract executes and a violated one is aborted before any output is written. See [Prove Enforcement with the Regression Pack](/guides/running-the-regression-pack/). |
| `./dev/regression-docker` | Same regression pack, in a self-contained Docker image — no local JDK/sbt/Spark needed. |
| `./dev/report` | Start the results web UI on `http://localhost:3000`. See [View Verification Results](/guides/viewing-results/). |

## Per-module commands

Each module is its own sbt build; there is no build at the repository root. Run these from
inside a module's directory (`contract`, `ir`, `fingerprint`, `verification-core`,
`spark-adapter`, `beam-adapter`, `plugin`, ...):

| Command | Purpose |
|---|---|
| `sbt test` | Run that module's test suite in isolation. |
| `sbt assembly` | Build the module's jar. In `beam-adapter`, this builds the checker jar Beam Python pipelines need, `target/scala-2.12/invaract-beam-checker-0.1.0.jar`. |

## Contract command-line tools

These check contracts without running any job, which makes them useful in a contracts
repository's own CI. Run them from the repository root; file paths are relative to
`contract/`, or absolute.

| Command | Purpose | Guide |
|---|---|---|
| `cd contract && sbt "runMain com.invaract.contract.cli.MergeDraftsCli --id <id> --version <version> <drafts-dir>"` | Merge the dry-run drafts of a job with several writes into one contract. | [Dry-run mode](/guides/dry-run-mode/#merging-drafts-from-a-job-with-several-writes) |
| `cd contract && sbt "runMain com.invaract.contract.cli.OrgPolicyLintCli <org-policy.yaml> <contracts-dir>"` | Check every contract in a directory against an organizational policy. | [Enforce an organizational policy](/guides/enforcing-organizational-policy/) |
| `cd contract && sbt "runMain com.invaract.contract.cli.CrossContractLintCli <contracts-dir>"` | Check that contracts which feed each other agree on the dataset types between them. | [Declare input and output types](/guides/declaring-input-and-output-types/) |

For example, from a real run against the demo's own contracts:

```text
$ cd contract && sbt "runMain com.invaract.contract.cli.CrossContractLintCli ../demo/contracts/"
[ OK ] 10 contract(s) checked, no cross-contract issues found
```

## Direct `spark-submit` invocation

`./dev/test` and `./dev/regression` wrap this; running it directly is useful for trying a
contract against the demo job without the full harness (as in
[Your First Contract](/getting-started/first-contract/)):

```bash
spark-submit \
  --class com.invaract.runner.DemoJobHarness \
  --master local[*] \
  --jars plugin/target/scala-2.12/invaract-spark-plugin-0.2.0.jar \
  runner/target/scala-2.12/invaract-spark-runner.jar \
  <input-path> <output-path> <report-path> <contract-path> <notify-config-path>
```

All five positional arguments are optional and default to the demo's own paths (the
notification config defaults to none — see
[Configure a Notification Sink](/guides/notification-sinks/)) — you can override only the
ones you need, e.g. just the contract path to try a different contract against the same
demo data, or just the fifth to turn on notification without changing anything else
(pass the demo's own defaults for the earlier positional arguments to reach it). Add
`--dry-run` anywhere in the argument list (before or after the positional ones) to run
with no contract at all — see
[Infer a Starting Contract with Dry-Run Mode](/guides/dry-run-mode/). Add
`--conf spark.invaract.locationMap=<path>` (a Spark configuration flag, positioned before
the jar like `--jars` above — not an argument to `DemoJobHarness` itself) to resolve any
`ref://` locations the contract declares from that `.properties` file before verification
runs — see [Resolve Dataset Locations at Runtime](/guides/resolving-dataset-locations/).

## Exit codes

Every script here follows the same convention: exit code `0` means success — for
`./dev/test`, that the engine actually verified a real Spark write, not just that code
compiled. A non-zero exit means something failed; check the script's console output and
`demo/output/report.json` first (see
[Troubleshooting](/troubleshooting/common-problems/)).
