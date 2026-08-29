---
title: Common Problems
description: Known issues and how to diagnose a failing run.
sidebar:
  order: 1
---

import { Aside } from '@astrojs/starlight/components';

## `./dev/test` exits non-zero

Work through this in order:

1. **Check the console output** — the script names which of its 7 steps failed.
2. **Review the report**:
   ```bash
   cat demo/output/report.json | jq .
   ```
   Look at `status`, `contractVerification` (is this an engine rejection, or something
   else?), and `error`.
3. **Check the web UI** (`./dev/report`) for the Contract Verification and Plugin Events
   sections.
4. **Narrow down where the problem is**:
   - Verification/translation issue → run `cd spark-adapter && sbt test` before assuming
     it's the demo transformation.
   - A `MISSING_OUTPUT_FIELD`/`OUTPUT_FIELD_TYPE_MISMATCH`/similar violation → your
     contract and your job's actual output disagree; see
     [Reference → Violation Types](/Invariant/reference/violation-types/).
   - `UNVERIFIABLE_WRITE` → your job writes via a shape Invariant doesn't recognize; see
     below.

## A write is rejected with `UNVERIFIABLE_WRITE`

This means Invariant found a command that changes data but couldn't translate it into a
recognized write — so it refused to guess, and rejected it. This is deliberate; see
[Fail-Closed by Default](/Invariant/concepts/fail-closed/).

Check whether your write goes through a shape listed in
[Reference → Connector Support](/Invariant/reference/connector-support/). If it doesn't
(a DataSourceV2 catalog write, a streaming write, or `MERGE`/`UPDATE`/`DELETE` outside the
connectors this covers), support for it may not exist yet.

## A rule check is rejected with `RULE_UNVERIFIABLE_DML`

Your contract declares a DML rule (`merge_condition`, `forbid_unconditional_delete`, or
`allowed_update_columns`) against an operation Invariant recognizes as real DML but can't
extract the specific fact that rule needs from — today, this is known to happen for
Iceberg's merge-on-read `UPDATE`. See
[Enforce Row-Level DML Rules](/Invariant/guides/enforcing-dml-rules/) for exactly which
combinations are covered per connector.

## `InaccessibleObjectException` running `sbt test` directly on JDK 17+

<Aside type="caution">
Spark reflectively accesses JDK-internal classes (`sun.nio.ch.DirectBuffer`, via
`org.apache.spark.storage.StorageUtils`) that JDK 17+'s module system closes by default.
</Aside>

`spark-submit`'s own launch scripts inject the required `--add-opens` flags
automatically, which is why `./dev/test`'s primary path needs no changes. If you invoke a
module's tests directly (`sbt test` inside `spark-adapter` or `plugin`) or use `dev/test`'s
`java -cp` fallback path (used when `spark-submit` isn't on `PATH`), those flags are
already set in `spark-adapter/build.sbt`/`plugin/build.sbt`/`dev/lib.sh` — if you're
invoking Spark some other way and hit this error, add the same flag set yourself (see
`dev/lib.sh` for the full list).

## Contract parses but is rejected as invalid

A document can be syntactically valid YAML and still fail `ContractValidator` — missing
`outputs`, a duplicate field name, an empty schema. Check `INVALID_CONTRACT` in your
violations list, and see
[Reference → Contract Format](/Invariant/reference/contract-format/#validator-checks) for
the full list of checks.

## A location doesn't match even though it "looks right"

Locations are matched by normalized suffix, not exact string equality — see
[Reference → Contract Format](/Invariant/reference/contract-format/#location-matching).
If a match still fails, double-check for a typo in the declared path relative to where
your job actually runs from, since the suffix match still requires a real path-boundary
match, not a substring anywhere in the string.

## Still stuck

1. Re-check [CLAUDE.md](https://github.com/mlltx/Invariant/blob/main/CLAUDE.md) in the
   repository for the full development guide.
2. Check the relevant module's own test suite (`contract`, `ir`, `spark-adapter`,
   `plugin`) for a similar, working example.
3. Open an issue on [GitHub](https://github.com/mlltx/Invariant).
