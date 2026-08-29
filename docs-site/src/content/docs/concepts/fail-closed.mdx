---
title: Fail-Closed by Default
description: Why an unrecognized write is rejected outright, rather than silently let through.
sidebar:
  order: 4
---

import { Aside } from '@astrojs/starlight/components';

A verification tool that silently lets through what it doesn't understand creates false
confidence — worse than no tool at all, since a team relying on it believes writes are
being checked when some genuinely aren't. Invariant is built to fail closed instead: a
write it can't verify is rejected, not passed.

## The gap this closes

Every Spark write shape Invariant hasn't been taught to translate — an early gap in
Delta Lake support, and any future write shape the adapter doesn't yet recognize — shares
the same failure mode: `SparkPlanAdapter` produces `ir.Unsupported` instead of `ir.Write`.
Early on, `ContractEnforcementRule` treated *any* non-`ir.Write` plan as "not a write,
nothing to gate" — so a write Invariant simply didn't recognize was indistinguishable
from a `SELECT` or a `.count()`, and was silently let through, contract or no contract.

## The fix: reject unrecognized commands, with an explicit safe list

A plan that's `Command`-shaped (Spark's own marker for "produces a side effect, not
rows") and doesn't translate to `ir.Write` is now rejected outright —
`ContractViolationException` with an `UNVERIFIABLE_WRITE` violation — rather than
silently passed.

<Aside type="caution" title="Why not just 'reject every unrecognized Command'?">
That was the first design considered, and it's unsafe. A real, jar-level survey of every
concrete class implementing Spark's `Command` trait found that Spark's own `Command`
hierarchy does **not** distinguish "writes data" from "pure catalog/session metadata" —
a data-writing command and a schema-only `CREATE TABLE` can implement the exact same
base trait, with no structural marker separating them. A blanket "reject every
unrecognized `Command`" policy would also reject ordinary `CREATE TABLE`,
`ANALYZE TABLE`, `CACHE TABLE`, `SHOW TABLES`, and dozens of other legitimate
DDL/administrative operations — a severe regression, not a safety improvement.
</Aside>

The actual mechanism is an explicit, documented allowlist of commands judged — by their
documented SQL semantics — to never change a table's committed row content
(schema/namespace/function DDL, `SHOW`/`DESCRIBE`/`ANALYZE`/`CACHE`, session config,
storage maintenance like `VACUUM`/`OPTIMIZE`). Anything not on that list, if it's
`Command`-shaped and isn't a recognized write, is rejected. This is deliberately
asymmetric: a safe command missing from the list costs one unnecessary rejection until
someone adds it (annoying, cheap to fix, loud); a data-mutating command wrongly added to
the list would silently defeat the entire feature (invisible, expensive). Every uncertain
case found during the survey was left off the safe list on that basis.

## The same principle applies to DML rules

A contract's `merge_condition`/`forbid_unconditional_delete`/`allowed_update_columns`
rules (see [Enforce Row-Level DML Rules](/guides/enforcing-dml-rules/)) need
specific facts extracted from a real `MERGE`/`UPDATE`/`DELETE` plan — and one case,
Iceberg's merge-on-read `UPDATE`, rewrites into a plan shape with no per-column
before/after pairing to extract those facts from. Rather than silently reporting "no
violation found" with nothing actually checked, this case is classified
`Unverifiable` and rejected with `RULE_UNVERIFIABLE_DML` — but only when the active
contract actually declares a rule that operation kind would need to be checked against;
an unrelated rule never blocks an operation it doesn't apply to.

## What this means in practice

If a job you've put under contract starts failing with `UNVERIFIABLE_WRITE` or
`RULE_UNVERIFIABLE_DML` after upgrading Spark, Delta, or Iceberg, or after changing how a
write is issued, that's Invariant telling you it can no longer confirm the write is safe
— not a false positive to work around. See
[Troubleshooting](/troubleshooting/common-problems/) and
[Reference → Connector Support](/reference/connector-support/) for what's
recognized today.
