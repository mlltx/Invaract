---
title: The Transformation IR
description: Why Invaract translates Spark plans into an engine-independent representation before checking them.
sidebar:
  order: 2
---

Before a Spark write is checked against a contract, it's translated into Invaract's own
**transformation IR** (intermediate representation) — an engine-independent algebra of
plan and expression nodes. Plan nodes: `Read`, `Write`, `Project`, `Filter`, `Join`,
`Aggregate`, `Union`, `Sort`, `Limit`, and `Window`. Expression nodes: `ColumnReference`,
`Literal`, `Alias`, `Cast`, `Arithmetic`, `Comparison`, a boolean combinator (`AND`/`OR`/
`NOT`), `Conditional` (`CASE WHEN`), `Function` (the catch-all for built-in functions),
`UDF` (a user-defined function, kept explicitly distinct — see below), and `AggregateCall`.

## Why not check Spark's plan directly

Spark's Catalyst `LogicalPlan` is Spark-specific and version-sensitive — its exact node
shapes vary across Spark versions and aren't fully specified. Verification logic written
directly against Catalyst would be coupled to Spark's internals and brittle across
versions. The IR is the layer that logic is actually written against: any future engine
front end (a different Spark version, or in principle a different engine entirely) only
needs to translate into the same IR, not reimplement verification.

## What gets translated

`SparkPlanAdapter.translate` walks a Spark logical plan and produces an `ir.Plan`. A
worked example, run against this project's own demo data
(`SELECT id, SUM(value) AS lifetime_value FROM orders GROUP BY id`):

```
Write(gold.customer_orders)
└─ Aggregate
   ├─ Read(file:/.../sample.csv)
   ├─ GROUP BY id
   ├─ id = id
   └─ lifetime_value = SUM(value)
```

The IR doesn't carry a full schema for each dataset — an `ir.Read` records only which
columns were *referenced*, not a dataset's complete column set. Schema information
(field presence, type, nullability) is supplied separately, from Spark's own resolved
`StructType`, at verification time.

## Column-level lineage

`Lineage.trace` walks a translated plan and reports, for each output column, which source
columns it derives from — and whether it passed through an aggregation:

```
ColumnLineage(id, Set(.../sample.csv.id), aggregated = false)
ColumnLineage(lifetime_value, Set(.../sample.csv.value), aggregated = true)
```

This is structural provenance traced from the plan itself, not business-logic
verification — it answers "where did this column's value come from," not "is the formula
correct."

## Degrading, never crashing

A real adapter will meet constructs it has no precise translation for. Invaract's answer
is: **degrade, never crash.**

- An unrecognized **plan** node becomes `ir.UnknownPlan(description, sourceType, children)`
  — its own children are still translated and remain inspectable.
- An unrecognized **expression** falls back to `ir.Function`, built generically from
  Catalyst's own `prettyName`/`children`, covering most of Spark SQL's built-in functions
  without hardcoding each one. (The IR also has an explicit `ir.UnknownExpression` node
  for a front end that genuinely can't represent a construct at all — Spark's own
  expression algebra is generic enough that this translator essentially never needs it,
  but the node exists for the same reason `ir.UnknownPlan` does: an unrepresentable
  construct must be visible, never silently dropped.)
- A user-defined function's body is opaque (it can't be reasoned about), so it's
  translated as an explicit `ir.UDF` node — never conflated with a real `ir.Function` —
  carrying its declared name (when the engine exposes one meaningfully) and its
  argument dependencies, with a diagnostic flagging that lineage tracing can't see
  inside its actual logic.

Every degradation is paired with a diagnostic, so a partially understood pipeline is
still useful to check — rather than an exception that discards everything the adapter
*did* understand. This behavior is checked by property-based fuzzing across randomly
generated operation chains, not just hand-picked examples.

## Where this fits

The IR is what [structural verification](/concepts/verification-vs-enforcement/)
actually compares against a contract — it's the "actual plan" side of "does the actual
plan match what the contract declares." See
[Reference → Connector Support](/reference/connector-support/) for exactly
which Spark write shapes translate to a recognized `ir.Write` today.
