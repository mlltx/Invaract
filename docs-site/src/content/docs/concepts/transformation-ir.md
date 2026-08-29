---
title: The Transformation IR
description: Why Invariant translates Spark plans into an engine-independent representation before checking them.
sidebar:
  order: 2
---

Before a Spark write is checked against a contract, it's translated into Invariant's own
**transformation IR** (intermediate representation) — an engine-independent algebra of
plan and expression nodes: `Read`, `Write`, `Project`, `Filter`, `Join`, `Aggregate`,
`Union`, `Sort`, `Window`, and their expression-level counterparts.

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

A real adapter will meet constructs it has no precise translation for. Invariant's answer
is: **degrade, never crash.**

- An unrecognized **plan** node becomes `ir.Unsupported(description, children)` — its own
  children are still translated and remain inspectable.
- An unrecognized **expression** falls back to a generic translation built from
  Catalyst's own `prettyName`/`children`, covering most of Spark SQL's built-in functions
  without hardcoding each one.
- A user-defined function is opaque (its body can't be reasoned about), so it's
  translated as a function call over its declared arguments, with a diagnostic flagging
  that lineage tracing can't see inside it.

Every degradation is paired with a diagnostic, so a partially understood pipeline is
still useful to check — rather than an exception that discards everything the adapter
*did* understand. This behavior is checked by property-based fuzzing across randomly
generated operation chains, not just hand-picked examples.

## Where this fits

The IR is what [structural verification](/Invariant/concepts/verification-vs-enforcement/)
actually compares against a contract — it's the "actual plan" side of "does the actual
plan match what the contract declares." See
[Reference → Connector Support](/Invariant/reference/connector-support/) for exactly
which Spark write shapes translate to a recognized `ir.Write` today.
