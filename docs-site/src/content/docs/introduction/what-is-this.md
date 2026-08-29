---
title: What is Invariant?
description: What Invariant does, what it verifies, and what it doesn't do yet.
sidebar:
  order: 1
---

Invariant is a framework for verifying that a Spark data transformation conforms to a
machine-readable **data contract** — and stopping the write if it doesn't.

Today, most teams handle this with hope: a contract document describes what a dataset
*should* look like, and a separate transformation job is trusted to produce it correctly.
Nothing actually checks the two against each other before data lands. Invariant closes
that gap for Spark jobs.

## What it does

1. **You write a contract** — a YAML document describing a transformation's inputs and
   outputs: their locations, schemas, and save modes. The format is shaped after the
   [Open Data Contract Standard (ODCS)](https://github.com/opendatadiscovery/open-data-contracts-standard).
2. **You install the enforcement rule** in your Spark job, once, at session construction.
3. **Every write your job attempts** is translated from Spark's real Catalyst logical plan
   into an engine-independent intermediate representation (IR), and checked structurally
   against the contract: does the output exist at the declared location, with the declared
   schema, in the declared format and save mode?
4. **A violation aborts the write.** Spark never executes it. No output file is created.
   A passing write proceeds exactly as it would have without Invariant installed.

```
Data contract + Spark transformation
              ↓
          Invariant
              ↓
      VERIFIED / REJECTED
```

Beyond schema and location, a contract can also declare rules that constrain row-level
`MERGE`/`UPDATE`/`DELETE` statements — for example, requiring a `MERGE`'s `ON` clause to
match on a specific column, or forbidding an unconditional `DELETE`. See
[Enforce Row-Level DML Rules](/guides/enforcing-dml-rules/).

## What it verifies today

Invariant's checks are **structural**, not semantic:

- Does the declared input/output exist, at the declared location?
- Does its schema match — field presence, type, nullability?
- Does the write use the declared format and save mode?
- For declared DML rules: does a `MERGE`/`UPDATE`/`DELETE` respect them?

It does **not** yet verify the transformation's business logic (that a `SUM` is the
*correct* sum, for instance), governance rules (masking, residency), or compatibility
between a transformation and a specific contract version. See
[Data Contracts](/concepts/data-contracts/) for the full picture and
[Reference → Violation Types](/reference/violation-types/) for exactly what's
checked.

## Who it's for

Invariant is for teams running **Apache Spark** batch or streaming jobs who want a
guarantee — not a convention — that a job's output matches what downstream consumers
were promised. If your organization already writes data contracts (ODCS or otherwise)
but has no automated way to enforce them against real Spark code, this is that missing
layer.

## Project status

Invariant is early-stage: the core verification engine (contract parsing, the
transformation IR, and the Spark adapter) is implemented and tested against real Spark
jobs, with growing connector coverage (Delta Lake, Iceberg, Parquet, CSV, Hive, Avro,
ClickHouse). There is no published package yet — see
[Installation](/getting-started/installation/) for how to use it from source
today.
