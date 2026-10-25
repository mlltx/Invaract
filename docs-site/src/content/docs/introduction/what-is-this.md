---
title: What is Invaract?
description: What Invaract does, what it verifies, and what it doesn't do yet.
sidebar:
  order: 1
---

Invaract is a framework for verifying that a data transformation conforms to a
machine-readable **data contract** — and stopping the write if it doesn't. It works with
**Apache Spark** jobs and with **Apache Beam** pipelines written in Java or Python.

Today, most teams handle this with hope: a contract document describes what a dataset
*should* look like, and a separate transformation job is trusted to produce it correctly.
Nothing actually checks the two against each other before data lands. Invaract closes
that gap.

## What it does

1. **You write a contract** — a YAML document describing a transformation's inputs and
   outputs: their locations, schemas, and save modes. The format is shaped after the
   [Open Data Contract Standard (ODCS)](https://github.com/opendatadiscovery/open-data-contracts-standard).
2. **You attach Invaract to the job** with a setting, not a code change: a
   `spark-submit --conf` on Spark, or a `--runner` pipeline option on Beam.
3. **Invaract reads the engine's own description of the job** — Spark's logical plan of
   each write, or Beam's pipeline graph before it runs — and translates it into an
   engine-independent representation (the IR). It checks that against the contract: does
   each output land at the declared location, with the declared schema, in the declared
   format and save mode?
4. **A violation stops the write.** On Spark, the write is aborted before it executes; on
   Beam, the pipeline is never handed to its runner. No output is created. A job that keeps
   to its contract runs exactly as it would without Invaract.

```
Data contract + Spark job or Beam pipeline
              ↓
          Invaract
              ↓
      VERIFIED / REJECTED
```

Beyond schema and location, a contract can declare rules about the transformation itself,
such as grouping by the right key or never joining without a condition, and, on Spark, rules
that constrain row-level `MERGE`/`UPDATE`/`DELETE` statements. Not every check is available
on every engine: [What Each Engine Checks](/reference/what-each-engine-checks/) has the full table.

## What it verifies today

Invaract's checks are **structural**, not semantic:

- Does the declared input/output exist, at the declared location?
- Does its schema match — field presence, type, nullability?
- Does the write use the declared format and save mode?
- For declared DML rules: does a `MERGE`/`UPDATE`/`DELETE` respect them?
- For declared transformation shape rules: does the plan group, join, or filter on the
  right columns? See
  [Enforce Transformation Shape Rules](/guides/enforcing-transformation-rules/).
- For a declared static data-quality constraint (`nullable: false`, `equals`, `oneOf`,
  `range`): does the transformation's own logic *prove* an output field always satisfies
  it — never merely predict it will? See
  [Verify Static Data Quality](/guides/verifying-static-data-quality/).

It does **not** yet verify the transformation's broader business logic (that a `SUM` is
the *correct* sum, for instance — as opposed to the narrow, provable value-domain
properties above), or governance rules such as masking and residency. See
[Data Contracts](/concepts/data-contracts/) for the full picture and
[Reference → Violation Types](/reference/violation-types/) for exactly what's
checked.

## Who it's for

Invaract is for teams running **Apache Spark** jobs (batch or streaming) or **Apache
Beam** pipelines who want a guarantee — not a convention — that a job's output matches
what downstream consumers were promised. If your organization already writes data
contracts (ODCS or otherwise) but has no automated way to enforce them against real job
code, this is that missing layer. A platform team can attach it to jobs it doesn't own.

## Project status

Invaract is early-stage. The Spark adapter is tested against real Spark jobs across Delta
Lake, Iceberg, Parquet, CSV, Hive, Avro and ClickHouse. The Beam adapters are tested against
real pipelines for the Java and Python SDK releases listed in
[Engine Version Support](/reference/spark-version-support/). There is no Maven Central release
yet: build from source ([Installation](/getting-started/installation/)) or use the
[prebuilt jars](/guides/using-prebuilt-jars-without-sbt/).
