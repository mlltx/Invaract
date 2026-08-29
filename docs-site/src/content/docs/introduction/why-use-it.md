---
title: Why use it?
description: The problem Invariant solves, and why structural verification before the write matters.
sidebar:
  order: 2
---

## The problem

Modern data platforms usually split contract-adjacent concerns across several tools:

- **Data contracts** describe what a dataset is supposed to look like.
- **Lineage systems** describe what a job actually did, after the fact.
- **Data quality systems** check data after (or during) execution.
- **Catalogs** describe datasets and their relationships.

None of these actually answer the question that matters before data lands:

> Does this job's output match what the contract for this dataset promised?

A pipeline can claim "this job produces `customer_orders`." A lineage system can later
observe "this job read `orders` and wrote `customer_orders`." Neither one proves that the
transformation's actual output — schema, location, save mode — matches what the contract
requires. Usually nobody finds out until a downstream consumer breaks.

## Why check *before* the write, not after

Post-hoc data quality checks (run a validation query after the table is written) catch
problems late: bad data has already landed, and something downstream may have already
read it. Invariant instead hooks into Spark's own query analysis, via
`SparkSessionExtensions`, and rejects a violating write **before Spark executes it**. The
target file or table is never created. See
[Verification vs. Enforcement](/Invariant/concepts/verification-vs-enforcement/) for how
this differs from Invariant's own post-hoc reporting path.

## Why a real Spark plan, not a schema diff

Comparing "the contract's declared output schema" against "the DataFrame's schema right
before `.write()`" sounds sufficient, but it can't see how the data actually got there —
whether a `MERGE` matched on the columns it should have, or whether a `DELETE` had a
filter at all. Invariant translates Spark's actual Catalyst logical plan into an
engine-independent IR (see [The Transformation IR](/Invariant/concepts/transformation-ir/)),
so verification is checking the transformation Spark is about to run, not a description of
it.

## Why it's worth trusting

A verification tool that silently lets through what it doesn't understand is worse than no
tool at all — it creates false confidence. Invariant is deliberately built to
**fail closed**: a write shape it doesn't recognize is rejected, not passed through
unverified. See [Fail-Closed by Default](/Invariant/concepts/fail-closed/) for why, and
how that policy was arrived at from a real gap found during development (an early version
silently let an unrecognized Delta write through with no verification at all).

## When Invariant is a good fit

- You already write, or want to write, machine-readable data contracts (ODCS-shaped or
  close to it) for tables your Spark jobs produce.
- You want a write to genuinely fail — not just log a warning — when it violates its
  contract.
- Your jobs write via Delta Lake, Iceberg, Parquet, CSV, Hive, Avro, or ClickHouse — see
  [Connector Support](/Invariant/reference/connector-support/) for what's covered today.

## When it isn't (yet)

- You need to verify business logic correctness (is the aggregation itself right?), not
  just structural shape — out of scope today, see
  [What is Invariant?](/Invariant/introduction/what-is-this/#what-it-verifies-today).
- You're not on Apache Spark — the only adapter that exists today translates Spark's
  Catalyst plans; the IR itself is engine-independent, but no other front end has been
  built yet.
