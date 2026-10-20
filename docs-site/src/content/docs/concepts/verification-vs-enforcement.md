---
title: Verification vs. Enforcement
description: Two mechanisms, two moments — observing a write after the fact versus blocking it before it happens.
sidebar:
  order: 3
---

On Spark, Invaract ships two distinct mechanisms that are easy to conflate, since both end
up checking a write against a contract. They run at different moments, for different
purposes, and a real job typically uses both together. (Beam works differently: see
[On Apache Beam](#on-apache-beam) below.)

## `SparkAdapterListener` — observe, after the fact

A `QueryExecutionListener`, registered once at session construction, invoked by Spark
*after* every query executes. It translates the write's real plan into the IR for
reporting purposes — this is what feeds `demo/output/report.json`'s `transformationIR`
section and the results web UI.

By definition, this runs after Spark has already run the query. It's useful for
reporting and observability. It cannot prevent anything.

## `ContractEnforcementRule` — gate, before execution

A `SparkSessionExtensions` check rule, installed when the `SparkSession` is built,
invoked on the analyzed logical plan of every query *before* Spark executes it. Only a
plan that translates to a recognized write is checked; a violation throws
`ContractViolationException`, and the query never runs.

```
Spark application → Logical plan → Invaract → PASS → execute
                                             └─→ FAIL → abort
```

This was confirmed empirically, not assumed: a probe registering a check rule that
unconditionally threw on a write command showed the exception propagating out of
`DataFrame.write.parquet(...)` unwrapped — and the target file was never created.

## Why two mechanisms instead of one

`QueryExecutionListener` fires on success — it has no way to reject a query, only to
observe it after the fact. Preventing a write requires a hook that runs *before*
execution and can throw. Spark provides that through a different extension point
(`SparkSessionExtensions.injectCheckRule`) entirely. Neither mechanism can do the other's
job:

| | `SparkAdapterListener` | `ContractEnforcementRule` |
|---|---|---|
| Runs | After a query executes | Before a query executes |
| Can block a write | No | Yes |
| Purpose | Reporting / observability | Prevention |
| Extension point | `QueryExecutionListener` | `SparkSessionExtensions` check rule |

`InvaractSparkSessionExtension` always installs the check rule, which decides whether a
write happens at all. With a notification sink configured, it also registers the listener,
which reports the writes that went ahead. See
[Install the Enforcement Rule](/guides/installing-the-enforcement-rule/) for
how to wire both into your own job.

Each mechanism can also publish an event to an external system at the moment it acts —
`ContractEnforcementRule` a `ContractValidationEvent` for every check (PASS or FAIL),
`SparkAdapterListener` a `WriteEvent` once a write actually completes. See
[Configure a Notification Sink](/guides/notification-sinks/) — this is opt-in and
changes nothing about either mechanism's own behavior when no sink is configured.

## On Apache Beam

Beam builds the whole pipeline before any step runs, so Invaract checks it all at once, at
that moment. `InvaractRunner`, named by `--runner`, receives the finished pipeline, checks
every write in it against the contract, and only then hands the pipeline to the real runner
(Dataflow, Flink, the Direct runner). A violation stops the job before anything has read or
written data.

```
Beam pipeline → InvaractRunner → PASS → hand to the real runner
                               └─→ FAIL → nothing is submitted
```

There is no after-the-fact listener on Beam, so it publishes a `ContractValidationEvent` for
each check but no `WriteEvent`. See
[Use Invaract with Apache Beam](/guides/using-invaract-with-apache-beam/).
