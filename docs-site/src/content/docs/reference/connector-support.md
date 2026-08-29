---
title: Connector Support
description: Which Spark data sources Invariant recognizes, and what's checked for each.
sidebar:
  order: 2
---

import { Aside } from '@astrojs/starlight/components';

Invariant only verifies a write it can translate into a recognized `ir.Write` (see
[The Transformation IR](/concepts/transformation-ir/)). A write shape it
doesn't recognize is rejected outright rather than silently passed — see
[Fail-Closed by Default](/concepts/fail-closed/).

## Supported connectors

| Connector | Reads | Writes | Row-level DML (`MERGE`/`UPDATE`/`DELETE`) | Notes |
|---|---|---|---|---|
| **Delta Lake** | ✅ | ✅ | ✅ | Row-level DML translated via a dedicated extractor; schema-evolution and generated-column edge cases investigated and fixed. |
| **Iceberg** | ✅ | ✅ | ✅ (copy-on-write fully; merge-on-read `UPDATE` fails closed) | Catalog-based DSv2 connector. `CALL` procedures (e.g. `rollback_to_snapshot`) are classified individually — some verified against a contract, others safely allowed through, per procedure. |
| **Parquet** | ✅ | ✅ | — | Not a separate library — Spark's own bundled file format. |
| **CSV** | ✅ | ✅ | — | Same shape as Parquet. |
| **Hive** | ✅ | ✅ | — | Table reads plus multiple write shapes (including `CREATE TABLE ... AS SELECT` and static-partition `INSERT`). |
| **Avro** | ✅ | ✅ | — | Reuses the same generic write recognition as Parquet/CSV — no Avro-specific code needed. |
| **ClickHouse** | ✅ | ✅ | `DELETE` only | `UPDATE`/`MERGE` are not yet translated for this connector. Verified against a real standalone ClickHouse server. |
| **JDBC** (generic) | ✅ | — | — | Reads recognized precisely, including the resolved connection/table identity. |

Parquet, CSV, and JDBC don't support `MERGE`/`UPDATE`/`DELETE` via Spark SQL at all —
Spark itself rejects those statements before any plan reaches Invariant, so there's
nothing to verify there.

## Recognized Spark write commands

Any write that reaches one of these real Spark/Catalyst commands is recognized,
regardless of which connector issued it:

| Spark command | Typical trigger |
|---|---|
| `InsertIntoHadoopFsRelationCommand` | `df.write.parquet(...)`, `.csv(...)`, `.avro(...)` and similar file-format writes |
| `SaveIntoDataSourceCommand` | `df.write.format("delta").save(...)`, and any other `CreatableRelationProvider`-based `.save(...)` |
| `CreateDataSourceTableAsSelectCommand` | `df.write.saveAsTable(...)` / `CREATE TABLE ... AS SELECT` against a new table |

Most new connectors need **zero new recognition code** — any connector built on
`CreatableRelationProvider`'s `.save(...)` convention is already covered by
`SaveIntoDataSourceCommand` above. A connector only needs dedicated support when it
introduces a genuinely new write-command shape Spark has no existing generic node for
(row-level DML, DSv2 catalog writes, and similar).

## What happens for a write shape that isn't recognized

If your contract is active and a write doesn't match a recognized shape above, the write
is **rejected** — `UNVERIFIABLE_WRITE` — rather than silently allowed through. See
[Fail-Closed by Default](/concepts/fail-closed/) for why, and
[Reference → Violation Types](/reference/violation-types/) for the full
violation vocabulary.

<Aside type="tip">
Streaming writes and DataSourceV2 catalog writes outside the shapes above currently fall
into this fail-closed path too — support for a specific connector or write shape you
need may already exist or be straightforward to add. Check the project's issue tracker
or open one.
</Aside>
