---
title: Engine Capabilities
description: What each engine adapter verifies, and where it can stop a bad write.
sidebar:
  order: 7
---

<!-- GENERATED from each adapter's invaract-capabilities-<adapter>.yaml and the conformance scenarios (adapter-testkit). Do not edit by hand: change the declaration and run ./dev/capabilities. -->

Invaract checks a data transformation against a contract through an *engine adapter*. Every adapter
declares, for every capability below, whether it is supported, partly supported, unsupported, or does
not apply to its engine — an adapter cannot leave a gap undeclared. If a contract relies on something
an adapter declares **unsupported** (a catalog requirement, a rule, a nested type, a format, ...), the
write is rejected with `UNSUPPORTED_CONTRACT_FEATURE` rather than passed as if it had been checked.
A **partial** capability never blocks on its own; the notes below say which shapes are not covered.

A declaration is a claim, and a shared conformance suite checks it: the same engine-neutral scenarios run
as real jobs on every adapter, and each must come out the way that adapter's declaration promises. A
capability the suite has no scenario for yet is listed by the suite as unverified rather than assumed.

✅ supported · ◐ partial · ✖ unsupported · — not applicable

## Where each adapter enforces

| Adapter | Engine | Enforcement point |
|---|---|---|
| `beam` | Apache Beam (Java SDK) | Gates before the job is submitted — A wrapper runner, named by --runner, receives the constructed pipeline in run(), reads its graph (before any step executes), verifies it against the contract, and either throws (the pipeline is never submitted) or hands it to the real runner. Nothing inside a running pipeline is intercepted. |
| `spark` | Apache Spark 3.5 | Blocks inside the engine, before the operation executes — A SparkSessionExtensions check rule runs on the analyzed plan and aborts the write before it executes (ContractEnforcementRule); a QueryExecutionListener additionally reports completed writes. |

## Capability matrix

### Operations an adapter recognizes

| Capability | What it means | Suite check | beam | spark |
|---|---|---|---|---|
| `read.batch` | A batch read is recognized as one of the contract's inputs. | verified (46) | ✅ | ✅ |
| `read.streaming` | A streaming read is recognized as one of the contract's inputs. | verified (2) | ✅ | ✅ |
| `write.batch` | A batch write is recognized and checked against the contract before it executes. | verified (47) | ✅ | ✅ |
| `write.streaming` | A streaming write is recognized and checked against the contract. | verified (2) | ✅ | ◐ |
| `write.rowLevelDml` | Row-level MERGE / UPDATE / DELETE is recognized as a write. | verified (4) | — | ✅ |
| `write.stateChange` | A non-write operation that commits a schema change at a location (for example a snapshot rollback) is checked. | attested | — | ✅ |

### Structural checks

| Capability | What it means | Suite check | beam | spark |
|---|---|---|---|---|
| `check.inputExistence` | Declared inputs must be read (MISSING_INPUT), and undeclared reads can be rejected (UNDECLARED_INPUT). *(blocks if unsupported)* | verified (8) | ✅ | ✅ |
| `check.location` | A write lands where the contract says (OUTPUT_LOCATION_MISMATCH). *(blocks if unsupported)* | verified (4) | ✅ | ◐ |
| `check.outputsWritten` | A job that never writes a declared output is refused (MISSING_OUTPUT). | verified (1) | ✅ | ◐ |
| `check.schema` | Field presence, type and nullability of inputs and outputs. *(blocks if unsupported)* | verified (7) | ◐ | ✅ |
| `check.nestedTypes` | Nested types (array, map, struct) are compared structurally, not just by keyword. *(blocks if unsupported)* | verified (2) | ✅ | ✅ |
| `check.format` | A declared output format is checked (OUTPUT_FORMAT_MISMATCH). *(blocks if unsupported)* | verified (2) | ◐ | ✅ |
| `check.saveMode` | A declared save mode is checked (OUTPUT_SAVE_MODE_MISMATCH). *(blocks if unsupported)* | verified (2) | ◐ | ✅ |
| `check.catalogRegistration` | A declared catalog requirement is checked for inputs and outputs. *(blocks if unsupported)* | verified (2) | ◐ | ✅ |

### Rules

| Capability | What it means | Suite check | beam | spark |
|---|---|---|---|---|
| `rules.dml` | DML rules: merge_condition, forbid_unconditional_delete, allowed_update_columns. *(blocks if unsupported)* | verified (2) | — | ◐ |
| `rules.planShape` | Transformation-shape rules: required_group_by, forbid_cross_join, required_join_columns, required_filter_columns. *(blocks if unsupported)* | verified (8) | ◐ | ✅ |
| `rules.custom` | Custom rule types (customRuleTypes) resolve and run. *(blocks if unsupported)* | attested | ✖ | ✅ |

### Analysis and lineage

| Capability | What it means | Suite check | beam | spark |
|---|---|---|---|---|
| `analysis.staticDataQuality` | Static data-quality proof of nullable and constraint declarations (opt-in). *(blocks if unsupported)* | verified (3) | ✖ | ✅ |
| `analysis.roleConsistency` | A dataset's declared role (SOURCE / CONTROL / DATA_ASSET) is checked against its observed use (opt-in). *(blocks if unsupported)* | verified (2) | ✖ | ✅ |
| `analysis.fingerprint` | A semantic fingerprint of the transformation is computed (opt-in). | verified (2) | ✖ | ✅ |
| `analysis.functionCatalog` | Engine-native function names are mapped onto the canonical function catalog, so non-determinism is classified - and fingerprints stay comparable - the same on every engine. | verified (2) | ✖ | ✅ |
| `analysis.sensitivityPropagation` | Input sensitivity tags are propagated to the output columns derived from them (report-only). | verified (1) | ✖ | ✅ |
| `lineage.columnLevel` | Column-level lineage through the transformation (what each output column derives from). | verified (2) | ✖ | ◐ |
| `lineage.boundaryResolution` | A point that erases lineage (a checkpoint, a cache) is seen through to the work behind it. | verified (2) | — | ◐ |

### Governance

| Capability | What it means | Suite check | beam | spark |
|---|---|---|---|---|
| `policy.organizational` | Organizational policy layers are enforced on the contract itself. | attested | ✅ | ✅ |

### Attaching it to a job

| Capability | What it means | Suite check | beam | spark |
|---|---|---|---|---|
| `config.zeroCodeInstall` | Installed on a job through the engine's own configuration, with no change to the job's source. | attested | ◐ | ✅ |
| `config.locationRefs` | ref://<id> locations in a contract resolve through configuration. | attested | ✅ | ✅ |
| `config.contractRegistry` | The contract can be named as registry://<id>@<version> and fetched from a registry. | attested | ✅ | ✅ |

### Reporting

| Capability | What it means | Suite check | beam | spark |
|---|---|---|---|---|
| `reporting.notifications` | Validation and write events are published to a notification sink. | verified (2) | ✅ | ✅ |
| `reporting.dryRunInference` | Dry-run mode infers a draft contract from a real write. | attested | ◐ | ✅ |

### Fail-closed behaviour

| Capability | What it means | Suite check | beam | spark |
|---|---|---|---|---|
| `failClosed.unverifiableWrites` | An operation that looks like it writes but cannot be translated is rejected rather than passed unchecked. | verified (1) | ◐ | ✅ |

## What the conformance suite checks

The *Suite check* column says how far a declaration is verified rather than taken on trust.

- **verified** — scenarios run as real jobs on every adapter that declares the capability, and each must come out as the declaration promises.
- **attested** — no job can check it by its nature; the adapter's own tests carry it.
- **gap** — a job could check it, but the suite cannot yet; a declaration of support is a claim only.

Attested:

- `write.stateChange` — a state change is not a read-transform-write job; each engine has its own
- `rules.custom` — a custom rule type is a class resolved by the engine's own classpath; no neutral way to supply one
- `policy.organizational` — applied to the contract at session start, not part of a job
- `config.zeroCodeInstall` — how an adapter attaches to a job is engine-specific, not a job
- `config.locationRefs` — how an adapter attaches to a job is engine-specific, not a job
- `config.contractRegistry` — how an adapter attaches to a job is engine-specific, not a job
- `reporting.dryRunInference` — dry-run mode is a mode of installing, not a job

## Conformance results

The suite has 53 scenarios. For each adapter, the scenarios it is held to conform to: its checked-in baseline, which its own test run must match exactly (a scenario that stops conforming fails the build, and one that starts must be added).

| Adapter | Scenarios conforming | Of |
|---|---|---|
| `beam` | 42 | 53 |
| `spark` | 53 | 53 |

## Notes

### beam

- `read.batch` — supported: For the reads whose source the graph states - TextIO, AvroIO, ParquetIO and TFRecordIO reads, FileIO.match() and the readFiles() that read what it matches, BigQueryIO tables, KafkaIO topics, JdbcIO tables and plain single-table queries, PubsubIO topics. A read from outside the pipeline that states no location (a BigQuery or complex JDBC query, Read.from a source it does not know, patterns the job computes) is still seen: a declared input it may hide is reported unverifiable rather than missing, and with rejectUndeclaredInputs the write is refused.
- `read.streaming` — supported: The same reads, unbounded - FileIO.match().continuously(), TextIO.read().watchForNewFiles(), KafkaIO, PubsubIO. The graph is the same before submission whether a collection is bounded or not, so a stream is checked like a batch; the conformance kit's streaming scenarios run one to completion on the in-process runner.
- `write.batch` — supported: File writes (TextIO, AvroIO, TFRecordIO, FileIO.write with the Parquet, Avro and text sinks), BigQueryIO tables, KafkaIO topics, JdbcIO plain INSERT INTO one table, PubsubIO topics. A terminal step it does not recognise, or one whose destination the job computes per element (dynamic destinations), is refused as unverifiable (failClosed.unverifiableWrites).
- `write.streaming` — supported: Windowed file writes, BigQueryIO, KafkaIO and PubsubIO writes of an unbounded collection, checked as their batch forms are.
- `write.rowLevelDml` — not-applicable: Beam has no row-level MERGE, UPDATE or DELETE; a pipeline reads collections and writes new output.
- `write.stateChange` — not-applicable: Beam has no operation that commits a schema change without writing rows.
- `check.location` — supported: TextIO states its read pattern as given but its write prefix as an absolute path, so both are normalised; a FileIO write states no location in its display data, so its output directory is read from FileIO.Write by reflection (Beam 2.77.0; a write it cannot read is refused as unverifiable).
- `check.outputsWritten` — supported: The whole pipeline is known before it runs, so a declared output that no recognised write lands on refuses the job before anything starts.
- `check.schema` — partial: Known where the sink or the collection carries a schema (Parquet and Avro schemas, BigQuery table schemas, Beam row schemas). A text sink, or a collection of plain Java objects with no schema, has none to compare; its write is refused unless the output declares no columns.
- `check.nestedTypes` — supported: Beam ROW, ARRAY and MAP, and Avro records, arrays and maps, map to struct, array and map wherever a schema is known.
- `check.format` — partial: The format is the IO transform (ParquetIO is parquet, AvroIO is avro). TextIO cannot tell csv from json from plain text.
- `check.saveMode` — partial: BigQueryIO writes state one (WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY are append, overwrite, error). File, Kafka, JDBC and Pub/Sub writes have no write disposition in Beam, so a contract that declares a save mode for one of them has the write refused as unverifiable rather than passed unchecked.
- `check.catalogRegistration` — partial: A BigQueryIO table is registered in the bigquery catalog under its project, dataset and table, and is checked as such (on the read side too). File, Kafka and JDBC datasets have no catalog in Beam, so a contract requiring one sees them as unregistered.
- `rules.dml` — not-applicable: There is no row-level DML in Beam.
- `rules.planShape` — partial: Read from Beam's schema transforms - Filter.create().whereFieldName(s), Group.byFieldNames/byFieldIds (with or without aggregation) and Group.globally(), Join (inner, outer, broadcast) and CoGroup - whose fields are read by reflection from members the version matrix checks on every Beam release. A filter, grouping or join written as code (Filter.by, a DoFn, GroupByKey, CoGroupByKey) or in SQL (SqlTransform) is opaque: a required_* rule is not met by it, and under forbid_cross_join a step that combines several collections in code refuses the write as unverifiable, as does a schema transform whose fields cannot be read under any of these rules.
- `rules.custom` — unsupported: Custom rule verifiers are given a row-level change to judge, and Beam has none.
- `analysis.staticDataQuality` — unsupported: Proving a property of an output column needs to see how it is computed; functions in Beam are opaque Java code.
- `analysis.roleConsistency` — unsupported: Needs column-level usage, which the graph does not give.
- `analysis.fingerprint` — unsupported: A fingerprint of the graph alone would not capture the logic inside functions and would mislead.
- `analysis.functionCatalog` — unsupported: Beam jobs call Java methods, not named engine functions.
- `analysis.sensitivityPropagation` — unsupported: Needs column-level lineage.
- `lineage.columnLevel` — unsupported: Needs to see how each output column is computed, which opaque functions hide.
- `lineage.boundaryResolution` — not-applicable: A Beam pipeline is an immutable graph; there is no checkpoint or cache that erases what produced a collection.
- `config.zeroCodeInstall` — partial: --runner names the wrapper and the Invaract settings are Beam options registered through ServiceLoader (both shown to work). A job that sets its runner in its own code bypasses it.
- `reporting.dryRunInference` — partial: Inputs and outputs and their schemas can be inferred where the graph states them; a text write of strings gets a draft output that declares no columns. Each draft, self-checked against its own write, then one job summary merging them, is published before the pipeline runs.
- `failClosed.unverifiableWrites` — partial: A terminal step that is not a recognised sink is refused. A function that writes to an outside system in the middle of the pipeline is invisible to the graph.

### spark

- `read.batch` — supported: V1 relations, DataSourceV2 catalog tables, Hive tables and JDBC reads are recognized. ([details](https://github.com/mlltx/Invaract/blob/main/docs/SPARK_ADAPTER.md))
- `read.streaming` — supported: Legacy V1 and DataSourceV2 streaming sources are recognized as contract inputs. ([details](https://github.com/mlltx/Invaract/blob/main/docs/SPARK_ADAPTER.md))
- `write.batch` — supported: Every write shape of the connectors listed in the connector support page (save, saveAsTable, insertInto, writeTo, SQL INSERT/CTAS). ([details](https://github.com/mlltx/Invaract/blob/main/docs/SPARK_ADAPTER.md))
- `write.streaming` — partial: Recognized for file and Delta streaming sinks (WriteToStream). Connectors that do not implement a streaming sink at all (for example ClickHouse) cannot be written to by Spark itself. ([details](https://github.com/mlltx/Invaract/blob/main/docs/connectors/delta.md))
- `write.rowLevelDml` — supported: Delta MERGE/UPDATE/DELETE, Iceberg copy-on-write and merge-on-read, and DELETE FROM are recognized. ([details](https://github.com/mlltx/Invaract/blob/main/docs/connectors/iceberg.md))
- `write.stateChange` — supported: Nine Iceberg CALL procedures that commit a schema change are checked against the contract. ([details](https://github.com/mlltx/Invaract/blob/main/docs/connectors/iceberg.md))
- `check.inputExistence` — supported: Also scoped per output through derivedFrom; a read hidden behind an unresolved lineage boundary is reported as unverifiable, not missing.
- `check.location` — partial: Declared locations are matched by normalized path suffix. A CTAS with no pre-existing physical path (Hive) has a known, documented location-resolution gap, and ClickHouse reads and writes report locations in different formats. ([details](https://github.com/mlltx/Invaract/blob/main/docs/SPARK_ADAPTER.md))
- `check.outputsWritten` — partial: Spark is checked one write at a time, as each runs, so a declared output never written is found only when the application ends. It is reported then (a FAILED validation and an error in the driver log) after the job's other writes have happened; with spark.invaract.failOnMissingOutput=true the driver also exits with status 1.
- `check.schema` — supported: Column names follow spark.sql.caseSensitive.
- `check.nestedTypes` — supported: array, map and struct are compared structurally, ignoring nullability inside a container.
- `check.catalogRegistration` — supported: Hive, Delta and Iceberg catalog registrations are recognized. ([details](https://github.com/mlltx/Invaract/blob/main/docs/SPARK_ADAPTER.md))
- `rules.dml` — partial: Fully verified for Delta and for Iceberg copy-on-write. Iceberg merge-on-read UPDATE cannot have its changed columns extracted, so allowed_update_columns on it fails closed (RULE_UNVERIFIABLE_DML) rather than passing. ([details](https://github.com/mlltx/Invaract/blob/main/docs/connectors/iceberg.md))
- `analysis.staticDataQuality` — supported: Opt-in. Only a proven violation blocks; everything else is reported. ([details](https://github.com/mlltx/Invaract/blob/main/docs/STATIC_DATA_QUALITY_VERIFICATION.md))
- `analysis.roleConsistency` — supported: Opt-in. Only a high-confidence contradiction blocks.
- `analysis.fingerprint` — supported: Opt-in. Disclosed when it crosses a lineage boundary it could not see through. ([details](https://github.com/mlltx/Invaract/blob/main/docs/SEMANTIC_LINEAGE_FINGERPRINTING.md))
- `lineage.columnLevel` — partial: A user-defined function body is opaque; it is translated to an explicit UDF node over its declared arguments, and constructs the translator has no case for degrade to an UnknownPlan with a diagnostic. ([details](https://github.com/mlltx/Invaract/blob/main/docs/TRANSFORMATION_IR.md))
- `lineage.boundaryResolution` — partial: .checkpoint() and cached relations are resolved back to the work behind them when the enforcement rule saw it; a boundary created before the rule was installed, or an evicted or ambiguous one, stays opaque and is reported as unverifiable. ([details](https://github.com/mlltx/Invaract/blob/main/docs/SPARK_ADAPTER.md))
- `config.zeroCodeInstall` — supported: spark.sql.extensions=com.invaract.sparkadapter.InvaractSparkSessionExtension with spark.invaract.* settings; no change to the job.
- `config.contractRegistry` — supported: Needs the registry client on the classpath and spark.invaract.registryUrl; a job that does neither is unaffected. ([details](https://github.com/mlltx/Invaract/blob/main/docs/CONTRACT_REGISTRY.md))
- `reporting.dryRunInference` — supported: Infers from plain writes; row-level DML is inferred from only weakly and state-changing calls are skipped.
- `failClosed.unverifiableWrites` — supported: A Command-shaped plan that does not translate and is not on the known-safe list is rejected (UNVERIFIABLE_WRITE). ([details](https://github.com/mlltx/Invaract/blob/main/docs/SPARK_ADAPTER.md))

