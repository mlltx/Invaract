---
title: Engine Capabilities
description: What each engine adapter verifies, and where it can stop a bad write.
sidebar:
  order: 7
---

<!-- GENERATED from each adapter's invaract-capabilities-<adapter>.yaml by CapabilityMatrix (verification-core). Do not edit by hand: change the declaration and run ./dev/capabilities. -->

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
| `spark` | Apache Spark 3.5 | Blocks inside the engine, before the operation executes — A SparkSessionExtensions check rule runs on the analyzed plan and aborts the write before it executes (ContractEnforcementRule); a QueryExecutionListener additionally reports completed writes. |

## Capability matrix

### Operations an adapter recognizes

| Capability | What it means | spark |
|---|---|---|
| `read.batch` | A batch read is recognized as one of the contract's inputs. | ✅ |
| `read.streaming` | A streaming read is recognized as one of the contract's inputs. | ✅ |
| `write.batch` | A batch write is recognized and checked against the contract before it executes. | ✅ |
| `write.streaming` | A streaming write is recognized and checked against the contract. | ◐ |
| `write.rowLevelDml` | Row-level MERGE / UPDATE / DELETE is recognized as a write. | ✅ |
| `write.stateChange` | A non-write operation that commits a schema change at a location (for example a snapshot rollback) is checked. | ✅ |

### Structural checks

| Capability | What it means | spark |
|---|---|---|
| `check.inputExistence` | Declared inputs must be read (MISSING_INPUT), and undeclared reads can be rejected (UNDECLARED_INPUT). *(blocks if unsupported)* | ✅ |
| `check.location` | A write lands where the contract says (OUTPUT_LOCATION_MISMATCH). *(blocks if unsupported)* | ◐ |
| `check.schema` | Field presence, type and nullability of inputs and outputs. *(blocks if unsupported)* | ✅ |
| `check.nestedTypes` | Nested types (array, map, struct) are compared structurally, not just by keyword. *(blocks if unsupported)* | ✅ |
| `check.format` | A declared output format is checked (OUTPUT_FORMAT_MISMATCH). *(blocks if unsupported)* | ✅ |
| `check.saveMode` | A declared save mode is checked (OUTPUT_SAVE_MODE_MISMATCH). *(blocks if unsupported)* | ✅ |
| `check.catalogRegistration` | A declared catalog requirement is checked for inputs and outputs. *(blocks if unsupported)* | ✅ |

### Rules

| Capability | What it means | spark |
|---|---|---|
| `rules.dml` | DML rules: merge_condition, forbid_unconditional_delete, allowed_update_columns. *(blocks if unsupported)* | ◐ |
| `rules.planShape` | Transformation-shape rules: required_group_by, forbid_cross_join, required_join_columns, required_filter_columns. *(blocks if unsupported)* | ✅ |
| `rules.custom` | Custom rule types (customRuleTypes) resolve and run. *(blocks if unsupported)* | ✅ |

### Analysis and lineage

| Capability | What it means | spark |
|---|---|---|
| `analysis.staticDataQuality` | Static data-quality proof of nullable and constraint declarations (opt-in). *(blocks if unsupported)* | ✅ |
| `analysis.roleConsistency` | A dataset's declared role (SOURCE / CONTROL / DATA_ASSET) is checked against its observed use (opt-in). *(blocks if unsupported)* | ✅ |
| `analysis.fingerprint` | A semantic fingerprint of the transformation is computed (opt-in). | ✅ |
| `analysis.sensitivityPropagation` | Input sensitivity tags are propagated to the output columns derived from them (report-only). | ✅ |
| `lineage.columnLevel` | Column-level lineage through the transformation (what each output column derives from). | ◐ |
| `lineage.boundaryResolution` | A point that erases lineage (a checkpoint, a cache) is seen through to the work behind it. | ◐ |

### Governance

| Capability | What it means | spark |
|---|---|---|
| `policy.organizational` | Organizational policy layers are enforced on the contract itself. | ✅ |

### Attaching it to a job

| Capability | What it means | spark |
|---|---|---|
| `config.zeroCodeInstall` | Installed on a job through the engine's own configuration, with no change to the job's source. | ✅ |
| `config.locationRefs` | ref://<id> locations in a contract resolve through configuration. | ✅ |
| `config.contractRegistry` | The contract can be named as registry://<id>@<version> and fetched from a registry. | ✅ |

### Reporting

| Capability | What it means | spark |
|---|---|---|
| `reporting.notifications` | Validation and write events are published to a notification sink. | ✅ |
| `reporting.dryRunInference` | Dry-run mode infers a draft contract from a real write. | ✅ |

### Fail-closed behaviour

| Capability | What it means | spark |
|---|---|---|
| `failClosed.unverifiableWrites` | An operation that looks like it writes but cannot be translated is rejected rather than passed unchecked. | ✅ |

## Notes

### spark

- `read.batch` — supported: V1 relations, DataSourceV2 catalog tables, Hive tables and JDBC reads are recognized. ([details](https://github.com/mlltx/Invaract/blob/main/docs/SPARK_ADAPTER.md))
- `read.streaming` — supported: Legacy V1 and DataSourceV2 streaming sources are recognized as contract inputs. ([details](https://github.com/mlltx/Invaract/blob/main/docs/SPARK_ADAPTER.md))
- `write.batch` — supported: Every write shape of the connectors listed in the connector support page (save, saveAsTable, insertInto, writeTo, SQL INSERT/CTAS). ([details](https://github.com/mlltx/Invaract/blob/main/docs/SPARK_ADAPTER.md))
- `write.streaming` — partial: Recognized for file and Delta streaming sinks (WriteToStream). Connectors that do not implement a streaming sink at all (for example ClickHouse) cannot be written to by Spark itself. ([details](https://github.com/mlltx/Invaract/blob/main/docs/connectors/delta.md))
- `write.rowLevelDml` — supported: Delta MERGE/UPDATE/DELETE, Iceberg copy-on-write and merge-on-read, and DELETE FROM are recognized. ([details](https://github.com/mlltx/Invaract/blob/main/docs/connectors/iceberg.md))
- `write.stateChange` — supported: Nine Iceberg CALL procedures that commit a schema change are checked against the contract. ([details](https://github.com/mlltx/Invaract/blob/main/docs/connectors/iceberg.md))
- `check.inputExistence` — supported: Also scoped per output through derivedFrom; a read hidden behind an unresolved lineage boundary is reported as unverifiable, not missing.
- `check.location` — partial: Declared locations are matched by normalized path suffix. A CTAS with no pre-existing physical path (Hive) has a known, documented location-resolution gap, and ClickHouse reads and writes report locations in different formats. ([details](https://github.com/mlltx/Invaract/blob/main/docs/SPARK_ADAPTER.md))
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

