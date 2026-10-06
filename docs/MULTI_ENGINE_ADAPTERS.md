# Multi-Engine Adapters

How Invaract moves from "a verification engine with one Spark adapter" to "a
verification engine any execution engine can plug into" — BigQuery, Apache
Beam, and whatever comes after — **without** large, undocumented capability
gaps between the engines.

This is the working plan and its status. It was written from a review of
`spark-adapter` (what leaks Spark past the IR, and why ARCHITECTURE.md's ADR-001
"adding an engine is additive" was not yet true), and each stage below lands as
its own pull request, stacked on the previous one. Update the status table in the
same PR as the work.

## Why the engine-independence claim was not yet true

`contract`, `ir` and `fingerprint` have no Spark dependency, and `ir.Plan`/`ir.Expr`
are a genuinely engine-neutral algebra. But `spark-adapter` is not a thin adapter:
about 7,100 of its 11,300 lines import Spark, and several files that *look* neutral
depended on Spark types or Spark's spelling of things. The review found eight leaks:

| # | Leak | Effect on a second engine | Addressed in |
|---|---|---|---|
| 1 | The verification core took Spark's `StructType`: `StructuralVerifier`, `InputChecker`, `OutputChecker`, `SchemaChecker`, `ContractInference`. A contract's `type:` was compared against Spark's `DataType.typeName` and parsed with Spark's `DataType.fromDDL`. | BigQuery's `INT64`/`NUMERIC`/`REPEATED`, Beam's schema `FieldType` had nowhere to land. | Stage 1 |
| 2 | `ContractEnforcementRule.verifyOrThrow` is the real adapter interface, written inline and interleaved with `LogicalPlan`, `SQLConf` and `WriteCommandSupport`. | A second adapter would copy ~200 lines of orchestration. | Stage 2 |
| 3 | Neutral code (`Violation`, `VerificationResult`, `VerificationOptions`, the notification events, `spark.invaract.*` keys, `applicationId`) lives in `com.invaract.sparkadapter`. | A BigQuery adapter would have to depend on `spark-adapter`, whose Spark dependency is `provided`. | Stage 2 |
| 4 | `ir.Function` names are Spark's `prettyName`; `fingerprint`'s `NonDeterminism` lists `spark_partition_id`, `input_file_name`, … | BigQuery's `GENERATE_UUID()` would be classified deterministic — a silently wrong fingerprint. The same logic on two engines would fingerprint differently. | Stage 5 |
| 5 | `LocationMatching` assumes file paths (`file:`, `\`, `/`-boundary suffix). | `proj.ds.tbl`, `proj:ds.tbl`, backticks, Beam IO targets don't fit. | Stage 2 |
| 6 | The IR has no `Distinct`, set operations, `UNNEST`/`explode`, pivot, sample, window frames or lateral joins. | They surface only as `UnknownPlan` diagnostics. | After Stage 5 |
| 7 | Fail-closed is Spark-shaped (`FailClosedCommands`, an allowlist of Spark `Command` class names). | No engine-neutral way to say "this write is unverifiable". | Stage 2 |
| 8 | Capability gaps are tracked per Spark *connector*, in prose (`docs/connectors/*.md`). | Nothing records "this adapter does not check catalogs / DML rules / lineage boundaries at all". | Stages 3 and 4 |

## Target shape

```
contract/   ir/   fingerprint/         engine-neutral (as today)
verification-core/ (new, Stage 2)      checkers, rules, result model, notification,
                                       location + registry resolution, org-policy
                                       enforcement, the adapter SPI
spark-adapter/                         Catalyst → IR translation, write recognition,
                                       the Spark extension and listener
bigquery-adapter/  beam-adapter/       later — only after the stages below
adapter-testkit/ (new, Stage 4)        engine-neutral conformance scenarios
```

An adapter owes the core: a translation into `ir.Plan`, a `TypeMapper` into
`contract.LogicalType`, a location normalizer, a config source, and a
**declared capability set** — including *where* it can enforce (blocking inside
the engine, a pre-submit gate, or observe-only). Anything the contract declares
that the adapter cannot check must be reported as not evaluated, never silently
passed.

## Stages

Stage 1 is split deliberately: the schema type model first (the biggest leak, and
independently reviewable), then the result-model/SPI extraction with the rest of the
module move in Stage 2.

| Stage | Item | Benefit | Status |
|---|---|---|---|
| 1 | Engine-neutral logical type model: `contract.LogicalType`/`LogicalSchema`; the schema/input/output checkers and `ContractInference` stop importing Spark types; Spark mapping isolated in `SparkSchemas`, pinned to Spark's own output by `SparkSchemasSpec` | Removes leak #1: an adapter for any engine supplies a `LogicalSchema`; no existing verdict or message changes | Done — see below |
| 2 | Extract `verification-core` and the adapter SPI (lift `verifyOrThrow`'s body; move the result model, notification, location and registry code; neutral `invaract.*` config namespace) | Removes leaks #2, #3, #5, #7: a second adapter no longer depends on `spark-adapter` or copies its orchestration | Not started |
| 3 | Per-adapter capability declaration (machine-readable YAML), generated docs matrix, CI drift check | Removes leak #8: gaps between engines are visible and checked, not discovered | Not started |
| 4 | `adapter-testkit`: engine-neutral conformance scenarios, Spark as the first adapter | Every adapter passes or declares N/A (with a reason) on the same scenarios | Not started |
| 5 | Function canonicalisation (canonical catalog + per-adapter aliases; catalog-driven non-determinism) | Removes leak #4: the same logic fingerprints the same, and `GENERATE_UUID()` is not "deterministic" | Not started |
| — | BigQuery / Beam adapters | The goal; deliberately last | Not started — needs a go-ahead after Stage 5 |

## Stage 1 — the logical type model

**What changed**

- `contract.LogicalType` (a sealed ADT: scalars, `DecimalType(p,s)`, `ArrayType`, `MapType`,
  `StructType`, and `OtherType` for a native type with no logical equivalent),
  `LogicalField`, `LogicalSchema`, and `LogicalType.parse` — the contract's own
  parser for the type grammar a contract has always used for nested types
  (`array<int>`, `map<string,decimal(10,2)>`, `struct<a:int,b:string>`). It replaces
  `DataType.fromDDL` in the checker.
- `SchemaChecker`, `InputChecker`, `OutputChecker`, `StructuralVerifier` and
  `ContractInference` take `LogicalSchema` and no longer import anything from
  `org.apache.spark`.
- `SparkSchemas` (new, in `spark-adapter`) is the one place a Spark `DataType`
  becomes a `LogicalType`. A Spark type with no logical equivalent becomes
  `OtherType` under Spark's own spelling: it equals only itself, so a mismatch stays
  visible instead of being hidden by a guessed mapping.
- `ContractEnforcementRule` and `DryRunReporter` convert at the Spark boundary.

**What did not change** — and how that was checked. No verdict, violation type,
violation message or `fieldType` written by dry-run mode changed. Rather than assert
that, `SparkSchemasSpec` pins both swaps against Spark itself: for every mapped type
the logical `typeName`/`catalogString` equals Spark's, and `LogicalType.parse` reads 33
declared nested types to exactly what `DataType.fromDDL` reads (and rejects what Spark
rejects). The existing `SchemaCheckerSpec` and `StructuralVerifierSpec` pass unmodified
apart from converting their Spark schemas at the call.

**Known limitations, deliberately kept**

- A bare declared `decimal` does not match an actual `decimal(10,2)` (a decimal's
  `typeName` carries its parameters, as it always did in Spark); declare `decimal(10,2)`.
  Inside `array<...>`/`map<...>`/`struct<...>` a bare `decimal` is `decimal(10,0)`, as in Spark.
- A declared nested type using a construct this grammar does not parse (e.g.
  `array<interval day to second>`) is reported as a type mismatch, the same as an
  unparseable declaration was before.
- `WriteFieldInfo` in the notification events still derives its type string from
  Spark's `typeName` (same strings); routing it through `LogicalType` belongs with the
  notification move in Stage 2.
- The IR's own `Literal.literalType`/`Cast.targetType` remain plain strings.
