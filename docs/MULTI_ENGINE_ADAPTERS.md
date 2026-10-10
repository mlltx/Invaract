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
| 3 | Neutral code (`Violation`, `VerificationResult`, `VerificationOptions`, the notification events, `spark.invaract.*` keys, `runId`) lives in `com.invaract.sparkadapter`. | A BigQuery adapter would have to depend on `spark-adapter`, whose Spark dependency is `provided`. | Stage 2 |
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
| 1 | Engine-neutral logical type model: `contract.LogicalType`/`LogicalSchema`; the schema/input/output checkers and `ContractInference` stop importing Spark types; Spark mapping isolated in `SparkSchemas`, pinned to Spark's own output by `SparkSchemasSpec` | Removes leak #1: an adapter for any engine supplies a `LogicalSchema`; no existing verdict or message changes | Done (PR #102) — see below |
| 2a | Extract the `verification-core` module: the engine-neutral checkers, verifiers, result model, notification and location code move out of `spark-adapter`; seams for the three Spark couplings (lineage-boundary types, DML kind, inferred write) | Removes leak #3 (and the module half of #5/#7): a second adapter depends on `verification-core`, not `spark-adapter`; no user-visible change at the time (the package rename is Stage 2c) | Done — see below |
| 2b | Adapter SPI: `VerificationPipeline` (the write / state-change / fail-closed branches of `verifyOrThrow`) and `VerificationSetup` + `ConfigSource` (options, `ref://` locations, org policy, read through a neutral config view) | Removes leak #2: a second adapter calls three entry points instead of copying ~200 lines of orchestration, and gets every `--conf`-style capability by spelling the neutral keys its own way | Done — see below (the fail-closed wording is deferred, see "Not done in 2b"; `ContractSource` and the attach decisions moved later, see "Attaching to a job") |
| 2c | Neutral package names: `verification-core` is `com.invaract.verification` (`.notification`, `.location`), `notification-kafka` follows; no forwarding classes | Removes the last Spark-flavoured name from the engine-neutral surface; breaking, taken before anything is released | In review |
| 3 | Per-adapter capability declaration (machine-readable YAML), generated docs matrix, CI drift check | Removes leak #8: gaps between engines are visible and checked, not discovered; a contract that relies on something an adapter declares unsupported is rejected, not passed unchecked | Done — see below |
| 4 | `adapter-testkit`: engine-neutral conformance scenarios, Spark as the first adapter | Every adapter passes the same scenarios or declares N/A (with a reason), and a declaration is checked against behaviour | In review |
| 5 | Function canonicalisation (canonical catalog + per-adapter aliases; catalog-driven non-determinism) | Removes leak #4: the same logic is classified the same on every engine, and `GENERATE_UUID()` is not "deterministic" | In review |
| — | BigQuery / Beam adapters | The goal; deliberately last | Not started — needs a go-ahead. Prerequisites in progress: kit hardening (Stage 4), module registration and the adapter guide, and the engine research (`docs/ENGINE_RESEARCH_BIGQUERY_BEAM.md`) |

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

## Stage 2a — the `verification-core` module

**What changed**

- New sbt module `verification-core` (no Spark dependency; `contract`, `ir`, `fingerprint`
  as dependencies; `slf4j-api` and Hadoop's client API `provided`, as before). It now holds
  the structural checkers, the rule/data-quality/role verifiers, the result model
  (`Violation`, `VerificationResult`, `ContractViolationException`), `ContractInference`,
  and the whole `notification/` and `location/` packages — moved with `git mv` so
  history follows.
- `spark-adapter` depends on it (0.10.0 → 0.11.0) and its assembly jar bundles it, exactly
  as it already bundles `fingerprint`, so installing the one jar is unchanged.
- The Spark-free specs moved with the code and run in the new module without Spark
  (24 suites, 402 tests; coverage 91.54% statements / 89.88% branches; whole-module mutation
  score 89.18%); `SchemaCheckerSpec` and `InputOutputCheckerSpec` were rebuilt on
  `LogicalSchema` (a tiny `Cols` builder) so the checkers are tested in the module that owns
  them. `spark-adapter` keeps 28 suites / 684 tests, all passing.

**Three seams, because three things were genuinely Spark-specific**

| Was | Now |
|---|---|
| `InputChecker` read `CheckpointRegistry.BoundarySourceTypes` (Spark node names) | The adapter passes the `ir.UnknownPlan` source types it treats as a lineage boundary to `StructuralVerifier.verify`; the default is empty ("this engine has none"), so a boundary-less engine can never report an input as merely "unverifiable" |
| `RuleVerifier` took `RowMutationSupport.Kind` | It takes the neutral `MutationKind`; `RowMutationSupport.Kind` is now an alias of it |
| `ContractInference.infer` took Spark's `WriteCommandInfo` | It takes an engine-neutral `InferredWrite(location, format, saveMode, outputSchema)` |

**Package names.** Stage 2a moved the code between modules and kept the package, on the
reasoning that deployed `notify.properties` files name built-in sinks by fully-qualified class
name (`sink.class=com.invaract.sparkadapter.notification.FileNotificationSink`), as do custom
rule types and plug-ins. That reasoning was superseded in Stage 2c: there are no deployed
consumers, so the neutral names were adopted at once instead of carrying a forwarding layer.

**MiMa.** Moving public classes out of `spark-adapter` is a deliberate break *of that
artifact* (every moved class is a `MissingClassProblem`), though nothing changes for a user
of the assembled jar. It is declared in `spark-adapter/build.sbt`'s `mimaBinaryIssueFilters`
with the reason, per CLAUDE.md's API Compatibility Requirement, and `verification-core` gets
its own MiMa entry (nothing to compare against until it is released).

**Not done in 2a, deliberately**

- Published to Maven Central by `release.yml` since review pass 3 (`spark-adapter`'s published POM
  depends on it, so a release without it would not resolve).
- `registry/ContractSource` (reads a `SparkSession`) and `WriteFieldInfo`'s Spark type
  strings stay in `spark-adapter`; both belong with 2b's neutral config namespace.
  (`ContractSource` has since moved: see "Attaching to a job".)
- Test helpers `TestNotificationSink`, `EventSchema` and `CustomRuleVerifierFixtures` are
  duplicated in both modules' test sources (separate sbt builds cannot share test classes).

## Stage 2b — the adapter SPI

An adapter's job is now exactly what is engine-specific: recognize an operation, translate it
into `ir.Plan`, map its schemas into `LogicalSchema`, classify DML. Everything after that is the
core's.

**`VerificationPipeline`** (`verification-core`) is the body of what used to be
`ContractEnforcementRule.verifyOrThrow`, with the Spark parts removed. Three public entry points,
one per thing an adapter can find:

| Entry point | When an adapter calls it |
|---|---|
| `verifyWrite(contract, write: => CheckedWrite, options, sink, runId, capabilities, job)` | a recognized write |
| `verifyStateChange(contract, description, location, resultingSchema, caseSensitive, options, sink, runId, job)` | a state-changing, non-write operation that commits a schema change (Spark: Iceberg `CALL` procedures) |
| `rejectUnverifiableWrite(contract, operation, translatedPlan, sink, runId, job)` | the fail-closed response to something that looks like a write but could not be translated |

`job` is the adapter's `JobInfo` (below): pass it on every call and it rides on every event the call publishes.

`CheckedWrite` is what an adapter hands over for a write: the translated plan, input/output
`LogicalSchema`s, case sensitivity, its DML classification (`MutationClassification`), the
lineage-boundary node types it uses, and any resolution caveats. `write` is by-name on purpose:
the pipeline validates the contract *first* and only then asks the adapter to build the write, so an
adapter cannot get that order wrong (an invalid contract must never reach checks that assume a sound
one, and building the write can fail on a plan the contract has no bearing on). The pipeline owns the
verifier ordering, fingerprinting, event publishing and the explanation text, so two engines cannot
drift apart on them.

**`VerificationSetup` + `ConfigSource`** are the settings-driven half, read once at session start:
`ref://` location resolution, the five `VerificationOptions` flags overlaid from configuration, and
organizational policy layering (reject a non-compliant contract, inject required rules, raise the
option floor). They read configuration only through a `ConfigSource` keyed by the neutral names in
`InvaractConf` (`locationMap`, `orgPolicy`, ...). An adapter spells those its own way: Spark's
`SparkConfigSource` is `spark.invaract.<name>`, so every existing `--conf` key is unchanged
(`SparkConfigSourceSpec` pins that), and another engine maps the same names onto its own options —
which is how a new adapter satisfies CLAUDE.md's External Attachability Requirement without
re-implementing any of it.

**Seams.** `MutationClassification` (`Extracted`/`Unverifiable`) joins `MutationKind` in the core;
`RowMutationSupport.Classification` is an alias of it. `ContractEnforcementRule` keeps thin,
same-signature delegating methods (`resolveContractLocations`, `resolveVerificationOptions`,
`enforceOrgPolicy`, `resolveOrgPolicyLayers`, `explain`) so nothing that called them changes.

**Tests.** The pipeline and setup are tested in `verification-core` with hand-built `ir` plans and a
map-backed `ConfigSource` — no engine at all (`VerificationPipelineSpec`, `VerificationSetupSpec`,
`ConfigSourceSpec`) — which is the practical proof the SPI is engine-free. `ToyEngineAdapterSpec` goes
one step further: a complete toy adapter (its own job type, BigQuery-flavoured column types, translation
into `ir.Plan`, a type mapper and an env-style `ConfigSource`) written against only the public SPI, giving
the same verdicts as Spark for the same contract and letting a platform turn on
`rejectUndeclaredFields` through the toy engine's own configuration with no change to its job; it is
also the seed of the Stage 4 conformance kit. `spark-adapter`'s existing suites exercise the same path
end to end through a real `SparkSession`; `SparkConfigSourceSpec` pins every public `spark.invaract.*`
key to its neutral name. `verification-core` is now 28 suites / 454 tests, 95.39% statement / 92.75%
branch coverage; the three SPI files score 100% under mutation testing.

**Not done in 2b, deliberately**

- *Neutral package names.* Done in Stage 2c (below).
- *`registry/ContractSource`* and the attach decisions (`contract`, `dryRun`, `notifyConfig`,
  `registryUrl`, `registryClientClass`). Done in the pre-adapter review: `ContractReference` and
  `AttachSetup` in `verification-core` (below, "Attaching to a job"). What stays in `spark-adapter` is
  installing the result: the check rule, the listener, the dry-run reporter and `jobId`/metadata keys.
- *Fail-closed wording.* `UnverifiableWrite`'s remediation text still names Spark's
  `FailClosedCommands`; `rejectUnverifiableWrite` is engine-neutral, the message is not yet.

### Attaching to a job

Attaching to a job whose source a platform does not own has the same few decisions in every engine, and
they depend only on configuration, so they are made once in `verification-core` instead of once per adapter:

- `ContractReference`: the `contract` setting is a file path or a `registry://<id>@<version>` reference;
  `resolve(raw, config)` parses the file or fetches from the registry named by `registryUrl`
  (`registryClientClass` overrides the client). The registry client is found by class name with no
  compile-time dependency, and it is loaded without being initialised and constructed only after it is
  shown to have the methods the call needs, so naming an unrelated class runs none of its code.
- `AttachSetup.select(config)`: returns an `AttachPlan`, `Enforce(contract, sink)` or `DryRun(sink)`.
  `dryRun=true` ignores `contract` and tolerates a sink that cannot be set up (one WARN, no sink, the job is
  unaffected); enforcement requires `contract` and fails at start-up on a sink it cannot set up.

An adapter supplies a `ConfigSource` spelling `contract`, `dryRun`, `notifyConfig`, `registryUrl` and
`registryClientClass` its own way, calls `select`, then acts on the plan with its engine's hook. Spark's
`InvaractSparkSessionExtension.checkRuleFor` is exactly that (installing a check rule, a listener, or the
dry-run reporter); `registry.ContractSource` keeps its names and signatures as Spark's face of
`ContractReference`. The location map, option overlay and organizational policy are still
`VerificationSetup`'s, applied to the contract a plan carries.

## Stage 3 — capability declaration

Leak #8 was that nothing recorded what an adapter does *not* check. Spark's gaps were tracked
per connector in prose; a second engine would have had no place to say "no catalogs, no DML rules,
no lineage boundaries" at all, and a contract author would find out by a bad write getting through.

**The vocabulary** (`Capability`, `verification-core`) is a fixed list of capabilities (29 when this stage landed; 30 after Stage 5) in six
groups — what an adapter *recognizes* (batch/streaming writes, DML, state changes), what it *checks*
(location, format, save mode, catalog, schema, nested types, ...), the *rules* it evaluates, the
*analyses* it can run, *lineage*, and the *fingerprint*. Each carries an `enforcesContract` flag:
whether leaving it unsupported would let a contract requirement go unchecked (a missing catalog check
does; a missing fingerprint does not).

**The declaration** is one YAML file per adapter, `invaract-capabilities-<adapter>.yaml`, shipped as
a resource of that adapter (Spark's: `spark-adapter/src/main/resources/`). It names the adapter, its
engine, its *enforcement point* (`in-engine-blocking` / `pre-submit-gate` / `observe-only` — whether
a bad write can actually be stopped, which differs between engines far more than any single check),
and a status for every capability: `supported`, `partial`, `unsupported` or `not-applicable`, each
with a `note` where it is not plainly supported. **An undeclared capability is a parse error**, and
so is an unknown one, so a new capability added to the vocabulary forces every adapter to take a
position on it, and a gap cannot exist by omission.

**Enforcement.** `CapabilityCheck` reads the contract (and the `VerificationOptions`) and works out
which capabilities it relies on. `VerificationPipeline.verifyWrite` takes the adapter's
`AdapterCapabilities` and, for each relied-on capability the adapter declares `unsupported`, emits
`UNSUPPORTED_CONTRACT_FEATURE` — a fail-closed violation, so the write is rejected and the message
names the capability and the adapter's own reason. `partial` never blocks on its own (it would turn
every nested-type contract into a rejection); its note is what the generated page shows. Spark
declares nothing unsupported, so no Spark verdict changes; the check exists for the adapters that will.

**The matrix and its drift check.** `./dev/capabilities` runs `CapabilityMatrix` over every
`invaract-capabilities-*.yaml` in the repo and regenerates
`docs-site/.../reference/engine-capabilities.md`. `CapabilityMatrixSpec` fails when the checked-in page
differs from what the declarations generate, so editing a declaration without regenerating (or editing
the page by hand) fails the `verification-core` tests — in CI, with no separate job.

**Tests.** `CapabilitySpec` (parsing, every rejection path), `CapabilityCheckSpec` (each relied-on
shape, each side of every condition), `CapabilityMatrixSpec` (rendering and drift),
`SparkCapabilitiesSpec` (Spark's declaration loads, is complete, and a broken one fails closed), and a
`VerificationPipelineSpec` case for the violation ordering.

**Not done in 3, deliberately.** Spark's `docs/connectors/*.md` per-connector gaps stay prose — they are
about *data sources*, not about the adapter's own capabilities; the testkit (Stage 4) is what will
check the declarations are *true*, since a declaration only says what an adapter claims.

## Stage 2c — neutral package names

`verification-core` was extracted in Stage 2a with its packages unchanged. With no consumers
to protect, the rename was made at once instead of being deferred behind a forwarding layer:

| Was | Now |
|---|---|
| `com.invaract.sparkadapter.{Violation, VerificationPipeline, StructuralVerifier, ...}` | `com.invaract.verification.*` |
| `com.invaract.sparkadapter.notification.*` (sinks, events, config) | `com.invaract.verification.notification.*` |
| `com.invaract.sparkadapter.location.*` | `com.invaract.verification.location.*` |
| `com.invaract.sparkadapter.notification.kafka.KafkaNotificationSink` | `com.invaract.verification.notification.kafka.KafkaNotificationSink` |

What stays in `com.invaract.sparkadapter` is what is Spark's: the enforcement rule, the
session extension, the listener, plan translation, dry-run reporting, `SparkSchemas`,
`SparkConfigSource`, `SparkCapabilities`, `registry/`. Configuration that names a class by
fully-qualified name must be updated — `sink.class=` in notification properties, and any
custom `customRuleTypes` plug-in that imported the moved types. The `spark.*` configuration
keys and `InvaractSparkSessionExtension`'s own class name are unchanged.

Members the adapter needs from the core that were `private[sparkadapter]` are
`private[invaract]` (the two modules now sit in different packages). `spark-adapter`'s MiMa
filters record the move; the one signature of this module's own surface that changes is
`ContractEnforcementRule.forContract`.

## Conventions every adapter follows

These are the rules that make one contract mean the same thing on every engine. They are not
enforced by the type system, so they are written down here and, where a test can see them,
checked by the conformance kit.

**Locations are canonical `/`-separated strings.** The pipeline compares a contract's declared
location with the location the adapter reports using one rule (`LocationMatching`): equal, or the
declared one is a `/`-boundary suffix of the reported one. An engine whose names are not paths
converts them first with `LocationMatching.fromParts(Seq("project", "dataset", "table"))`
(`project/dataset/table`; it trims, drops one pair of backticks or double quotes and empty parts, and the
adapter splits the engine's identifier by the engine's own rules), and a contract author writes
`dataset/table` (or the full form to pin one project). A dotted declaration (`ds.orders`) is one path
segment and never matches the converted name, so write `/`. An absolute declaration (`/data/orders`, `gs://bucket/orders`) matches only itself;
a relative one matches any location ending in it, so a contract that must distinguish tenants
declares it absolute.

**Formats and catalog technologies use canonical names too.** `ir.Write.format` and
`CatalogIdentity.technology` are `com.invaract.contract.Formats` (`parquet`, `orc`, `avro`, `csv`, `json`,
`text`, `table`, `delta`, `iceberg`, `hudi`, `hive`, `jdbc`, `bigquery`, `kafka`) and
`CatalogTechnologies` (`hive`, `delta`, `iceberg`, `hudi`, `jdbc`, `glue`, `bigquery`, `in-memory`). Both are
open in the same way as write modes: an engine whose format is none of these reports its own name, and a
contract declaring a name outside the set still validates, with a warning. What an adapter must not do is
spell a listed format its own way (`BIGQUERY_TABLE`, `SparkCatalog`): Spark reports a data source's
registered short name and maps its catalog plugins (`DeltaCatalog` -> `delta`, `SparkCatalog` ->
`iceberg`), both guarded by `FormatVocabularySpec`. `UnknownPlan.sourceType` is deliberately not a
vocabulary: it is the engine's own label for a node it could not translate, there for diagnostics.

**Write modes use the four canonical names.** `ir.Write.saveMode` is `append`, `overwrite`,
`ignore` or `error` (`com.invaract.contract.SaveModes`), or `None` when the adapter cannot tell.
An adapter maps its engine's dispositions onto them; one with no equivalent leaves it `None`
rather than inventing a name, because a contract can only match a name every adapter spells the
same way. A contract that declares another string still validates, with a warning.

| Engine disposition | Canonical |
|---|---|
| Spark `SaveMode.Append` / `Overwrite` / `ErrorIfExists` / `Ignore` | `append` / `overwrite` / `error` / `ignore` |
| BigQuery `WRITE_APPEND` / `WRITE_TRUNCATE` / `WRITE_TRUNCATE_DATA` / `WRITE_EMPTY` | `append` / `overwrite` / `overwrite` / `error` (approximate: fails only on a table that has data) |
| Beam `BigQueryIO` `WRITE_APPEND` / `WRITE_TRUNCATE` (file loads only) / `WRITE_EMPTY` | `append` / `overwrite` / `error` (approximate, as above) |
| `MERGE` / `UPDATE` / `DELETE` (row-level DML) | `None`: reported through `rowMutation`, not as a write mode |

The Spark row is what ships today. The BigQuery and Beam rows are the intended mapping for adapters
that do not exist yet. They are checked against the engines' own source (`docs/ENGINE_RESEARCH_BIGQUERY_BEAM.md`,
which also records the defaults that differ by job kind and what could not be checked) but not yet exercised by an
adapter.

**Types map into `LogicalType`.** An adapter converts its engine's schema into `LogicalSchema`;
anything with no neutral equivalent becomes `OtherType(typeName, catalogString)`, which compares
by its catalog string and never matches a declared neutral type. The intended mappings (same status as the
write-mode table; the research document has the reasoning, including the narrow integer types, picosecond
timestamps and `BIGNUMERIC` precision that this summary leaves out):

| Neutral type | BigQuery | Beam schema |
|---|---|---|
| `string` / `boolean` / `long` / `double` | `STRING` / `BOOL` / `INT64` / `FLOAT64` | `STRING` / `BOOLEAN` / `INT64` / `DOUBLE` |
| `decimal(p,s)` | `NUMERIC` (default `(38,9)`, or the declared `(p,s)`); `BIGNUMERIC(p,s)` only when `p <= 38`, otherwise `OtherType("BIGNUMERIC", ...)` | `DECIMAL` carries no precision or scale: `OtherType("DECIMAL", ...)` |
| `byte` / `short` / `integer` / `float` | none (`INT64` and `FLOAT64` only) | `BYTE` / `INT16` / `INT32` / `FLOAT` |
| `date` / `timestamp` / `timestamp_ntz` | `DATE` / `TIMESTAMP` (microseconds; a 12-digit one is `OtherType`) / `DATETIME` | the `SqlTypes.DATE` logical type / the primitive `DATETIME` (an instant) / the `SqlTypes.DATETIME` logical type |
| `time` | `TIME` | the `SqlTypes.TIME` logical type |
| `json` | `JSON` | none: Beam reads a BigQuery `JSON` as `STRING`, so it is a `string` |
| `geography` | `GEOGRAPHY` | none: read as `STRING` |
| `interval` | `INTERVAL` | none (Spark's calendar interval is `interval`; its year-month and day-time intervals stay `OtherType`) |
| `binary` | `BYTES` | `BYTES` |
| `array` / `struct` / `map` | `ARRAY` / `STRUCT` (BigQuery has no map: a repeated `STRUCT<key,value>` stays an array) | `ARRAY` (or `ITERABLE`) / `ROW` / `MAP` |
| no neutral type | `RANGE` | custom logical types (Beam's own BigQuery reader rejects `RANGE`) |

`time`, `json`, `geography` and `interval` are real logical types (`LogicalType.TimeType`, `JsonType`,
`GeographyType`, `IntervalType`), so a contract can declare them. An engine without one reports the type
it does have, and the declaration then fails as a type mismatch rather than passing: the
`extended-type-not-produced` scenario holds every adapter to that. Each adapter documents its own table
next to its capability declaration and adds a conformance scenario for any type whose mapping is not
obvious.

**Events are engine-neutral.** A published event names no engine's concepts: `runId` is the
engine's own run identifier, and `JobInfo` carries `engine`, `engineVersion` and a free-form
`engineDetails` map for whatever else the adapter wants to record. An adapter fills those in;
it does not add engine-named fields to the event schema.

**Every event names its engine and job.** `ContractValidationEvent`, `WriteEvent` and `JobSummaryEvent` carry
`job: Option[JobInfo]` (the dry-run events always had a `job`): which engine produced the event
(`engine`, `engineVersion`), the job's stable identity across runs (`jobId`), the run (`runId`) and whatever the
platform attached through the engine's own configuration (`attributes`). That is what lets a consumer receiving
events from more than one engine tell them apart, and group one job's events across runs. An adapter that
publishes events passes its `JobInfo` to the pipeline (the `job` parameter above) and to any `WriteEvent` it
builds itself. The conformance kit holds it to that: an adapter that declares `reporting.notifications` must
publish validation events whose `job.engine` is its own name (the `adapter:` in its capability declaration),
and `BrokenAdapterSpec` proves the kit fails an adapter that names none or another engine. Spark's `JobInfo` is
`DryRunReporter.jobInfoOf(session)`: `engine = "spark"`, `jobId` from `spark.invaract.jobId`, and
`attributes` from `spark.invaract.job.metadata.<key>`.

**A sink that needs an engine's storage library lives in that engine's adapter.** The core has
no Hadoop dependency: `HadoopFsNotificationSink` is in `spark-adapter`, and a dead letter with a
`scheme://` path names its sink with `deadLetter.class`.

## Stage 4 — the adapter conformance kit

Stage 3 made an adapter state what it does; a statement nobody checks only moves the problem. The
`adapter-testkit` module is the check: one catalogue of engine-neutral scenarios that every adapter
runs, judged against that adapter's *own declaration*.

**A scenario** is a contract, a job described in no engine's terms, and a verdict. The job
(`ScenarioJob`) is: read one or two inputs (two are inner-joined), optionally filter a column,
project columns (a pass-through, a cast, or a never-null constant), write one output with a format and
save mode. That is deliberately small - just enough to build every shape the 41 scenarios need on any
engine - and every check the engine makes is about shape, not values, so inputs are empty datasets of
the scenario's schema. The scenarios cover location, schema (presence, type, nullability, undeclared
columns), nested types, declared-input existence, format, save mode, a transformation-shape rule, an
invalid contract, the PASSED/FAILED events published, non-determinism in the fingerprint (Stage 5),
fail-closed behaviour, and the analyses (below).

**An adapter** implements `ConformanceAdapter`: its `AdapterCapabilities`, and `run(...)`, which
turns the neutral job into a real job on its engine, runs it through the *real* enforcement path, and
reports `Passed` or `Rejected(violation types)` plus the validation statuses it published. Spark's
`SparkConformanceAdapter` builds a Spark job on a local session and runs it through
`ContractEnforcementRule.forContract`, the builder `InvaractSparkSessionExtension` registers. The
adapter mixes `AdapterConformanceSpec` into a test, which registers one test per scenario.

**What an adapter is held to** follows from its declaration, not from the neutral verdict alone:

| The adapter declares... | The scenario's expected result |
|---|---|
| everything the scenario needs `supported` / `partial` | the neutral verdict, exactly (the same violation types) |
| a capability the scenario's contract relies on `unsupported` (and it `enforcesContract`) | a rejection including `UNSUPPORTED_CONTRACT_FEATURE` - a quiet pass is a failure |
| a needed capability `not-applicable`, or a needed operation unsupported | the scenario is **canceled** with the adapter's own note, visible in the report |
| `reporting.notifications` supported | exactly one `PASSED` (or `FAILED`) validation event |

**Row-level changes and catalog tables have job shapes too.** A `ScenarioJob` with `rowChange` set changes the
rows of its output in place (a `DELETE`, with or without a predicate) instead of reading and writing; the
adapter creates the dataset first with no contract active, then runs the change under enforcement. The
three DML scenarios check that such a change is recognised as a write to its table, that
`forbid_unconditional_delete` blocks a `DELETE` with no predicate, and that it lets a filtered one
through. A `ScenarioOutput` with `registeredAs` set registers the data in the engine's catalog under that
name; two scenarios check that a contract's `catalog: required` blocks a bare path and allows a catalog
table. Spark runs the DML scenarios on a Delta table (the conformance session enables Delta, which
changes nothing for the parquet, csv and json scenarios).

**Fail-closed has its own job shape.** A `ScenarioJob` with `untranslatableWrite` set is not a
read-transform-write at all but a data-changing operation the adapter has no translation for; the
adapter picks its engine's representative (Spark: `TRUNCATE TABLE` on a managed table) and must block
it as `UNVERIFIABLE_WRITE` rather than let it through unchecked. This is the property that makes
"supported" safe to rely on, so it is verified, not attested.

**Streaming and boundaries are job shapes too.** `streaming` reads the one input as a stream and writes the
output as a stream sink, run to completion; two scenarios hold it to the batch job's verdicts (the stream is
the declared input, a sink elsewhere is blocked). `boundary` runs the work so far (read, join, filter)
through the engine's checkpoint or cache before the projection; the scenarios require a filter made before
the boundary to still satisfy a `required_filter_columns` rule, no declared input to be reported hidden, and
a genuinely unread input to still be missing. An adapter that cannot see through its boundary fails them.

**The analyses are read from what the run reported and from the adapter's own translation.** The outcome
carries the static data-quality results, the role-consistency verdicts and the inputs reported hidden
behind a boundary, all read off the last `ContractValidationEvent` (so an adapter fills them in by finishing
a run with `RecordingSink.passed` / `rejected(...)`, never by building an outcome by hand). Lineage and
sensitivity are properties of the translated `ir.Plan`, and no event carries them, so the adapter exposes
its translation (`ConformanceAdapter.translation`: the plan its enforcement would check for a neutral job,
nothing written) and the kit runs `ir.Lineage` and `SensitivityLineage` over it, comparing with what the
scenario says each output column derives from. An adapter that claims either capability and exposes no
translation diverges: a claim the kit cannot look at is not a pass.

An exception out of `run` is a divergence, not a verdict. A capability an adapter claims that no
scenario is evidence for is listed as *unverified*, never counted as passing, and the report says which
kind of unverified it is: **attested** (`Scenarios.attested`) when no job could check it by its nature
- how an adapter attaches to a job, what it applies before a job exists, a mode of installing - so the
adapter's own tests carry it; or a **gap** (`Scenarios.gaps`) when a job could check it but the kit
cannot yet. There are no gaps today: every capability a job can show is shown by a scenario. The kit's own
tests fail if a capability is in neither list or in both.

**Attested claims have a standard shape.** For each attested capability an adapter declares `supported` or
`partial`, the adapter names the test that demonstrates it (`Attestation(suite, test)`), by mixing
`AttestedClaimsSpec` into a test (Spark's is `SparkAttestedClaimsSpec`). The kit checks the named test exists
(the suite loads and has a test of that exact name), that no attestation is left for a capability the adapter
does not claim, and that nothing is attested that a scenario already checks; `AttestedClaims.requirements`
says what each test must show, which is the review checklist for the named test (the kit cannot judge
that). A rename or deletion of the test fails the build, and a new claim without a test fails it too. The generated engine-capabilities page says the same per
capability in its *Suite check* column (`verified (n)`, `attested`, `gap`); the page is generated by the
kit, not by `verification-core`, because only the kit knows the scenarios (`./dev/capabilities`).

**The kit is tested against itself.** `ReferenceAdapter` is a complete adapter built on nothing but
the SPI (no engine; about forty lines of translation), and it passes every scenario - so the
scenarios are consistent with the SPI before any real engine is involved. Then deliberately dishonest
adapters must be caught: one that claims `check.format` but never checks it fails exactly the format
scenario; one that lets everything through fails every scenario expecting a rejection; one that
blocks everything fails every scenario expecting a pass; one that claims notifications and publishes
none fails all of them; one that throws is a divergence.

**Every scenario is also shown to be able to fail.** `BrokenAdapterSpec` subclasses the reference adapter
with one mistake at a time - a filter lost in translation, a join lost, a format spelled the engine's own
way, a catalog identity never reported, a unique-id function under its native name, a column's derivation
lost, a stream not recognised as a read, a checkpoint it cannot see through, and so on - and lists the
scenarios that must catch each. The suite fails if a mistake gets past one of them, and fails if a scenario
is the target of no mistake and is not listed as a control with the reason (the ones left are the shared
checks `verification-core` already holds, and the control for each option's default). A scenario added
later therefore arrives with the mistake it is there to catch; one that nothing could make fail says
nothing about a real adapter.

**Adding an adapter** is therefore: write its capability declaration, implement `ConformanceAdapter`,
mix in `AdapterConformanceSpec`, and see which scenarios fail or are canceled. The step-by-step process, the
questions to answer about an engine first, and the generator (`dev/new-adapter`) that creates the module and
registers it in `modules.json` are in `docs/ADDING_AN_ENGINE_ADAPTER.md`. Where Spark's own
behaviour and the neutral expectation disagree, the scenario or the adapter is wrong - there is no
third place for the difference to hide.

**Held to the same gates as the engine.** The kit is held to all three: MiMa (it is what a third-party adapter compiles its tests against),
line/branch coverage, and mutation testing (`mutation-testing-adapter-testkit`; a whole-module score of 88.1%
measured with the break threshold at 70%). It defines "conformant", so a judge or a reference translation a
mutant can change without `ConformanceKitSpec` noticing would be a hole in every adapter's guarantee.
`Scenarios.scala` alone is excluded: it is the declarative catalogue, and the code Stryker injects pushes its
object initialiser past the JVM's 64KB method limit; the reference adapter and every real adapter's run hold
its content. It is published to Maven Central with the other engine modules (`docs/RELEASING.md`), because a third-party
adapter compiles its conformance tests against it.

## Stage 5 — function canonicalisation

Leak #4: an IR `Function` carried whatever name its engine's translator wrote - Spark's `prettyName`
- and `fingerprint`'s non-determinism classifier was a hand-kept list of *Spark* names
(`spark_partition_id`, `input_file_name`, ...). On BigQuery `GENERATE_UUID()` would have been
classified deterministic: a silently wrong fingerprint annotation, and the same logic on two engines
would have been described differently.

**`ir.FunctionCatalog`** is the one engine-independent place that says what a function *is*: twelve
canonical names (`UUID`, `RAND`, `RANDN`, `CURRENT_TIMESTAMP`, `CURRENT_DATE`, `UNIX_TIMESTAMP`,
`ROW_ID`, `SOURCE_FILE_NAME`, `SOURCE_BLOCK_START`/`_LENGTH`, `PARTITION_ID`, `SHUFFLE`) with two
properties: non-deterministic, and seed-bearing (a call carries an analyzer-injected seed the
fingerprint must leave out). A function the catalog does not list is not flagged - it lists what is
*known* - so keeping an engine's table complete is the adapter's job, and is checked rather than
trusted (below).

**`ir.FunctionAliases`** is an engine's mapping from its own spellings onto the catalog. An adapter
applies it where it writes a `Function` into the IR (Spark: `SparkPlanAdapter`, through
`SparkFunctionAliases`: `random` -> `RAND`, `now` -> `CURRENT_TIMESTAMP`, `curdate` ->
`CURRENT_DATE`, `monotonically_increasing_id` -> `ROW_ID`, and so on). A name with no alias passes
through upper-cased; an alias whose target is not a catalog name is rejected at construction, since
a typo there would leave a non-deterministic function unflagged. `fingerprint` now asks the catalog
(`NonDeterminism`, and `Canonicalizer`'s seed exclusion) and no longer lists names itself.

**Checked, not trusted.** `SparkFunctionAliasesSpec` sweeps Spark's own function registry: for every
function that can be called with no arguments, anything Catalyst reports non-deterministic must come
out of translation flagged. The first run found two functions the hand-kept list had missed
(`input_file_block_start`, `input_file_block_length`); they are catalog entries now. The sweep is
one-directional on purpose: Spark treats the clock functions as deterministic within a query, but
they differ between runs, which is what the catalog records.

**A new capability and two scenarios.** `analysis.functionCatalog` joins the vocabulary (so every
adapter had to take a position; Spark and the reference adapter declare it supported), and the
conformance kit gains two scenarios: a job of deterministic columns reports nothing non-deterministic,
and a job with an engine-generated unique id (`ColumnSource.UniqueId` - Spark's `uuid()`, the reference
adapter's `UUID`, what BigQuery would spell `GENERATE_UUID()`) reports exactly that column. A kit test
shows an adapter whose names never reach the catalog is caught. `analysis.fingerprint` moves from
"claimed but unverified" to covered.

**Versions.** `ir` 0.5.0 -> 0.6.0 and `fingerprint` 0.3.0 -> 0.4.0 (and `verification-core`,
`adapter-testkit`, whose POMs pin them) for the Ivy-coordinate reason described in each `build.sbt`.
`FingerprintHasher.CurrentVersion` went 2 -> 3: an aliased function now hashes under its canonical
name, so fingerprints of jobs calling `random()` or `now()` differ from before.

**Not done in 5, deliberately.** No BigQuery or Beam alias table exists - the tests use a BigQuery-shaped
one as an example of what such an adapter declares. Functions that depend on their arguments to be
non-deterministic (`unix_timestamp(x)` is not; `unix_timestamp()` is) are classified by name alone, as
before. The catalog says nothing yet about other properties an engine's functions have (null
handling, monotonicity); each would be another field on `CanonicalFunction`.
