# ClickHouse support

[← Back to Spark Adapter](../SPARK_ADAPTER.md#connector-support)

Seventh connector onboarded via the `add-spark-connector` skill's process.
Originally scoped as BigQuery; scoped out at Phase 0 before any code was
written because, unlike every connector this repo has onboarded so far,
BigQuery has no local/offline testing mode — it requires a real GCP
project, billing, and credentials, conflicting with this repo's "real
Spark execution, no mocking" testing philosophy (ARCHITECTURE.md's
ADR-005) and unavailable in the onboarding session. ClickHouse was chosen
as a substitute specifically because it *is* genuinely testable without
a cloud account: `com.clickhouse.spark %% clickhouse-spark-runtime-3.5 %
0.10.0` (confirmed the latest release on Maven Central for exactly this
Spark 3.5/Scala 2.12 combination at onboarding time; no known blocking
compatibility issue against it), added to `spark-adapter/build.sbt` as a
`% "test"` dependency, Linux/macOS-only (excluded on Windows — see below).

**A genuinely new kind of test dependency: a real, separate server
process, not just a session extension or embedded metastore.**
`ClickHouseTestServer` (test sources) launches a real, standalone
`clickhouse` binary as a subprocess — downloaded once from ClickHouse's
own GitHub releases and cached locally, started with a per-test
HTTP/TCP port pair, and torn down in `afterAll`. Confirmed directly
during Phase 0 investigation (before any test code existed): a real
`clickhouse server` process starts, binds, and answers real
`CREATE TABLE`/`INSERT`/`SELECT` queries over HTTP with zero Docker
dependency. This was a deliberate choice over testcontainers/Docker
(the connector's own upstream test suite uses this) — Docker's daemon
could not be started in the onboarding session (`ulimit: error setting
limit (Operation not permitted)`, a hard container restriction), so any
testcontainers-based test could only ever be *hoped* to pass in CI, never
actually run and confirmed by the person adding it. **Windows scope**:
ClickHouse has no supported native Windows server build (a hard platform
constraint, unlike Iceberg's JDK-version-only exclusion) —
`ClickHouseTestServer`/`ClickHouseConnectorSpec.scala` and the
`clickhouse-spark-runtime` dependency itself are excluded on Windows in
`build.sbt`, the same "exclude the dependency, not just the test class"
pattern Iceberg's JDK<17 exclusion already established (Spark's
ServiceLoader-based `DataSourceRegister` lookup scans every provider on
the classpath for *any* format-based read, so a merely-resolvable jar can
affect unrelated tests). **A real environment-specific caveat, stated
plainly rather than left implicit**: macOS provisioning
(`clickhouse-macos`/`clickhouse-macos-aarch64` release assets) is
implemented and the download URLs confirmed to exist via a direct HTTP
check, but **not independently runtime-verified by this session** — no
macOS environment was available to actually start and query a server
there. Linux (the platform this session ran on) is fully verified: real
queries against a real started server, both during investigation and via
`ClickHouseConnectorSpec` itself.

**A second real environment-specific fix, general beyond ClickHouse**:
a real `UnsupportedOperationException: sun.misc.Unsafe or
java.nio.DirectByteBuffer.<init>(long, int) not available` surfaced the
first time any ClickHouse write executed under this sandbox's JDK 21 —
confirmed to be a genuine external incompatibility (apache/arrow#35053,
fixed in Arrow 13.0.0): JDK 21 changed `DirectByteBuffer`'s private
constructor signature, and Spark 3.5.1 bundles Arrow 12.0.1 (confirmed
via the resolved test classpath), which predates the fix. ClickHouse's
`.writeTo(...)` path is the first connector in this module to actually
exercise Arrow-based serialization (its own bulk-load mechanism — Delta/
Iceberg's `.writeTo()` tests never hit this). Not fixable via
`--add-opens` (both `java.base/java.nio` and `jdk.unsupported/sun.misc`
were already open in this module's `Test/javaOptions`; the failure is a
missing constructor overload, not a reflective-access denial). Fixed via
a `dependencyOverrides` entry pinning `arrow-vector`/`arrow-memory-core`/
`arrow-memory-netty` to 14.0.1 for the *test* classpath only — confirmed
compatible with every other spec exercising Arrow-adjacent code paths in
this suite, and with zero effect on the shipped `spark-adapter` jar
(Arrow is never a compile/runtime dependency of this module, only pulled
in transitively by Spark's own `provided`/test dependencies).

**A real reflective jar scan of `clickhouse-spark-runtime` (6,181
classes) found zero `Command`-shaped classes** — the second connector
(after Avro) with no SQL-extension commands at all. The entire operation
surface routes through Spark's own generic DSv2 command family:
`AppendData`/`OverwriteByExpression`/`CreateTableAsSelect`/
`ReplaceTableAsSelect` were already generic (from Iceberg's pass) and are
confirmed here to cover ClickHouse "for free," including the same
nested-double-write `StagedTable` pattern Delta/Iceberg's own new-table
creates already handle. `CreateNamespace`/`CreateTable`(plain)/
`AnalyzeTable`/`ShowTables` were already safe-listed generically — zero
new `FailClosedCommands` entries needed.

**One genuinely new, connector-agnostic `WriteCommandSupport` case:
`DeleteFromTable`.** Confirmed empirically that a real predicate-based
`DELETE FROM ch.db.tbl WHERE id = 1` executes successfully against
ClickHouse — unlike Parquet/CSV/Avro's plain tables (which reject DELETE
outright) and structurally different from Iceberg's row-level DML
(`RowLevelWrite`/`ReplaceData`/`WriteDelta`, produced by Spark's
`RewriteRowLevelOperation` optimizer rule for connectors implementing
`SupportsRowLevelOperations`). ClickHouse's connector doesn't implement
that interface — it implements plain `SupportsDelete` instead, so the
`DeleteFromTable` node is never rewritten and reaches
`ContractEnforcementRule` as-is, a shape no prior connector's pass had
ever seen translated. `DeleteFromTable.table` is a plain `LogicalPlan`
(possibly `SubqueryAlias`-wrapped), not a `NamedRelation` directly, so
the new case locates the underlying relation via `collectFirst` rather
than the `namedRelationLocationAndFormat` helper's direct-parameter form.
Scope matches Delta/Iceberg's row-level DML precedent exactly:
structural only (target location/schema) — the delete predicate itself
has no IR representation and isn't checked.

**A real, found-but-deliberately-undebugged finding on `MERGE INTO`**:
confirmed empirically that a `MERGE INTO` statement against ClickHouse
fails with `AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION]`
*before* ever producing a `Command`-shaped analyzable plan at all — a
genuine analysis-time rejection, consistent with the connector's own
documentation stating DML beyond append/overwrite isn't supported. Not
investigated further to find the exact root cause (unlike `UPDATE`,
which cleanly reaches `UpdateTable` and is then rejected by Spark's own
generic "not supported temporarily" message) — the practical conclusion
(MERGE never produces a translatable plan for this connector) is the
same regardless, and chasing the precise resolver internals was judged
out of scope for this pass.

**A real, confirmed read/write location-format asymmetry**, general to
how `Table.properties()` behaves for this connector, not a bug in
Invariant's own logic: writes resolve to a computed 3-part qualified
identifier (`catalog.namespace.table`, e.g. `ch.probe_db.t`) via
`WriteCommandSupport`'s existing `qualifiedIdentifier` helper, since
`ClickHouseTable` exposes no `"location"` property either side. Reads go
through `DataSourceV2Relation.name` (`ClickHouseTable`'s own `name()`
implementation), confirmed empirically to return a backtick-quoted
**2-part** `` `namespace`.`table` `` with no catalog prefix at all —
genuinely different from the write side's convention for the identical
table. A contract's declared `location` for a ClickHouse *input* must
therefore use this quoted 2-part form (` "`probe_db`.`read_catalog_tbl`"
`), not the 3-part form outputs use — confirmed and exercised directly in
`ClickHouseConnectorSpec`'s read test. **Next step, not attempted this
pass**: a possible future unification (making the read side also prefer
a computed qualified identifier over raw `Table.name()`, matching the
write side's convention) — a connector-agnostic change affecting every
DSv2 connector's read-side location resolution, judged too large a scope
change for this pass given it works correctly today, just with a
real quirk contract authors need to know about.

**Feature surface**: ClickHouse's own `ORDER BY`/sorting-key nullability
constraint (`allow_nullable_key`) confirmed genuinely orthogonal to
Invariant — a source DataFrame column can correctly report
`nullable = false` while `DataFrameWriterV2`'s `.create()` path still
doesn't propagate that into the generated ClickHouse DDL, so ClickHouse
itself rejects a nullable-typed sorting key regardless of what a
contract declares or what Invariant's own structural checks see — the
same "confirmed orthogonal" pattern as Delta's `CHECK` constraints.
Invariant neither causes nor can prevent this; a satisfying write (with
`settings.allow_nullable_key=1` supplied) is unaffected either way,
confirmed by every other `.create()`/`.createOrReplace()` test in this
file succeeding once that workaround is applied.

**Follow-up pass closing the 4 remaining ❓ rows.** Zero
`spark-adapter/src/main/scala` changes — every finding below is either a
confirmed-transparent behavior or a confirmed, permanent connector
limitation, not a translation gap (see the ledgers below for the
disposition each closed to).

- **Streaming read**: confirmed rejected outright, the same way streaming
  write already was — both `spark.readStream.format("clickhouse")...load()`
  (`SparkUnsupportedOperationException: Data source clickhouse does not
  support streamed reading`) and the catalog-based
  `spark.readStream.table(...)` (`AnalysisException: ... does not support
  either micro-batch or continuous scan`) fail before any Command-shaped
  plan exists. Nothing for `WriteCommandSupport`/`SparkPlanAdapter` to
  translate.
- **`.saveAsTable()` onto an existing table**: confirmed empirically (not
  assumed from the already-covered `.insertInto()`/`.writeTo().append()`
  shapes) to produce the identical `AppendData` node. No new case needed.
- **Maintenance operations** (`OPTIMIZE`, `ALTER TABLE ... DELETE`,
  `VACUUM`): confirmed genuinely unreachable through Spark SQL with this
  connector at all — every attempt fails with a `ParseException` at
  Spark's own SQL parser, before analysis, because the connector installs
  no `SparkSessionExtensions` parser extension for them (unlike Delta,
  which registers its own parser for `OPTIMIZE`/`VACUUM`). Not a 🚫
  fails-closed row in the usual sense (nothing reaches a Command-shaped
  plan to classify) — a genuine absence of Spark-visible surface, closer
  in kind to the connector's own already-documented `.save(path)` N/A row.
- **Richer type system**: split into three genuinely different findings,
  not one. **Array/Map/Struct** (Spark's own complex types): confirmed
  fully transparent — `CREATE TABLE`, a real write producing the already-
  known `AppendData` shape, read-back, and contract verification against
  a contract declaring `array`/`map`/`struct` field types all confirmed
  working with a real round-trip test. **`LowCardinality`**: confirmed
  transparent on read (the connector's own `SchemaUtils` class, decompiled
  directly rather than assumed from its docs, has no `LowCardinality`
  special-casing at all — the wrapped base type flows through as-is) and
  confirmed **not requestable from the Spark side on write** — two
  plausible-looking `TBLPROPERTIES` mechanisms were tried and both
  silently accepted without applying anything, confirmed against the real
  server's own `system.columns` (`Nullable(String)`, not
  `LowCardinality(String)`), ruling out "maybe it's an undocumented
  option" before concluding "no mechanism exists." **`Nested`**: a real,
  worth-documenting distinction, not a bug — Spark's
  `ARRAY<STRUCT<...>>` DDL produces ClickHouse's `Array(Tuple(...))`
  (confirmed against `system.columns`), a structurally similar but
  distinct type from ClickHouse's own `Nested(...)` (parallel-arrays/
  sub-column-addressing semantics `Array(Tuple(...))` doesn't have). A
  contract author should not assume `ARRAY<STRUCT<...>>` gives them true
  `Nested` semantics — it doesn't, and there's no Spark-side mechanism to
  request true `Nested` on write either. (The connector's `SchemaUtils`
  does contain real *read*-side unwrap logic for a genuinely pre-existing
  `Nested` column, per its `getNestedColumns`/`nestedCols` symbols — not
  independently exercised this pass; noted as the row's own remaining
  next step below, distinct from the write-side finding above.)

**A real bonus finding, out of scope to fix here: a malformed contract
crashes instead of failing cleanly, and it's not ClickHouse-specific.**
Investigating the type-mapping contract-verification path with a
contract YAML missing its top-level `outputs:` key didn't produce a
clean rejection — it crashed `StructuralVerifier.verify` with an
unguarded `java.util.NoSuchElementException: head of empty list` at
`contract.outputs.head`. Root-caused precisely, not just observed:
`ContractParser.parse` never calls `ContractValidator.validate` (a
separate step a caller must invoke explicitly), and nothing on
`ContractEnforcementRule`'s runtime enforcement path calls it either —
even though `ContractValidator` already has an "outputs must be
non-empty" check that would catch exactly this. This reproduces with
*any* connector, since it's a `contract`/`spark-adapter` boundary gap,
not a ClickHouse translation issue — deliberately left unfixed this pass
(the same precedent as Avro's decimal contract-type-vocabulary gap: out
of scope for a spark-adapter connector pass to change contract
validation), pinned with a permanent test in `StructuralVerifierSpec`
rather than left as a remembered probe result. **Next step**:
`ContractEnforcementRule.verifyOrThrow` (or `StructuralVerifier.verify`
itself) should run `ContractValidator.validate` first and surface its
errors as a normal `VerificationResult` failure.

## ClickHouse operation-surface coverage ledger

| Operation | Status | Evidence / next step |
|---|---|---|
| `.read.format("clickhouse").load(...)` (TableProvider, no catalog) | ✅ Covered | `DataSourceV2Relation`, same generic case as catalog reads. `ClickHouseConnectorSpec`'s "PASS: TableProvider format-based read/write round-trips real data." |
| Catalog table reference (`spark.table(...)`/`SELECT * FROM t`) | ✅ Covered | `DataSourceV2Relation`, already generic from Iceberg's pass. `ClickHouseConnectorSpec`'s PASS read test — with the read/write location-format asymmetry above confirmed and worked around. |
| Time travel / snapshot reads | 🚫 **N/A, not a real gap** | ClickHouse's MergeTree engine has no Iceberg/Delta-style snapshot-versioning concept exposed through this connector. Nothing to translate. |
| Streaming read (`readStream`) | 🚫 **N/A — connector doesn't implement `SupportsRead` with any streaming mode** | Confirmed empirically, both `TableProvider`- and catalog-based: `SparkUnsupportedOperationException`/`AnalysisException` before any Command-shaped plan exists. `ClickHouseConnectorSpec`'s dedicated rejection test. **Next step**: none — a genuine, permanent connector limitation, the same category as streaming write. |
| Change-data-feed / incremental read | 🚫 **N/A, not a real gap** | No CDC mechanism exists for this connector's plain MergeTree tables. Nothing to translate. |
| `.save(path)` (path-based, no catalog) | 🚫 **N/A — connector has no path-based write at all** | Confirmed via this connector's own documentation and empirically: writes are either catalog-based (`.writeTo(...)`) or `TableProvider`-based with explicit `host`/`database`/`table` options, never a bare filesystem path. Not a gap — there is no `.save(path)` shape this connector produces. |
| `.saveAsTable(...)`, new table | ✅ Covered | Same `ReplaceTableAsSelect` shape `.writeTo(...).createOrReplace()` uses, confirmed via probe (not exercised as a dedicated permanent test — `ClickHouseConnectorSpec`'s `createOrReplace()` test covers the identical plan shape). |
| `.saveAsTable(...)`, existing table (append) | ✅ **Covered — closed this pass** | Confirmed empirically to produce `AppendData`, identical to `.insertInto()`/`.writeTo().append()`. `ClickHouseConnectorSpec`'s dedicated test. |
| `.insertInto(...)` | ✅ Covered | `AppendData`, already generic. `ClickHouseConnectorSpec`'s translation test. |
| `.writeTo(...)`, all sub-ops (`.append()`/`.overwrite(cond)`/`.create()`/`.createOrReplace()`) | ✅ Covered | `AppendData`/`OverwriteByExpression`/`CreateTableAsSelect`/`ReplaceTableAsSelect`, all already generic from Iceberg's pass, confirmed to cover ClickHouse via real PASS/FAIL pairs (`.overwritePartitions()`, DataFrameWriterV2's dynamic-partition-overwrite sub-op, not investigated — see next row). |
| `.writeTo(...).overwritePartitions()` | 🚫 **N/A — connector doesn't support partition-level overwrite** | Confirmed via this connector's own documented caveat ("The connector doesn't currently support partition-level overwrite operations"), not independently re-probed this pass. **Next step**: a real probe to confirm the exact rejection mode (Spark-level or connector-level), if ever doubted. |
| Format-specific DML — `DELETE FROM ... WHERE ...` | ✅ **Covered — closed this pass, genuinely new** | `DeleteFromTable`, a new connector-agnostic `WriteCommandSupport` case (see above). PASS/FAIL pair in `ClickHouseConnectorSpec`, structural verification only. |
| Format-specific DML — `UPDATE`/`MERGE INTO` | 🚫 **N/A — rejected before any write occurs** | `UPDATE` reaches `UpdateTable` then is rejected by Spark's own generic "not supported temporarily" message; `MERGE` fails at analysis time before any Command-shaped plan exists at all. Neither is a real Phase-4 case-3 (data-mutating, unmodeled) operation for this connector — both are N/A the same way Parquet/CSV/Avro's own DML rejections are. `ClickHouseConnectorSpec`'s combined rejection test, asserting the target table is unchanged. |
| Streaming write | 🚫 **N/A — connector doesn't implement `SupportsStreamingWrite`** | Confirmed empirically: `AnalysisException: Table ... doesn't support streaming write - ClickHouseTable(...)`, rejected by Spark itself before any Command-shaped write plan is produced. `ClickHouseConnectorSpec`'s dedicated rejection test. **Next step**: none — this is a genuine, permanent connector limitation as of 0.10.0, not something Invariant's translation could close even in principle without the connector itself adding the capability. |
| Maintenance operations that touch data (`OPTIMIZE`, `ALTER TABLE ... DELETE`, `VACUUM`) | 🚫 **N/A — unreachable through Spark SQL with this connector** | Confirmed empirically: every attempt fails with `ParseException` at Spark's own parser, before analysis — the connector registers no SQL extension for them, unlike Delta's own `OPTIMIZE`/`VACUUM`. `ClickHouseConnectorSpec`'s dedicated test. **Next step**: none found this pass — would require the connector itself to add a parser extension or `CALL`-style procedure mechanism (it currently has neither); TTL-driven expiry specifically remains a real ❓ (see feature surface below), since it's configured at `CREATE TABLE` time, not invoked as a separate operation. |

## ClickHouse feature-surface coverage ledger

| Feature | Status | Evidence / next step |
|---|---|---|
| `ORDER BY`/sorting-key nullability (`allow_nullable_key`) | ✅ Confirmed orthogonal | A source DataFrame's correct `nullable = false` isn't propagated into the generated DDL by `DataFrameWriterV2.create()`; ClickHouse enforces its own constraint independently of Invariant either way. `ClickHouseConnectorSpec`'s dedicated feature-surface test. |
| Read/write location-format asymmetry (`Table.properties()`/`Table.name()`) | ✅ Confirmed and worked around | Not a "feature" in the schema-evolution sense, but a real, confirmed connector-specific behavior contract authors must account for. See the write-up above; exercised directly in the read test. |
| Compression/codec options, `PARTITION BY`, replicated engines, materialized views | ❓ **Not investigated** | Out of scope for this pass — this connector has a substantially larger feature surface than any prior one (ClickHouse's own engine/storage model is far more configurable than Parquet/Avro/Delta's). **Next step**: a dedicated future pass investigating each, the same "real probe against a real table with the feature on" methodology this document already establishes. |
| TTL-driven expiry | ❓ **Not investigated** | Configured at `CREATE TABLE`/`ALTER TABLE` time as a table property, not invoked as a separate operation — whether Invariant's structural checks interact with it at all (they shouldn't, since it's a storage-layer concern) wasn't confirmed this pass. **Next step**: a real probe against a table with `TTL` configured, confirming writes/reads are unaffected. |
| Type mapping — `Array`/`Map`/`Struct` (Spark's own complex types) | ✅ **Confirmed transparent — closed this pass** | Real round-trip: `CREATE TABLE`, write (`AppendData`, the already-known shape), read-back, and contract verification against a contract declaring `array`/`map`/`struct` field types all pass. `ClickHouseConnectorSpec`'s dedicated test. |
| Type mapping — `LowCardinality` | ✅ **Confirmed transparent on read; confirmed not requestable from Spark on write — closed this pass** | The connector's own `SchemaUtils` (decompiled directly) has no `LowCardinality` handling at all — the wrapped base type flows through on read. Two plausible `TBLPROPERTIES` mechanisms tried on write, both silently ignored — confirmed against the real server's `system.columns` (`Nullable(String)`, not `LowCardinality(String)`). `ClickHouseConnectorSpec`'s dedicated test. **Next step**: none — no mechanism exists in this connector version to request it from Spark. |
| Type mapping — `Nested` | 🔧 **Found a real distinction, not a bug — closed this pass, one genuine ❓ remains** | Spark's `ARRAY<STRUCT<...>>` produces ClickHouse's `Array(Tuple(...))`, not true `Nested(...)` — confirmed against `system.columns`. A contract author should not assume `ARRAY<STRUCT<...>>` gives true `Nested` semantics. `ClickHouseConnectorSpec`'s dedicated test pins the real server-side type. **Next step**: whether *reading* a genuinely pre-existing `Nested` column (created outside Spark) round-trips correctly wasn't independently exercised — the connector's `SchemaUtils` has real unwrap logic for it (`getNestedColumns`/`nestedCols`, confirmed present via decompilation) that this pass didn't test end-to-end. |

Net assessment: the operation-surface ledger is now fully closed — every
row has a ✅/🚫 disposition, no ❓ remaining. The feature-surface ledger
has three real, honestly-scoped ❓ rows left (compression/`PARTITION BY`/
replicated engines/materialized views, TTL-driven expiry, and whether a
genuinely pre-existing `Nested` column round-trips on *read*) —
ClickHouse's engine/storage model remains substantially more configurable
than any prior connector's, and these three specifically need either a
dedicated future pass or, for the `Nested`-read question, a table created
via raw SQL passthrough rather than Spark DDL. What *is* now closed: the
entire core write path (`AppendData`/`OverwriteByExpression`/
`CreateTableAsSelect`/`ReplaceTableAsSelect`/`DeleteFromTable`), the
entire core read path including streaming's rejection, maintenance
operations' absence from Spark's plan machinery, and the richer-type
findings above — all confirmed empirically, none assumed.
`ClickHouseConnectorSpec`: 21 tests, all passing (up from 15).

---

**Last Updated:** 2026-08-27
**Status:** Spark adapter — initial implementation (ROADMAP.md Phase 1c, Spark Adapter sub-phase)
