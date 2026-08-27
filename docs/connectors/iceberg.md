# Iceberg support

[← Back to Spark Adapter](../SPARK_ADAPTER.md#connector-support)

Added via the `add-spark-connector` skill's process
(docs/ADDING_A_SPARK_CONNECTOR.md), investigated against a real
Iceberg-enabled session
(`spark.sql.extensions=...IcebergSparkSessionExtensions`,
`spark.sql.catalog.local=...SparkCatalog` with `type=hadoop`, no external
metastore needed for local/test use). Pinned to
`org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0` — a checked
compatibility issue before pinning, per Phase 0's "any known
compatibility issues" step: 1.10.0 had a confirmed `NoSuchMethodError`
from an Avro 1.11/1.12 API mismatch against Spark 3.5/3.4
([apache/iceberg#14232](https://github.com/apache/iceberg/issues/14232)),
fixed via an Avro 1.12.1 upgrade that landed before 1.11.0. Test-scope
only, same reasoning as `delta-spark` above — no compile-time or runtime
dependency for a non-Iceberg job.

**A JDK-11 CI-only gap found later, not at onboarding time**: this
version's jar is compiled to class file version 61 (Java 17) —
confirmed via a real CI failure (`UnsupportedClassVersionError` on
`SparkCatalog`/`IcebergSource`) on this repo's `ubuntu-latest`/Java-11
matrix leg, and — since this module's suites share one forked JVM —
cascaded into three unrelated suites in the same run, not just
`IcebergConnectorSpec`. A genuine, external constraint of the library
(unlike Delta 3.2.0 above, which loads fine under JDK 11), not fixable
here. `spark-adapter/build.sbt` excludes only `IcebergConnectorSpec`,
only under JDK <17 (`Test / testOptions`'s `Tests.Filter`) — every other
test, in this module and every other, still runs on JDK 11. The
module's own compiled bytecode target (`-target:jvm-1.8`) is unaffected;
this is purely a test-only dependency's own runtime floor.

Iceberg is a "pure" DataSourceV2 connector — its catalog (`SparkCatalog`/
`SparkSessionCatalog`) is the same *kind* of thing Delta's `DeltaCatalog`
is, but unlike Delta, Iceberg's *reads* never happen to be a
`LogicalRelation`-wrapped V1 relation the way Delta's `DeltaLog$$anon$2`
is. That difference is what made this investigation surface two real
gaps that predate Iceberg entirely — general DSv2-connector gaps Delta's
own investigation never hit, because Delta's read/write shapes happened
to avoid them:

- **Batch `DataSourceV2Relation` reads had no translation case at all.**
  `SparkPlanAdapter.Translator.translatePlan` had cases for
  `LogicalRelation` (V1 batch), `StreamingRelation` (V1 streaming), and
  `StreamingRelationV2` (V2 streaming) — nothing for a *batch* V2 catalog
  read, so it fell through to the generic `Unsupported` fallback.
  Confirmed by direct code inspection (not assumed): `StructuralVerifier.collectReads`
  only recognizes `ir.Read` nodes, so any pure-V2 connector's catalog
  reads could never satisfy a contract's declared input — the same class
  of bug streaming reads had before that was fixed (see "A read
  discovered through Delta's own investigation" note below), just for a
  batch read this time. Fixed by adding a `DataSourceV2Relation` case
  reusing `tableLocationAndFormat` (the same `Table.properties()` lookup
  `StreamingRelationV2`'s case and the write side's `AppendData`/
  `OverwriteByExpression` cases already share) — connector-agnostic, not
  Iceberg-specific. `ContractEnforcementRule.verifyOrThrow`'s input-schema
  collection needed the matching case too (both were previously
  duplicated three times across two `plan.collect` sites; adding a fourth
  case to both by hand was the moment that duplication got extracted into
  one shared `recognizedRead: PartialFunction`, reused by both sites, the
  same "one source of truth" reasoning `WriteCommandSupport.combined`
  already applies to write recognition).
- **`CreateTableAsSelect` (explicit-create V2 CTAS) and
  `OverwritePartitionsDynamic` had no `WriteCommandSupport` case.**
  `.writeTo(...).create()` (fails if the table exists, unless
  `ignoreIfExists` — `CREATE TABLE IF NOT EXISTS ... AS SELECT`) is a
  *different* Spark command than `.saveAsTable()`/`.writeTo(...).createOrReplace()`
  (`ReplaceTableAsSelect`, already covered) — genuinely never probed for
  any connector before, Delta included, since nothing had tried a bare
  `.writeTo(...).create()`. Both were Command-shaped, on neither
  `WriteCommandSupport` nor `FailClosedCommands`' safe list, so both were
  already failing closed rather than silently passing — real gaps, now
  closed for every DSv2 connector at once (`createTableAsSelect` reuses
  `ReplaceTableAsSelect`'s own location-resolution helper, renamed
  `v2CreateOrReplaceLocation`; `overwritePartitionsDynamic` reuses
  `AppendData`/`OverwriteByExpression`'s `namedRelationLocationAndFormat`
  directly, since `OverwritePartitionsDynamic` shares the identical
  `NamedRelation table` + `LogicalPlan query` shape via the same
  `V2WriteCommand` supertype).
- **Row-level DML (`MERGE`/`UPDATE`/`DELETE`) goes through a genuinely
  different, more standard mechanism than Delta's.** Confirmed
  empirically: Iceberg implements Spark's own `SupportsRowLevelOperations`
  API, and Spark's `RewriteRowLevelOperation` optimizer-rule family
  rewrites `MERGE`/`UPDATE`/`DELETE` into one of two *stable, public*
  Spark classes — `ReplaceData` (copy-on-write) or `WriteDelta`
  (merge-on-read) — both implementing the shared `RowLevelWrite` trait
  (itself extending `V2WriteCommand`, the same shape `AppendData` uses).
  This is a real, structural difference from Delta's proprietary
  `MergeIntoCommand`/`UpdateCommand`/`DeleteCommand` classes, which
  needed reflection (`deltaRowLevelDml`) precisely because they're
  Delta-internal, undocumented, no-cross-version-guarantee types. The new
  `dsv2RowLevelWrite` case needs no reflection at all — `RowLevelWrite`
  is a real, importable Spark type — making it a genuinely
  connector-agnostic case: any future DSv2 connector using Spark's
  standard row-level-operation API is covered by this same case, not a
  per-connector copy, unlike `deltaRowLevelDml`. Scope mirrors
  `deltaRowLevelDml` deliberately: structural verification only (the
  target's *current* schema against the contract; the merge
  condition/update columns/delete predicate itself is not checked — see
  ROADMAP.md's "Full semantic DML verification" item).
- **A second staged-table trap, this time in the opposite direction from
  Delta's.** `.writeTo(...).create()`/`.saveAsTable()` on a new table
  produces both the outer `CreateTableAsSelect`/`ReplaceTableAsSelect`
  *and* a nested `AppendData` against a `StagedTable` (Spark's public
  2-phase-commit protocol for atomic CTAS/RTAS) — the same "shared
  pitfall" documented above for Delta, whose fix's own doc comment
  claimed the two "now always agree... not by coincidence." That claim
  didn't generalize: confirmed by a real `OUTPUT_LOCATION_MISMATCH` test
  failure, not assumed, that Iceberg's `StagedTable` *does* report a
  `"location"` property pre-commit (Delta's doesn't), so the outer
  command's qualified-identifier resolution and the inner `AppendData`'s
  physical-path resolution disagreed. Fixed properly this time: keyed on
  the `StagedTable` marker interface itself (Spark's own "not committed
  yet" signal), not on whether `properties()` happens to omit
  `"location"` — a staged table's reported location isn't trustworthy
  regardless of whether a given connector happens to populate it, so
  `namedRelationLocationAndFormat` now forces the qualified-identifier
  tier unconditionally whenever `v2.table.isInstanceOf[StagedTable]`,
  before even consulting `tableLocationAndFormat`.
- **`CALL <catalog>.system.<proc>(...)` (Iceberg's maintenance-operation
  mechanism — `rewrite_data_files`/`expire_snapshots`/
  `rollback_to_snapshot`/etc.) is deliberately left unmodeled.** All of
  Iceberg's own SQL-extension commands are `Command`-shaped classes
  found via the same reflective jar-scan technique used for Delta
  (`JarFile` + `Class.forName` + `Command.isAssignableFrom`, this time
  against `iceberg-spark-runtime-3.5_2.12` — 14 classes found). Thirteen
  are genuinely metadata/ref-only (branch/tag create-or-replace/drop,
  partition-spec and identifier-field evolution, write-distribution/
  ordering config, view create/drop/show) and are now on
  `FailClosedCommands`' safe list. The fourteenth, `Call`, represents
  *every* system procedure through one shared class — no structural way
  to tell which procedure a given instance invokes without inspecting
  runtime arguments, and those procedures span genuinely safe
  (`expire_snapshots`) to genuinely row-content-mutating
  (`rollback_to_snapshot`). Safe-listing the class would silently pass
  all of them; left off both lists instead, so every `CALL` fails closed
  today, confirmed by a real test — a real, documented limitation (see
  docs/ADDING_A_SPARK_CONNECTOR.md's "Known limitations"), not an
  oversight.
- **`iceberg-spark-runtime-3.5_2.12:1.11.0`'s own published Maven POM is
  missing a real dependency.** Confirmed empirically, the hard way (a
  `NoClassDefFoundError: scala/jdk/CollectionConverters$` escaping a
  `scala.util.Try` wrapper — `LinkageError` isn't `NonFatal`, so it isn't
  caught): the runtime jar's SQL-extensions parser
  (`IcebergSparkSqlExtensionsParser.isIcebergProcedure`, exercised
  specifically by `CALL` syntax) needs `scala.jdk.CollectionConverters`
  (native in Scala 2.13, backported for 2.12 via
  `scala-collection-compat`), but the jar's own POM doesn't declare that
  dependency. Added as a `% "test"` dependency here, purely so this
  module's own test suite can exercise `CALL`-based operations against a
  real session — a real Invariant user running Iceberg `CALL` procedures
  in their own job would need this on their own runtime classpath too,
  independent of anything `spark-adapter` does.

## Delta feature-by-feature confidence pass, revisited: a read discovered through Delta's own investigation

Streaming reads (`StreamingRelation`/`StreamingRelationV2`) were already
closed before this Iceberg pass — see the Delta operation-surface ledger
in docs/connectors/delta.md. The batch `DataSourceV2Relation` gap this
section opens with is the same *class* of bug (a read shape with no
translation case, silently never satisfying a contract's `input`), just
for the one read shape Delta's own investigation never exercised, because
Delta's batch reads happen to be `LogicalRelation`-wrapped. Documented
here, not folded silently into the Delta doc, specifically so a future
connector's investigation can search for "batch DataSourceV2Relation" and
find this, rather than rediscovering the same gap a third time.

## Iceberg CALL procedure classification

A follow-up pass targeting the one row the initial Iceberg investigation
left `🚫 Fails closed, deliberately left unmodeled`: `CALL
<catalog>.system.<proc>(...)` procedures. Every procedure — safe and
unsafe alike — analyzes to the *same* Spark class,
`org.apache.spark.sql.catalyst.plans.logical.Call`, so `FailClosedCommands`'
usual technique (match the plan's own class name) can't distinguish them.

**What actually identifies a procedure at the point `injectCheckRule`
sees it**: not `Call`'s own class, and not a name string — the
pre-analysis `CallStatement` node carries a `name: Seq[String]` (e.g.
`Seq("system", "rewrite_data_files")`), but that node is gone by the time
the analyzer resolves it into `Call`, which the check rule only ever sees
post-analysis. What survives is `Call.procedure(): Procedure` — and
`Procedure` itself (confirmed via `javap`) exposes no `name()` either.
But each procedure is a genuinely distinct concrete class (confirmed via
`javap` on `SparkProcedures`' builder registry — `newBuilder("rewrite_data_files")`
instantiates the real `RewriteDataFilesProcedure` class directly, no
dynamic proxy), so `Call.procedure().getClass().getName()` is a stable,
real per-procedure signal — the same reflection-and-class-name-matching
technique `WriteCommandSupport`'s `deltaRowLevelDml` already uses for
Delta's MERGE/UPDATE/DELETE, just one field deeper. `FailClosedCommands.isKnownSafe`
special-cases `Call`'s class name and delegates to this check; any
reflection failure (a future Iceberg version reshaping `Call`) falls back
to `false` — fails closed, never silently safe.

**Classification** (iceberg-spark-runtime 1.11.0's `org.apache.iceberg.spark.procedures`
package has exactly 20 concrete procedure classes — enumerated from the
jar directly, not guessed), grounded in each procedure's own delegate
action class (confirmed via `javap`) and Iceberg's own documentation for
the ones whose semantics weren't obvious from the class name alone:

| Procedure | Disposition | Reasoning |
|---|---|---|
| `rewrite_data_files` | ✅ Safe-listed | Storage compaction, preserves the same logical rows — same category as Delta's `OptimizeTableCommand`, already safe-listed |
| `rewrite_manifests` | ✅ Safe-listed | Metadata-file compaction, never touches data files |
| `rewrite_position_delete_files` | ✅ Safe-listed | Compacts delete files, preserves the same logical deletes |
| `remove_orphan_files` | ✅ Safe-listed | Deletes files unreferenced by any live metadata — same category as Delta's `VacuumTableCommand`, already safe-listed |
| `expire_snapshots` | ✅ Safe-listed | Removes old snapshots/unreachable files, doesn't touch the *current* snapshot |
| `register_table` | ✅ Safe-listed | Catalog-pointer registration from an existing `metadata.json` — no data touched |
| `ancestors_of` | ✅ Safe-listed | Read-only snapshot-lineage introspection |
| `compute_table_stats` | ✅ Safe-listed | Writes auxiliary CBO statistics only, same category as `ANALYZE TABLE` |
| `compute_partition_stats` | ✅ Safe-listed | Same reasoning, partition-scoped |
| `create_changelog_view` | ✅ Safe-listed | Creates a read-only temp view, mutates nothing |
| `rollback_to_snapshot` | 🔧 **Genuinely verified — closed this pass** | See "Verifying `rollback_to_snapshot`" below — no longer blanket-rejected; checked against the contract for real. |
| `rollback_to_timestamp` / `set_current_snapshot` | 🚫 Left unmodeled | Same effect as `rollback_to_snapshot` (changes which snapshot is "current") but not yet wired to `StateChangingCallSupport` — a small, mechanical extension of the same pattern, not attempted this pass |
| `cherrypick_snapshot` / `publish_changes` | 🚫 Left unmodeled | Applies a staged (WAP) snapshot's changes into the current table — content-affecting |
| `fast_forward` | 🚫 Left unmodeled | Advances a branch's current snapshot — content-affecting for that branch's readers |
| `add_files` | 🚫 Left unmodeled | Imports external files as new data — genuinely adds rows |
| `migrate` | 🚫 Left unmodeled | Converts an existing table's format to Iceberg in place — structurally significant, one-time, not routine maintenance |
| `snapshot` | 🚫 Left unmodeled | Iceberg's own docs confirm it never modifies the *source* table, but it creates an entirely new table from data — new persisted content the safe-list mechanism (a silent, unverified no-op) shouldn't quietly wave through |
| `rewrite_table_path` | 🚫 Left unmodeled | Same reasoning as `snapshot` — doesn't touch the source table, but stages new metadata elsewhere; kept conservative rather than silently trusted |

Verified against real CALL statements, not asserted: `IcebergConnectorSpec`'s
`cherrypick_snapshot` fail-closed test (proving a genuinely unsafe,
still-unmodeled procedure is still rejected — `rollback_to_snapshot` is no
longer the example used here, since it now has real support; see
"Verifying `rollback_to_snapshot`" below) and a regression test sampling
five of the ten safe-listed procedures (`rewrite_data_files`,
`rewrite_manifests`, `expire_snapshots`, `remove_orphan_files`,
`ancestors_of`) across compaction, GC, and read-only introspection —
proving they run under an active, otherwise-checking contract instead of
being wrongly rejected.

Mutation testing scoped to `FailClosedCommands.scala`: **88.42%** (of
total), 89.36% (of covered code) — comfortably above the 70% bar.
Mutation testing itself found a real gap along the way, not just
confirmed the score: `isKnownSafeIcebergProcedureCall`'s reflection
fallback (`case _: Throwable => false` — the line that keeps a future
Iceberg version reshaping `Call` failing closed, rather than silently
resolving to "safe") had zero coverage from `IcebergConnectorSpec`'s
real-session tests, since a genuine Iceberg `Call`'s `procedure()`
reflection never actually fails there. Closed with a focused,
session-free unit test (`FailClosedCommandsSpec`, no `SparkSession`
needed) that calls the fallback directly with a plan lacking a
`procedure()` method — killing the mutant for real, not just documenting
around it, since this is the single highest-stakes line in the whole
change (a wrongly-flipped `true` here would silently defeat the entire
feature for any future Iceberg version).

**What this now does, in full** (updated after the final extension below —
see "Verifying the remaining harder procedures"): all 20 of Iceberg's
system CALL procedures now have a real, evidenced disposition. Nine
genuinely change a table's state and are genuinely verified
(`rollback_to_snapshot`/`rollback_to_timestamp`/`set_current_snapshot`/
`cherrypick_snapshot`/`publish_changes`/`fast_forward`/`add_files`/
`migrate`/`snapshot`); eleven are confirmed safe no-ops
(`FailClosedCommands.safeIcebergProcedureClasses`, including
`rewrite_table_path`). Real investigation found the original three-tier
split assumed here — "small extension" vs. "needs new CALL-argument
parsing" — was wrong about which procedures actually needed which: only
`snapshot` turned out to need genuinely new mechanism; `add_files` and
`migrate` fit the existing nine-procedure mechanism unchanged, and
`rewrite_table_path` needed no verification mechanism at all. See below
for how each was actually found to work, not assumed.

## Verifying `rollback_to_snapshot`

A pilot, not the full set: the first of the ten unmodeled state-changing
procedures to get real verification instead of a blanket rejection,
scoped deliberately narrow to prove the approach before generalizing (see
ROADMAP.md for the original scoping decision).

**The first design was wrong, and mutation testing caught it, not
inspection.** The initial implementation extracted the *target snapshot's
own* historical schema — via Iceberg's `SparkTable.copyWithSnapshotId(id).schema()`,
confirmed reachable via a real probe (since deleted) that showed the
extracted schema correctly omitting a column added *after* that snapshot
— and checked that against the contract. This turned out to verify
something that doesn't correspond to what the operation actually does:
a real end-to-end test (rollback, then an explicit `refreshTable`) showed
the table's schema *unchanged* — still reflecting the latest evolution,
not the target snapshot's. Independently corroborated by Apache Iceberg's
own issue tracker (apache/iceberg#15165, open and unresolved as of this
writing): a maintainer's own report says `current-schema-id` "always
points to the latest schema" regardless of which snapshot a rollback
makes current. Schema evolution and snapshot rollback are independent in
Iceberg's model — a rollback can never change a table's schema at all, so
comparing against the target snapshot's own schema was answering a
question with no real referent.

**The corrected design** checks the two things a rollback can actually
be judged against, together:

- **Location** — does this CALL even target the contract's declared
  output? Confirmed as a *scoping* gate, not a violation: a state-changing
  operation on a table the active contract doesn't govern is now
  correctly allowed, not swept up by an unrelated contract the way every
  rollback was before this file existed (`StructuralVerifier.verifyStateChange`
  returns a clean pass, skipping the schema check entirely, when location
  doesn't match — deliberately different from `verify`'s `Write` case,
  where a wrong-location write IS a violation, since something was
  genuinely written to the wrong place there).
- **Current schema** — since the operation can't change it, checking the
  table's schema *right now* (via the plain, connector-agnostic
  `Table.columns()`, not Iceberg's `SparkTable`) is exactly checking what
  will still be true afterward. Catches a table already out of compliance
  (e.g. an out-of-band schema change) at the point of a state-changing
  operation on it.

Extraction needs only two reflective hops now (down from three) —
`BaseProcedure.tableCatalog()` (protected, `setAccessible`) and
`TableCatalog.loadTable(Identifier)` (fully public) — since the
snapshot-specific `copyWithSnapshotId` hop is no longer needed at all.

**Verified with real tests, including a real bug the tests themselves
caught**: `IcebergConnectorSpec`'s PASS test (current schema satisfies
the contract → allowed), FAIL test (current schema doesn't → rejected
before touching the table), and a location-scoping test (targets a table
the active contract doesn't govern → allowed). The location-scoping
test's first version passed even though a mutation-testing survivor
proved it wasn't actually distinguishing "scoping worked" from "the
contract happened to be lenient enough to pass anyway" — its contract
only required an already-present, optional field. Fixed by requiring a
field the unrelated table genuinely lacks, so a real schema check (if
scoping were broken) would provably fail. Mutation testing on the three
changed files (`StateChangingCallSupport.scala`, `StructuralVerifier.scala`,
`ContractEnforcementRule.scala`): **100%** (51/51) after that fix — up
from 96.08% (2 survivors) beforehand. `mimaReportBinaryIssues` clean.

## Extending to the five procedures that share `rollback_to_snapshot`'s shape

Generalized the pilot to `rollback_to_timestamp`, `cherrypick_snapshot`,
`publish_changes`, `set_current_snapshot`, and `fast_forward` — every
remaining procedure whose only effect is moving which snapshot is
current, per the "small, mechanical extension" scoping ROADMAP.md set out
when the pilot was first sequenced.

**Investigated before generalizing, not assumed from the pilot alone.**
`javap` against the real `iceberg-spark-runtime-3.5_2.12:1.11.0` jar
confirmed each procedure's declared parameter list, followed by a real
probe (since deleted) exercising each one against a live Iceberg session
to confirm the actual `Call.args` shape and observable table-state
effect — not just the declared parameter names:

- `rollback_to_timestamp` and `cherrypick_snapshot` and `publish_changes`
  each declare exactly two parameters (table, plus a timestamp/snapshot-id/
  WAP-id respectively) — the identical two-argument shape
  `rollback_to_snapshot`'s own extraction already handled, confirmed via
  probe that none of the three can touch a table's schema and that
  `cherrypick_snapshot`/`publish_changes` both move `main`'s current
  snapshot (confirmed via row counts before/after).
- `set_current_snapshot` declares **three** parameters (table,
  `snapshot_id`, `ref`) — `snapshot_id` and `ref` are mutually exclusive,
  confirmed via probe that the real `Call.args` array is always
  3-wide with exactly one of `args(1)`/`args(2)` a non-null `Literal`
  depending on which was supplied. Neither value is read by the
  extraction, the same way `rollback_to_snapshot`'s own snapshot id
  isn't — only `args.head` (the table) is ever needed, and Iceberg's own
  analyzer (`ResolveProcedures`) already guarantees a well-formed,
  fully-bound `args` array by the time this check rule sees the plan, so
  no further shape validation was added.
- `fast_forward` (`FastForwardBranchProcedure`) declares three parameters
  too (table, `branch` — the branch being moved, `to` — the source ref) —
  genuinely different from the other five: confirmed via probe that
  fast-forwarding `"main"` changes the table's default read (row count
  changed), while fast-forwarding any *other* named branch leaves the
  default read completely unchanged. This looked at first like it might
  need special-casing (only check when `branch == "main"`), but doesn't:
  the existing check only asserts an invariant — current schema can't
  move — that holds regardless of which branch a given call targets, not
  a claim about what that specific call changes. A dedicated test proves
  this concretely rather than leaving it as a documentation-only claim:
  fast-forwarding a non-`"main"` branch is still rejected when the
  table's current schema violates the contract, even though that
  specific call's own effect never touches `main` either way.

**Implementation generalized cleanly, not by branching per procedure.**
`StateChangingCallSupport`'s single hardcoded `RollbackToSnapshotProcedure`
class-name check became a `Map[String, String]` from procedure class name
to its CALL-syntax name (used only for the error message via a new
`StateChangeInfo.callName` field) — the extraction logic itself,
verification logic, and `ContractEnforcementRule` wiring are all
unchanged and already generic; no code duplicated per procedure.

**Tests**: one PASS test per newly-recognized procedure in
`IcebergConnectorSpec` (`rollback_to_timestamp`, `cherrypick_snapshot`,
`publish_changes`, `set_current_snapshot` — both its `snapshot_id`-form
and `ref`-form arg bindings — `fast_forward` on `"main"`), a FAIL test
for `rollback_to_timestamp` proving the generalized FAIL path still
works, and the `fast_forward`-on-a-non-`"main"`-branch FAIL test
described above. The old "`cherrypick_snapshot` still fails closed" test
(no longer true) was replaced with an `add_files` fail-closed test, since
`add_files` remains genuinely unmodeled.

Mutation testing scoped to the two changed files: `StateChangingCallSupport.scala`
**80%** (4/5) — the sole survivor is a genuinely equivalent mutant on the
`Call`-class-name guard (mutated to always-true), defended by the
surrounding `try`/`catch`: any real non-`Call` plan lacks a `procedure()`
method, so the outer catch produces an identical `None` either way,
regardless of whether the guard ran — the same category of near-equivalent
guard mutant already accepted for this exact pattern during the original
pilot. `ContractEnforcementRule.scala`: **100%** (10/10).
`mimaReportBinaryIssues` clean (both touched types remain
`private[sparkadapter]`). Full `./dev/build`/`./dev/test`/`./dev/regression`
pipeline passing.

## Verifying the remaining harder procedures: `add_files`, `migrate`, `snapshot`, `rewrite_table_path`

The last four procedures, previously assumed (when this work was first
scoped) to all need new CALL-argument-parsing mechanism. Real
investigation — `javap` against the real jar, then a live probe (since
deleted) against a real Iceberg session for each — found that assumption
wrong for three of the four, in both directions: two needed *less* new
work than expected, one needed *none at all*.

**`add_files(table, source_table, ...)` and `migrate(table, ...)` turned
out to fit the existing nine-procedure mechanism exactly, unchanged.**
Confirmed via probe: importing a source with an *extra* column left
`add_files`'s target schema unchanged (the extra column is silently
dropped, not imported), and importing a source *missing* a column the
target has still succeeded (Iceberg NULL-fills it — the same
narrower-append behavior `outputSchemaWithTargetOnlyFields` already
handles for ordinary writes). So `add_files` never changes its target's
schema, and `source_table` is never read at all — only `args.head`
(`table`), exactly like `rollback_to_snapshot` and the rest. `migrate`
converts its table *in place* (same identifier before and after); probed
using the actual production code path (`TableCatalog.loadTable`, not a
`spark.table(...)` DataFrame read) that this correctly resolves the
table's *pre*-migration schema — which is exactly right, since this check
rule runs during analysis, before the procedure's own `call()` executes,
and Iceberg's migrate always preserves the schema unchanged anyway
(confirmed by comparing schemas before/after a real migration). Both
procedures needed zero new code beyond adding their classes to
`currentStateChangingProcedureClasses`.

**`snapshot(source_table, table, ...)` is the one procedure that's
genuinely different**, confirmed via probe: it creates a *new* table
(`table`, `args(1)`) whose schema comes from an *existing*, different
table (`source_table`, `args.head`) — the opposite pairing from every
other procedure, where the same identifier supplies both the contract's
location and the schema. Both arguments can also be qualified with a
catalog *different* from the one the CALL itself was invoked against —
confirmed via probe that an unqualified destination resolves under the
*session's* current/default catalog, not `BaseProcedure.tableCatalog()`
(the CALL's own bound catalog, which every other procedure's extraction
relies on). This needed genuinely new resolution: `resolveIdentifier`
re-implements the same first-segment-is-a-registered-catalog-name check
Iceberg's own `Spark3Util.catalogAndIdentifier` uses — confirmed to match
via probe, not assumed — using `SparkSession.active`'s `CatalogManager`
(both fully public Spark APIs — notably *not* reflection, unlike every
other procedure's `tableCatalogOf`, since there's no equivalent public API
for "the catalog a specific procedure instance was resolved against," but
there is one for "the catalog a plain identifier string resolves to").
One real access-control surprise along the way: `CatalogManager`'s own
type can't be named directly in this module's source even though its
bytecode is `public` (`cannot be accessed in package
org.apache.spark.sql.connector.catalog` — Spark marks it `private[sql]`
at the Scala level, which downstream `javap` doesn't reveal). Worked
around by passing `SparkSession` itself instead of a `CatalogManager`
value, and letting `resolveIdentifier` call `.sessionState.catalogManager`
internally, where Scala's own type inference never needs to write the
inaccessible type's name.

**`rewrite_table_path(table, source_prefix, target_prefix, ...)` needed no
verification mechanism at all — a real, positive finding, not a gap.**
Confirmed via probe: it never touches the table's own catalog entry,
current schema, or current snapshot (all identical before/after a real
run), and registers no new catalog table itself — it only writes a
portable copy of metadata/data file *references* at a target path prefix,
for physically relocating a table's storage; an external process is
expected to register a new table against that copy later, which is a
separate operation this file doesn't need to see. Nothing a contract
could ever check is affected, so it joined `FailClosedCommands`'
safe-listed no-ops instead of `StateChangingCallSupport` — the same
disposition as the ten original compaction/GC/introspection procedures,
not a harder one.

**A real environment obstacle, solved without a new dependency.**
Testing `migrate` and `snapshot`'s cross-catalog resolution needs a
*default* catalog that can resolve both plain native (non-Iceberg) tables
and Iceberg CALL procedures — `IcebergConnectorSpec`'s existing `"local"`
catalog is Hadoop-type, which only ever holds Iceberg tables. Configuring
`spark_catalog` itself as `SparkSessionCatalog` with `type=hadoop` failed
for `migrate` specifically: Hadoop-type catalogs reject *any* table with a
pre-existing "custom" location (`Cannot set a custom location for a
path-based table`), which every native Spark table has, by construction —
confirmed via a real probe, not worked around by guessing at path
formats (tried matching Iceberg's own expected path convention exactly;
still rejected — the restriction is unconditional, not a path-matching
bug). A real Hive metastore would sidestep this, but needs a new
`spark-hive` test dependency this module doesn't otherwise need. Used
Iceberg's `JdbcCatalog` instead (`catalog-impl` pointed at an H2 database
file) — H2 was already transitively present on this module's test
classpath, so this added no new dependency at all, and `JdbcCatalog`
doesn't share Hadoop catalog's custom-location restriction.

**Tests**: one PASS test each for `add_files` and `migrate`; for
`snapshot`, a PASS test (deliberately cross-catalog — source under the
default catalog, destination under `"local"` — to prove the new
per-argument resolution actually works, not just the single-catalog
case), a second PASS test specifically for the
namespace-but-no-catalog-prefix boundary (`resolveIdentifier`'s
`currentCatalog` fallback), and a FAIL test (the *source's* schema
violates the contract governing the *destination* — proving the
opposite-pairing design is checked correctly, and that no new table gets
created). The suite's previous "some CALL still fails closed" role (held
first by `rollback_to_snapshot`, then by `cherrypick_snapshot`, most
recently by `add_files`) has no real procedure left to demonstrate with,
now that all 20 have a disposition — retired rather than kept on a
contrived case; the underlying fail-closed wiring for an unrecognized
`Command` remains covered by `FailClosedCommandsSpec`'s direct unit test
and this module's other non-CALL `UnverifiableWrite` tests.

Mutation testing scoped to the two changed files (the second PASS test
above was added specifically in response to two real survivors found on
the first mutation run, not just to pad coverage — see the inline
comments at each accepted survivor for why the other two are near-
equivalent, not gaps): `StateChangingCallSupport.scala` **85%** (17/20).
`FailClosedCommands.scala` (the new `rewrite_table_path` safe-list entry):
**100%** (4/4 non-excluded). `mimaReportBinaryIssues` clean. Full
`./dev/build`/`./dev/test`/`./dev/regression` pipeline passing.

## Iceberg operation-surface coverage ledger

| Operation | Status | Evidence / next step |
|---|---|---|
| `.load(path)` (bare filesystem path, unqualified) | 🚫 **Not Iceberg's supported access pattern** | Confirmed empirically: a bare `.format("iceberg").save(path)`/`.load(path)` with no catalog qualification fails hard (`NoClassDefFoundError` reaching for a default `HiveCatalog` no metastore client is on the classpath for), regardless of named-catalog config. Not a gap to close — Iceberg's real path-based mechanism is a path identifier *under* a named Hadoop-type catalog (`` local.`/abs/path` ``, confirmed working, covered by the `AppendData`/`DataSourceV2Relation` rows below). |
| Catalog table read (batch) | ✅ **Covered — closed this pass** | New `DataSourceV2Relation` case in `SparkPlanAdapter`, connector-agnostic. `IcebergConnectorSpec`'s "translates a batch Iceberg catalog read" test, plus a direct-construction no-location fallback test in `SparkPlanAdapterSpec`. |
| Time travel / snapshot reads (`VERSION AS OF`, `snapshot-id` option) | ✅ Covered | Both resolve to the same `DataSourceV2Relation` shape as an ordinary catalog read (confirmed via probe — the time-travel option doesn't change the node type), so the new case covers them without separate handling. No dedicated permanent test beyond the plain catalog-read one — the shape is identical. |
| Streaming read (`readStream`) | ✅ Covered | `StreamingRelationV2`, already generic (closed during the Delta pass). Confirmed for Iceberg via `IcebergConnectorSpec`'s streaming `.toTable()` test (exercises both the read and write halves together). |
| Change-data-feed / incremental read (`start-snapshot-id`) | ✅ Covered | Same `DataSourceV2Relation` shape as time travel — confirmed via probe, no separate case needed. |
| `.save(path)` (bare filesystem path) | 🚫 **N/A — see the `.load(path)` row above** | Same finding, write side. |
| `.saveAsTable(...)`, new table | ✅ Covered | `ReplaceTableAsSelect`, already generic (closed during the Delta pass) — confirmed for Iceberg via probe (`.saveAsTable()` on a new table maps to `ReplaceTableAsSelect` under a V2 catalog, same as Delta). |
| `.saveAsTable(...)`, existing table (append) | ✅ Covered | `AppendData`, already generic. `IcebergConnectorSpec`'s PASS/FAIL pair. |
| `.insertInto(...)` | ✅ Covered | Same `AppendData` shape, confirmed via probe. |
| `.writeTo(...).append()`/`.overwrite(cond)` | ✅ Covered | `AppendData`/`OverwriteByExpression`, already generic. |
| `.writeTo(...).overwritePartitions()` | ✅ **Covered — closed this pass** | New `OverwritePartitionsDynamic` case, connector-agnostic. `IcebergConnectorSpec`'s PASS test. |
| `.writeTo(...).create()` | ✅ **Covered — closed this pass** | New `CreateTableAsSelect` case, connector-agnostic. `IcebergConnectorSpec`'s PASS/FAIL pair plus a direct-inspection `saveMode` test. |
| `.writeTo(...).createOrReplace()`/`.replace()` | ✅ Covered | `ReplaceTableAsSelect`, already generic — confirmed via probe. |
| Format-specific DML (`MERGE`/`UPDATE`/`DELETE`) | ✅ **Covered — structurally, closed this pass** | New `dsv2RowLevelWrite` case (`ReplaceData`/`WriteDelta`, via the standard `RowLevelWrite` trait — no reflection, unlike Delta's `deltaRowLevelDml`), connector-agnostic. Same structural-only scope as Delta's row-level DML row. `IcebergConnectorSpec`'s PASS/FAIL pair for MERGE, plus PASS tests for UPDATE/DELETE. |
| Streaming write (`writeStream`) | ✅ Covered | `WriteToStream`, already generic (closed during the Delta pass) — confirmed for Iceberg via `IcebergConnectorSpec`'s streaming `.toTable()` test, using the `catalogTable`-populated fallback tier (Iceberg's streaming sink didn't need the reflective `DeltaSink`-style fallback Delta's did). |
| Maintenance operations that touch data (`CALL system.*` procedures) | ✅ **Fully covered — closed this pass** | Procedure-name-aware classification (see "Iceberg CALL procedure classification" above): all 20 procedures now have a real, evidenced disposition, none left unmodeled. 11 (10 original storage/metadata compaction, GC, catalog registration, stats, read-only introspection, plus `rewrite_table_path` — see "Verifying the remaining harder procedures" below) run as verified no-ops. 9 (`rollback_to_snapshot`, `rollback_to_timestamp`, `cherrypick_snapshot`, `publish_changes`, `set_current_snapshot`, `fast_forward`, `add_files`, `migrate`, `snapshot` — see "Verifying `rollback_to_snapshot`", "Extending to the five procedures...", and "Verifying the remaining harder procedures" below) are genuinely verified via a catalog-level schema-plus-location check — the table's *current* schema and location for the first eight (none can change it, confirmed per-procedure), the *source* table's current schema paired with the *destination*'s location for `snapshot` (the one procedure with a genuinely different argument shape, needing new per-argument catalog resolution rather than fitting the shared mechanism unchanged). `IcebergConnectorSpec`'s PASS/FAIL/scoping tests for all nine genuinely-verified procedures, plus a safe-procedures regression test (5 of the 11 no-ops), prove both tiers. |
| Iceberg's own metadata/ref DDL (branch/tag/partition-spec/identifier-fields/write-ordering/views) | ✅ Covered by policy classification | 13 classes added to `FailClosedCommands`' safe list, reasoning in that file's own comment. `IcebergConnectorSpec`'s regression test proves none are blocked under an active, otherwise-rejecting contract. |

## Iceberg feature-surface coverage ledger

Iceberg's own distinguishing behaviors beyond its write-command shapes -
per docs/ADDING_A_SPARK_CONNECTOR.md's "The feature surface", a
connector-specific list, not the fixed operation-surface template above.

| Feature | Status | Evidence / next step |
|---|---|---|
| Copy-on-write vs. merge-on-read row-level operations | ✅ Confirmed | Both modes rewrite to the same `RowLevelWrite`-family shape (`ReplaceData` for copy-on-write, `WriteDelta` for merge-on-read) — `dsv2RowLevelWrite` matches the shared trait, so either mode is recognized identically. `IcebergConnectorSpec`'s MERGE/UPDATE/DELETE tests exercise Iceberg's default mode; not separately tested per mode (the mechanism is provably mode-agnostic by construction — it matches the trait both extend, not either concrete class). |
| Staged-table location reporting differs from Delta's | 🔧 **Found and fixed** | See "A second staged-table trap" above. Fixed by keying the location-resolution fallback on the `StagedTable` marker interface itself, not on whether a `"location"` property happens to be absent. |
| Partition evolution (`ADD`/`DROP`/`REPLACE PARTITION FIELD`) | ✅ Confirmed | Metadata-only, safe-listed; `IcebergConnectorSpec`'s regression test exercises `ADD PARTITION FIELD` directly under an active contract. |
| Branching and tagging (named refs to a snapshot) | ✅ Confirmed | Metadata/ref-only, safe-listed; `IcebergConnectorSpec`'s regression test exercises create/drop of both directly. |
| `CALL` system procedures (maintenance) | ✅ **Found and fully fixed** | See "Iceberg CALL procedure classification", "Verifying `rollback_to_snapshot`", "Extending to the five procedures...", and "Verifying the remaining harder procedures" above, and the operation-surface row above — all 20 procedures reclassified from wrongly-rejected: 11 to correctly-allowed no-ops, 9 to genuinely verified. None left fail-closed for lack of a mechanism; the classification itself (not "still unmodeled") is now the complete, permanent disposition for every procedure this connector version has. |
| Iceberg SQL views (`CREATE`/`DROP`/`SHOW ICEBERG VIEWS`) | ✅ Confirmed | Metadata-only (view definitions carry no data of their own, matching this file's existing Spark/Delta view-command entries), safe-listed. Not separately tested beyond the safe-list regression test above — no distinguishing behavior beyond "doesn't touch row content," the same reasoning already applied to Spark's own `ShowViews`/`CreateViewCommand` entries. |
| `iceberg-spark-runtime`'s missing `scala-collection-compat` dependency | 🔧 **Found — a library gap, not an Invariant one** | See above; documented as a real, external finding, not a bug this module can fix (adding it as anything but a test dependency would be exactly the kind of unwanted runtime dependency this module's whole design avoids). |
| Deletion vectors / merge-on-read positional deletes | ✅ **Confirmed — closed this pass** | Real probe (since deleted): a `DELETE` against a real `format-version = 3` table (Iceberg's deletion-vector spec) still produces a plain `ReplaceData` node — the same class `dsv2RowLevelWrite` already matches via the shared `RowLevelWrite` trait. The storage mechanism behind a merge-on-read delete (position-delete file vs. deletion vector) isn't visible at the `LogicalPlan` level this adapter operates on at all, so no code change was needed. `IcebergConnectorSpec`'s new "PASS: a DELETE against a format-version=3 (deletion vector) Iceberg table..." test. |
| Schema evolution on write (`write.spark.accept-any-schema` table property + `mergeSchema` write option) | 🔧 **Found and fixed** | Real bug, but the *opposite* direction from the one predicted before investigating: adding a genuinely new column via `mergeSchema` was already correct (`cmd.query.schema` — the writer's own DataFrame — already includes it, confirmed empirically; unlike Delta's MERGE, `AppendData`'s query *is* the writer-supplied data, not a re-derived plan that could go stale). The real bug is a *narrower* write: with `accept-any-schema` enabled, Iceberg accepts an append missing a column the target already has, NULL-filling it — `outputSchema` (from `query.schema` alone) omitted that column entirely, so a contract requiring it was wrongly `MISSING_OUTPUT_FIELD`-rejected. See "Generalizing the generated-columns fix" below — fixed by the same mechanism that now also covers Delta's generated columns. |
| Identity/generated columns | ✅ **Confirmed — closed this pass** | Real probes (since deleted), not assumed from docs: both Spark's `GENERATED ALWAYS AS` syntax and column `DEFAULT` values are rejected outright by this Iceberg catalog integration — `AnalysisException` (`UNSUPPORTED_FEATURE.TABLE_OPERATION`, "does not support generated columns"/"does not support column default value"), thrown by Spark's own analyzer before any plan is ever produced, regardless of `write.spark.accept-any-schema` (tried explicitly, made no difference). So unlike Delta, there's no Iceberg analog to generated columns reachable through Spark SQL with this connector version — nothing for Invariant to translate, verify, or fix. `IcebergConnectorSpec`'s new "GENERATED ALWAYS AS is rejected outright..." and "a column DEFAULT value is rejected outright..." tests. |

## Generalizing the generated-columns fix: target-only fields, not just generated columns

Investigating the schema-evolution row above found that Delta's
generated-columns fix (`outputSchemaWithGeneratedColumns`/
`deltaGeneratedFields`, described in docs/connectors/delta.md's "Delta
feature-by-feature confidence pass" section — reflecting into
`DeltaTableV2.initialSnapshot()` to read the `delta.generationExpression`
metadata key) and Iceberg's narrower-write case are two connector-specific
*mechanisms* for the same
underlying situation: a resolved write target can legitimately have
fields the write's own `query` doesn't supply, that will still exist in
the committed row. Confirmed empirically that this generalizes further
than either mechanism alone: `cmd.table.schema()`/`cmd.table.columns()`
(the resolved `NamedRelation`'s current, already-committed schema — a
plain public API, no reflection) already carries a Delta generated
column's *name* even though its `.metadata` (the part that would have
identified it as specifically "generated") is stripped — so detecting
*which* target-only fields are generated was never actually necessary,
only whether the target has fields the query doesn't.

**Why unioning in every target-only field is safe, not just convenient**:
by the time a `DataSourceV2Relation`-based write reaches this check rule
at all, Spark's own analyzer has already validated the write's schema is
acceptable against the target — confirmed empirically (a real probe,
since deleted) that an Iceberg table *without* `accept-any-schema`
rejects a narrower write with `AnalysisException` before it ever
produces an `AppendData` node for this rule to see (verified by a
permanent test, `IcebergConnectorSpec`'s "a narrower append is still
rejected by Spark itself... before reaching Invariant"). So a
genuinely-missing required field (the case `MISSING_OUTPUT_FIELD` exists
to catch) is never silenced by this: either Spark's analyzer already
rejected the write for real (this code never runs), or the target's own
connector has already endorsed the field's absence as valid, meaning
unioning it into `outputSchema` reports what will actually be committed,
not merely what the writer provided.

`outputSchemaWithGeneratedColumns`/`deltaGeneratedFields` (reflective,
Delta-specific) were replaced outright by `outputSchemaWithTargetOnlyFields`
(no reflection, connector-agnostic) — `AppendData`/`OverwriteByExpression`/
`OverwritePartitionsDynamic` all call the new one. `Table.columns()`
(not the deprecated `Table.schema()`) is used for the field list — a
`Column` only guarantees `name()`/`dataType()`/`nullable()`, exactly
what's needed; no other `Column` field (default value, generation
expression, comment) is read. Verified: `IcebergConnectorSpec`'s
existing generated-column-equivalent tests (the narrower-write PASS/FAIL
pair) and the pre-existing Delta generated-column test both still pass
unchanged against the new mechanism. Mutation testing rescoped to
`WriteCommandSupport.scala` after this simplification: **76.92%**
(20/26 non-excluded mutants killed) — the new `outputSchemaWithTargetOnlyFields`/
`unionNewFields` code has zero survivors; all 6 remaining are
pre-existing, already-documented from earlier sub-phases (`catalogTable.isDefined`
×2, the `deltaDmlClassNames` guard, `WriteFiles`/`DeltaSink` near-equivalents).

## Closing Iceberg's last two ❓ feature-surface rows: deletion vectors and generated/default columns

A follow-up pass targeting exactly the two rows the initial Iceberg
investigation left as ❓ Not investigated. Both closed with **no
production code changes** — each was a real question with a real,
testable answer, not a gap needing a fix.

**Deletion vectors** (Iceberg's V3 merge-on-read spec, the successor to
position-delete files): a real probe against a genuine `format-version =
3` table, with a real `DELETE` run against it, confirmed the resulting
plan is still a plain `ReplaceData` — the same class `dsv2RowLevelWrite`
already matches via the shared `RowLevelWrite` trait Iceberg's row-level
API rewrites both copy-on-write and merge-on-read operations into. The
storage representation of the delete (a position-delete file vs. a
deletion vector) is an Iceberg-internal detail below the `LogicalPlan`
level this adapter translates at all — so there was never anything for
this layer to special-case. Closed by adding one permanent test, not by
changing any translation logic.

**Identity/generated columns**: two real probes — Spark's `GENERATED
ALWAYS AS` syntax, and a column `DEFAULT` value (Iceberg's V3
`initial-default`/`write-default` mechanism) — both against this
connector's real catalog integration. Both are rejected outright by
Spark's own analyzer (`AnalysisException`,
`UNSUPPORTED_FEATURE.TABLE_OPERATION`) before any `LogicalPlan` is ever
produced, and setting `write.spark.accept-any-schema` doesn't change
that outcome (tried explicitly). So, unlike Delta, this Iceberg
integration has no generated/default-column concept reachable through
Spark SQL at all — there's no feature here for
`outputSchemaWithTargetOnlyFields` (or anything else) to need to handle.
Closed by adding two permanent tests asserting the rejection, the same
pattern already used for "a narrower append is still rejected by Spark
itself... before reaching Invariant."

Both findings are `IcebergConnectorSpec` tests (19 tests total in that
suite after this pass, up from 17). No `spark-adapter` main source
changed this pass, so CLAUDE.md's mutation-testing requirement (scoped to
changed/added `src/main/scala` files) doesn't apply; `sbt
mimaReportBinaryIssues` stayed clean as expected for a test-only change.

