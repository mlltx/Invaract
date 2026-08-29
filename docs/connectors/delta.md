# Delta Lake support

[← Back to Spark Adapter](../SPARK_ADAPTER.md#connector-support)

Delta writes are translated via `SaveIntoDataSourceCommand` (see
"Translation coverage" above) — added after investigating what a real
Delta write actually analyzes to, empirically, not assumed: a fresh
Delta-enabled `SparkSession`
(`spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`,
`spark.sql.catalog.spark_catalog=...DeltaCatalog`), a real
`df.write.format("delta").save(path)`, and a `QueryExecutionListener`
logging every analyzed plan Spark produced along the way. The write's
final analyzed plan was `SaveIntoDataSourceCommand`, not a Delta-specific
class at all — Spark's own generic command for any
`CreatableRelationProvider`-based `.save(...)` write, the non-`FileFormat`
counterpart to `InsertIntoHadoopFsRelationCommand`.

**No Delta dependency, compile-time or runtime, is needed to translate
this.** `SaveIntoDataSourceCommand` and `DataSourceRegister` are both
plain, public `org.apache.spark.sql` classes already on this module's
existing `provided` Spark dependency; Delta's `DeltaDataSource` was
confirmed (same investigation) to implement `DataSourceRegister` with
`shortName() == "delta"`, exactly the mechanism `formatOf` already used
for built-in file formats — so `formatOf`'s parameter type was simply
widened from `FileFormat` to `AnyRef` and reused as-is, rather than
writing Delta-specific translation code. `delta-spark` appears only as a
`% "test"` dependency (pinned to 3.2.0, not the latest 3.2.x: a confirmed
real bug in 3.2.1 affects exactly this Scala 2.12 + Spark 3.5.1
combination — see
[delta-io/delta#3737](https://github.com/delta-io/delta/issues/3737)) —
its only job is spinning up a real Delta session to test against, the
same role `com.h2database` plays for the JDBC precedent, never something
the main translation code imports or needs present at runtime for a
non-Delta job to run.

This is also why the single assembled `spark-adapter` jar (see
CLAUDE.md's "What's the product, and what's the test harness" and
ARCHITECTURE.md) needs no Delta-specific variant: nothing about Delta
support depends on Delta being bundled in, or even present, unless a
user's own job actually writes Delta — confirmed directly, not assumed,
by inspecting the assembled jar after adding this support and finding
zero Delta classes in it, same size as before.

An initial attempt at this feature assumed Delta's own command classes
would need recognizing directly, which would have required a real
`delta-spark` compile dependency (`provided`, to keep it out of the
assembled jar) and Delta-specific pattern-matching code — abandoned once
the actual analyzed plan turned out to already be a generic Spark class,
making that unnecessary entirely.

Two real bugs surfaced fixing this, neither about translation itself:

- **`ContractEnforcementRule.verifyOrThrow`'s output-schema derivation**
  only special-cased `InsertIntoHadoopFsRelationCommand`; for any other
  plan (including the new `SaveIntoDataSourceCommand` case) it fell back
  to the command node's *own* `.schema` — empty, since `Command` nodes
  don't produce rows — rather than the schema of the query being written.
  A real Delta write, correctly matching every field its contract
  declared, still failed with `MISSING_OUTPUT_FIELD` on every one of
  them, because the schema being checked against was always empty
  regardless of what was actually written. Fixed by adding the same
  `cmd.query.schema` case `SaveIntoDataSourceCommand` needs.
- **`SparkAdapterListener.onSuccess`** has its own independent "is this a
  write" check, entirely separate from `SparkPlanAdapter.translatePlan`'s
  and `ContractEnforcementRule.verifyOrThrow`'s — also hardcoded to only
  `InsertIntoHadoopFsRelationCommand`. A Delta write correctly translated
  and correctly enforced, yet the listener-based report
  (`demo/output/report.json`'s `transformationIR` section) never
  captured it. Fixed the same way, in a third, separate location — three
  independent places recognizing "is this a write command" was, at the
  time, left as a noted design smell rather than fixed. It has since been
  fixed — see "Write command recognition: a single registry" below.

Both were caught by a real, real-Spark integration test failing (the new
Delta PASS/FAIL pair in `ContractEnforcementRuleSpec` and the translation
test in `SparkPlanAdapterSpec`), not by inspection — consistent with this
module's general testing philosophy.

**Known limitation:** `.save(path)`-style writes are recognized here;
`.saveAsTable(...)` against a *new* V1 data source table is a separate,
later addition (`CreateDataSourceTableAsSelectCommand` — see "Fail-closed
on unverifiable writes" below). DataFrameWriterV2 and SQL `MERGE INTO`
against a Delta (or any DataSourceV2 catalog) table go through a different
plan shape entirely (`AppendData`/`OverwriteByExpression`/Delta's own
`MergeIntoCommand`/similar) that still has no translation — not a silent
gap any more, though: see "Fail-closed on unverifiable writes" for why
these now abort instead of passing through unverified.

## Delta Lake reads

Everything above is the write side; Delta as a contract *input* was a
separate, previously unexplored question, prompted directly by asking
whether read recognition had the same "recognition duplicated across
independent match sites" problem the write side did before "Write command
recognition: a single registry" fixed it.

**Investigated empirically, not assumed — and the answer turned out to be
simpler than expected.** A real Delta-enabled session with an
`injectCheckRule` probe (the same mechanism `ContractEnforcementRule`
actually uses) was run against both `.load(path)` and a catalog table
reference (`spark.table(...)`/`SELECT * FROM tbl`/`SELECT * FROM
delta.\`path\``). Both produce a `LogicalRelation` wrapping
`org.apache.spark.sql.delta.DeltaLog$$anon$2` — and that class is an
**anonymous subclass of Spark's own `HadoopFsRelation`**, not a distinct
relation type the way it first appeared. `locationOf`'s existing
`case h: HadoopFsRelation => h.location.rootPaths...` branch (and the
identical guard in `translatePlan`'s `LogicalRelation` case) already
matches it through ordinary Scala subtyping, and already extracts the
precise physical path — for `.load(path)` *and* for a catalog table
reference alike, confirmed by checking `catalogTable.storage.locationUri`
against what `locationOf` actually returns.

**Net result: zero new code.** No new `translatePlan` case, no location
fallback to improve, and — directly answering the motivating question —
**no registry consolidation needed on the read side.** The write side's
duplication bug was real and specific: three sites recognized *different*
concrete `Command` classes, so one could add support for a class the
others didn't know about. Reads have no analogous risk today: both
consumer sites (`SparkPlanAdapter.translatePlan`'s `LogicalRelation` case
and `ContractEnforcementRule.verifyOrThrow`'s `plan.collect { case lr: LogicalRelation => ... }`
for input-schema collection) gate on the *same* single Spark type, so
they cannot disagree with each other by construction — and Delta's read
relation turned out to already be that type, by inheritance, not a
second type needing its own case.

This does **not** mean the read side can never have the write side's
problem — it means it doesn't have it *today*, for *this* connector. A
future connector whose read genuinely produces something other than
`LogicalRelation` (a `DataSourceV2Relation`, most plausibly — the same
"Known limitations" gap already noted for DataSourceV2 catalog writes)
would need a real second case added in both of those sites, and *that*
would reintroduce the write side's exact risk unless done as a shared
mechanism from the start. Treat that as the trigger for a
`ReadRelationSupport`-style registry, not a reason to build one now for a
shape that doesn't exist yet — see docs/ADDING_A_SPARK_CONNECTOR.md.

**Verified through real enforcement, not just translation in isolation:**
a translation test (`SparkPlanAdapterSpec`) confirms both read shapes
produce a precise `ir.Read` with no fallback diagnostic, and a PASS/FAIL
enforcement pair (`ContractEnforcementRuleSpec`) confirms a contract's
declared input schema is genuinely checked against a real Delta read's
actual schema — including a real, incidental finding along the way: Delta
reports every column nullable on read-back regardless of what was
written, a genuine Delta behavior (not a bug here) that the FAIL test's
contract works around the same way a real contract author would need to.

## Delta Lake operation-surface coverage ledger

`add-spark-connector` was run for Delta twice — once for writes, once for
reads — and each time declared its narrower scope done without stating
what was still untouched. That gap (see
docs/ADDING_A_SPARK_CONNECTOR.md's "coverage ledger" requirement, added
directly because of it) is what this section closes: every row of the
canonical operation surface, investigated empirically against a real
Delta-enabled `SparkSession` via `injectCheckRule` — the exact mechanism
`ContractEnforcementRule` uses — with one of three dispositions per row.
"❓ Not investigated" would be a legitimate answer for a row; a missing
row is not, so every row below has one. **Every 🚫 row also carries a
next step, not just a citation** — see
docs/ADDING_A_SPARK_CONNECTOR.md's "What 'fails closed' means (and
doesn't)": fail-closed is a safety net for an operation not yet
translated, not a verdict that it shouldn't be. A row landing on 🚫 here
is future work, unless stated otherwise.

## Read

| Operation | Status | Evidence / next step |
|---|---|---|
| `.read.format("delta").load(path)` | ✅ Covered | `LogicalRelation` wraps `DeltaLog$$anon$2`, an anonymous `HadoopFsRelation` subclass matched by ordinary subtyping — see "Delta Lake reads" above. `SparkPlanAdapterSpec`, `ContractEnforcementRuleSpec` PASS/FAIL pair. |
| Catalog table reference (`spark.table(...)`/`SELECT * FROM tbl`) | ✅ Covered | Same relation shape as above, confirmed for both forms — see "Delta Lake reads" above. |
| Time travel / snapshot reads (`versionAsOf`/`timestampAsOf`) | ✅ Covered | Probed empirically: produces the identical `LogicalRelation(relation=HadoopFsRelation)` shape as a plain read. Zero new code needed. |
| Streaming read (as a contract-declared *input*) | ✅ **Covered — closed this pass** | Previously a real false-positive gap: neither `StreamingRelation` (the legacy V1 path Delta itself uses — `.readStream.format("delta").load(path)` analyzes to this, not `StreamingRelationV2`) nor `StreamingRelationV2` (the modern DataSourceV2 path, used by `rate`/Kafka/similar) is a `LogicalRelation`, so `ContractEnforcementRule.verifyOrThrow`'s input-schema collection never saw either, and a contract declaring a streaming source as a required `input` always reported `MISSING_INPUT` even though data was genuinely being read. Closed by teaching both `SparkPlanAdapter`'s translation and `ContractEnforcementRule`'s input-schema collection to recognize both shapes, via two shared, non-duplicated helpers (`streamingRelationLocationOf`/`streamingRelationV2LocationOf`) rather than two independent matches — the exact duplication risk this module's write side already learned from. `StreamingRelation.dataSource.options("path")` and `sourceName` give location/format with no reflection needed (both are plain public spark-sql classes, unlike `WriteToStream`'s sink); `StreamingRelationV2.table` reuses the same `Table.properties()` lookup `AppendData`/`OverwriteByExpression` use below. Verified through real enforcement: a PASS/FAIL pair in `ContractEnforcementRuleSpec` proving a contract's declared input schema is genuinely checked against a real streaming Delta source. |
| Change-data-feed / incremental read (`readChangeFeed`) | ✅ Covered (with a precision caveat) | Probed empirically: produces `LogicalRelation(relation=CDCReader$$DeltaCDFRelation)`, a class distinct from `HadoopFsRelation` — but `translatePlan`'s generic `LogicalRelation` case (not the `HadoopFsRelation`-specific branch) already handles any relation type, producing a correct `ir.Read`. Because this relation has no populated `catalogTable` for a path-based read, it takes the existing "fallback" branch and reports a location diagnostic — the location string is the relation's `toString()`, not a clean physical path. Schema verification is unaffected; only location precision is reduced. **Next step:** none required for correctness; a future enhancement could special-case `DeltaCDFRelation` for a cleaner location string. |

## Write

| Operation | Status | Evidence / next step |
|---|---|---|
| `.save(path)`, all save modes | ✅ Covered | `SaveIntoDataSourceCommand` — see "Delta Lake support" above. |
| `.saveAsTable(...)`, new table | ✅ **Covered — closed this pass** | Non-Delta: `CreateDataSourceTableAsSelectCommand`, translated (see "Translation coverage" above). Delta: `.format("delta").saveAsTable(...)` on a *new* table analyzes to the V2 `ReplaceTableAsSelect`, now a real `WriteCommandSupport` entry. `tableSpec.provider` gives format directly; location is the target's qualified catalog identifier (`"spark_catalog.default.<table>"`), since a not-yet-existing table has no physical path to resolve at analysis time. Verified via a PASS/FAIL pair in `ContractEnforcementRuleSpec`. |
| `.saveAsTable(...)`, existing table (append) | ✅ **Covered — closed this pass** | Analyzes to `AppendData`, now a real `WriteCommandSupport` entry. Location prefers the resolved `DataSourceV2Relation`'s `Table.properties()["location"]` (the physical warehouse path, confirmed empirically) over its qualified identifier. Verified via a PASS/FAIL pair. |
| `.insertInto(...)` | ✅ **Covered — closed this pass** | Same `AppendData` shape, same `WriteCommandSupport` entry, same test. |
| `.writeTo(...)` (DataFrameWriterV2), all sub-ops | ✅ **Covered — closed this pass** | `.append()` → `AppendData`; `.overwrite(cond)` → `OverwriteByExpression`, mapped to the contract's `saveMode: overwrite` uniformly (the delete predicate itself isn't modeled — `StructuralVerifier`'s save-mode check doesn't need it, so no IR extension was needed, contrary to what was first assumed); `.createOrReplace()` → `ReplaceTableAsSelect`, same entry as the new-table `.saveAsTable()` row above. All verified via the same PASS/FAIL pairs. |
| Format-specific DML (`MERGE INTO`/`UPDATE`/`DELETE`) | ✅ **Covered — structurally, deliberately not semantically** | All three (`MergeIntoCommand`/`UpdateCommand`/`DeleteCommand`) confirmed empirically to be Delta-internal classes, matched by reflection (public `target()`/`catalogTable()`/`source()` methods, no compile-time Delta dependency) and recognized as real `WriteCommandSupport` entries. **What this checks:** the operation's *target* against the contract's declared output location and current schema (catching the wrong-table mistake and schema drift) — MERGE's `source` is additionally checked as a contract input. **What this deliberately does not check, and cannot yet:** the actual row-level logic — the merge condition, which columns an `UPDATE` touches, whether a `DELETE` is unconditional. There is no contract vocabulary for that (see docs/CONTRACT_MODEL.md's `rules` field — recorded, not interpreted). Verified through real enforcement: PASS/FAIL pairs for all three, including a FAIL proving the target-schema check and a separate FAIL proving the source-as-input check (which needed a real fix along the way — see below). Full semantic verification (a real `ir.Merge`/`ir.RowMutation` IR node plus contract rules to check it against) is tracked as deliberate future work in ROADMAP.md's "Full semantic DML verification" item, not attempted here — see docs/ADDING_A_SPARK_CONNECTOR.md's "What 'fails closed' means" for why building the IR node without the rules to consume it would be premature. |
| Streaming write | ✅ **Covered — closed this pass** | Previously the most serious gap found: `WriteToStream` (the streaming write's top-level plan) isn't `Command`-shaped, so `ContractEnforcementRule`'s fail-closed policy — which only gates `Command`-shaped plans — never saw it at all. Confirmed empirically (not assumed): a probe found zero of the plans `injectCheckRule` saw during a real streaming Delta write were `Command`-shaped, and `javap` on Spark's catalyst jar confirmed `WriteToStream` doesn't implement `Command`. This was categorically worse than every other row here: not "fails closed but unverified," but genuinely unenforced — a streaming write committed silently, with no contract check at all. **Closed by adding `WriteToStream` as a real `WriteCommandSupport` entry** (see "Write command recognition: a single registry" below) rather than special-casing it in the fail-closed check: `WriteToStream.inputQuery` gives the schema being written; location comes from a resolved `catalogTable` (`.toTable(...)`, confirmed to carry `storage.locationUri`/`provider`), or from the sink's `name()` when that doesn't throw (a genuine V2 sink), or — since Delta's `DeltaSink` is a legacy V1 `Sink` wrapper whose `name()`/`schema()` unconditionally throw, confirmed empirically — a reflective call to its public `path()` accessor, the same reflection-over-a-class-this-module-has-no-compile-time-dependency-on technique `jdbcLocationOf` already uses for `JDBCRelation`. Verified through real enforcement: a PASS/FAIL pair for `.start(path)`, a PASS test for `.toTable(...)` (the `catalogTable`-populated path), and a test confirming a streaming write to a location unrelated to the active contract is correctly rejected (`OUTPUT_LOCATION_MISMATCH`) — the same behavior batch writes have always had, not special-cased for streaming. All in `ContractEnforcementRuleSpec`. |
| Maintenance operations that touch data (`OPTIMIZE`/`VACUUM`/`RESTORE`/`CLONE`/`CONVERT TO DELTA`) | ✅ Covered by policy classification | `FailClosedCommands.scala`'s `knownSafe` set already includes `VacuumTableCommand`/`OptimizeTableCommand` (rewrites/removes files, doesn't change a table's committed row content) and deliberately excludes `RestoreTableCommand`/`CloneTableCommand`/`ConvertToDeltaCommand` (row-content-changing) — built from a class-by-class enumeration of all 164 `Command` subclasses across Spark 3.5.1 + Delta 3.2.0, reasoned and documented in that file's header comment. Not re-probed individually this pass (the classification predates it); if any one of these is ever doubted, a targeted probe test is cheap to add. |

## A shared pitfall: atomic CTAS/RTAS issues a second, nested write

Closing the `ReplaceTableAsSelect`/`AppendData` rows above surfaced a real
correctness trap, worth documenting for whichever row a future connector
adds next: a *single* `.saveAsTable(...)` call on a brand-new table
produces **two** separate write-shaped plans through `injectCheckRule`,
confirmed empirically — the top-level `ReplaceTableAsSelect` the user's
code actually wrote, *and* an internal, nested `AppendData` against a
`StagedTable` (Spark's own public 2-phase-commit protocol for atomic
CTAS/RTAS — Delta's `StagedDeltaTableV2` implements it). Both are
genuinely visible to `ContractEnforcementRule.verifyOrThrow`, meaning it
runs *twice* for what's one logical write.

The trap: a `StagedTable`'s `Table.properties()` has no `"location"` yet
(the table doesn't physically exist until commit), so a naive
`AppendData` translation would fall back to the table's bare, unqualified
`name()` — a *different* string than `ReplaceTableAsSelect`'s qualified
catalog identifier for the exact same destination. Whichever one a
contract's declared location happened to match, the other invocation
would report `OUTPUT_LOCATION_MISMATCH` and abort an otherwise
contract-satisfying write — caught by a real test failure, not by
inspection. Fixed by having `WriteCommandSupport`'s `AppendData`/
`OverwriteByExpression` case fall back to `DataSourceV2Relation`'s own
`catalog`/`identifier` fields (confirmed populated even for a staged
table) and compute the *same* qualified-identifier string
`ReplaceTableAsSelect`'s case does, via one shared helper
(`qualifiedIdentifier`) — the two now agree by construction, not by
coincidence. Any future connector adding a `WriteCommandSupport` case
for a command that can appear nested inside another (anything using
Spark's staging-catalog protocol) should check for this same trap.

## A second shared pitfall: `plan.collect` doesn't reach a leaf command's own fields

Closing the row-level DML row surfaced a second, distinct trap, just as
worth documenting: `ContractEnforcementRule.verifyOrThrow`'s input-schema
collection (`plan.collect { case lr: LogicalRelation => ... }`) walks the
analyzed plan's `children` — which works for every other write shape here
because their `query`/`target` are genuine children in the tree. Delta's
row-level DML commands are not: `MergeIntoCommand`/`UpdateCommand`/
`DeleteCommand` are effectively leaf nodes in the tree-traversal sense —
`source`/`target` are ordinary case-class fields, never exposed via
`children` — so `plan.collect` on the command itself finds nothing inside
it, confirmed empirically by a real FAIL test never throwing (asserted
`intercept[ContractViolationException]`, got none) rather than assumed to
"just work" the way it does everywhere else. Fixed by having
`ContractEnforcementRule.verifyOrThrow` also walk `WriteCommandSupport`'s
already-extracted `query` field (MERGE's `source`) in addition to the raw
plan — a real, independently traversable `LogicalPlan` unlike the outer
command. Any future connector whose write command hides its "real" query
behind a similarly leaf-shaped node should check for this same trap.

## Delta feature-by-feature confidence pass

The row-level DML ledger row above says "✅ Covered," but covering the
*write-command shape* isn't the same as having tried every Delta table
*feature* against it — those are orthogonal axes (a MERGE is one write
shape; whether its target has schema evolution, generated columns,
deletion vectors, column mapping, liquid clustering, or CHECK constraints
enabled is a property of the target, not the command). Before this pass,
that gap was implicit: "expected to work, never actually tried." This
pass tried each one against a real Delta table, not assumed from
documentation — two were real, found-and-fixed false-rejection bugs; four
are confirmed transparent with a permanent regression test; one is
confirmed untestable in this environment, not silently skipped.

- **Schema evolution (`MERGE` + `spark.databricks.delta.schema.autoMerge.enabled`)
  — real bug, fixed.** `target.schema` at analysis time is the
  *pre-merge* schema — confirmed empirically to not yet include columns
  schema evolution is about to add. A contract requiring a field a
  schema-evolving MERGE would legitimately add was previously rejected
  with `MISSING_OUTPUT_FIELD` for a write that would have satisfied it.
  `deltaRowLevelDml` now checks `MergeIntoCommand.schemaEvolutionEnabled()`
  (public, confirmed via `javap`) and unions the source's new fields into
  `target.schema` as a best-effort approximation — not a full simulation
  of Delta's evolution rules (type widening, nested-struct merging,
  column reordering aren't modeled), with a diagnostic making the
  approximation visible. The same fix also had to distinguish "the
  source legitimately has a field the commit will never write" from "the
  source has an evolved field": confirmed empirically (not assumed) that
  with `autoMerge` disabled, `INSERT *` silently drops a source column
  the target doesn't have — the MERGE succeeds, the table's schema never
  gains it — so `schemaEvolutionEnabled()` being false must keep
  `outputSchema` at `target.schema` alone, not the source's schema.
  `ContractEnforcementRuleSpec` has PASS tests for both directions: one
  proving the evolving case is no longer falsely rejected, and one
  proving a non-evolving MERGE with a source-only extra column doesn't
  falsely *pass* a `rejectUndeclaredFields: true` contract (this second
  test exists specifically because it's the one that kills the mutant a
  naive `isMerge` check-only implementation would leave alive — see
  "Mutation testing" below).

- **Generated columns (`GENERATED ALWAYS AS (...)`) — real bug, fixed.**
  The same class of false-rejection: a generated column is computed by
  Delta at commit time, never supplied by the writer, so
  `AppendData`/`OverwriteByExpression`'s `outputSchema` (previously always
  `cmd.query.schema`) never included it. Confirmed empirically, the hard
  way, that this can't be detected from any DataFrame-facing schema at
  all — not `spark.read.format("delta").load(path).schema`, not
  `spark.table(name).schema`, not even the DSv2 `Table` handle's own
  `.schema()` (confirmed specifically for `DeltaTableV2.schema()`, the
  exact handle these two write shapes resolve their target to) carries
  `delta.generationExpression` metadata key Delta itself sets on a
  generated column's `StructField`. Only Delta's internal
  `Snapshot.schema()` does (reached via `DeltaTableV2.initialSnapshot()`,
  itself reached via the write's own `DataSourceV2Relation.table`).
  `outputSchemaWithGeneratedColumns`/`deltaGeneratedFields` read that
  reflectively (same no-compile-time-dependency, `Try`-wrapped convention
  as `deltaRowLevelDml`) and union the target's generated-only columns
  into `outputSchema` — checking the metadata key directly rather than
  reflecting into Delta's own `GeneratedColumn.isGeneratedColumn` helper,
  since `StructField.metadata` is already a plain public Spark type on
  this module's main classpath. Verified with a PASS test using the
  `io.delta.tables.DeltaTable` builder API (raw `CREATE TABLE ...
  GENERATED ALWAYS AS (...)` SQL DDL fails outright in this Spark 3.5.1 +
  Delta 3.2.0 environment with `[UNSUPPORTED_FEATURE.TABLE_OPERATION]`,
  confirmed empirically, even with explicit `TBLPROPERTIES` for
  reader/writer version — the builder API was the only way found to
  actually create one here) — a contract requiring the generated column
  is satisfied by an append that never supplies it.

  **Superseded**: `outputSchemaWithGeneratedColumns`/`deltaGeneratedFields`
  above were later replaced by a connector-agnostic, reflection-free
  mechanism once an Iceberg investigation found the same underlying
  situation under a different mechanism — see "Generalizing the
  generated-columns fix" in docs/connectors/iceberg.md. The finding
  stands (this is *why* the fix was needed); the implementation described
  here no longer exists in the code.

- **Deletion vectors, column mapping mode (`'name'`), liquid clustering
  (`CLUSTER BY`) — confirmed transparent, no fix needed.** Real writes
  and DML against tables with each of these enabled are recognized by
  `WriteCommandSupport` exactly as they would be without the feature —
  correct location, correct schema, no diagnostics. `ContractEnforcementRuleSpec`
  has a permanent PASS test for each, replacing what was previously only
  throwaway probe evidence.

- **CHECK constraints — confirmed orthogonal, no fix needed.** Delta
  enforces these itself, independently, at commit time — Invaract has no
  rule vocabulary for a row-level condition like `CHECK (id >= 0)` (see
  docs/CONTRACT_MODEL.md's `rules` field). A write violating a CHECK
  constraint is recognized by `WriteCommandSupport` identically to a
  satisfying one (no diagnostic — Invaract's structural checks simply
  don't apply here), and is then independently rejected by Delta's own
  `DeltaInvaractViolationException` before commit — confirmed with a
  permanent test asserting both halves: Invaract raises nothing, Delta
  does.

- **Identity columns (`GENERATED ALWAYS AS IDENTITY`) — confirmed
  untestable in this environment, not investigated further.** Spark
  3.5.1's own SQL parser rejects the syntax outright
  (`[PARSE_SYNTAX_ERROR] ... extra input 'IDENTITY'`), confirmed via a
  dedicated probe with no `try`/`catch` that could have masked a
  different failure. This is very likely a Databricks Runtime-only SQL
  extension not present in vanilla OSS Spark 3.5.1's grammar at all, not
  a Delta or Invaract limitation — there is nothing for
  `WriteCommandSupport` to translate because Spark itself never produces
  an analyzed plan to see. Left as ❓ **Not investigated** rather than
  claimed as covered.

**Net assessment:** Delta is not "100% supported" and no single pass makes
it so — but the gap is now fully enumerated instead of implicit, and every
row that isn't ✅ Covered states exactly what would close it, not just
that it's currently rejected — and today, every row *is* ✅ Covered.
Two rows that were genuine, unenforced holes
(streaming writes, and streaming reads as a declared input — the
difference between "fails closed" and "silently unchecked"/"falsely
rejected" is exactly the distinction this ledger exists to keep visible)
are now ✅ Covered, alongside every V2 catalog write shape
(`AppendData`/`OverwriteByExpression`/`ReplaceTableAsSelect`) and
row-level DML (`MERGE`/`UPDATE`/`DELETE`) — the last of these
*structurally* rather than semantically, deliberately: verifying the
actual merge condition/update columns/delete predicate needs both a real
IR extension and contract rules to check it against, neither of which
exist yet, and building the IR half alone would be speculative API
surface nothing could consume. That's real, scoped future work, tracked
explicitly in ROADMAP.md's "Full semantic DML verification" item, not a
silent gap.

