---
name: add-spark-connector
description: Guides adding full read/write support for a new Spark data connector (Iceberg, ClickHouse, Avro, JDBC-based sources, or any format/table library beyond what spark-adapter already handles) to Invariant's spark-adapter module. Use this whenever a contributor wants to add, extend, or investigate connector support in spark-adapter — including requests phrased as "support X format", "add a Y adapter", "read/write Z tables", or "does Invariant work with <connector>" where the honest answer requires checking. Also use it before claiming any connector has "full" or "comprehensive" coverage, since that claim is only true once every step in this workflow has actually been run, not assumed. Do not hand-roll a one-off translatePlan case without this skill — the fail-closed policy and FailClosedCommands' safety depend on the full survey this skill runs, and skipping it is exactly how the Delta Lake gaps happened (twice).
---

# Adding a Spark Connector

Source of truth for *why* each step matters and the exact reasoning behind
every rule below: `docs/ADDING_A_SPARK_CONNECTOR.md`. Read it in full
before starting — this file is the runnable checklist version of that
doc, not a replacement for it. When a step below references a doc
section, open it; don't guess at the reasoning from the step name alone.

This is a multi-hour, architecturally significant task. Work through the
phases in order and **pause for explicit user confirmation at every
checkpoint marked ⏸** — especially Phase 5, where a wrong "this command
is safe" call silently defeats the entire fail-closed feature
(`spark-adapter/src/main/scala/com/example/sparkadapter/FailClosedCommands.scala`).
Getting Phase 5 wrong doesn't fail loudly; it fails invisibly, months
later, on someone else's data. Slow down there specifically.

## Phase 0 — Scope the connector

Ask (or research, if the user already named the connector and library):

- Which library/version, and which Spark provider interfaces does it
  implement — `CreatableRelationProvider`/`RelationProvider` (the
  `SaveIntoDataSourceCommand` family, usually needs zero connector code),
  `FileFormat` (the `InsertIntoHadoopFsRelationCommand` family), or a
  DataSourceV2 `TableProvider`/catalog (a different, currently-unhandled
  plan family per docs/ADDING_A_SPARK_CONNECTOR.md's "Known limitations")?
- Does it register a catalog (so `.saveAsTable`/`spark.table(...)`/SQL DDL
  apply to it)?
- Does it support row-level DML (`MERGE`/`UPDATE`/`DELETE`)? Streaming?
- Any known compatibility issues with this repo's pinned Scala 2.12 /
  Spark 3.5.1 (check the library's own issue tracker — this is how
  Delta's 3.2.1 bug was caught before it caused a problem)?

⏸ **Checkpoint**: confirm the connector, library coordinate, and pinned
version with the user before touching any code. If the answers above
suggest this connector is DataSourceV2-catalog-based, say so explicitly —
that's the one case docs/ADDING_A_SPARK_CONNECTOR.md's "Known
limitations" flags as unsolved even for Delta, so scope expectations
accordingly rather than silently taking on that extra research.

## Phase 1 — Add the dependency, test-scope only

In `spark-adapter/build.sbt`, add the connector as `% "test"` — never
`compile`/`provided` at this stage. See docs/ADDING_A_SPARK_CONNECTOR.md
"1. Add the dependency as `% "test"` only" for why this is almost always
sufficient (most connectors need zero connector-specific compiled code)
and when a compile dependency would actually be justified (only after
Phase 3/4 prove a plan shape genuinely needs a connector-defined type,
and even then prefer string-matching by class name — see Phase 4).

Run `sbt compile` in `spark-adapter/` to confirm the module still builds
with nothing else changed.

## Phase 2 — Probe real plan shapes

Build a throwaway test (delete it before finishing — it's investigation
scaffolding, not part of the deliverable) that spins up a real
connector-enabled `SparkSession` and observes plans through
**`injectCheckRule`**, not `QueryExecutionListener`. Read
docs/ADDING_A_SPARK_CONNECTOR.md's "2. Probe with *both* Spark extension
points, not one" section first — the two see genuinely different things,
and `ContractEnforcementRule` only cares about what `injectCheckRule`
sees.

Exercise every operation listed in that doc's Phase 2 section: a plain
read, `.save(...)` (all four save modes if the shape might differ),
`.saveAsTable(...)` against both a new and an existing table,
`.insertInto(...)`, `.writeTo(...)` (DataFrameWriterV2), any
format-specific DML, streaming if supported, plus a non-`AS SELECT`
`CREATE TABLE`, `ANALYZE TABLE`, and `SHOW TABLES` for later regression
coverage. Record each operation's resulting plan class name(s) — you'll
need this list for Phase 4.

## Phase 3 — Reflectively survey the connector's Command classes

Don't rely on Phase 2 alone. Write a throwaway scan (see
docs/ADDING_A_SPARK_CONNECTOR.md's "3. Reflectively survey the
connector's `Command` classes" for the exact technique — `JarFile` +
`Class.forName` + `isAssignableFrom` against
`org.apache.spark.sql.catalyst.plans.logical.Command`) over the
connector's jar. This is what caught `CreateDataSourceTableAsSelectCommand`
and Delta's `MergeIntoCommand` — neither showed up from just trying the
obvious operations in Phase 2. Expect no structural shortcut: a
data-writing command and a metadata-only one can implement the identical
Spark trait, so don't try to filter programmatically — every class needs
a human read of its actual SQL semantics in Phase 5.

Delete the scan test once you have the class list; it's investigation
scaffolding.

## Phase 4 — Classify every class found

For each concrete `Command` class from Phase 2 + Phase 3, decide one of:

1. **Real write, translatable** → goes to Phase 6.
2. **Confirmed non-data-mutating** (DDL/catalog/session metadata, `SHOW`/
   `DESCRIBE`/`ANALYZE`/`CACHE`, storage maintenance) → goes to
   `FailClosedCommands`'s safe list in Phase 6, with the one-line "why
   this doesn't touch row content" reasoning every existing entry has.
3. **Genuinely data-mutating but unmodeled** (row-level DML, destructive
   `DROP`/`REPLACE`, connector-specific maintenance with real data
   effects) → leave off both. It fails closed automatically; note it for
   the "Known limitations" writeup in Phase 9.

If a connector class's semantics are genuinely unclear from its name/docs
and you can't find primary-source confirmation, treat it as case 3, not
case 2 — see docs/ADDING_A_SPARK_CONNECTOR.md's asymmetry argument (a
missing safe-list entry costs one rejection; a wrongly-added one silently
defeats the whole feature).

⏸ **Checkpoint**: present the full classification table (class name →
category → one-line reasoning) to the user before writing any code. This
is the highest-stakes review point in the whole workflow — get sign-off
here, not after `FailClosedCommands` is already merged.

## Phase 5 — Confirm the plan before implementing

Summarize for the user: which write shapes will be translated (and
therefore fully verified against a contract), which commands join the
safe list (and therefore stay silent no-ops), and which known
data-mutating operations will deliberately fail closed. Get explicit
agreement this matches what "supporting this connector" should mean for
this project before writing implementation code — scope surprises are
much cheaper to catch here than after tests are written against the wrong
shape.

⏸ **Checkpoint**: explicit go-ahead from the user before Phase 6.

## Phase 6 — Implement

- `SparkPlanAdapter.scala`: add a `translatePlan` case per translatable
  write/read shape from Phase 4, following the existing three write cases
  as templates (`InsertIntoHadoopFsRelationCommand` for `FileFormat`
  writes, `SaveIntoDataSourceCommand` for `CreatableRelationProvider`
  `.save(...)`, `CreateDataSourceTableAsSelectCommand` for new-table
  `.saveAsTable(...)`). Reuse `formatOf`/`locationOf` where the shape
  matches; extend them only if the connector's format/location can't be
  derived through `DataSourceRegister`/`HadoopFsRelation`/`catalogTable`.
- `ContractEnforcementRule.scala`: extend the `outputSchema` derivation
  match for any new write `Command` type — this is the exact bug that hit
  Delta the first time (defaulting to the command node's own empty
  `.schema` instead of the query's).
- `SparkAdapterListener.scala`: extend the write-detection match the same
  way, for `demo/output/report.json` reporting.
- `FailClosedCommands.scala`: add Phase 4's safe-list entries, matched by
  **fully-qualified class name string** (`Set[String]`), not
  `classOf[...]`/`isInstanceOf` — the connector library isn't on the main
  compile classpath, so a hard reference would break compilation for
  users who don't have it. This is the same reason `jdbcLocationOf`/
  `unwrapWriteWrapper` in `SparkPlanAdapter.scala` use string matching.

## Phase 7 — Test

Against a real connector-enabled `SparkSession` — no mocking (see
ARCHITECTURE.md ADR-005):

- A translation test per translated read/write shape.
- A PASS/FAIL enforcement pair per translated write shape (mirror the
  existing Parquet/Delta/`.saveAsTable()` pairs in
  `ContractEnforcementRuleSpec`).
- A fail-closed test for at least one real Phase 4 case-3 operation,
  asserting the target data is unchanged before/after the rejected
  attempt (see the Delta `MERGE INTO` test for the pattern).
- A regression test proving the connector's own case-2 (safe-list) DDL
  isn't blocked under a contract that would reject anything it actually
  checks.

## Phase 8 — Verify, don't assert

- `sbt stryker --mutate "..."` scoped to changed/added files — must clear
  70% (CLAUDE.md's "Mutation Testing Requirement"). Investigate every
  real survivor; don't just cite the percentage.
- `sbt mimaReportBinaryIssues` — must be clean.
- Confirm zero added dependency for non-users of this connector by
  running `./dev/build` once with the new dependency present and
  inspecting the assembled `spark-adapter` jar (`unzip -l`) for connector
  classes — there should be none, the same way Delta's jar was verified
  unchanged in size and contents.
- `./dev/build`, `./dev/test`, `./dev/regression` — all three, against
  real `spark-submit` (per CLAUDE.md's "Critical Requirement"), not just
  `sbt test`.

## Phase 9 — Document

Three places, each stating **precisely** what is and isn't covered
(read/write/DML/streaming/maintenance — operation by operation, the way
`docs/SPARK_ADAPTER.md`'s "Delta Lake support" and "Fail-closed on
unverifiable writes" sections do), never a blanket "full support" claim:

- A new "`<Connector>` support" section in `docs/SPARK_ADAPTER.md`.
- A `ROADMAP.md` sub-phase under Phase 1c.
- A `CHANGELOG.md` entry under `[Unreleased]`.

## Phase 10 — Final review

Walk the "Definition of done" checklist in
`docs/ADDING_A_SPARK_CONNECTOR.md` top to bottom. Every box needs
something concrete to point at (a test, a real command's output, a cited
mutation score) — not a restated assertion. If any box can't be checked,
say so in "Known limitations" rather than letting the PR imply otherwise.

⏸ **Checkpoint**: walk the completed checklist with the user before
calling the connector done.
