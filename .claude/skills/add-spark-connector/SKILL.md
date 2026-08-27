---
name: add-spark-connector
description: Guides adding full read/write support for a new Spark data connector (Iceberg, ClickHouse, Avro, JDBC-based sources, or any format/table library beyond what spark-adapter already handles) to Invariant's spark-adapter module. Use this whenever a contributor wants to add, extend, or investigate connector support in spark-adapter — including requests phrased as "support X format", "add a Y adapter", "read/write Z tables", or "does Invariant work with <connector>" where the honest answer requires checking. Also use it before claiming any connector has "full" or "comprehensive" coverage, since that claim is only true once every operation *and* every format-specific feature (schema evolution, generated columns, constraints, and the like) has an explicit disposition backed by a real permanent test, not assumed from documentation or left standing on a deleted probe's output. Always ends by producing two coverage ledgers — operation surface (per-shape ✅ covered / 🚫 fails closed / ❓ not investigated) and feature surface (per-behavior ✅ confirmed / 🔧 found and fixed / ❓ not investigated) — even for a narrowly-scoped ask — never lets a partial pass be silently mistaken for "done", which is exactly how the Delta Lake gaps happened, more than once. Do not hand-roll a one-off translatePlan case without this skill.
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

**This skill does not end without producing both coverage ledgers in
Phase 11 — operation surface and feature surface — full stop, no
exceptions, including for a narrowly-scoped invocation.** This isn't a
nice-to-have: Delta Lake's own onboarding shipped as "done" twice while
most of "The operation surface" checklist in
docs/ADDING_A_SPARK_CONNECTOR.md had never been touched, and even after
that ledger was complete, two real bugs (schema evolution, generated
columns) still shipped silently on rows already marked ✅ Covered, because
nothing forced testing the format's own features once its command shapes
were recognized — see Phase 8 and docs/ADDING_A_SPARK_CONNECTOR.md's "The
feature surface" for why that's a second, separate axis, not a detail of
the first. If you're invoked for a narrow question ("does this connector
need X", "let's just add reads"), say so explicitly at the start, and
still close with both full ledgers — every operation-surface row gets ✅
Covered / 🚫 Fails closed / ❓ Not investigated, every feature-surface row
gets ✅ Confirmed / 🔧 Found and fixed / ❓ Not investigated, **each with a
reason and a next step**, never silence. A user reading only your final
message must be able to tell, per operation and per feature, whether it
works, is safely rejected, or was never checked — not infer "probably
fine" from the parts you happened to mention.

**🚫 Fails closed is not a synonym for "not supported, and that's fine."**
It exists to catch operations Invariant hasn't translated *yet* — a
safety net, not a verdict. Every 🚫 row needs a next step the same way
every ❓ row does: either what real translation work would close it (the
default assumption — most 🚫 rows exist because a pass ran out of scope,
not because the operation doesn't deserve support), or, rarely, a
specific documented reason it should stay rejected forever. See
docs/ADDING_A_SPARK_CONNECTOR.md's "What 'fails closed' means (and
doesn't)" before writing any ledger row with this disposition.

**A note on prior context**: if this session already did some of this
connector's investigation earlier in the conversation, don't treat that
as license to skip straight to whatever's left. Start by building the
ledger for what's already been verified — cite the real tests/docs each
row's disposition points to — *then* work the remaining ❓ rows through
the phases below. The ledger has to be complete and accurate for the
whole operation surface, not just the delta since last time.

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

Exercise **every row of "The operation surface"** in
docs/ADDING_A_SPARK_CONNECTOR.md (read and write both — reads are just as
easy to under-scope as writes were the first time around), plus a
non-`AS SELECT` `CREATE TABLE`, `ANALYZE TABLE`, and `SHOW TABLES` for
later regression coverage. For a row that plainly doesn't apply to this
connector (no catalog, no streaming support), note that now — it still
needs a row in Phase 11's ledger, marked N/A with why, not skipped
silently. Record each operation's resulting plan class name(s) — you'll
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

Phase 2's probing and this reflective scan are both self-contained: each
starts from a known Spark/connector version and produces a bounded
finding (a list of plan shapes, a list of class names) that doesn't
depend on anything else in the session. Where that's true, consider
running the phase in a background agent instead of inline — the sbt
compile/test noise and reflection output stay out of the main
conversation, and only the finding comes back. Phase 4's classification
still needs to happen with full context and the user in the loop; don't
delegate that one.

## Phase 4 — Classify every class found

For each concrete `Command` class from Phase 2 + Phase 3, decide one of:

1. **Real write, translatable** → goes to Phase 6.
2. **Confirmed non-data-mutating** (DDL/catalog/session metadata, `SHOW`/
   `DESCRIBE`/`ANALYZE`/`CACHE`, storage maintenance) → goes to
   `FailClosedCommands`'s safe list in Phase 6, with the one-line "why
   this doesn't touch row content" reasoning every existing entry has.
3. **Genuinely data-mutating but unmodeled** (row-level DML, destructive
   `DROP`/`REPLACE`, connector-specific maintenance with real data
   effects) → leave off both. It fails closed automatically — a safety
   net for an unimplemented operation, not a verdict that it shouldn't be
   implemented. Note it for the "Known limitations" writeup in Phase 10
   *with a next step* (what translating it would take), not just "this
   fails closed."

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

Write translation, enforcement, and reporting used to each need their own
match statement, kept in lockstep by hand — that's what let a write shape
added for translation go missing from enforcement or reporting, twice.
That's fixed: all three now consult one registry
(`WriteCommandSupport.scala`), so a new write shape touches one file, not
three. See docs/SPARK_ADAPTER.md's "Write command recognition: a single
registry" for the full reasoning before editing this.

- `WriteCommandSupport.scala`: for each translatable write shape from
  Phase 4, add one more `PartialFunction[LogicalPlan, WriteCommandInfo]`
  following the existing three as templates
  (`InsertIntoHadoopFsRelationCommand` for `FileFormat` writes,
  `SaveIntoDataSourceCommand` for `CreatableRelationProvider`
  `.save(...)`, `CreateDataSourceTableAsSelectCommand` for new-table
  `.saveAsTable(...)`), and chain it into `combined` via `orElse`.
  `WriteCommandInfo` bundles location, the untranslated query, format,
  save mode, and output schema in one value — supplying all of them is
  what closes the exact bug that hit Delta the first time (a write
  translated correctly but its schema defaulting to the command node's
  own empty `.schema` elsewhere). Reuse `SparkPlanAdapter.formatOf`/
  `SparkPlanAdapter.locationOf` where the shape matches; extend them only
  if the connector's format/location can't be derived through
  `DataSourceRegister`/`HadoopFsRelation`/`catalogTable`.
  `WriteCommandSupport.scala` only grows over time (one `PartialFunction`
  per connector-specific write shape, chained via `orElse`) — grep for
  the existing case closest to the new shape rather than reading the
  whole file; a scoped Read around that anchor is enough context for the
  edit.
- **Nothing else needs a matching write-recognition change.**
  `SparkPlanAdapter.Translator.translatePlan`, `ContractEnforcementRule.verifyOrThrow`,
  and `SparkAdapterListener.onSuccess` all already consult
  `WriteCommandSupport.combined` — adding an entry there is enough for
  all three to pick it up automatically. Read-side translation (a
  connector introducing a new `LogicalRelation`-wrapped relation kind, or
  something that isn't `LogicalRelation` at all) is a separate change in
  `SparkPlanAdapter.scala`'s own read-handling case — not covered by this
  registry, since it's not a write-recognition problem.
- `FailClosedCommands.scala`: add Phase 4's safe-list entries, matched by
  **fully-qualified class name string** (`Set[String]`), not
  `classOf[...]`/`isInstanceOf` — the connector library isn't on the main
  compile classpath, so a hard reference would break compilation for
  users who don't have it. This is the same reason
  `SparkPlanAdapter.jdbcLocationOf`/`WriteCommandSupport`'s
  `unwrapWriteWrapper` use string matching.

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

## Phase 8 — Test the feature surface

This is a *different* axis from Phase 7, not a subset of it. Phase 7
proves every write/read *shape* is recognized; this phase proves
recognition stays correct once this format's own distinguishing
*behaviors* are actually exercised. Both of Delta's real bugs (schema
evolution silently using the pre-merge schema, a generated column never
appearing in any DataFrame-facing schema) existed on operation-surface
rows that were already ✅ Covered — Phase 7 passing does not imply this
phase would.

Read docs/ADDING_A_SPARK_CONNECTOR.md's "The feature surface
(format-specific behaviors)" section first — unlike Phase 2's fixed
operation-surface checklist, there is no connector-independent list here;
spend real time with this connector's own docs/changelog identifying what
actually makes it different from plain Parquet/CSV (schema-affecting
writer-doesn't-control-it behaviors, storage/representation mechanisms,
self-enforced constraints, versioning quirks — see that section for the
full category breakdown and why each matters).

For each feature found:

1. Build a throwaway probe (same technique as Phase 2 — `injectCheckRule`,
   disposable) against a real table with the feature turned on.
2. Read what `WriteCommandSupport`/`StructuralVerifier` actually report.
   "Should work based on the docs" is not evidence; the probe's real
   output is.
3. **Regardless of the answer** — a real bug (fix it, the same way the
   schema-evolution/generated-columns fixes went into
   `WriteCommandSupport.scala`), or already fine (no code change) —
   convert the exact scenario into a permanent `PASS`-style test in the
   connector's `*Spec.scala` file. "Already fine" is not a reason to skip
   this step; it's the common case, and it's exactly the case most likely
   to silently regress later with nothing to catch it.
4. Delete the probe only after its finding lives in a real test. A
   probe's output is memory of what was once true; a test in the suite is
   a standing check that it still is. Never let a probe be the cited
   evidence for a Phase 11 feature-surface ledger row.

## Phase 9 — Verify, don't assert

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

Each of these is a long-running background command — mutation testing and
the real-server-backed suites especially. Run them via a single
backgrounded shell invocation per step (e.g. one `nohup ... &` per
command above, or chain sequential sbt tasks like
`stryker && mimaReportBinaryIssues` into one invocation where the repo's
own tooling allows it) and wait for each with one long-running-aware
check — not a tight poll loop, and not a status update every time you
look. Narrate a result when a command actually finishes, not the wait
itself.

sbt test/stryker output and CI job logs are large and mostly noise. Pull
them through a filter (`grep -A5 -i 'error\|fail\|survived'`, or a
`tail`/`head_limit` on the read) before they land in context, rather than
reading the raw output and finding the failure by eye — this applies
whether the command ran locally or its log was fetched from CI.

## Phase 10 — Document

Two places, each stating **precisely** what is and isn't covered
(read/write/DML/streaming/maintenance — operation by operation, the way
`docs/connectors/delta.md` and `docs/SPARK_ADAPTER.md`'s "Fail-closed on
unverifiable writes" section do), never a blanket "full support" claim.
Include the feature-surface findings from Phase 8, not just the
operation-surface ones from Phase 7 — a fixed bug and a confirmed-
transparent feature are both worth stating explicitly, the way
`docs/connectors/delta.md`'s "Delta feature-by-feature confidence pass"
section does:

- A new `docs/connectors/<connector>.md` file (see the existing files
  there for the shape — findings, both coverage ledgers, and any
  bugs found/fixed), plus one row added to the index table in
  `docs/SPARK_ADAPTER.md`'s "Connector support" section. Don't write the
  full section inline into `docs/SPARK_ADAPTER.md` itself — that file is
  cross-cutting architecture content shared by every connector, and
  inlining kept it growing by a few hundred lines on every single pass
  until the split that introduced `docs/connectors/`.
- A `ROADMAP.md` sub-phase under Phase 1c — a short summary plus a link
  to the new `docs/connectors/<connector>.md` file, not a restatement of
  its content.

## Phase 11 — Coverage ledger and close-out

Two things, in order. Neither is optional, and neither can be skipped by
scope ("this was just about reads") — a narrow pass still produces both,
scoped honestly.

**1. Walk the "Definition of done" checklist** in
`docs/ADDING_A_SPARK_CONNECTOR.md` top to bottom. Every box needs
something concrete to point at (a test, a real command's output, a cited
mutation score) — not a restated assertion.

**2. Produce two coverage ledgers — operation surface (Phase 7) and
feature surface (Phase 8) — every row filled in, none silently omitted:**

Operation surface, one row per item in "The operation surface":

| Operation | Status | Evidence / reason + next step |
|---|---|---|
| `.load(path)` | ✅ / 🚫 / ❓ | ... |
| catalog table read | ✅ / 🚫 / ❓ | ... |
| ... | | |

- **✅ Covered** — cite the translation test and the PASS/FAIL
  enforcement pair.
- **🚫 Fails closed** — cite the fail-closed test proving rejection, and
  state the next step: what real translation work would close it (the
  default — a 🚫 row is future work, not a verdict), or, rarely, the
  specific reason it should never be translated. A 🚫 row with no next
  step is indistinguishable from "not supported, and that's fine" — see
  docs/ADDING_A_SPARK_CONNECTOR.md's "What 'fails closed' means".
- **❓ Not investigated** — state why (out of scope for this pass,
  genuinely deferred) and the next step (what a future pass needs to do
  — not just "TODO"). This is a legitimate, honest answer. An *absent*
  row is not — every row must appear.

Feature surface, one row per format-specific behavior found in Phase 8
(the list is connector-specific — there's no fixed template the way the
operation surface has one):

| Feature | Status | Evidence / reason + next step |
|---|---|---|
| e.g. schema evolution | ✅ / 🔧 / ❓ | ... |
| e.g. generated columns | ✅ / 🔧 / ❓ | ... |
| ... | | |

- **✅ Confirmed** — tested against a real table with the feature on, no
  fix needed; cite the permanent test (never a deleted probe's
  remembered output — see Phase 8).
- **🔧 Found and fixed** — a real bug existed; cite the fix and the test
  proving it no longer reproduces.
- **❓ Not investigated / not testable** — state why (out of scope, or
  genuinely can't be exercised in this environment, like Delta's identity
  columns) and the next step.

Post both tables to the user as the closing message, however narrow the
pass was — a session that only touched two rows of either still renders
all of them, with the untouched ones marked ❓. Then write them into the
same two documentation sites Phase 10 already touched
(docs/connectors/<connector>.md/ROADMAP.md), so both ledgers are durable
and the next session (or the next person) doesn't have to
reconstruct them from conversation history.

⏸ **Checkpoint**: walk both completed ledgers with the user before
calling any part of the connector done. "Are we at 100%?" must always be
answerable directly from these tables, never from a general impression of
how much work happened.
