# Adding a Spark Connector

This is the reusable process for giving `spark-adapter` full read/write
support for a data source it doesn't yet understand — Delta Lake today
(see docs/connectors/delta.md and docs/SPARK_ADAPTER.md's "Fail-closed on
unverifiable writes" section), Iceberg, ClickHouse, Avro, or anything
else a future contributor wants to add. Per-connector write-ups and
coverage ledgers live under docs/connectors/ — see
docs/SPARK_ADAPTER.md's "Connector support" section for the index.

It exists because Delta support was built twice: once for `.save(...)`
writes, then again — separately — for `.saveAsTable(...)` and the
fail-closed policy, because the first pass didn't survey the connector's
full operation surface before calling it done. Both gaps were real: a
Delta write silently passing a broken contract, and `.saveAsTable(...)`
falling through to the same silent no-op. This document is how the next
connector gets that surveyed up front instead of discovered incident by
incident.

The same investigation also surfaced *why* those gaps kept recurring:
translation, enforcement, and reporting each recognized write commands
with their own independent match statement, so a write shape added to
one could silently go missing from another. That's since been fixed —
see docs/SPARK_ADAPTER.md's "Write command recognition: a single
registry" — so adding a write shape now touches one file
(`WriteCommandSupport.scala`), not three. Step 4 below reflects this.

There is also a Claude Code skill (`add-spark-connector`) that walks
through this process interactively — see its `SKILL.md` for the
step-by-step version of what's described here.

### What "fails closed" means (and doesn't)

Easy to misread the coverage ledger below as: ✅ = done, 🚫 = also done
(just a different flavor — "not supported, and that's fine"), ❓ =
the only real gap. **That reading is wrong, and defeats the point of the
policy.** Fail-closed exists to catch operations Invaract *hasn't gotten
around to translating yet* — a deliberate safety net so an unrecognized
write aborts loudly instead of silently passing an unverified contract.
It was never meant to be a resting state equivalent to "we've decided not
to support this." A 🚫 row is *always* implicitly future work: either
real translation is worth adding later (the common case — most 🚫 rows
exist because this pass ran out of scope, not because the operation is
somehow unworthy of support), or there's a specific, rare, documented
reason it should stay rejected forever (an operation whose semantics are
genuinely ambiguous, or arbitrary passthrough to an external system that
can't be classified either way). Silence on which of those two a 🚫 row
is is exactly the gap this section exists to close — see "The coverage
ledger" below, which requires a next step for 🚫 the same way it already
does for ❓.

## The operation surface (canonical checklist)

Every investigation, and the coverage ledger below, is scoped against
this exact list — not "whatever operations came up." An item that
doesn't apply to a given connector (e.g. no catalog, no streaming) is
still listed and marked N/A with why, not silently dropped.

**Read:**
1. `.read.format(x).load(path)` — direct path read
2. Catalog table reference (`spark.table(...)`/`SELECT * FROM t`) — if
   the connector registers a catalog
3. Time travel / snapshot reads (`VERSION AS OF`/`TIMESTAMP AS OF` or
   equivalent) — if the format versions data
4. Streaming read (`readStream`)
5. Change-data-feed / incremental read — if the format supports one

**Write:**
1. `.save(path)` — each save mode (`append`/`overwrite`/`ignore`/`error`)
   that produces a different plan shape
2. `.saveAsTable(...)` — a *new* table
3. `.saveAsTable(...)` — an *existing* table (append)
4. `.insertInto(...)` — an existing table
5. `.writeTo(...)` (DataFrameWriterV2) — `.append()`/`.overwrite(cond)`/
   `.overwritePartitions()`/`.create()`/`.createOrReplace()`/`.replace()`
6. Format-specific DML (`MERGE`/`UPDATE`/`DELETE`/`UPSERT`)
7. Streaming write (`writeStream`)
8. Maintenance operations that touch data (compaction, vacuum, restore,
   clone, format conversion, or equivalent)

## The feature surface (format-specific behaviors)

The operation surface above is about plan *shapes* — is a `.saveAsTable()`
recognized, does a `MERGE` get matched at all. It says nothing about
whether a recognized write shape stays correct once the *format's own*
distinguishing features are in play. Those are a second, orthogonal axis,
and it's the one that produced two real, found-and-fixed bugs during
Delta's own "the operation surface is fully covered" pass: `target.schema`
silently being the pre-merge schema under schema evolution, and a
generated column silently never appearing in any DataFrame-facing schema
at all. Both bugs existed on operation-surface rows that were already
marked ✅ Covered — closing every row of "The operation surface" is
necessary, not sufficient.

Unlike the operation surface, there is no fixed, connector-independent
list here — each format has its own distinguishing behaviors. Spend real
time (the connector's own docs, changelog, and "what makes this format
different from plain Parquet/CSV" framing) enumerating this connector's
actual list before writing any code, the same deliberateness Phase 0
already asks for scoping the operation surface. Categories worth checking
for, not a checklist to fill in blindly — most formats have some subset:

- **Schema-affecting behaviors the writer doesn't fully control**: schema
  evolution/merge-on-write, computed/generated columns, identity/auto-
  increment columns, default values applied server-side. Anything where
  the schema Invaract can read *before* a write differs from the schema
  the write actually commits is a candidate for exactly the false-
  rejection (or worse, false-pass) bug class Delta's schema evolution and
  generated-columns fixes both were.
- **Storage/representation mechanisms that shouldn't change verification
  behavior, but might**: deletion vectors or other soft-delete
  representations, column mapping / physical-vs-logical name
  indirection, clustering or partitioning strategies, compaction/
  optimization operations. The bar here is usually "confirm this doesn't
  break anything," not "implement new handling" — but "usually" isn't
  "always," and it isn't proven without a real write against a real table
  with the feature turned on.
- **Constraints or invariants the format enforces itself**: `CHECK`
  constraints, `NOT NULL`, foreign-key-like relationships, uniqueness. The
  question to answer for each is specifically the boundary: does
  Invaract's own structural verification interact with the format's
  enforcement at all (double-check, silently duplicate, silently
  conflict), or are they genuinely orthogonal — confirm with a test that
  exercises *both* a satisfying and a violating write, the way Delta's
  `CHECK`-constraint test does (Invaract passes both; the format itself
  then rejects the violating one).
- **Versioning/time-travel semantics**, if not already covered by "The
  operation surface"'s time-travel read row.

For each item found: **write a real test against a real
connector-enabled table with the feature turned on, and check what
Invaract actually reports** — never reason from the format's
documentation about what "should" happen. A throwaway probe is the right
first move (same as the operation-surface investigation), but **a probe
is not the deliverable and never stands in as the evidence for a coverage
ledger row.** Every scenario a probe touches — including ones where the
probe's answer was "this is already fine, no fix needed" — gets promoted
into a permanent test in the connector's `*Spec.scala` file before the
probe is deleted. "We tried it once and it worked" is not the same
confidence level as "there is a test in the suite that will fail if this
regresses," and this document exists specifically because "should work,
never actually verified" was allowed to pass as done twice already.

## Definition of done

A connector is **not** done because compilation succeeds or because one
`.save(...)` call round-trips. It's done when every item below is true —
each with something to point at, not just an assertion — **and** the
coverage ledger this section ends with is complete.

- [ ] **Every read path the connector supports is investigated**, not
      assumed: `.read.format(x).load(path)`, and a catalog table
      reference (`spark.table(...)`/`SELECT * FROM t`) if the connector
      registers a catalog. For each one found to matter, `locationOf`
      and `translatePlan`'s `LogicalRelation`/relation-specific handling
      either translates it precisely, or there's a documented reason it
      falls back to the generic `catalogTable`/`toString` fallback.
- [ ] **Every write path is investigated**: `.save(path)` (all four save
      modes if they change the plan shape), `.saveAsTable(...)` against
      both a *new* and an *existing* table, `.insertInto(...)`,
      DataFrameWriterV2 (`.writeTo(...)`), and any format-specific DML
      (`MERGE`/`UPDATE`/`DELETE`/`UPSERT`) or streaming write the format
      supports. For each, one of two things is true and documented:
      it's translated to `ir.Write` and verified, or it's deliberately
      left untranslated with a stated reason (most often: it doesn't fit
      `ir.Write`'s "write a dataset to a location" shape — see
      "Known limitations" below).
- [ ] **A real reflective survey of the connector's own `Command`
      classes was performed**, not skipped because "we tried the obvious
      operations." This is what caught `CreateDataSourceTableAsSelectCommand`
      and Delta's `MergeIntoCommand` — neither was the first thing anyone
      tried. See "The investigation methodology" below for the exact
      technique.
- [ ] **Every reachable `Command`-shaped plan is classified**: translated
      write, added to `FailClosedCommands`'s known-safe list (with the
      same "does it change a table's committed row content?" reasoning
      every existing entry has), or deliberately left off both — which
      means it fails closed, not silently passes.
- [ ] **Zero added runtime or compile-time dependency** for a user who
      doesn't use this connector. The new connector's library is a
      `% "test"` dependency only, unless a real investigation shows the
      plan shapes genuinely require a type only that library defines
      (uncommon — `SaveIntoDataSourceCommand`/`DataSourceRegister` covers
      most "arbitrary external format" cases with zero connector-specific
      code at all; see "Delta Lake support" for why).
- [ ] **A translation test and a PASS/FAIL enforcement pair exist for
      every write shape actually translated**, against a real
      connector-enabled `SparkSession` — no mocking (see
      ARCHITECTURE.md's ADR-005).
- [ ] **A fail-closed test exists for at least one real, concrete
      operation the connector supports that Invaract deliberately
      doesn't translate** (if any exist), proving it's rejected — and
      that nothing was written — rather than silently passed.
- [ ] **A regression test proves the connector's own non-data
      administrative commands aren't blocked** by the fail-closed policy,
      under a contract that would reject anything it actually checked.
- [ ] **Every item on "The feature surface" this connector actually has
      was tested against a real table with the feature enabled, not
      assumed from documentation, and every finding — including "this is
      already fine" — is a permanent test in the suite, not just a
      throwaway probe's output.** A feature found to genuinely need a fix
      (the schema-affecting-behavior category is the likely source, per
      Delta's schema-evolution and generated-columns bugs) has both the
      fix and a test proving it; a feature confirmed transparent has a
      test proving that too, so a future regression doesn't have to be
      rediscovered from scratch. See "The feature surface (format-specific
      behaviors)" above.
- [ ] Mutation testing scoped to the changed/added files clears 70% (see
      CLAUDE.md's "Mutation Testing Requirement"); `mimaReportBinaryIssues`
      is clean.
- [ ] `./dev/build`, `./dev/test`, and `./dev/regression` all pass against
      real `spark-submit` (see CLAUDE.md's "Critical Requirement") —
      including once with the new connector's dependency present and once
      without, to prove the "zero added dependency for non-users" claim
      isn't just asserted.
- [ ] A `docs/connectors/<connector>.md` file exists (mirroring
      `docs/connectors/delta.md`), plus a ROADMAP.md sub-phase — each
      stating plainly what *is* and *isn't* covered, the same way this
      document's own retrospective does for Delta.

If any box can't be checked, the connector isn't done — it's a partial
read or partial write shape, and the honest thing to do is say so in
"Known limitations," the same way Delta's row-level DML and streaming
writes are called out today rather than implied to work.

### The coverage ledger — mandatory, not optional

This is the rule this document exists to enforce, added after Delta's
own onboarding was declared "done" twice while a majority of "The
operation surface" list above had never been touched: **no pass through
this process — full or partial, a brand-new connector or one line item
on an existing one — ends without producing a complete ledger against
every item in "The operation surface."** Every single item gets exactly
one of these three dispositions, and every disposition needs something
concrete to point at:

- **✅ Covered** — translated and verified. Cite the translation test and
  the PASS/FAIL enforcement pair.
- **🚫 Fails closed** — not yet translated, verified to abort rather than
  silently pass. Cite the fail-closed test, **and** state the next step:
  either what real translation work would close it (the default
  assumption — see "What 'fails closed' means" above), or, in the rare
  case it should never be translated, the specific reason why. A 🚫 row
  with no next step reads as "not supported, and that's fine" — which is
  precisely the framing this section exists to rule out.
- **❓ Not investigated** — state *why* (out of scope for this pass, ran
  out of the checkpoint's approved scope, genuinely deferred) and the
  *next step* (what a future pass needs to do, not just "TODO").

**"Not investigated" is an allowed answer. A missing row is not.** A
narrowly-scoped invocation — "just check whether reads have the write
side's bug," say — is completely legitimate, but it still has to open by
stating which rows of the operation surface it intends to touch, and it
still has to close with the full ledger, marking everything outside its
scope ❓ with "out of scope for this pass" rather than leaving it
unmentioned. The failure this section exists to prevent isn't scoping a
pass narrowly — it's a narrow pass's silence being later read as "this
connector is done." Silence is exactly what let Delta's write support
ship as if `.saveAsTable()` didn't exist, and later let "let's support
Delta reads" ship as if `.insertInto()`/`.writeTo()`/streaming/time
travel didn't exist either — both times because nothing forced an
explicit accounting of what the pass *didn't* cover.

The same requirement applies to "The feature surface" above, with the
same three dispositions adapted to what a feature check actually
produces: **✅ Confirmed** (tested against a real table with the feature
on, permanent test exists — whether or not a fix was needed), **🔧 Found
and fixed** (a real bug was found; cite the fix and the test proving it),
or **❓ Not investigated / not testable** (state why — out of scope, or,
like Delta's identity columns, genuinely can't be exercised in this
environment — and the next step). A feature surface with rows that only
cite a deleted probe's remembered output, rather than a live test in the
suite, does not satisfy this — see "The feature surface" above for why a
probe alone was ruled out as sufficient evidence.

A "done" connector's docs/connectors/<connector>.md file and ROADMAP.md
sub-phase (below) are exactly these two ledgers, formatted for their
audience — never a blanket "full support" sentence standing in for
either.

## The investigation methodology

This is the part that's easy to shortcut and expensive to shortcut. Every
step below was learned by getting it wrong first during the Delta work —
follow them in order.

### 1. Add the dependency as `% "test"` only

Never `compile`/`provided` on the first attempt. Most connectors that
implement Spark's public provider interfaces
(`CreatableRelationProvider`, `RelationProvider`, `DataSourceRegister`)
need **zero** connector-specific code to translate — the plan node Spark
produces (`SaveIntoDataSourceCommand`, `LogicalRelation`) is already a
plain `org.apache.spark.sql` class. Only reach for a compile-time
dependency after a real investigation (steps 2–4 below) shows a plan
shape that genuinely can't be represented without a type the connector's
own library defines — and even then, prefer matching by
`getClass.getName` string (see `SparkPlanAdapter.jdbcLocationOf`/
`unwrapWriteWrapper`, `FailClosedCommands`) over a hard import, so a user
who doesn't have that library on their runtime classpath never hits a
`ClassNotFoundException`.

Pin the version deliberately, not to "latest" — check the connector's own
issue tracker for known bugs against this repo's pinned Scala/Spark
combination first (Delta 3.2.1 had exactly this kind of bug against Scala
2.12 + Spark 3.5.1 — see docs/SPARK_ADAPTER.md's citation).

### 2. Probe with *both* Spark extension points, not one

This module has two different ways of observing a plan, and **they see
different things**:

- `QueryExecutionListener.onSuccess` — execution-level, fires only after
  a query's action (`.collect()`, `.save()`, ...) has already run.
- `SparkSessionExtensions.injectCheckRule` — analysis-level, fires
  earlier, on *every* analyzed plan a `Dataset` construction produces,
  including ones a caller never acts on.

`ContractEnforcementRule` uses the second one. A probe built only around
the first can make a plan look absent when the check rule would actually
see it (confirmed directly this way: a bare `CREATE TABLE ... (no data)`
never fired `QueryExecutionListener.onSuccess` in testing, but did reach
the check rule). Build a throwaway probe test using `injectCheckRule` (a
function that just logs the plan's runtime class instead of throwing) —
that's the mechanism whose output actually matters for the fail-closed
policy.

Exercise, at minimum, everything in "Every write path is investigated"
above, plus: a plain read, a non-`AS SELECT` `CREATE TABLE`, `ANALYZE
TABLE`, and `SHOW TABLES` — the last three are what prove the fail-closed
policy won't have false positives once step 4 classifies them.

### 3. Reflectively survey the connector's `Command` classes

Don't rely on what step 2's probe happened to trigger — enumerate what
*exists*. Open every relevant jar (the connector's own, plus
`spark-sql`/`spark-catalyst` if the connector adds SQL commands that
route through Spark's own command types) with `java.util.jar.JarFile`,
`Class.forName` each `.class` entry, and keep the ones where
`classOf[org.apache.spark.sql.catalyst.plans.logical.Command].isAssignableFrom(clazz)`
and `!clazz.isInterface`. This is exactly the technique that found
Delta's `MergeIntoCommand` and `CreateDataSourceTableAsSelectCommand` —
neither showed up by just trying the operations someone thought to try.

Expect Spark's own `Command` hierarchy to give **no reliable signal**
about which of these write data — `SaveIntoDataSourceCommand` (writes
data) and `CreateDataSourceTableCommand` (schema-only `CREATE TABLE`, no
data) implement the exact same `LeafRunnableCommand` trait. Don't try to
find a structural shortcut here; there isn't one. Classification (step 4)
has to be done by reading each class's documented SQL semantics.

### 4. Classify every class the survey found

For each concrete `Command` class found:

- **Translates to a real write this connector needs** → implement one
  more `PartialFunction[LogicalPlan, WriteCommandInfo]` in
  `WriteCommandSupport.scala`, following the existing three
  (`InsertIntoHadoopFsRelationCommand` for `FileFormat`-based writes,
  `SaveIntoDataSourceCommand` for `CreatableRelationProvider`-based
  `.save(...)`, `CreateDataSourceTableAsSelectCommand` for new-table
  `.saveAsTable(...)`) as templates, and chain it into `combined`. That
  one registry is what `SparkPlanAdapter`, `ContractEnforcementRule`, and
  `SparkAdapterListener` all consult — nothing else needs a matching
  change (see docs/SPARK_ADAPTER.md's "Write command recognition: a
  single registry"). Most connectors need zero entries here at all: the
  point of the Delta investigation was that `SaveIntoDataSourceCommand`
  already covers any `CreatableRelationProvider`-based `.save(...)`,
  connector-specific or not — only add one when a connector genuinely
  introduces a write-command *shape* Spark doesn't already have a generic
  node for.
- **Confirmed not to change a table's committed row content** (schema/
  namespace/function DDL, `SHOW`/`DESCRIBE`/`ANALYZE`/`CACHE`, session
  config, storage maintenance like compaction) → add its fully-qualified
  class name to `FailClosedCommands`'s safe list, with the same one-line
  reasoning style every existing entry has.
- **Genuinely changes data, but doesn't fit `ir.Write`'s shape, or this
  pass doesn't have scope to translate it now** (row-level `MERGE`/
  `UPDATE`/`DELETE`, `LOAD DATA`, `TRUNCATE`, catalog-destructive `DROP`/
  `REPLACE`, connector maintenance ops with real data-mutating semantics)
  → leave it off both the translation code and the safe list. It fails
  closed automatically — but that's a safety net catching an
  *unimplemented* operation, not a verdict that it doesn't deserve
  support (see "What 'fails closed' means" above). Document it as a known
  limitation *with a next step* (what translating it would take), not
  just a bare mention that it's rejected.

When genuinely uncertain whether a class mutates data, leave it off the
safe list. `FailClosedCommands`'s own doc comment explains why this
asymmetry is deliberate: a safe command missing from the list costs one
loud, cheap-to-fix rejection; a data-mutating command wrongly added would
silently defeat the entire feature.

### 5. Verify, don't assert

Every claim in the "Definition of done" checklist needs the same kind of
evidence the Delta work produced: a real test against a real
connector-enabled session (never a mock — see ARCHITECTURE.md's ADR-005),
a real `unzip -l`/jar inspection to confirm the "zero added dependency"
claim, a real mutation-testing run with the actual score cited (not
"should be fine"), and a real `./dev/test`/`./dev/regression` run through
real `spark-submit`. If something wasn't actually checked — the read
side, streaming, a specific DML operation — say so explicitly rather than
letting a reader assume "full support" covers it. "Do we now have 100%
coverage?" should always be answerable precisely, operation by operation,
not with a yes/no guess.

### 6. Test the feature surface, not just the operation surface

Steps 1–5 prove every write/read *shape* is recognized. That leaves the
question "The feature surface" above exists for: does recognition stay
correct once this format's own distinguishing behaviors are actually
exercised? Enumerate them (see "The feature surface" for the categories
to check), then for each one, build a throwaway probe exactly like step
2's — same technique, same disposability — against a real table with the
feature turned on, and read what `WriteCommandSupport`/`StructuralVerifier`
actually report.

The one place this step differs from steps 1–5: **the probe's job ends
the moment you have an answer, not when the code is confirmed correct.**
"This is already fine, no fix needed" is a legitimate, common outcome —
Delta's deletion vectors, column mapping, liquid clustering, and `CHECK`
constraints were all exactly that. It is not, on its own, a stopping
point: convert the scenario the probe just exercised into a permanent
test in the connector's `*Spec.scala` file (a `PASS` test asserting the
write succeeds and the contract holds, mirroring the existing PASS/FAIL
pairs), *then* delete the probe. A probe's output is memory of what was
once true; a test in the suite is a standing check that it's still true.
Treat "I tried it and it worked" as equivalent to "untested" until it's
been turned into something CI runs on every future change — the
difference is exactly what separates a real coverage-ledger row from a
sentence in a chat transcript nobody can re-run.

## Known limitations (the general pattern, not connector-specific)

This section is updated as each limitation is closed — treat a claim
here as current, not historical; **an unrevised copy of this section is
exactly how "DataSourceV2 catalog writes are unsolved" would have kept
looking true after Delta's own work solved the general case.** Verify
against `WriteCommandSupport.scala` directly if this section and the
code seem to disagree.

- **Row-level DML (`MERGE`/`UPDATE`/`DELETE`) has *structural*
  verification only, not full semantic verification.** `WriteCommandSupport`'s
  `deltaRowLevelDml` case (Delta-specific today, matched by reflection)
  checks the operation's target against a contract's declared output
  location and current schema — but the actual row-level logic (the
  merge condition, which columns an `UPDATE` touches, whether a `DELETE`
  is unconditional) has no IR representation and isn't checked.
  `ir.Write` models "write a dataset to a location," not "conditionally
  mutate existing rows" — closing this for real needs a `com.example.ir`
  design decision (a new IR node, e.g. `ir.Merge`/`ir.RowMutation`, plus
  contract `rules` vocabulary to check it against — see
  docs/CONTRACT_MODEL.md's `rules` field and ROADMAP.md's "Full semantic
  DML verification" item for the concrete design sketch), not a small
  addition to this file. A *second* connector's row-level DML (if it
  exists and takes a genuinely different shape than Delta's — see
  "Row-level operations" note below) is a real opportunity to find out
  whether `deltaRowLevelDml`'s structural approach generalizes past one
  connector, or whether it was accidentally Delta-shaped.
- **Streaming writes**: closed for Delta specifically (`WriteToStream` is
  a real `WriteCommandSupport` entry — see docs/SPARK_ADAPTER.md's
  "Delta Lake support" write-shape ledger). Not yet confirmed
  connector-agnostic: `WriteToStream`'s location/format resolution
  (`streamSinkLocationAndFormat`) has fallback branches written and
  tested against Delta's specific sink shapes (a populated
  `catalogTable`, or `DeltaSink`'s reflective `path()` accessor) — a
  second connector's streaming sink is the first real test of whether
  those fallbacks (or the generic `sink.name()` tier) are sufficient, or
  whether they were unknowingly Delta-specific too.
- **DataSourceV2 catalog writes**: `AppendData`/`OverwriteByExpression`/
  `ReplaceTableAsSelect` are closed as real, connector-agnostic
  `WriteCommandSupport` entries (matched on Spark's own generic classes,
  not anything Delta-specific — see docs/SPARK_ADAPTER.md's "Write
  command recognition: a single registry"), so any DSv2-catalog
  connector using plain append/overwrite/CTAS-RTAS should be recognized
  "for free," pending empirical confirmation per-connector (Phase 2 of
  this process, never assumed). **Still open**: `OverwritePartitionsDynamic`,
  and DSv2's dedicated *row-level operation* commands
  (`ReplaceData`/`WriteDelta`, produced by Spark's
  `RewriteRowLevelOperation` optimizer rule family for connectors that
  implement `SupportsRowLevelOperations` instead of a proprietary
  command class the way Delta's `MergeIntoCommand`/`UpdateCommand`/
  `DeleteCommand` do) — no `WriteCommandSupport` case exists for either
  yet. A connector whose `MERGE`/`UPDATE`/`DELETE` goes through this
  standard DSv2 mechanism (rather than connector-proprietary commands
  like Delta's) needs this closed as new, connector-agnostic
  `WriteCommandSupport` cases — a stronger outcome than Delta's
  reflection-based `deltaRowLevelDml`, since `ReplaceData`/`WriteDelta`
  are stable public Spark classes, not connector-internal ones requiring
  reflection at all.

---

**Last Updated:** 2026-08-24
