# Adding a Spark Connector

This is the reusable process for giving `spark-adapter` full read/write
support for a data source it doesn't yet understand — Delta Lake today
(see docs/SPARK_ADAPTER.md's "Delta Lake support" and "Fail-closed on
unverifiable writes" sections), Iceberg, ClickHouse, Avro, or anything
else a future contributor wants to add.

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
policy.** Fail-closed exists to catch operations Invariant *hasn't gotten
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
      operation the connector supports that Invariant deliberately
      doesn't translate** (if any exist), proving it's rejected — and
      that nothing was written — rather than silently passed.
- [ ] **A regression test proves the connector's own non-data
      administrative commands aren't blocked** by the fail-closed policy,
      under a contract that would reject anything it actually checked.
- [ ] Mutation testing scoped to the changed/added files clears 70% (see
      CLAUDE.md's "Mutation Testing Requirement"); `mimaReportBinaryIssues`
      is clean.
- [ ] `./dev/build`, `./dev/test`, and `./dev/regression` all pass against
      real `spark-submit` (see CLAUDE.md's "Critical Requirement") —
      including once with the new connector's dependency present and once
      without, to prove the "zero added dependency for non-users" claim
      isn't just asserted.
- [ ] A "`<Connector>` support" section exists in docs/SPARK_ADAPTER.md
      (mirroring "Delta Lake support"), a ROADMAP.md sub-phase, and a
      CHANGELOG.md entry — each stating plainly what *is* and *isn't*
      covered, the same way this document's own retrospective does for
      Delta.

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

A "done" connector's docs/SPARK_ADAPTER.md section, ROADMAP.md sub-phase,
and CHANGELOG.md entry (below) are exactly this ledger, formatted for
their audience — never a blanket "full support" sentence standing in for
it.

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

## Known limitations (the general pattern, not connector-specific)

Every connector added this way will likely share these gaps unless a
future contributor specifically closes them:

- **Row-level DML (`MERGE`/`UPDATE`/`DELETE`) has no IR representation.**
  `ir.Write` models "write a dataset to a location," not "conditionally
  mutate existing rows." Translating these meaningfully is a `com.example.ir`
  design question (a new IR node, or an explicit decision to never verify
  row-level operations), not just a missing `SparkPlanAdapter` case —
  don't treat it as a small addition.
- **Streaming writes are unexplored.** Nothing in this repo's Delta work
  investigated `writeStream`; the same investigation methodology applies,
  but the plan shapes and timing (a streaming query's micro-batches each
  produce their own analyzed plan) haven't been probed even once.
- **DataSourceV2 catalog writes** (`AppendData`/`OverwriteByExpression`/
  `OverwritePartitionsDynamic`/`ReplaceData`/`WriteDelta`, and
  `CreateTableAsSelect`/`ReplaceTableAsSelect` for V2 CTAS) are real,
  recurring write shapes across V2-catalog-backed connectors generally,
  not just Delta — worth a dedicated investigation once a second
  connector needs them, so the pattern gets solved once instead of
  per-connector.

---

**Last Updated:** 2026-08-23
