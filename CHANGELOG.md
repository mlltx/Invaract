# Changelog

All notable changes to Invariant will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Project foundation documentation (MISSION.md, ROADMAP.md, ARCHITECTURE.md)
- Contribution guidelines (CONTRIBUTING.md)
- Code of conduct (CODE_OF_CONDUCT.md)
- Security policy (SECURITY.md)
- Apache 2.0 license with SPDX headers in source files
- Mobile-responsive results viewer (Next.js web UI)
- GitHub Actions CI/CD workflow
- Comprehensive test harness (`./dev/test`)
- **Contract model (Phase 1a)**: new `contract` module representing data
  contracts, modeled after ODCS
  - `ContractParser`: YAML → object model, fail-fast on structural errors
  - `ContractValidator`: structural validation (duplicate names, empty
    schemas, contradictory flags, unrecognized types)
  - `ContractCompatibility`: diffs two contract versions and classifies the
    required MAJOR/MINOR/PATCH bump; flags version bumps inconsistent with
    the actual scope of change
  - 31 unit tests, real YAML fixtures (valid, additive, breaking, invalid)
  - Documentation: [docs/CONTRACT_MODEL.md](docs/CONTRACT_MODEL.md)
- **Transformation IR (Phase 1b)**: new `ir` module, an engine-independent
  representation of a data transformation
  - `Expr`/`Plan` algebras: `Read`, `Write`, `Project`, `Filter`, `Join`,
    `Aggregate`, `Union`, `Sort`, `Window`, `ColumnReference`, `Literal`,
    `FunctionCall`, `AggregateCall`, `NamedExpr`
  - Deliberately does not mirror Spark Catalyst's expression class
    hierarchy: one `FunctionCall` node covers all scalar operators, and
    naming (`NamedExpr`) is not an expression the way Catalyst's `Alias` is
  - `Lineage.trace`: structural column-level provenance tracing (no
    exprId-based resolution) through renames, aggregation, joins, unions,
    and windows, flagging which output columns pass through aggregation
  - `PlanPrinter`: ASCII tree rendering
  - 27 unit tests (initial 21, plus coverage added for `Unsupported`/
    `UnsupportedExpr` — see Spark adapter below), including the worked
    example from the design spec
  - Documentation: [docs/TRANSFORMATION_IR.md](docs/TRANSFORMATION_IR.md)
- **Spark adapter (Phase 1c, Spark Adapter sub-phase)**: new
  `spark-adapter` module translating Spark's Catalyst logical plan into
  the transformation IR
  - `SparkPlanAdapter.translate`: Read/Write (via
    `InsertIntoHadoopFsRelationCommand`), relations, projections,
    expressions, filters, joins, aggregations (`GROUP BY`), aliases
    (`SubqueryAlias` → self-join disambiguation), casts, unions, windows,
    and arbitrarily nested expressions
  - Never throws: an unrecognized construct becomes
    `ir.Unsupported`/`ir.UnsupportedExpr` (new, engine-agnostic IR nodes)
    paired with a `Diagnostic`, not an exception
  - `SparkAdapterListener`: a `QueryExecutionListener`-based capture,
    the least invasive of Spark's plan-inspection extension points for
    observing (not rewriting) a query's plan
  - Integrated into `runner/PluginRunner.scala`: the real plugin
    pipeline's write is translated end-to-end, with the rendered plan and
    traced lineage added to `demo/output/report.json` and printed to the
    console via `./dev/test`
  - 9 integration tests against real Spark 3.5.1 DataFrames (no mocks)
  - Documentation: [docs/SPARK_ADAPTER.md](docs/SPARK_ADAPTER.md)
- **Structural verification (Phase 1c / Phase 4)**: `StructuralVerifier.verify`
  checks a `Contract`'s declared inputs *and* output against a real Spark
  job's actual reads, write, and schemas — existence, location, and
  schema (required fields, unexpected fields rejectable, type
  compatibility, nullability compatibility) for both sides, per
  MISSION.md §8's full "Structural" check class
  - Result matches the spec's exact shape: `{status, contract,
    violations}`, with a 12-member violation-type vocabulary
    (`MISSING_INPUT`/`UNDECLARED_INPUT`/`MISSING_OUTPUT`/
    `OUTPUT_LOCATION_MISMATCH`/`*_FIELD`/`UNDECLARED_*_COLUMN`/
    `*_TYPE_MISMATCH`/`*_NULLABILITY_MISMATCH`)
  - Location matching normalizes a contract's portable relative paths
    against Spark's absolute `file:` URIs at runtime; nullability is
    checked directionally (stricter-than-required is not a violation);
    unexpected inputs/columns are opt-in rejectable via
    `VerificationOptions`, off by default
  - `demo/contracts/invariant_output.yaml`: a real contract for the demo
    pipeline's actual inputs and output
  - Wired into `runner/PluginRunner.scala`: every `./dev/test` run
    verifies the real plan's actual inputs/output against the real
    contract, reported in its own `contractVerification` section (kept
    distinct from `ExecutionReport.status` — job success and contract
    compliance are different questions)
  - 22 tests: every violation type fires against real or
    realistically-constructed Spark schemas, both `VerificationOptions`
    toggles exercised, the real pipeline passing its own contract, and a
    golden test reproducing the Phase 4 spec's own worked example exactly
  - Supersedes the earlier `ContractVerifier` (output schema only, no
    inputs, no nullability, no undeclared-column rejection), removed
    rather than kept alongside
- **Contract-aware Spark execution (Phase 1c / Phase 5)**:
  `ContractEnforcementRule` moves verification into the Spark execution
  lifecycle — a write is verified *before* Spark runs it and aborted, no
  data written, if it violates its contract
  - Built on `SparkSessionExtensions.injectCheckRule` rather than
    `SparkAdapterListener`/`QueryExecutionListener`, which only fires
    after successful execution and so cannot prevent a write; confirmed
    empirically that throwing inside a check rule aborts
    `DataFrame.write` with the target file never created
  - Schemas (input and output) are read directly off the analyzed
    Catalyst plan via `.schema` — no materialized `DataFrame` needed,
    so verification genuinely runs before any execution, not just before
    the write completes
  - Every `Violation` gained a `remediation` field (`StructuralVerifier`):
    a concrete next step, not just a restatement of what's wrong
  - `ContractEnforcementRule.explain` builds a single deterministic
    message answering all four required questions: what the contract
    expected, what the plan contains, why it violates the contract, how
    to correct it — proven byte-identical across repeated runs of the
    same violation
  - Wired into `runner/PluginRunner.scala`: the contract is now loaded
    and the check rule installed before the `SparkSession` is built; the
    write itself is the verification gate
  - Live-demonstrated against the real pipeline via `spark-submit` and a
    deliberately-broken contract
    (`demo/contracts/invariant_output_broken_example.yaml`): exits 1,
    output file never created, full explanation printed — not just
    unit-tested
  - 7 tests, real Spark: PASS/FAIL, the four-part explanation, message
    determinism across repeated attempts, non-write queries never
    triggering verification, `VerificationOptions` threading through,
    and the public `forContract` entry point
- **Contract regression pack**: `./dev/regression` re-runs the Phase 5
  PASS/FAIL demonstration as an assertion script instead of a transcript to
  read — real `spark-submit`, real exit codes, real files checked on disk
  - `dev/build`: builds every sbt module (`contract`, `ir`, `plugin`,
    `spark-adapter`, `runner`) in the dependency order their
    `unmanagedJars` cross-references require; fixes a latent bug where
    `./dev/test` and CI only ever built `plugin`+`runner` directly and
    silently depended on the other three modules' jars already existing
    on disk from an earlier build — a genuinely fresh clone failed
    `runner`'s compile step (`not found: type ContractViolationException`)
  - `dev/lib.sh`: extracts the spark-submit/java-fallback invocation
    shared by `dev/test` and `dev/regression` into one place
  - `dev/regression` asserts, against real command output: the satisfied
    case exits 0, reports PASS, and writes its output; the violated case
    exits non-zero, reports FAIL with `MISSING_OUTPUT_FIELD`, and — the
    core guarantee — never creates the output file at all
  - `docker/Dockerfile` + `dev/regression-docker`: a self-contained image
    (JDK 21, sbt, Spark 3.5.1) that builds every module at image-build
    time and runs the regression pack as its entrypoint, so any
    contributor with just Docker installed can run
    `./dev/regression-docker` and get the same result with nothing else
    on their machine
  - CI (`.github/workflows/test.yml`) now builds via `dev/build` and runs
    `dev/regression` on every matrix leg, giving the contract-abort path
    CI coverage for the first time (previously only the PASS case was
    checked in CI)
  - CI also gained a dedicated `docker-regression` job that builds
    `docker/Dockerfile` and runs it, exercising the same
    `./dev/regression-docker` path a Docker-only contributor would use —
    not just the native-toolchain path the matrix job covers
- **Broader Spark operation and file-format translation coverage**
  (`SparkPlanAdapterSpec`): tests proving `SparkPlanAdapter` against
  operations the existing suite didn't exercise — `Sort` (direction and
  null ordering), every `JoinType` (left/right/full outer, semi, anti,
  cross), multi-way join chains, `COUNT`/`AVG`/`MIN`/`MAX`/
  `COUNT(DISTINCT ...)` aggregates, a multi-argument aggregate (`corr`)
  wrapped in `ARGS(...)`, `CASE WHEN`/`IS NULL` via the generic expression
  fallback, `.limit(n)`, `.distinct()`, and `.repartition()`/`.coalesce()`
  as transparent pass-throughs — plus a test reading the same data via
  CSV, JSON, and Parquet to prove translation is genuinely format-agnostic
  (works at the `HadoopFsRelation` level, not per-format).
  - This surfaced five real gaps, all now fixed (not just documented):
    - **Output format was never verified.** `ir.Write` gained a `format:
      Option[String]` field, populated via Spark's own
      `DataSourceRegister.shortName()`; `StructuralVerifier` gained
      `OUTPUT_FORMAT_MISMATCH`, checked only when both the contract's
      declared format and the actual write's format are known (either
      side unknown skips the check rather than risking a false
      rejection). Proven end-to-end, including in `dev/regression`'s own
      rendered plan output, which now shows `format=parquet`.
    - **`Distinct`/`Deduplicate`** fell through to the opaque
      `Unsupported` placeholder instead of being translated — fixed as a
      transparent pass-through (doesn't change columns, only row count).
    - **`Repartition`/`Coalesce`/`RepartitionByExpression`** had the same
      problem and the same fix.
    - **`SaveMode` wasn't captured at all.** `ir.Write` gained a
      `saveMode: Option[String]` field, populated from
      `InsertIntoHadoopFsRelationCommand.mode`; the contract model gained
      a matching `Dataset.saveMode`, and `StructuralVerifier` gained
      `OUTPUT_SAVE_MODE_MISMATCH` — the same both-sides-known convention
      as the format check. Proven end-to-end: `dev/regression`'s rendered
      plan now shows `saveMode=overwrite`, matching the real demo
      contract's declared `saveMode: overwrite`.
    - **JDBC/non-file sources got a lower-fidelity location.**
      `JDBCRelation` fell through `SparkPlanAdapter.locationOf`'s generic
      `catalogTable`/`.toString` fallback (and was flagged with a fallback
      `Diagnostic` on every JDBC read). Since `JDBCRelation` is
      `private[sql]` in Spark and can't be named as a pattern-match type
      here, it's now identified by class name with its public
      `jdbcOptions()` accessor fetched reflectively, giving a precise
      `"jdbc:<url>/<table>"` location and no diagnostic. Proven with a
      real H2 in-memory-database read, not a mock.
- **Property-based fuzzing of `SparkPlanAdapter`** (`SparkPlanAdapterFuzzSpec`,
  `scalatestplus-scalacheck-1-17`): the first of several "market leading
  regression testing" guardrails identified for this project (others —
  mutation testing, `report.json` golden-file snapshots, a multi-Spark-
  version compatibility matrix, coverage gating, API-compatibility
  checking — remain future scope, tracked in ROADMAP.md Phase 1c).
  Generates random chains (1-6 steps, ~200 cases/run) of the same
  operations `SparkPlanAdapterSpec` tests individually — filter, recomputed
  columns, sort, aggregate, self-join, union, distinct, limit, repartition/
  coalesce, `CASE WHEN` — composed in random order and depth against a
  real `local[*]` session, asserting `translate`/`render`/`trace` never
  throw and any `Unsupported` node carries a `Diagnostic`, directly testing
  the adapter's own "never throws" design promise across combinations the
  hand-written suite doesn't reach. Validated to actually catch
  regressions (not just pass by construction): a join-type translation was
  temporarily broken to throw, the fuzz spec failed on its very first
  case with the full analyzed plan and exception in the failure message,
  then the break was reverted.
- **Mutation testing, whole-module, blocking CI at 50%** (Stryker4s): the
  second regression-testing guardrail (remaining scope — golden-file
  `report.json` snapshots, a multi-Spark-version compatibility matrix,
  coverage gating, API-compatibility checking — tracked in ROADMAP.md
  Phase 1c). Mutates a source file (flip `==`/`!=`, `&&`/`||`,
  `exists`/`forall`, delete a string literal, ...) and reruns the real
  test suite per mutant, answering "does a passing test actually verify
  this line's behavior" rather than just "does it execute the line" —
  something line coverage can't distinguish. Scoped to whole-module
  (`ir` and `spark-adapter`, not just `Lineage.scala`/
  `StructuralVerifier.scala`), gated at 50% via each module's
  `strykerThresholdsBreak`, and wired into CI as a new `mutation-testing`
  job that fails the build below threshold and publishes each module's
  HTML report as a build artifact. Required bumping `sbt.version` to
  `1.11.7` in just those two modules (Stryker4s 1.1.1 needs sbt ≥
  1.11.2); both modules' full test suites, the whole 5-module
  `./dev/build`, and a real `./dev/test` run all confirmed unaffected by
  the bump. Real, actionable survivors were fixed via new/strengthened
  tests rather than by excluding them: `Join`'s ambiguous-aggregation
  propagation and `Project`'s column-name matching in `Lineage`, plus the
  previously wholly-uncovered `Aggregate`/`Window`/`Union` cases of
  `resolveInScope` and several `PlanPrinter` branches; the
  `exists`/`forall` input-matching predicates, `field.required` handling,
  and `contextPrefix` branch selection in `StructuralVerifier`; and
  `ContractEnforcementRule.explain`'s violation-count pluralization and
  optional-field marking. Final scores: `ir` 86.36% (76/100 mutants),
  `spark-adapter` 57.06% (93/177) — documented with file/line references
  in docs/SPARK_ADAPTER.md and docs/TRANSFORMATION_IR.md. CLAUDE.md now
  requires 70% on the specific file(s) a feature adds or changes to that
  file as a standing step when a feature touches either module — a
  stronger, PR-author-level bar than the whole-module 50% CI gate. That
  bar is now automated for PRs too, not just a manual step: CI's
  `mutation-testing` job diffs against the PR's base commit and reruns
  `sbt stryker` scoped (via a brace-expansion `--mutate` glob, e.g.
  `"{FileA.scala,FileB.scala}"`) to just each module's changed
  `src/main/scala/**/*.scala` files, overriding the whole-module 50% gate
  with `--thresholds.break 70` on the CLI only — `build.sbt`'s own
  setting is untouched. Stryker4s has no incremental/diff mode of its own
  (unlike StrykerJS), so this is a small CI-level wrapper around the same
  `--mutate` scoping used throughout, not a built-in capability, and it
  only runs on `pull_request` events. Verified locally both ways: passes
  on a real historical multi-file `ir` diff (86.21%), and correctly fails
  on a `spark-adapter` file pair scoring below 70% together (52.9%) even
  though the whole module clears 50%.
- **Uplifted `spark-adapter`'s whole-module mutation score to 91.53%**
  (54/59 mutants, 93.1% of covered code), clearing the same 70% bar
  CLAUDE.md already required for new/changed code, by setting
  `strykerExcludedMutations := Seq("StringLiteral")` in
  `spark-adapter/build.sbt`. Of the 84 mutants undetected at 57.06%, 79
  were `StringLiteral` mutants on message/remediation/type-name text —
  the category CLAUDE.md's "Mutation Testing Requirement" already treats
  as an acceptable, documented exclusion, since asserting an exact error
  string doesn't verify behavior and is the kind of test a harmless
  wording change breaks for no reason. Excluding the category repo-wide
  (rather than writing ~79 brittle exact-match tests, or leaving the
  module's real coverage permanently capped by prose) makes that judgment
  explicit instead of ad hoc, and leaves mutation testing fully active for
  every mutator that changes actual behavior. `spark-adapter`'s thresholds
  moved to 90/80/70 (high/low/break, matching the incremental PR check's
  values); `ir` is untouched at 86.36%, already above 70%. The 5 mutants
  still undetected after the exclusion are the same real,
  already-investigated gaps documented before this change (the
  `JDBCRelation`-guard near-equivalence, the untestable-without-a-Hive-
  metastore fallback branch, and `unwrapWriteWrapper`'s branch unreachable
  under the pinned Spark 3.5.1) — left in place with their rationale
  rather than chased further. Verified via a real `sbt stryker` run before
  and after (57.06% → 91.53%), not estimated.
- **JSON Schema for the contract format**
  (`contract/schema/invariant-contract.schema.json`, Draft 2020-12): the
  actual public, language-agnostic interface for authoring or generating
  an Invariant contract outside Scala — distinct from (and a better fit
  than) an earlier idea to publish a schema for `demo/output/report.json`,
  which turned out to be solving a problem the demo harness's own output
  format doesn't have (nothing external consumes it; see CLAUDE.md's
  "What's the product, and what's the test harness"). The schema mirrors
  `ContractParser`'s hard parse failures (id/version shape, dataset/field
  required keys) plus `ContractValidator`'s Error-level checks where a
  bare parse-only schema would accept a document validation immediately
  rejects anyway (non-empty `outputs`, non-empty `fields`); it
  deliberately does not attempt duplicate-name detection or other
  cross-field business rules, and does not restrict `field.type` to an
  enum, since an unrecognized type is only a `ContractValidator` Warning,
  not a rejection. New `ContractSchemaSpec` (6 tests) validates the schema
  against the same real fixtures used elsewhere in the module, both ways
  — every valid fixture (including one with real validator *warnings*)
  conforms, both invalid fixtures are rejected — so the schema can't
  silently drift from the parser/validator it documents. `demo/contracts/
  *.yaml` gained a `yaml-language-server` `$schema` comment for live
  editor validation. Documented in docs/CONTRACT_MODEL.md's new "JSON
  Schema" section. Verified via a full local `./dev/build` + `./dev/test`
  + `./dev/regression` run (all pass) after adding the schema and the
  editor-hint comments.
- **API-compatibility checking, mandatory PR gate** (MiMa /
  `sbt-mima-plugin`): the third regression-testing guardrail, answering a
  different question than fuzzing or mutation testing — not "does the
  code work" but "does this change silently break everyone who already
  depends on the previous version's compiled jar." Wired into `contract`,
  `ir`, and `spark-adapter` only (`plugin`/`runner` excluded, same scoping
  as every other guardrail here). No Maven Central release exists yet to
  compare against, so `mimaPreviousArtifacts` in each module's `build.sbt`
  points at its own `0.1.0` coordinate, and a new CI job
  (`api-compatibility` in `.github/workflows/test.yml`) publishes the PR's
  base branch to the runner's local Ivy cache under that coordinate
  first, then runs `sbt mimaReportBinaryIssues` against the PR's head — a
  module that doesn't exist yet at the base commit is skipped gracefully
  (see the "Changed" entry below for why this landed on a fixed base
  rather than a sliding one, after a detour through both). Runs on every
  `pull_request` event automatically and feeds into the
  `summary` gate like every other job, making it a mandatory check, not
  an opt-in one. Verified detection
  actually works, not just that the task runs: temporarily removed
  `Contract.input` locally, confirmed `mimaReportBinaryIssues` failed with
  the exact symbol and a ready-to-use `ProblemFilters.exclude[...]`
  suggestion, then reverted and confirmed a clean pass. CLAUDE.md gained
  an "API Compatibility Requirement" section (the two legitimate responses
  to a failure: fix an accidental break, or add a documented exclusion for
  a deliberate one), and docs/CONTRACT_MODEL.md, docs/TRANSFORMATION_IR.md,
  and docs/SPARK_ADAPTER.md each gained an "API compatibility" section.
- **Delta Lake write support**: `SparkPlanAdapter` now recognizes
  `SaveIntoDataSourceCommand`, the plan node Delta (and any other
  `CreatableRelationProvider`-based `.save(...)` source) actually
  analyzes to — confirmed empirically against a real Delta-enabled
  `SparkSession`, not assumed. Previously such a write translated to
  `ir.Unsupported`, so `ContractEnforcementRule` silently treated it as a
  no-op and let it through completely unverified, contract or no
  contract. `formatOf` was widened from `FileFormat` to `AnyRef` and
  reused: Delta's `DeltaDataSource` implements `DataSourceRegister` the
  same way every built-in format already does (`shortName() ==
  "delta"`), so no Delta-specific type is needed at all. Net result:
  **zero added runtime or compile-time dependency** for non-Delta users
  — `delta-spark` (pinned to 3.2.0; 3.2.1 has a confirmed real bug on
  Scala 2.12 + Spark 3.5.1, `delta-io/delta#3737`) is `% "test"` only, to
  spin up a real Delta session to test against. Confirmed via `unzip -l`
  that the assembled jar is unchanged in size and bundles zero Delta
  classes. New Delta translation test in `SparkPlanAdapterSpec` and a
  Delta PASS/FAIL enforcement pair in `ContractEnforcementRuleSpec`
  (mirroring the existing Parquet pair). Mutation testing scoped to the
  3 changed files scored 82.14% (bar: 70%), whole-module score unchanged
  at 91.53%, `mimaReportBinaryIssues` clean. `.saveAsTable`/
  DataFrameWriterV2/SQL `MERGE INTO` writes are a different,
  DataSourceV2-based plan shape, not covered — documented as a known
  limitation. Documented in docs/SPARK_ADAPTER.md's new "Delta Lake
  support" section and ROADMAP.md's new "Delta Lake support" sub-phase.
- **Fail-closed on unverifiable writes**: `ContractEnforcementRule` now
  rejects a Spark write it cannot translate/verify, instead of silently
  letting it through the way the pre-fix Delta write above did — resolves
  the fail-open-vs-closed question that gap raised, generalized to any
  write shape, not just Delta. A real, jar-level reflective survey (every
  concrete class implementing Spark's `Command` marker across
  `spark-sql`/`spark-catalyst` 3.5.1 and `delta-spark` 3.2.0 — 164
  classes) found that Spark's own `Command` hierarchy does **not**
  distinguish "writes data" from "pure catalog metadata"
  (`SaveIntoDataSourceCommand` and `CreateDataSourceTableCommand`
  implement the exact same trait), ruling out a naive "reject anything
  unrecognized" policy as unsafe — it would have also blocked ordinary
  `CREATE TABLE`/`ANALYZE TABLE`/`CACHE TABLE`/etc. New `FailClosedCommands`
  holds an explicit, documented allowlist (~100 classes from the survey,
  matched by fully-qualified name since a sixth of them are Delta-specific
  and this module has no compile-time Delta dependency) of commands
  confirmed not to change a table's row content; anything `Command`-shaped
  that's neither a recognized write nor on that list is rejected with a
  new `ViolationType.UnverifiableWrite`. Deliberately asymmetric: a
  missing safe command costs one loud rejection; a wrongly-added
  data-mutating one would silently defeat the feature — every
  genuinely data-mutating command the survey found (DELETE/UPDATE/MERGE,
  LOAD DATA, TRUNCATE, DROP TABLE, Delta's RESTORE/CLONE/etc.) was left
  off the list. Also adds `CreateDataSourceTableAsSelectCommand`
  (`.saveAsTable(...)`/CTAS against a new V1 table) as a real recognized
  write — a third distinct write shape found by the same survey, same gap
  as the original Delta bug. Verified with new PASS/FAIL pairs, a
  fail-closed test proving a real unrecognized write (Delta `MERGE INTO`)
  is rejected before touching the table (byte-identical rows before/after
  the aborted merge), and a regression test proving ordinary DDL
  (`CREATE TABLE`/`ANALYZE TABLE`/`SHOW TABLES`) is never blocked.
  Mutation testing 91.67%/93.22% (up from 91.53%/93.1%), every mutant the
  new code introduced killed; `mimaReportBinaryIssues` clean. Documented
  in docs/SPARK_ADAPTER.md's new "Fail-closed on unverifiable writes"
  section and ROADMAP.md's matching sub-phase.
- **Reusable process for adding a Spark connector**: Delta Lake support
  was built twice — once for `.save(...)`, again separately for
  `.saveAsTable(...)` and the fail-closed policy — because the first pass
  didn't survey the connector's full operation surface up front. New
  `docs/ADDING_A_SPARK_CONNECTOR.md` writes up the investigation
  methodology that eventually got Delta right (test-scope-only
  dependency, probing with `injectCheckRule` specifically since it sees
  different plans than `QueryExecutionListener`, a reflective survey of
  every `Command` class the connector's jar defines, and a three-way
  classification: translatable write / confirmed-safe / fails closed) as
  a "Definition of done" checklist for the next connector (Iceberg,
  ClickHouse, Avro, ...), plus a new `add-spark-connector` Claude Code
  skill (`.claude/skills/add-spark-connector/`) that runs the same
  process as an interactive 10-phase workflow with explicit sign-off
  checkpoints before the fail-closed classification is implemented and
  before a connector is called done. Cross-linked from CLAUDE.md.
- **Write command recognition consolidated into a single registry**: "is
  this plan a write, and what does it mean" used to be implemented three
  separate times (`SparkPlanAdapter.translatePlan`,
  `ContractEnforcementRule.verifyOrThrow`'s output-schema derivation,
  `SparkAdapterListener.onSuccess`'s capture check), independently kept
  in lockstep by hand — exactly the structural hazard behind both of this
  session's real Delta bugs (a write shape added to one match, missed in
  another). New `WriteCommandSupport.scala` replaces all three with one
  `PartialFunction[LogicalPlan, WriteCommandInfo]`-per-write-shape
  registry (`combined`, built via `orElse`); `WriteCommandInfo` bundles
  location/query/format/saveMode/outputSchema together, so a write shape
  can no longer be added with its schema piece missing the way the
  original bug did. All three sites now consult `combined` instead of
  their own match. Verified behavior-preserving: full 59-test suite
  passed unchanged before and after, `mimaReportBinaryIssues` clean,
  `./dev/build`/`./dev/test`/`./dev/regression` all still pass against
  real `spark-submit`. Mutation testing caught one real, new gap the
  refactor introduced (`SparkAdapterListener`'s `isDefinedAt` check had
  no test for the negative case — a non-write action leaving `lastWrite`
  untouched), closed with a new test rather than left; final score
  91.94%/93.44% (up from 91.53%/93.1%), same 5 pre-existing survivors as
  always. docs/SPARK_ADAPTER.md, docs/ADDING_A_SPARK_CONNECTOR.md, and
  the `add-spark-connector` skill's Phase 6 all updated to describe the
  one-file-one-list story.
- **Delta Lake reads investigated and verified — zero code needed**: asked
  directly whether read recognition had the same duplicated-recognition
  problem the write side did before the registry above fixed it.
  Investigated with the `add-spark-connector` skill: probed `.load(path)`
  and a catalog table reference (`spark.table(...)`/`SELECT * FROM tbl`)
  against a real Delta session via `injectCheckRule`. Both produce a
  `LogicalRelation` wrapping `org.apache.spark.sql.delta.DeltaLog$$anon$2`,
  confirmed to be an anonymous subclass of Spark's own `HadoopFsRelation`
  rather than a distinct relation type, so the existing `locationOf`/
  `translatePlan` branches already match it precisely through ordinary
  subtyping — no new case, no location fallback to fix. Answer to the
  motivating question: reads don't have the write side's bug today,
  because both consumer sites gate on the identical single
  `LogicalRelation` type and can't disagree by construction (unlike
  writes, which had three sites recognizing different concrete classes).
  Explicitly not "solved forever" — a future connector whose read
  produces something other than `LogicalRelation` (most plausibly
  `DataSourceV2Relation`) would need a real second case in both sites,
  and *that's* the actual trigger for a `ReadRelationSupport`-style
  registry, not building one preemptively now. Verified with a
  translation test and a PASS/FAIL enforcement pair proving a contract's
  declared input schema is genuinely checked against a real Delta read
  (surfacing a real, separate finding: Delta reports every column
  nullable on read-back regardless of what was written). Zero production
  code changed; full 63-test suite passing, `mimaReportBinaryIssues`
  clean, full `./dev/build`/`./dev/test`/`./dev/regression` pass.
  Documented in docs/SPARK_ADAPTER.md's new "Delta Lake reads" section.
- **Mandatory coverage ledger required from `add-spark-connector`**: fixed
  a process gap the Delta work above exposed twice — the skill had been
  run once for writes and once for reads, each time declaring success on
  a scope narrower than "does this connector actually work," with no
  mechanism forcing the remaining gap to be stated. `docs/ADDING_A_SPARK_CONNECTOR.md`
  gained a canonical "operation surface" checklist (5 read rows, 8 write
  rows) and a mandatory "coverage ledger" close-out: every invocation,
  however narrowly scoped, must now end with every row disposed as ✅
  Covered, 🚫 Fails closed, or ❓ Not investigated (with reason + next
  step) — a missing row is the one disallowed outcome.
  `.claude/skills/add-spark-connector/SKILL.md` restructured to match
  (Phase 2 exercises every row; Phase 10 produces and posts the ledger).
- **Delta Lake operation-surface coverage ledger — every remaining row
  closed out**: ran the full canonical checklist against Delta
  specifically, empirically, per the new requirement above. New findings:
  V2 write commands (`AppendData`/`OverwriteByExpression`/
  `ReplaceTableAsSelect` — covering `.saveAsTable()`/`.insertInto()`/
  `.writeTo()` against existing tables, and `.format("delta").saveAsTable()`
  against a *new* table) all correctly fail closed (`UnverifiableWrite`,
  zero rows written — verified by comparing table contents before/after
  each rejected attempt); time-travel reads need no new code (identical
  plan shape to a plain read); change-data-feed reads are translated via
  the existing generic `LogicalRelation` fallback, with a location
  diagnostic since the CDC relation has no populated `catalogTable`; and
  maintenance operations (`OPTIMIZE`/`VACUUM` safe, `RESTORE`/`CLONE`/
  `CONVERT TO DELTA` fail closed) were already correctly classified.
  **Most significant finding: streaming writes to Delta have zero
  enforcement touchpoint, not "fails closed but unverified."**
  `WriteToStream`, the top-level plan for every streaming write, was
  confirmed via `javap` on Spark's own catalyst jar to not implement
  `Command` — so `ContractEnforcementRule`'s fail-closed policy (which
  only gates `Command`-shaped plans) structurally cannot ever see it,
  confirmed empirically by a probe showing zero of 9 plans seen by
  `injectCheckRule` during a real streaming Delta write were
  `Command`-shaped. A streaming write commits silently, with no contract
  check at all — the one row in this ledger needing a genuinely different
  enforcement mechanism (tracked as a new ROADMAP item), not more
  `WriteCommandSupport` coverage. Full ledger — all 13 rows, each with its
  disposition and evidence — in docs/SPARK_ADAPTER.md's new "Delta Lake
  operation-surface coverage ledger" section; ROADMAP.md's Delta "Scope
  (Future)" bullet corrected to no longer claim streaming writes fail
  closed. Zero production code changed this pass (findings only); full
  suite passing, `mimaReportBinaryIssues` clean, full
  `./dev/build`/`./dev/test`/`./dev/regression` pass.
- **Streaming writes to Delta: closed, superseding the "needs a
  genuinely different enforcement mechanism" line above.** That framing
  turned out to undersell it: `WriteToStream` already reaches
  `injectCheckRule` (the coverage-ledger pass above established this —
  it's just not `Command`-shaped), so no new Spark extension point was
  needed. Added as a real `WriteCommandSupport` entry instead — the same
  registry every other write shape goes through — using
  `inputQuery.schema` for the output schema and, for location, a
  resolved `catalogTable` (`.toTable(...)`) or a reflective call to
  Delta's `DeltaSink.path()` (its `name()`/`schema()` unconditionally
  throw, confirmed empirically — the same no-compile-time-dependency
  reflection technique already used for `JDBCRelation`). A streaming
  Delta write is now genuinely translated and verified before the query
  starts, not merely gated. New `ContractEnforcementRuleSpec` tests: a
  PASS/FAIL pair for `.start(path)`, a PASS test for `.toTable(...)`, a
  direct format-detection check, and a test confirming a streaming write
  to a location unrelated to the active contract is now correctly
  rejected — consistent with how batch writes have always behaved, no
  longer special-cased by omission.
- **`add-spark-connector`'s fail-closed framing corrected: it's a
  stopgap, not a verdict.** The ledger's 🚫 disposition — including the
  Delta ledger above — read as if "not yet translated, verified to
  abort" were a complete answer, no different in spirit from ✅ Covered.
  That's backwards: fail-closed exists to catch what Invariant hasn't
  translated *yet*. docs/ADDING_A_SPARK_CONNECTOR.md and the skill now
  require every 🚫 row to carry a next step (the translation work that
  would close it, or, rarely, a specific reason it never will); the
  Delta ledger in docs/SPARK_ADAPTER.md was rewritten to match.
- **Every remaining Delta write-side 🚫 row closed, plus the
  streaming-read-as-input gap.** `AppendData` (`.saveAsTable()` append,
  `.insertInto()`, `.writeTo().append()`), `OverwriteByExpression`
  (`.writeTo().overwrite(cond)`), and `ReplaceTableAsSelect`
  (`.format("delta").saveAsTable()` on a new table,
  `.writeTo().createOrReplace()`) are all now real `WriteCommandSupport`
  entries, genuinely translated and verified rather than merely rejected.
  `OverwriteByExpression` needed no `ir.Write` extension after all — its
  delete predicate maps to the contract's existing coarse-grained
  `saveMode: overwrite`, since `StructuralVerifier` never needed the
  predicate itself. Separately, `StreamingRelation`/`StreamingRelationV2`
  are now recognized as read shapes (in both `SparkPlanAdapter`'s
  translation and `ContractEnforcementRule`'s input-schema collection, via
  shared helpers so the two can't drift), closing a real false-positive:
  a contract declaring a streaming source as a required `input` used to
  always report `MISSING_INPUT` even though data was genuinely being
  read. New PASS/FAIL pairs for all four in `ContractEnforcementRuleSpec`.
  Found and fixed a genuine correctness trap along the way: a single
  `.saveAsTable()` on a *new* table produces two write-shaped plans
  through `injectCheckRule` — the top-level `ReplaceTableAsSelect` and an
  internal, nested `AppendData` against a `StagedTable` (Spark's public
  2-phase-commit protocol for atomic CTAS/RTAS) — which would otherwise
  resolve to two different, mismatched locations for the same
  destination and spuriously abort a contract-satisfying write; fixed via
  a shared `qualifiedIdentifier` helper so both agree by construction. Of
  the 13-row ledger, only row-level DML (`MERGE`/`UPDATE`/`DELETE`)
  remains 🚫, deliberately: it needs a real IR extension
  (`ir.Merge`/`ir.RowMutation`), not a new case against `ir.Write`'s
  existing shape. Full details in docs/SPARK_ADAPTER.md's Delta ledger,
  including a new "A shared pitfall" subsection documenting the staged-
  table trap for future connector work.
- **Row-level DML (`MERGE INTO`/`UPDATE`/`DELETE`): structural
  verification, closing the last row in the Delta operation-surface
  ledger.** `MergeIntoCommand`/`UpdateCommand`/`DeleteCommand` (all
  Delta-internal classes) are now real `WriteCommandSupport` entries,
  matched by reflection via their public `target()`/`catalogTable()`/
  `source()` methods and wrapped in `Try` so a future Delta API rename
  degrades to the pre-existing fail-closed default rather than crashing a
  real job. Checks the operation's target against the contract's declared
  output location and current schema, and recognizes MERGE's source as a
  contract input — deliberately does not check the actual row-level logic
  (the merge condition, which columns an `UPDATE` touches, whether a
  `DELETE` is unconditional), since there's no contract vocabulary for
  that yet. Full semantic verification is scoped out on purpose and
  documented in ROADMAP.md's new "Full semantic DML verification" item
  (with a concrete example of the `rules` vocabulary it would need) so
  it isn't lost, per an explicit decision to keep this pass structural-
  only. Found and fixed a second correctness trap along the way: MERGE's
  source was assumed to already be recognized as a contract input the
  same way every other write's read-side is, but a real FAIL test proved
  that wrong — Delta's DML commands are leaf nodes in the tree-traversal
  sense (`source`/`target` are case-class fields, not `children`), so
  `ContractEnforcementRule`'s existing `plan.collect` never reached them.
  Fixed by also walking `WriteCommandSupport`'s extracted `query` field.
  New PASS/FAIL pairs for all three DML operations, plus a direct-
  inspection test for a path-based operation with no catalog table at
  all. Mutation testing scoped to the two changed files: 85.71% overall.
  All 13 rows of the Delta operation-surface ledger are now ✅ Covered.
- **Delta feature-by-feature confidence pass**, closing the gap between
  "every write-command shape covered" and "every Delta table feature
  actually tried": schema evolution and generated columns each had a
  real false-rejection bug, found and fixed; deletion vectors, column
  mapping mode, liquid clustering, and CHECK constraints are confirmed
  transparent with a permanent test each, replacing what was previously
  only throwaway probe evidence; identity columns are confirmed
  untestable in this Spark 3.5.1 environment (parser-level rejection) and
  documented as such rather than silently skipped. See
  docs/SPARK_ADAPTER.md's new "Delta feature-by-feature confidence pass"
  subsection and ROADMAP.md for full details.
- **Apache Iceberg support**: second connector onboarded via the
  `add-spark-connector` skill's process (`iceberg-spark-runtime-3.5_2.12`
  1.11.0, test-scope only). Found and closed two real, connector-agnostic
  gaps that predate Iceberg entirely — batch DataSourceV2 catalog reads
  had no translation case at all, and explicit-create V2 CTAS
  (`.writeTo(...).create()`) / dynamic-partition overwrite
  (`.writeTo(...).overwritePartitions()`) had no write-recognition case,
  both silently failing closed until now. Closed row-level DML
  (`MERGE`/`UPDATE`/`DELETE`) via a new, connector-agnostic case matching
  Spark's own standard `RowLevelWrite` API (`ReplaceData`/`WriteDelta`) —
  no reflection needed, unlike Delta's proprietary DML classes. Found and
  fixed a second staged-table location-resolution bug (Iceberg's staged
  table reports a location Delta's doesn't, breaking the fix's own
  "always agree" claim from the prior pass) by keying the fallback on
  Spark's `StagedTable` marker interface instead of property presence.
  Deliberately left Iceberg's `CALL system.*` maintenance procedures
  unmodeled (one shared Spark class covers every procedure, safe and
  unsafe alike, with no structural way to tell them apart) — documented
  as a real limitation, not silently passed. See docs/SPARK_ADAPTER.md's
  new "Iceberg support" section (including both coverage ledgers) and
  ROADMAP.md for full details.
- **Closed Iceberg's schema-evolution gap by generalizing, not
  duplicating, Delta's generated-columns fix.** The predicted bug didn't
  exist (a `mergeSchema`-evolving append's new columns were already
  visible correctly); the real one was the opposite direction — a
  narrower append under `write.spark.accept-any-schema`, omitting a
  column Iceberg NULL-fills, was wrongly `MISSING_OUTPUT_FIELD`-rejected.
  Found this is the same underlying situation as Delta's generated
  columns under a different mechanism, and that a plain public API
  (`Table.columns()`, no reflection) already carries what's needed for
  both — replaced the Delta-specific reflective fix outright with one
  connector-agnostic mechanism. Verified the safety argument (a
  genuinely-missing field is still caught by Spark's own analyzer, not
  silenced) with a dedicated test, not just asserted. Mutation testing
  rescoped after the simplification: 76.92%, zero survivors in the new
  code.
- **Closed Iceberg's last two `❓` feature-surface rows: deletion vectors
  and identity/generated columns.** Both closed with real probes and
  permanent tests, zero production code changes. Deletion vectors
  (Iceberg's V3 merge-on-read spec): a `DELETE` against a real
  `format-version = 3` table still produces a plain `ReplaceData` node,
  already matched by the existing connector-agnostic `dsv2RowLevelWrite`
  case — the storage mechanism behind a merge-on-read delete isn't
  visible at the `LogicalPlan` level this adapter operates on.
  Identity/generated columns: confirmed, not assumed, that this Iceberg
  catalog integration rejects both `GENERATED ALWAYS AS` and column
  `DEFAULT` values outright with `AnalysisException`, before any plan is
  produced, unaffected by `accept-any-schema` — so unlike Delta, there is
  no generated/default-column concept reachable here for Invariant to
  translate or verify. `IcebergConnectorSpec`: 17 → 19 tests (19/19
  passing). Both of Iceberg's coverage ledgers (operation surface and
  feature surface) are now fully closed, no `❓` rows remaining.
- **Iceberg CALL procedure classification**: closed the last remaining
  Iceberg operation-surface gap by teaching `FailClosedCommands` to tell
  Iceberg's 20 system procedures apart, not just recognize the shared
  `Call` class they all analyze to. `Call.procedure().getClass().getName()`
  is a real, distinct class per procedure (confirmed via the jar, not
  guessed) — 10 procedures (storage/metadata compaction, GC of
  unreferenced files/snapshots, catalog registration, stats, read-only
  introspection) reclassified from wrongly-rejected to correctly-allowed;
  the other 10 (rollback/cherrypick/publish/fast-forward/add_files/migrate/
  snapshot/rewrite_table_path) stay fails-closed, deliberately, since
  each genuinely changes a table's current content or produces new
  persisted content. Mutation testing found a real gap in the new code —
  the reflection fallback that keeps this fails-closed if a future
  Iceberg version reshapes `Call` had zero coverage — closed with a
  dedicated, session-free unit test rather than just documented around.
  `FailClosedCommands.scala` mutation score: 88.42% (89.36% of covered
  code), zero survivors in the new code. Verifying the 10 unmodeled
  procedures' actual effect against a contract is scoped as separate
  future work (ROADMAP.md), piloting on `rollback_to_snapshot` alone
  first.

### Fixed

- **A pre-existing CI failure on `ubuntu-latest`/Java 11**, found while
  checking PR checks: `iceberg-spark-runtime-3.5_2.12:1.11.0`'s jar is
  compiled to Java 17 class file version and can't load under JDK 11,
  cascading into three unrelated `spark-adapter` suites sharing the same
  forked JVM. Fixed with a `Tests.Filter` in `spark-adapter/build.sbt`
  excluding only `IcebergConnectorSpec`, only under JDK <17 — every other
  test, in this module and every other, is unaffected.
- Two real bugs found while adding Delta Lake write support, both caught
  by genuinely failing tests rather than inspection:
  - `ContractEnforcementRule.verifyOrThrow`'s output-schema derivation
    special-cased only `InsertIntoHadoopFsRelationCommand`
    (`cmd.query.schema`), falling back to the write command node's own
    `.schema` — empty for a `Command` — for everything else. A Delta
    write's contract verification always reported every declared output
    field as missing, regardless of what was actually written. Fixed by
    adding the same `cmd.query.schema` handling for
    `SaveIntoDataSourceCommand`.
  - `SparkAdapterListener.onSuccess` had its own independent "is this a
    write" filter, also hardcoded to `InsertIntoHadoopFsRelationCommand`
    only, so it never captured a Delta write for `demo/output/report.json`
    reporting even once translation and enforcement were fixed. Fixed the
    same way.
- `./dev/test` and CI only ever built `plugin` and `runner` directly, never
  `contract`, `ir`, or `spark-adapter`. This worked only because those three
  modules' jars happened to already exist on disk in every environment they
  had been run in so far; on a genuinely fresh clone, `runner`'s compile
  step fails (`not found: type ContractViolationException`, `Lineage`,
  `PlanPrinter`, ...) since its `unmanagedJars` reference jars that were
  never built. Fixed by adding `dev/build`, which builds all five modules
  in the dependency order their cross-references require, and having both
  `./dev/test` and CI's workflow call it instead of building a subset
  directly
- `PluginRunner`: removed a dead `scala.io.Source.fromFile(reportPath, ...)`
  call that attempted to read the report file before it was ever written,
  crashing on a first run with no pre-existing `report.json`
- `PluginRunner`'s JSON serializer (`anyToJson`/`mapToJson`) did not escape
  special characters in string values (quotes, newlines); surfaced by the
  new multi-line rendered-plan report field, now fixed for all string
  values
- `plugin`'s and `spark-adapter`'s `sbt test`, and `dev/test`'s
  non-`spark-submit` fallback path, failed on JDK 17+ (`IllegalAccessError`
  reaching `sun.nio.ch.DirectBuffer`) because `spark-submit`'s own
  `--add-opens` injection wasn't in play; added the same flag set
  explicitly to each
- `dev/test`'s Step 7 parsed `report.json` with a naive `grep -o
  '"status": ...'`, assuming the key appeared exactly once. Adding
  `contractVerification.status` (nested, same key name as the top-level
  field) turned the parsed value into a multi-line shell variable,
  breaking the pass/fail exit code even when the top-level status was
  `PASS`. Fixed to take the first (top-level) match, which also silently
  fixed a pre-existing instance of the same issue with `pluginVersion`
  (duplicated between the top-level field and `buildInfo`)

### Changed

- **Renamed `runner`'s `PluginRunner` to `DemoJobHarness`**, and
  `dev/lib.sh`'s `run_plugin_runner` helper to `run_demo_job_harness`.
  `PluginRunner` read as if it might be part of Invariant's own Spark
  extension machinery (Spark has a real, unrelated `SparkPlugin`
  interface/`spark.plugins` config, making the collision worse); the new
  name states what the class actually is — an example Spark job used as a
  test harness — and is not the verification engine. Updated every
  `--class com.example.runner.PluginRunner` spark-submit invocation,
  `demo/contracts/invariant_output.yaml`'s comment, and all doc references
  (ARCHITECTURE.md, ROADMAP.md, docs/SPARK_ADAPTER.md). Verified via a
  full local `./dev/build` + `./dev/test` + `./dev/regression` run after
  the rename (all pass; the enforcement pass/fail pair still behaves
  correctly).
- **Rewrote ARCHITECTURE.md and CLAUDE.md** to reflect the actual current
  system rather than the pre-Phase-1 scaffold they still described (three
  components — `plugin`/`runner`/`web` — with the contract-verification
  engine listed under "Future Architecture Directions" as unbuilt, even
  though `contract`/`ir`/`spark-adapter` have been built, fuzzed, and
  mutation-tested for some time). Both docs now lead with an explicit
  "product vs. test harness" distinction — `contract`/`ir`/`spark-adapter`
  are the verification engine a real user would depend on;
  `plugin`/`runner`/`demo`/`web` are an example integration proving the
  engine works against a real Spark job via `./dev/test`, not something
  a user imports. ARCHITECTURE.md gained real component/data-flow/ADR
  content for the engine (translation → verification → enforcement, the
  check-rule-vs-listener split, why the IR doesn't mirror Catalyst) in
  place of the stale Phase 1 sketch. This distinction matters going
  forward: it's what scopes every regression-testing guardrail (fuzzing,
  mutation testing, and the still-outstanding compatibility matrix /
  coverage gating / API-compatibility checking) to `contract`/`ir`/
  `spark-adapter`, not to the demo harness.
- **Narrowed `spark-adapter`'s real public surface**: `SparkPlanAdapter`
  (the raw Catalyst-to-IR translator) and `StructuralVerifier` (the raw
  contract-verification function) are now `private[sparkadapter]` — a
  review of every cross-module reference (grep, not guessing) found
  neither is ever called outside this module; `ContractEnforcementRule`
  and `SparkAdapterListener` are the only real callers, both in the same
  package, and a real user gets both translation and verification
  automatically via the installed extension. `contract` and `ir` were
  reviewed the same way and needed no changes — both were already this
  tight. Surfaced a genuinely useful, non-obvious finding in the process:
  `private[sparkadapter]`, when the qualifier is a symbol's own
  containing package, is enforced by the Scala compiler, not the JVM —
  `javap` on the compiled classes confirms they stay `public final class`
  in raw bytecode either way. MiMa compares bytecode, so it correctly
  reports zero issues for this narrowing, not because an exclusion covers
  it but because nothing detectable to MiMa changed. The narrowing is
  still worth doing (it stops real Scala code from depending on either
  class by accident, the failure mode that actually matters) but isn't a
  MiMa-enforced guarantee the way the module's real public API is — now
  documented as such in both files' Scaladoc and
  docs/SPARK_ADAPTER.md's "API compatibility" section. Verified via
  `spark-adapter`'s own test suite (51/51, package-private doesn't block
  same-package test access) and a full local `./dev/build` + `./dev/test`
  + `./dev/regression` run.
- **Fixed a sliding-baseline soundness bug in both CI jobs that diff
  against a base commit** (`api-compatibility` and the incremental
  mutation-testing step) — caught by review before it caused real harm,
  not discovered via a live failure. Both had been changed, earlier in
  this branch's history, to diff against the *previous push's* HEAD
  (`github.event.before`) instead of the PR's actual base commit, to work
  around this repo's PR #1 predating `contract`/`ir`/`spark-adapter`
  entirely. That fix traded a real problem for a worse one: a sliding
  baseline can never durably catch a regression that's introduced and
  then never fixed. Concretely, if push N breaks something and the check
  correctly fails, push N+1 — even one that touches nothing relevant —
  diffs against N, where the break already looks like the status quo, so
  the check passes clean without anything having been fixed; the bar
  silently stops being enforced the moment CI reports it met, whether or
  not anything actually changed. Reverted both to diff against the PR's
  actual base commit (`github.event.pull_request.base.sha`) instead — a
  fixed anchor for the PR's lifetime, not a self-erasing one. For
  `api-compatibility`, the original crash this was meant to fix
  (`base-ref/contract: No such file or directory`) is now handled at its
  actual source: a module that doesn't exist yet at the base commit is
  skipped gracefully (nothing to compare, nothing can have broken) rather
  than changing which commit counts as the base. For the incremental
  mutation-testing step, reverting to the PR's base commit means it goes
  back to being redundant with the whole-module check for this one
  historical PR (as it was before that detour) — correct, if unhelpful,
  for this specific long-lived PR; sound for every future PR opened
  against an up-to-date base branch, which is what actually matters.
  Verified the fixed logic locally: a dry run of `api-compatibility`'s
  per-module skip loop against both a partial-existence scenario and PR
  #1's real all-three-modules-absent scenario, confirming clean skips and
  no crash either way.

### Deprecated

- (None yet)

### Removed

- (None yet)

### Fixed

- (None yet)

### Security

- (None yet)

## [0.1.0] - 2024-08-20

### Added

- **Project Launch**: Initial open-source project setup
  - Repository infrastructure (mlltx/Invariant)
  - Core documentation (README.md, MISSION.md)

- **Plugin System** (Scala/Spark 3.5.1)
  - Schema validation
  - Computed column transformation (value_squared)
  - Event logging for diagnostics
  - Comprehensive unit tests (4/4 passing)

- **Test Infrastructure**
  - PluginRunner for real Spark execution via spark-submit
  - ExecutionReport for structured results (JSON)
  - Integration test on demo data (CSV → Parquet)
  - Local Spark master for deterministic results

- **Developer Environment**
  - GitHub Codespaces Dev Container configuration
  - Auto-provisioning of JDK 21, sbt, Spark 3.5.1, Node.js 20
  - One-command test harness (`./dev/test`)
  - Mobile-friendly results viewer (`./dev/report`)

- **Web UI** (Next.js 14)
  - Status badge (✓ PASS / ✕ FAIL)
  - Build information display
  - Test results summary
  - Input/output schema and sample data visualization
  - Plugin events timeline
  - Mobile-responsive design (375px+)
  - Dark mode support

- **Documentation**
  - README.md with quick start and feature overview
  - MISSION.md: Product vision (14 sections)
  - ROADMAP.md: Phase 0-4 planning
  - CLAUDE.md: Development guide for Claude Code users
  - ARCHITECTURE.md: System design and decisions
  - SECURITY.md: Vulnerability reporting and practices
  - CODE_OF_CONDUCT.md: Community standards
  - CONTRIBUTING.md: Developer contribution guide

- **Build Configuration**
  - sbt build for plugin and runner
  - sbt-assembly for JAR packaging
  - Maven-style dependency management
  - Semantic versioning (0.1.0)

### Technical Details

- **Spark Version**: 3.5.1
- **Scala Version**: 2.12.18
- **Java Version**: 21
- **Build System**: sbt 1.9+
- **License**: Apache 2.0

### Performance

- Plugin build time: 5-10s (cold), 2-3s (incremental)
- Runner build time: 10-15s
- Spark job execution: 3-5s (demo data)
- Total test harness: 30-60s (after first build)

### Known Limitations

- Spark plugin name is misleading (actually a Spark application, not Spark extension)
  - To be clarified in Phase 1 documentation
- Web UI requires Codespace connection (no offline mode)
- No authentication/authorization in web UI
- Local Spark only (no remote cluster support)
- Report schema serialization shows Java object references (cosmetic, non-blocking)

### Future Work

- Phase 1: Core verification engine with contract analysis
- Phase 2: Multi-engine support (SQL, dbt, Trino, BigQuery, DuckDB)
- Phase 3: Contract registry and versioning
- Phase 4: AI agent interfaces and platform integrations
- Security audit (Phase 1 or 2)
- Maven Central publication
- Extended documentation (guides, tutorials, examples)

---

## Release Notes

### 0.1.0 - Initial Release

This is the first release of Invariant, establishing the project foundation.

**What's Included:**
- Working Spark plugin with transformation logic
- Complete test harness for real Spark execution
- Mobile-responsive results viewer
- Comprehensive project documentation
- Open-source community infrastructure

**Getting Started:**
1. Clone: `git clone https://github.com/mlltx/Invariant.git`
2. Open in GitHub Codespaces (auto-provisions environment)
3. Run tests: `./dev/test`
4. View results: `./dev/report`
5. Read: [CONTRIBUTING.md](CONTRIBUTING.md) to contribute

**Next Steps:**
- Try modifying plugin in `plugin/src/main/scala/.../InvariantPlugin.scala`
- Run `./dev/test` to validate changes
- View results in web UI at http://localhost:3000

**Feedback & Issues:**
- Bug reports: [GitHub Issues](https://github.com/mlltx/Invariant/issues)
- Discussions: [GitHub Discussions](https://github.com/mlltx/Invariant/discussions)
- Security: See [SECURITY.md](SECURITY.md)

---

**Note**: Changelog will be updated with each release. See [ROADMAP.md](ROADMAP.md) for upcoming phases.
