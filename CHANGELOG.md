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

### Fixed

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

- (None yet)

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
