# Invariant Roadmap

## Phase 0 — Establish the Project Foundations

### Objective

Create a credible open-source project before implementing substantial functionality.

The goal of Phase 0 is to establish the organizational, legal, and technical infrastructure required to accept external contributions without requiring architectural restructuring later.

---

### Work Items

#### 1. Repository Organization

- [x] **Repository name and organization**
  - Name: `Invariant`
  - Organization: `mlltx`
  - URL: `https://github.com/mlltx/Invariant`
  - Description: Open framework for verifying data transformations against contracts

- [ ] **Repository visibility**
  - Ensure repository is public
  - Enable discussions
  - Enable issues
  - Enable pull requests
  - Set up branch protection rules

#### 2. Licensing

- [ ] **Choose permissive licence**
  - Recommended: Apache 2.0 (aligns with Spark ecosystem)
  - Alternative: MIT (simpler, still permissive)
  - Document: LICENSE file at repository root
  - Include: SPDX header in source files

- [ ] **Copyright and attribution**
  - Define copyright holder(s)
  - Establish contributor attribution policy
  - Document in LICENSE and CONTRIBUTING.md

#### 3. Documentation

- [x] **README.md**
  - Project description ✓
  - Quick start guide ✓
  - Key features
  - Use cases
  - Links to documentation
  - Development setup
  - Contributing link

- [x] **MISSION.md**
  - Vision statement ✓
  - Problem definition ✓
  - Core concepts ✓
  - Long-term goals ✓

- [ ] **CONTRIBUTING.md**
  - How to set up development environment
  - Code style guidelines
  - Testing requirements
  - Pull request process
  - Commit message conventions
  - Developer workflow

- [ ] **CODE_OF_CONDUCT.md**
  - Community standards
  - Reporting mechanism
  - Enforcement policy
  - Inclusive language guidelines

- [ ] **ARCHITECTURE.md**
  - High-level system design
  - Component breakdown
  - Data flow diagrams
  - Decision records (ADRs)
  - Technology choices and rationale

- [ ] **SECURITY.md**
  - Reporting security vulnerabilities
  - Supported versions for security updates
  - Security best practices
  - Dependencies and vulnerability tracking

#### 4. Continuous Integration

- [ ] **GitHub Actions workflow**
  - Test on push to main branches
  - Test on pull requests
  - Run on multiple OS (Linux, macOS, Windows)
  - Run on multiple Java versions (11, 17, 21)
  - Code coverage reporting
  - Lint checks

- [ ] **Build configuration**
  - sbt configuration
  - Dependency resolution
  - Caching strategies
  - Artifact publishing

- [ ] **Quality gates**
  - Coverage thresholds
  - Style enforcement
  - Type checking
  - Documentation build

#### 5. Dependency Management

- [ ] **Dependency declaration**
  - Define all dependencies with versions
  - Separate compile, test, and provided dependencies
  - Use meaningful version constraints
  - Document rationale for major dependencies

- [ ] **Dependency updates**
  - Establish policy for dependency upgrades
  - Security update response timeline
  - Breaking change handling
  - Compatibility matrix

- [ ] **Reproducible builds**
  - Lock file for exact versions
  - Document build environment
  - Publish build metadata

#### 6. Release and Versioning

- [ ] **Versioning strategy**
  - Adopt Semantic Versioning (MAJOR.MINOR.PATCH)
  - Define compatibility guarantees
  - Establish deprecation policy
  - Document version lifecycle

- [ ] **Release process**
  - Define release criteria
  - Create release checklist
  - Automate version bumping
  - Tag releases in git
  - Publish to repositories (Maven Central, etc.)

- [ ] **Release notes**
  - Changelog format (CHANGELOG.md)
  - Breaking changes highlighted
  - Migration guides for major versions
  - Security update notification

- [ ] **Artifact distribution**
  - Maven Central or similar registry
  - GitHub Releases with binaries
  - Documentation on artifact locations

#### 7. Compatibility Policy

- [ ] **Java/JVM compatibility**
  - Minimum JDK version policy
  - Support matrix for multiple versions
  - End-of-life policy for old versions

- [ ] **Spark compatibility**
  - Supported Spark versions
  - Test matrix for multiple Spark versions
  - Adapter pattern for different Spark APIs

- [ ] **Scala compatibility**
  - Supported Scala versions
  - Binary compatibility guarantees
  - Cross-compilation strategy

- [ ] **Deprecation policy**
  - Timeline for deprecation warnings
  - Removal timeline for deprecated APIs
  - Communication strategy

#### 8. Architectural Principles

- [ ] **Document principles**
  - Technology independence (not Spark-specific)
  - Open standards preference
  - Modularity and composition
  - Explainability
  - Performance and scalability

- [ ] **ADR (Architecture Decision Records)**
  - Format: Use ADR template
  - Repository: docs/adr/
  - Examples:
    - Why Scala instead of Java
    - Why Spark as first implementation
    - Contract standard choice (ODCS)
    - Verification algorithm approach

- [ ] **Design patterns**
  - Abstraction for transformation IR
  - Adapter pattern for execution engines
  - Contract representation pattern
  - Verification result format

#### 9. Governance

- [ ] **Project governance model**
  - Decision-making process
  - Maintainer roles
  - Steering committee (if applicable)
  - Conflict resolution

- [ ] **Contribution levels**
  - Contributor
  - Committer
  - Maintainer
  - Requirements for each level

- [ ] **Roadmap and planning**
  - Public roadmap (this file)
  - Phase-based releases
  - Long-term vision alignment
  - Community feedback integration

#### 10. Community

- [ ] **Discussion channels**
  - GitHub Discussions for questions
  - Issues for bugs and features
  - Slack workspace (optional)
  - Email list (optional)

- [ ] **First-time contributor experience**
  - Good first issues tagged
  - Comprehensive onboarding docs
  - Responsive maintainers
  - Clear expectations

- [ ] **Recognition**
  - Contributors file (CONTRIBUTORS.md)
  - Changelog acknowledgments
  - Community highlights
  - Release notes credits

---

### Success Criteria

Phase 0 is complete when:

1. **Legal & Licensing**
   - [ ] Apache 2.0 license in place
   - [ ] SPDX headers in source files
   - [ ] Copyright clearly stated

2. **Documentation**
   - [ ] README with quick-start
   - [ ] CONTRIBUTING.md with clear process
   - [ ] CODE_OF_CONDUCT.md established
   - [ ] ARCHITECTURE.md with design decisions
   - [ ] SECURITY.md with vulnerability reporting

3. **CI/CD**
   - [ ] GitHub Actions workflows running
   - [ ] Tests pass on all supported versions
   - [ ] Code coverage tracked
   - [ ] Lint checks enforced

4. **Versioning**
   - [ ] Semantic versioning defined
   - [ ] Release process documented
   - [ ] Changelog maintained
   - [ ] Compatibility matrix published

5. **Repository Health**
   - [ ] Branch protection rules enforced
   - [ ] Issues enabled and triaged
   - [ ] Pull request template created
   - [ ] Stale issue management configured

6. **External Readiness**
   - [ ] Repository is public and discoverable
   - [ ] Documentation is complete and accurate
   - [ ] First external contribution can be accepted
   - [ ] No architectural blockers identified

---

### Timeline Estimate

| Activity | Effort | Duration |
|----------|--------|----------|
| Licensing & Legal | 2 hours | 1 day |
| Core Documentation | 8 hours | 2 days |
| CI/CD Setup | 4 hours | 1 day |
| Governance & Process | 4 hours | 1 day |
| Community Infrastructure | 2 hours | 1 day |
| **Total** | **20 hours** | **~1 week** |

---

### Dependencies & Blockers

**External Dependencies:**
- GitHub organization admin access
- License decision (recommend Apache 2.0)

**Internal Dependencies:**
- Complete MISSION.md (in progress)
- Stable API design (Phase 1)

**Potential Blockers:**
- License approval from stakeholders
- Organizational branding decisions
- CI/CD complexity

---

### Deliverables

Upon completion of Phase 0, the repository will include:

```
Invariant/
├── LICENSE                          # Apache 2.0
├── CONTRIBUTING.md                  # Contribution guidelines
├── CODE_OF_CONDUCT.md              # Community standards
├── SECURITY.md                      # Vulnerability reporting
├── ARCHITECTURE.md                  # Design decisions
├── CHANGELOG.md                     # Release notes
├── CONTRIBUTORS.md                  # Community credits
├── README.md                        # Project overview
├── MISSION.md                       # Vision & goals
├── ROADMAP.md                       # This file
├── docs/
│   ├── adr/                         # Architecture Decision Records
│   ├── development.md               # Developer setup
│   └── design/                      # Design documents
├── .github/
│   ├── workflows/                   # CI/CD pipelines
│   ├── PULL_REQUEST_TEMPLATE.md
│   └── ISSUE_TEMPLATE/
├── .gitignore
└── [Source code]
```

---

### Next Phase

**Phase 1 — Core Verification Engine** (follow this document)

Once Phase 0 is complete, implementation work can proceed with confidence that the project structure supports external contributions and future evolution.

---

## Phase 1 — Core Verification Engine

### Phase 1a — Contract Model

#### Objective

Represent the minimum contract required to verify a transformation while
remaining compatible with an established open contract standard.

ODCS (Open Data Contract Standard) was evaluated as the base representation.
Invariant avoids redefining concepts ODCS already standardizes (schema,
fields, types) and folds anything else it doesn't yet interpret into
`extensions` rather than rejecting it. See
[docs/CONTRACT_MODEL.md](docs/CONTRACT_MODEL.md) for the full design.

```yaml
id: customer_orders
version: 1.0

inputs:
  - location: raw.orders
    schema:
      ...

outputs:
  - location: gold.customer_orders
    schema:
      ...

rules:
  ...
```

#### Requirements

The model supports:

- [x] Contract identity
- [x] Version
- [x] Input datasets
- [x] Output datasets
- [x] Physical locations
- [x] Schemas
- [x] Field names
- [x] Types
- [x] Nullability
- [x] Required/optional semantics
- [x] Compatibility rules
- [x] Contract extensions

#### Deliverables

- [x] **Contract parser** — `ContractParser` (YAML → object model), fail-fast on structural errors (`contract/src/main/scala/com/example/contract/ContractParser.scala`)
- [x] **Contract object model** — `Contract`, `Dataset`, `Schema`, `Field`, `ContractVersion`, `ContractRule` (`contract/src/main/scala/com/example/contract/ContractModel.scala`)
- [x] **Contract validation** — `ContractValidator`, structural checks beyond parseability: duplicate names, empty schemas, contradictory flags, unknown types (`contract/src/main/scala/com/example/contract/ContractValidator.scala`)
- [x] **Versioning semantics** — `ContractCompatibility`, diffs two contract versions and classifies the required MAJOR/MINOR/PATCH bump (`contract/src/main/scala/com/example/contract/ContractCompatibility.scala`)
- [x] **Contract fixtures** — valid, additive, breaking, and invalid example contracts (`contract/src/test/resources/fixtures/`)
- [x] **Documentation** — [docs/CONTRACT_MODEL.md](docs/CONTRACT_MODEL.md)

31 unit tests across parser, validator, and compatibility engine, run via `cd contract && sbt test`.

#### Dependencies

- Phase 0 completion

---

### Phase 1b — Transformation Intermediate Representation

#### Objective

Create an engine-independent representation of a transformation, precise
enough to establish lineage and verify contracts. This is the most
important architectural component of the verification engine: without it,
lineage and verification logic would have to be written once per execution
engine (Spark, SQL, dbt, ...) instead of once against a shared IR.

Critical principle: the IR represents transformation *semantics*, not one
engine's implementation classes. See
[docs/TRANSFORMATION_IR.md](docs/TRANSFORMATION_IR.md) for the full design
rationale — in particular, why there is one `FunctionCall` node instead of
Spark Catalyst's dozens of expression subclasses, why there is no `Alias`
expression node, and why there is no exprId-based symbol resolution.

```
Write(gold.customer_orders)
  └── Project
       ├── customer_id    <- Read(raw.orders).customer_id
       └── lifetime_value <- SUM(Read(raw.orders).amount)
```

#### Requirements

The IR represents:

- [x] Dataset (`DatasetRef`)
- [x] Column (`ColumnRef`)
- [x] Expression (`Expr`)
- [x] Projection (`Project`)
- [x] Filter (`Filter`)
- [x] Join (`Join`, with `JoinType`)
- [x] Aggregation (`Aggregate`, `AggregateCall`)
- [x] Grouping (`Aggregate.groupBy`)
- [x] Union (`Union`)
- [x] Sort (`Sort`, `SortOrder`)
- [x] Window (`Window`)
- [x] Alias (`NamedExpr` — deliberately not an `Expr`; see design doc)
- [x] Literal (`Literal`)
- [x] Function (`FunctionCall`)
- [x] Write (`Write`)
- [x] Read (`Read`)

#### Deliverables

- [x] **Identifiers** — `DatasetRef`, `ColumnRef` (`ir/src/main/scala/com/example/ir/Identifiers.scala`)
- [x] **Expression algebra** — `Expr`, `ColumnReference`, `Literal`, `FunctionCall`, `AggregateCall`, `NamedExpr`, `SortOrder` (`ir/src/main/scala/com/example/ir/Expr.scala`)
- [x] **Plan algebra** — `Plan`, `JoinType`, `Read`, `Write`, `Project`, `Filter`, `Join`, `Aggregate`, `Union`, `Sort`, `Window` (`ir/src/main/scala/com/example/ir/Plan.scala`)
- [x] **Lineage tracing** — `Lineage.trace`, structural column-level provenance resolution through renames, aggregation, filters, joins, unions, and windows (`ir/src/main/scala/com/example/ir/Lineage.scala`)
- [x] **Plan rendering** — `PlanPrinter`, ASCII tree rendering for debugging and demonstration (`ir/src/main/scala/com/example/ir/PlanPrinter.scala`)
- [x] **Documentation** — [docs/TRANSFORMATION_IR.md](docs/TRANSFORMATION_IR.md)

21 unit tests covering construction, lineage resolution (including the
worked example above, `GROUP BY`, `Filter`/`Sort` passthrough, unambiguous
and ambiguous `Join` attribution, `Window`, and `Union`), and rendering, run
via `cd ir && sbt test`.

#### Dependencies

- Phase 0 completion

---

### Phase 1c — Verification Engine

#### Objective

Verify that a Spark transformation's actual behavior conforms to a parsed,
validated `Contract`, using the transformation IR as the common
representation between "what a Spark logical plan actually does" and "what
a contract requires."

#### Sub-phase: Spark Adapter (done)

The bridge from `plugin`/`runner`'s real Spark execution into `ir`:
translates Spark's Catalyst logical plan into the transformation IR.
Investigated Spark's plan-inspection extension points (direct
`Dataset.queryExecution` access, `QueryExecutionListener`,
`SparkSessionExtensions`) and used the least invasive one that fits —
see [docs/SPARK_ADAPTER.md](docs/SPARK_ADAPTER.md) for the comparison and
the empirical findings (grounded in real Spark 3.5.1 behavior, not
assumption) that shaped the design.

- [x] **Spark adapter** — `SparkPlanAdapter.translate`, covering Read/Write
      (via `InsertIntoHadoopFsRelationCommand`), relations (`LogicalRelation`
      + `HadoopFsRelation`), projections, expressions, filters, joins,
      aggregations (including `GROUP BY`), aliases (`SubqueryAlias` →
      `Read.alias`, for self-join disambiguation), casts, unions, windows,
      and arbitrarily nested expressions — never throws; an unrecognized
      construct becomes `ir.Unsupported`/`ir.UnsupportedExpr` paired with a
      `Diagnostic` (`spark-adapter/src/main/scala/com/example/sparkadapter/SparkPlanAdapter.scala`)
- [x] **Spark integration tests** — 9 tests against real Spark 3.5.1
      DataFrames (no mocks): the worked example, filter+cast, self-join
      alias disambiguation, union, window, a UDF (diagnostic, not a
      failure), an unsupported construct (`explode`, diagnostic +
      `Unsupported` node, not a crash), and a full write captured via
      `SparkAdapterListener`
      (`spark-adapter/src/test/scala/com/example/sparkadapter/SparkPlanAdapterSpec.scala`)
- [x] **Plan extraction examples** — see docs/SPARK_ADAPTER.md
- [x] **Unsupported-operation diagnostics** — `Diagnostic`/`TranslationResult`;
      translation always produces a best-effort IR, never an exception
- [x] **Integrated with the test Spark app** — `runner/PluginRunner.scala`
      registers `SparkAdapterListener`, captures the real write's
      translated IR and `Lineage.trace` output, prints the rendered plan to
      the console, and adds a `transformationIR` section to
      `demo/output/report.json`. Run via `./dev/test` — real Spark
      execution, not simulated.
- [x] Extended the IR itself: `ir.Unsupported` / `ir.UnsupportedExpr`
      (`ir/src/main/scala/com/example/ir/{Plan,Expr}.scala`), a principled,
      engine-agnostic "could not translate this" node any future front-end
      can use, not a Spark-adapter-specific workaround

#### Sub-phase: Structural verification (done)

The first *useful* verifier — checks a `Contract`'s declared inputs and
output against a real Spark job's actual reads, write, and schemas. Covers
the "Structural" checks from
[MISSION.md, §8](MISSION.md#8-contract-verification) completely: inputs
(existence, location, schema), outputs (existence, location, schema), and
schema (required fields, unexpected fields rejectable, type compatibility,
nullability compatibility).

- [x] `StructuralVerifier.verify(contract, plan, inputSchemas, outputSchema, options): VerificationResult`
      (`spark-adapter/src/main/scala/com/example/sparkadapter/StructuralVerifier.scala`) —
      existence/location read directly off the `Plan`'s `Read`/`Write`
      nodes (no Spark data needed); schema checks (presence, type,
      nullability) against caller-supplied real `StructType`s, since the
      IR itself carries no schema (see `ir.Read`'s doc in
      TRANSFORMATION_IR.md)
- [x] A structured result matching the spec's exact shape —
      `{"status": "PASSED"|"FAILED", "contract": "id@version", "violations": [...]}` —
      with a 12-member violation-type vocabulary
      (`MISSING_INPUT`/`UNDECLARED_INPUT`/`MISSING_INPUT_FIELD`/`UNDECLARED_INPUT_COLUMN`/
      `INPUT_FIELD_TYPE_MISMATCH`/`INPUT_FIELD_NULLABILITY_MISMATCH` and the
      `OUTPUT_*` equivalents), each violation carrying `type`, `message`,
      and whichever of `column`/`location`/`expected`/`actual` apply
- [x] `VerificationOptions(rejectUndeclaredInputs, rejectUndeclaredFields)` —
      both "unexpected X can be rejected" checks are opt-in, off by
      default, matching how most contract/schema tooling treats an
      unlisted extra column or input
- [x] Location matching bridges a contract's portable relative paths
      (`"demo/input/sample.csv"`) against Spark's absolute `file:` URIs at
      runtime (confirmed empirically — see docs/SPARK_ADAPTER.md) —
      without this, every real run would spuriously fail location checks
      for a reason unrelated to actual contract compliance
- [x] Nullability checked directionally, not by equality: a contract
      requiring non-null violated by an actual nullable column is a real
      violation; the reverse (actual guarantees more than required) isn't
- [x] Wired into `runner/PluginRunner.scala`: verifies the real plan's
      actual inputs/output against `demo/contracts/invariant_output.yaml`
      on every `./dev/test` run, adding a `contractVerification` section
      to `demo/output/report.json` and console output. Kept separate from
      `ExecutionReport.status` — "did the Spark job run" and "does its
      output satisfy the contract" are different questions.
- [x] 22 tests, all against real Spark (no mocks) — every violation type
      fires at least once against real or realistically-constructed
      schemas, both `VerificationOptions` toggles exercised on/off, the
      real demo pipeline passing its own contract, and a golden test
      reproducing the spec's own worked example
      (`UNDECLARED_OUTPUT_COLUMN`/`"country"`) exactly
      (`spark-adapter/src/test/scala/com/example/sparkadapter/StructuralVerifierSpec.scala`)

Supersedes the earlier, narrower `ContractVerifier` (output schema only,
no inputs, no nullability, no undeclared-column rejection) — removed
rather than kept alongside, to avoid two overlapping verifiers in the
codebase. Deliberately still out of scope: `rules` interpretation
(compatibility mode, quality expectations — the contract model records
these but nothing acts on them yet), and multi-output contracts (one
verification run checks the plan's single `Write` against
`contract.outputs.head`; matching against whichever of several declared
outputs a plan actually produced is unexercised anywhere in this repo).

#### Sub-phase: Contract-aware Spark execution (done)

Moves verification into the Spark execution lifecycle (this sub-phase's
own spec called it "Phase 5"): rather than verifying a write after the
fact (the `SparkAdapterListener`-based flow from the two sub-phases
above), a write is now verified *before* Spark executes it, and aborted —
no data written — if it violates its contract.

```
Spark application → Logical plan → Invariant → PASS → execute
                                             └─→ FAIL → abort
```

- [x] `ContractEnforcementRule`
      (`spark-adapter/src/main/scala/com/example/sparkadapter/ContractEnforcementRule.scala`) —
      builds a Spark check rule
      (`SparkSessionExtensions.injectCheckRule`) that verifies a write
      against a contract *before* Spark runs it, throwing
      `ContractViolationException` to reject the query if verification
      fails. Confirmed empirically, not assumed: a probe against a fresh
      `SparkSession` with a check rule that unconditionally throws showed
      the write's target file never created.
  - This is a deliberately different mechanism from `SparkAdapterListener`
    (used for `demo/output/report.json`'s summary): the listener only
    fires *after* successful execution — structurally incapable of
    preventing a write — while a check rule is Spark's purpose-built
    pre-execution validation hook. Both are used, for different moments in
    the same pipeline.
  - Fires on every analyzed plan in the session; only a plan that
    translates to an `ir.Write` is checked, so reads/counts/intermediate
    queries are unaffected.
- [x] **Deterministic, explainable failures** — `ContractEnforcementRule.explain`
      builds a message answering all four required questions from a single,
      fully-populated `VerificationResult`: what the contract expected
      (declared input/output schemas), what the plan contains (the
      rendered IR tree), why it violates the contract (each violation's
      `message`), and how to correct it (each violation's new
      `remediation` field, added to `Violation` in `StructuralVerifier`).
      Proven deterministic by test: the same violation produces a
      byte-identical explanation across repeated runs.
- [x] Wired into `runner/PluginRunner.scala`: the contract is loaded and
      the check rule installed *before* the `SparkSession` is built (a
      check rule can't be added to an already-built session); the real
      write is now the verification gate itself, not a separate step
      after it.
- [x] Live-demonstrated against the real pipeline, not just unit tests:
      running `PluginRunner` with a deliberately-broken contract
      (`demo/contracts/invariant_output_broken_example.yaml`, requiring a
      `customer_name` column the real plugin never produces) via
      `spark-submit` exits 1, the target parquet path is never created,
      and the console/report show the full four-part explanation.
- [x] 7 tests, all against real Spark (no mocks): PASS executes and
      creates output; FAIL aborts before any data is written; the
      explanation contains all four required sections; the same violation
      produces byte-identical explanations across repeated attempts;
      non-write queries never trigger verification even under a
      contract that would always fail; `VerificationOptions` thread
      through the enforcement path; `forContract`'s public entry point
      works directly
      (`spark-adapter/src/test/scala/com/example/sparkadapter/ContractEnforcementRuleSpec.scala`)

#### Sub-phase: Contract regression pack (done)

Turns the PASS/FAIL demonstration above from a transcript a reviewer reads
into a script any contributor (or CI) can re-run, with a real pass/fail
exit code, in an environment that requires nothing pre-installed but
Docker.

- [x] `dev/build` — builds `contract`, `ir`, `plugin`, `spark-adapter`, and
      `runner` in the dependency order their `unmanagedJars`
      cross-references require. Fixes a real bug this work surfaced:
      `./dev/test` and CI previously built only `plugin` and `runner`
      directly, silently relying on the other three modules' jars already
      existing on disk. Verified by deleting `contract/target`,
      `ir/target`, `spark-adapter/target` and confirming `runner`'s
      `sbt compile` then fails — a genuinely fresh clone would have hit
      this.
- [x] `dev/lib.sh` — the spark-submit/`java`-fallback invocation, shared by
      `dev/test` and `dev/regression` instead of duplicated
- [x] `dev/regression` — runs both cases from a real `spark-submit`
      invocation each and asserts on the actual results: exit code, report
      `status`, presence/absence of the output file on disk, and (for the
      FAIL case) that `MISSING_OUTPUT_FIELD` is the reported violation.
      Exits 0 only if both cases behaved as contracted.
- [x] `docker/Dockerfile` + `dev/regression-docker` — a self-contained
      image (JDK 21, sbt, Spark 3.5.1, mirroring
      `.devcontainer/post-create.sh`) that builds every module at
      image-build time and runs the regression pack as its entrypoint.
      `./dev/regression-docker` is the single command referenced by this
      sub-phase's name: build the image, run it, get a real pass/fail —
      no local JDK/sbt/Spark required.
- [x] CI (`.github/workflows/test.yml`) now builds via `dev/build` and runs
      `dev/regression` on every OS/Java matrix leg, so the contract-abort
      path has CI coverage for the first time — previously CI only checked
      the PASS case.
- [x] CI also runs a dedicated `docker-regression` job that builds
      `docker/Dockerfile` and runs it, so the Docker path
      (`dev/regression-docker`) is verified on every push too, not just
      documented — this environment's sandbox couldn't verify it directly
      (its egress policy blocks Docker Hub's image CDN), so CI is this
      path's actual verification.

#### Sub-phase: Output format verification, Distinct/Repartition translation (done)

Three gaps found while extending `SparkPlanAdapterSpec`'s translation
coverage (see git history — a "KNOWN GAP" characterization test in
`StructuralVerifierSpec` documented the format gap before this fix, and is
now replaced by tests proving the real behavior).

- [x] **Output format verification.** `ir.Write` gained a `format:
      Option[String]` field; `SparkPlanAdapter` populates it from the
      write's Spark `FileFormat` via `DataSourceRegister.shortName()` (the
      same clean identifier — `"parquet"`, `"csv"`, `"json"` — Spark uses
      everywhere else, including `df.write.format(...)`). `StructuralVerifier`
      now emits `OUTPUT_FORMAT_MISMATCH` when the contract's declared
      format and the actual write's format are both known and disagree
      (case-insensitively); either side being unknown skips the check
      rather than risking a false rejection. Proven against a real
      `spark-submit`-style write end-to-end, not just a hand-built plan —
      see `SparkPlanAdapterSpec`'s `"end to end: a real write..."` test
      and `dev/regression`'s own rendered plan output, which now shows
      `Write(location, format=parquet)`.
- [x] **`Distinct`/`Deduplicate` translation.** Previously fell through to
      the opaque `Unsupported` placeholder. `.distinct()` doesn't change
      which columns exist or their meaning, only row cardinality, so it's
      now transparent for translation — same treatment as `GlobalLimit`/
      `LocalLimit` already had.
- [x] **`Repartition`/`Coalesce`/`RepartitionByExpression` translation.**
      Same fix, same reasoning: `.repartition(n)`, `.coalesce(n)`, and
      `.repartition(col(...))` only change physical partitioning, not
      column shape — all three are now transparent pass-throughs rather
      than falling to `Unsupported`.

#### Sub-phase: SaveMode capture, JDBC location fidelity (done)

The last two gaps found while extending `SparkPlanAdapterSpec`'s translation
coverage (see the sub-phase above for the first three).

- [x] **SaveMode capture and verification.** `ir.Write` gained a `saveMode:
      Option[String]` field; `SparkPlanAdapter` populates it from
      `InsertIntoHadoopFsRelationCommand.mode` (Spark's own `SaveMode` enum,
      normalized to `"append"`/`"overwrite"`/`"error"`/`"ignore"`). The
      contract model (`Dataset.saveMode`, parsed the same way as `format`)
      and `StructuralVerifier` (`OUTPUT_SAVE_MODE_MISMATCH`, same
      both-sides-known convention as the format check) followed the exact
      pattern the format fix established. Proven end-to-end against a real
      `spark-submit`-style write — `dev/regression`'s own rendered plan
      output now shows `Write(location, format=parquet,
      saveMode=overwrite)`, and the real demo contract
      (`demo/contracts/invariant_output.yaml`) declares `saveMode:
      overwrite` to match `PluginRunner.scala`'s actual
      `.write.mode("overwrite")` call.
- [x] **JDBC location fidelity.** `SparkPlanAdapter.locationOf` previously
      sent every non-`HadoopFsRelation` relation — including `JDBCRelation`
      — through the generic `catalogTable`/`.toString` fallback, and flagged
      it with a fallback `Diagnostic`. `JDBCRelation` is `private[sql]` in
      Spark, so it can't be named as a pattern-match type from this module;
      it's now identified by simple class name and its public
      `jdbcOptions()` accessor (returning the fully-public `JDBCOptions`)
      is fetched reflectively, giving a precise `"jdbc:<url>/<table>"`
      location with no fallback diagnostic. Proven with a real H2
      in-memory-database read (`SparkPlanAdapterSpec`'s `"translates a JDBC
      read..."` test), not a mock.

#### Sub-phase: Property-based fuzzing of the Spark adapter (done)

The first of several regression-testing guardrails identified when
assessing what "market leading" regression coverage would need beyond the
example-based suites above (property-based fuzzing, mutation testing,
golden-file snapshots of `report.json`, a multi-Spark-version compatibility
matrix, coverage gating, and API-compatibility checking — this sub-phase is
the first; the rest remain future scope).

- [x] **`SparkPlanAdapterFuzzSpec`.** `SparkPlanAdapter`'s class doc
      promises it never throws on an unrecognized construct — a promise
      only as trustworthy as what's been thrown at it. The hand-written
      `SparkPlanAdapterSpec` exercises each translated construct once, in
      isolation; it never tests combinations or nesting depth. This spec
      (ScalaCheck via `scalatestplus-scalacheck-1-17`) generates random
      chains (1-6 steps, ~200 cases/run) of the same operations —
      `Filter`, recomputed columns, `Sort`, `Aggregate`, a self-`Join`,
      `Union`, `Distinct`, `Limit`, `Repartition`/`Coalesce`, `CASE WHEN`
      — composed in random order against a real `local[*]` session, and
      asserts `translate`/`PlanPrinter.render`/`Lineage.trace` never throw,
      and that any `Unsupported` node is paired with a `Diagnostic` as
      documented. Every step preserves a fixed canonical schema (including
      through `Aggregate` and the self-`Join`, both of which re-project
      back down to it), so the generator never needs to track a live
      schema — the randomness is in plan *shape*, not in producing
      intentionally-invalid SQL.
    - Validated to actually catch regressions, not just pass by
      construction: a join-type translation was temporarily broken to
      throw, confirmed the fuzz spec failed immediately (1 case, shrunk to
      a single-step `SelfJoinStep` chain, with the full analyzed plan and
      exception in the failure message), then reverted.

#### Scope (Future)

- [ ] Dependency checks beyond dataset-level existence — `StructuralVerifier`
      covers `MISSING_INPUT`/`UNDECLARED_INPUT` already; explicit
      "forbidden" inputs (distinct from merely undeclared) and dependency
      version constraints are not modeled
- [ ] Transformation checks beyond structural (join/aggregation/filter
      semantics against contract expectations)
- [ ] Governance checks (restricted field propagation, residency, purpose)
- [ ] Compatibility checks (classify a contract change against
      downstream consumers) — note `contract`'s `ContractCompatibility`
      already does this for two contract *versions*; this is the
      separate question of whether a *transformation* respects it
- [ ] Extend `VerificationResult`'s violation vocabulary to the check
      categories above as they're implemented — its `{status, contract,
      violations}` shape (matching the Phase 4 spec) is general enough to
      carry them; only the structural violation types exist today
- [ ] Interpreting `rules` from the contract model
- [ ] Enforcement currently only gates `InsertIntoHadoopFsRelationCommand`
      writes (matching `SparkPlanAdapter`'s translation coverage); other
      write command types (JDBC sinks, streaming writes, `saveAsTable`)
      are unexercised

#### Dependencies

- Phase 1a completion (contract model)
- Phase 1b completion (transformation IR)
- Spark adapter completion (above)
- Structural verification completion (above)
- Contract-aware Spark execution completion (above)

---

## Phase 2 — Multi-Engine Support

### Objective

Extend verification engine to support multiple execution engines.

### Scope (Future)

- [ ] Transformation IR abstraction
- [ ] SQL adapter
- [ ] dbt adapter
- [ ] Engine-agnostic verification
- [ ] Adapter pattern documentation

### Dependencies

- Phase 1 completion
- Engine-specific transformation plan specifications

---

## Phase 3 — Contract Registry & Governance

### Objective

Establish contract as a versioned, governed artifact.

### Scope (Future)

- [ ] Contract registry design
- [ ] Version management
- [ ] Compatibility checking
- [ ] Impact analysis
- [ ] Governance policies

### Dependencies

- Phase 1 completion
- Contract standard finalization

---

## Phase 4 — AI & Platform Integration

### Objective

Enable AI-ready verified metadata and platform integrations.

### Scope (Future)

- [ ] Verified lineage API
- [ ] Machine-readable verification results
- [ ] OpenLineage integration
- [ ] AI agent interfaces
- [ ] Platform connectors (catalogues, lineage systems)

### Dependencies

- Phase 1, 2, 3 completion
- OpenLineage specification alignment

---

## Long-Term Roadmap

### Year 1 Goals
- Complete Phase 0-2
- Establish community
- Release v0.1.0

### Year 2 Goals
- Complete Phase 3-4
- Multi-engine support
- Platform integrations
- Release v1.0.0

### Year 3+ Goals
- Ecosystem maturity
- Industry standard adoption
- Contract registry ecosystem
- Enterprise integrations

---

## How to Contribute to This Roadmap

1. Open an issue for feature requests or concerns
2. Discuss in GitHub Discussions
3. Submit PRs to update this roadmap
4. Provide feedback on priorities

This roadmap is a living document and will evolve based on community feedback and project needs.
