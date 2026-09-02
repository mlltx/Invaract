# Invaract Roadmap

## Phase 0 — Establish the Project Foundations

### Objective

Create a credible open-source project before implementing substantial functionality.

The goal of Phase 0 is to establish the organizational, legal, and technical infrastructure required to accept external contributions without requiring architectural restructuring later.

---

### Work Items

#### 1. Repository Organization

- [x] **Repository name and organization**
  - Name: `Invaract`
  - Organization: `mlltx`
  - URL: `https://github.com/mlltx/Invaract`
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
Invaract/
├── LICENSE                          # Apache 2.0
├── CONTRIBUTING.md                  # Contribution guidelines
├── CODE_OF_CONDUCT.md              # Community standards
├── SECURITY.md                      # Vulnerability reporting
├── ARCHITECTURE.md                  # Design decisions
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
Invaract avoids redefining concepts ODCS already standardizes (schema,
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
- [x] **JSON Schema** — `contract/schema/invaract-contract.schema.json` (Draft 2020-12), the public, language-agnostic contract format spec for authoring/generating contracts outside Scala; validated against the same fixtures via `ContractSchemaSpec` so it can't silently drift from the parser/validator it documents. `demo/contracts/*.yaml` carry a `yaml-language-server` `$schema` hint for live editor validation. See docs/CONTRACT_MODEL.md's "JSON Schema" section for what it does and deliberately does not enforce.
- [x] **Documentation** — [docs/CONTRACT_MODEL.md](docs/CONTRACT_MODEL.md)

38 unit tests across parser, validator, compatibility engine, and the JSON Schema's conformance, run via `cd contract && sbt test`.

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
- [x] **Integrated with the test Spark app** — `runner/DemoJobHarness.scala`
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
- [x] Wired into `runner/DemoJobHarness.scala`: verifies the real plan's
      actual inputs/output against `demo/contracts/invaract_output.yaml`
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
Spark application → Logical plan → Invaract → PASS → execute
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
- [x] Wired into `runner/DemoJobHarness.scala`: the contract is loaded and
      the check rule installed *before* the `SparkSession` is built (a
      check rule can't be added to an already-built session); the real
      write is now the verification gate itself, not a separate step
      after it.
- [x] Live-demonstrated against the real pipeline, not just unit tests:
      running `DemoJobHarness` with a deliberately-broken contract
      (`demo/contracts/invaract_output_broken_example.yaml`, requiring a
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
      (`demo/contracts/invaract_output.yaml`) declares `saveMode:
      overwrite` to match `DemoJobHarness.scala`'s actual
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
example-based suites above: property-based fuzzing, mutation testing, and
API-compatibility checking are done (this sub-phase, the one below it, and
"API compatibility checking" further down); a multi-Spark-version
compatibility matrix and coverage gating remain future scope. (An initial
idea to add golden-file snapshots of `report.json` was reconsidered and
redirected — that file is an internal test-harness artifact with no
external consumers, not a public interface worth pinning; the JSON Schema
for the *contract* format, under Phase 1a above, was the better-scoped
version of the same underlying idea.)

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

#### Sub-phase: Mutation testing, whole-module, blocking CI (done)

The second regression-testing guardrail (see the sub-phase above for the
first). [Stryker4s](https://github.com/stryker-mutator/stryker4s) answers
a question coverage percentage can't: not "does a test execute this line"
but "does a test actually verify this line's *behavior*" — it mutates a
source file (flip `==`/`!=`, `&&`/`||`, `exists`/`forall`, delete a string
literal, ...), reruns the real suite per mutant, and reports which mutants
survived (every test still passed despite the code now being wrong).

- [x] **Wired in and widened to whole-module scope, blocking CI**:
      `ir` and `spark-adapter`, via each module's `build.sbt`
      (`strykerMutate := Seq("src/main/scala/**/*.scala")` and
      `strykerThresholdsBreak` — `stryker4s.conf`'s equivalent
      `mutate`/`thresholds` keys were observed not to take effect with
      this sbt/plugin combination, documented in both `.conf` files).
      Required bumping `sbt.version` from `1.9.8` to `1.11.7` in just
      these two modules' `project/build.properties` (Stryker4s 1.1.1
      requires sbt ≥ 1.11.2); the full test suites for both modules, the
      whole 5-module `./dev/build`, and a real `./dev/test` run were all
      confirmed unaffected by the bump. A new `mutation-testing` job in
      `.github/workflows/test.yml` runs `sbt stryker` for both modules on
      every PR (one Linux/JDK-21 leg — mutation testing measures test
      quality, not OS/JDK compatibility, so it doesn't need the full
      matrix) and uploads each module's HTML report as a build artifact;
      its failure (a module's score dropping below `break`) fails the
      `summary` gate like any other CI job.
    - `ir`: widening from the initial single-file 44.4% pass to whole-
      module scope (pulling in `PlanPrinter.scala`) initially dropped the
      score, but fixing every real (non-`StringLiteral`) survivor across
      both files — `Join`'s ambiguous-aggregation propagation, `Project`'s
      `resolveInScope` column matching, the previously wholly-uncovered
      `Aggregate`/`Window`/`Union` cases of `resolveInScope`, exact
      branch/continuation-prefix rendering, and `PlanPrinter`'s untested
      `DISTINCT`/empty-`GROUP BY`/`Sort`/`Window` branches — brought it to
      **86.36%** (76/100 mutants).
    - `spark-adapter`: widening from the initial single-file 50.0% pass to
      whole-module scope (pulling in `SparkPlanAdapter.scala` and
      `ContractEnforcementRule.scala`) dropped it to 44.79%; fixing the
      `exists`/`forall` swaps, `field.required`, and the
      `contextPrefix == "INPUT"` branch selection in `StructuralVerifier`,
      plus `ContractEnforcementRule.explain`'s violation-count
      pluralization and optional-field marking, brought it to 57.06%
      (93/177 mutants). What remained was almost entirely low-priority
      `StringLiteral` mutants on message text (79 of them), plus the same
      five real mutants in `SparkPlanAdapter.scala` tied to the
      already-documented untested-Hive-relation gap and two near-
      equivalent/unreachable-branch mutants. Rather than write ~79
      brittle exact-message-text tests, `strykerExcludedMutations :=
      Seq("StringLiteral")` now disclose-excludes that category repo-wide
      (the same exception CLAUDE.md's "Mutation Testing Requirement"
      already names as acceptable) — bringing the module to **91.53%**
      (54/59 mutants, 93.1% of covered code), above the same 70% bar
      required for new code. `strykerThresholdsBreak` moved from 50 to
      70 accordingly. See docs/SPARK_ADAPTER.md for the full breakdown.
    - Full details, caveats, and the published HTML reports: see
      docs/SPARK_ADAPTER.md's "Mutation testing" section and
      docs/TRANSFORMATION_IR.md's equivalent.
    - CLAUDE.md's "Mutation Testing Requirement" additionally requires
      70% on the specific file(s) a feature adds or changes. This was
      originally a stronger, PR-author-level bar than the whole-module CI
      gate; now that `spark-adapter`'s own whole-module break threshold is
      also 70% (see above), the two line up for that module, while `ir`'s
      whole-module gate stays at 50% even though its actual score (86.36%)
      clears 70% too. Stryker4s has no incremental/diff-scoped mode of its
      own, but this is automated in CI anyway: a "Mutation test changed
      files" step diffs against the PR's base commit and reruns
      `sbt stryker` scoped (via a brace-expansion `--mutate` glob) to just
      the changed `src/main/scala/**/*.scala` files per module, with
      `--thresholds.break 70` passed on the CLI so it doesn't disturb
      either module's own whole-module setting in `build.sbt`. Runs only
      on `pull_request` events and skips a module with no changed files.
      Verified locally both ways before landing: passes on a real
      historical multi-file `ir` diff (86.21%), and correctly fails on a
      `spark-adapter` file pair that scores below 70% together (52.9%)
      even though the whole module clears 50% (its threshold at the time)
      — see docs/SPARK_ADAPTER.md's "Incremental checking in CI"
      subsection.

#### Sub-phase: API compatibility checking, mandatory PR gate (done)

The third regression-testing guardrail (see the two sub-phases above for
the first two). Answers a different question than either: not "does the
code work" but "does this change silently break everyone who already
depends on the previous version's compiled jar" — a real risk for
`contract`/`ir`/`spark-adapter` specifically, since those are the
verification engine a real user would depend on, and Scala case classes
(`Contract`, `Dataset`, `Field`, `Plan`, `Expr`, ...) make this easy to
break by accident (a reordered constructor parameter, a field added
mid-list) with no compile error anywhere in this repository itself to
catch it.

- [x] **Wired in via [MiMa](https://github.com/lightbend/mima)
      (`sbt-mima-plugin` 1.1.4)** — the same tool Apache Spark and Akka
      use for their own public API compatibility gating. Added to
      `contract`, `ir`, and `spark-adapter` (`project/mima.sbt` in each);
      `plugin`/`runner` excluded, consistent with every other guardrail
      here being scoped to the engine, not the example harness.
    - `mimaPreviousArtifacts` in each module's `build.sbt` points at its
      own `com.example %% <module> % 0.1.0` coordinate — there's no Maven
      Central release yet to compare against, so CI's new
      `api-compatibility` job (`.github/workflows/test.yml`) publishes
      the PR's base branch to the runner's local Ivy cache under that
      exact coordinate first (`sbt publishLocal`, after building
      `contract`/`ir`'s assembly jars so `spark-adapter`'s
      `unmanagedJars`-based compile succeeds in both checkouts), then runs
      `sbt mimaReportBinaryIssues` against the PR's head — that base
      branch stands in for "the previous release."
    - **Fixed twice on real CI runs, in opposite directions.** First run:
      diffing against `github.event.pull_request.base.sha` (the PR's base
      branch) failed outright (`base-ref/contract: No such file or
      directory`) — this repo's PR #1 has been open since before
      `contract`/`ir`/`spark-adapter` existed, so its base commit predates
      those modules entirely, the same root cause the incremental
      mutation-testing check's base-diffing bug had. First fix: diff
      against `github.event.before` (the previous push's HEAD) instead,
      matching that check's fix. Second run (after applying it to a real
      source change): recognized that fix was unsound, not just imperfect
      — a sliding baseline can never durably catch a regression that's
      introduced and then never fixed (push N breaks something and fails
      correctly; push N+1, even one that touches nothing relevant, diffs
      against N, where the break already looks like the status quo, so it
      passes clean without anything having been fixed). Second fix:
      revert to the PR's base branch (a fixed anchor for the PR's
      lifetime) as the primary comparison, and instead skip a module
      gracefully when it doesn't exist yet at that base commit — the
      actually-correct fix for the original crash, applied to the right
      variable. Applied the identical reasoning to the incremental
      mutation-testing check's `github.event.before` usage too, reverting
      it to the PR's base branch for the same soundness reason (that
      check's "diff shows nothing changed" failure mode is silent, not a
      crash, but the same underlying bug).
    - Verified the detection actually works, not just that the task runs
      cleanly: temporarily removed a public method
      (`Contract.input`) locally, confirmed
      `mimaReportBinaryIssues` failed with the exact symbol and a
      ready-to-paste `ProblemFilters.exclude[...]` suggestion, then
      reverted and confirmed a clean pass against the unmodified code.
    - **Mandatory, automatic PR check**: the job runs on every
      `pull_request` event (no manual trigger needed) and feeds into the
      `summary` gate exactly like `test`/`docker-regression`/
      `mutation-testing` — a real binary break fails the PR's overall
      status, not just a standalone, ignorable check.
    - CLAUDE.md's new "API Compatibility Requirement" section documents
      the two legitimate responses to a failure (fix an accidental break,
      or add a documented `ProblemFilters.exclude[...]` entry for a
      deliberate one) and explicitly rules out the third (loosening
      `mimaPreviousArtifacts` or disabling the check).
    - Full detail in each module's doc: docs/CONTRACT_MODEL.md,
      docs/TRANSFORMATION_IR.md, and docs/SPARK_ADAPTER.md's "API
      compatibility" sections.

#### Sub-phase: Delta Lake support (done)

Closed a real, previously-unknown correctness gap: a Delta Lake write
(`df.write.format("delta").save(path)`) translated to `ir.Unsupported`
instead of `ir.Write`, so `ContractEnforcementRule` silently let it
through completely unverified. Fixed by recognizing
`SaveIntoDataSourceCommand` (zero added dependency for non-Delta users)
plus two related output-schema bugs in enforcement and reporting, found
via genuinely failing tests. Full investigation and verification detail:
docs/connectors/delta.md.

#### Sub-phase: Fail-closed on unverifiable writes (done)

Resolves the fail-open-vs-closed question the Delta Lake sub-phase above
flagged and CLAUDE.md's "Mutation Testing Requirement" section referenced
as outstanding: should a write Invaract cannot translate/verify be
*rejected* by default, rather than silently passed through the way the
original (pre-fix) Delta gap worked? User decision: yes, fail closed —
"the contract being valid when it's not" is worse than an aborted write,
for any write shape, not just Delta specifically.

- [x] **Real reflective survey before deciding the mechanism, not a
      guess**: every concrete class implementing
      `org.apache.spark.sql.catalyst.plans.logical.Command` in Spark
      3.5.1's `spark-sql`/`spark-catalyst` jars and Delta 3.2.0's
      `delta-spark` jar was enumerated (`JarFile` + `Class.forName` +
      `isAssignableFrom` against the real jars) — 164 classes found.
    - This ruled out the obvious first design ("reject any `Command` we
      don't already translate") as unsafe: `SaveIntoDataSourceCommand`
      (writes data) and `CreateDataSourceTableCommand` (schema-only
      `CREATE TABLE`, no data) implement the *exact same*
      `LeafRunnableCommand` trait — Spark's own `Command` hierarchy has no
      structural marker separating "writes data" from "pure catalog
      metadata." A blanket policy would have rejected ordinary `CREATE
      TABLE`/`ANALYZE TABLE`/`CACHE TABLE`/`SHOW TABLES`/etc. the moment a
      contract was active.
- [x] **`FailClosedCommands`** (new file): an explicit, documented
      allowlist of ~100 classes from the survey, judged by their
      documented SQL semantics not to change a table's committed row
      content (DDL/catalog/session metadata, `SHOW`/`DESCRIBE`/`ANALYZE`/
      `CACHE`, storage maintenance like `VACUUM`/`OPTIMIZE`). Matched by
      fully-qualified class name (`Set[String]`), not `classOf[...]`,
      since roughly a sixth of the list is Delta-specific and this module
      has no compile-time Delta dependency (same reasoning as the Delta
      sub-phase above).
    - `ContractEnforcementRule.verifyOrThrow` now rejects a
      `Command`-shaped, non-`ir.Write` plan unless it's on that list —
      `ContractViolationException` with a new
      `ViolationType.UnverifiableWrite`, reusing the same
      what/what/why/how `explain()` machinery every other violation gets.
    - Deliberately asymmetric: a safe command missing from the list costs
      one loud, cheap-to-fix rejection; a data-mutating command wrongly
      added would silently defeat the whole feature. Every genuinely
      data-mutating command the survey found (`DELETE`/`UPDATE`/`MERGE`,
      `LOAD DATA`, `TRUNCATE`, `DROP TABLE`/`DATABASE`, Delta's
      `RESTORE`/`CLONE`/`CONVERT TO DELTA`/etc.) was deliberately left
      *off* the safe list rather than guessed at.
- [x] **`CreateDataSourceTableAsSelectCommand` added as a real recognized
      write** (`.saveAsTable(...)`/`CREATE TABLE ... AS SELECT` against a
      *new* V1 data source table) — a third distinct write shape found by
      the same survey, previously falling through to `Unsupported` exactly
      like the pre-fix Delta gap. `SparkAdapterListener` updated to
      capture it too (report.json), same as the Delta fix required.
- [x] **Verified end to end**: new PASS/FAIL enforcement pair for
      `.saveAsTable()` and a translation test (mirroring the Delta/Parquet
      pattern), a fail-closed test proving a real, concrete unrecognized
      write (Delta SQL `MERGE INTO`, confirmed via the survey to analyze
      to `org.apache.spark.sql.delta.commands.MergeIntoCommand`) is
      rejected with `UnverifiableWrite` *before* touching the target
      table (asserted via byte-identical rows before/after the aborted
      merge), and a regression test proving `CREATE TABLE`/`ANALYZE
      TABLE`/`SHOW TABLES` are never blocked under an active contract
      that would reject anything it actually checked. Full suite 59/59
      passing; mutation testing (whole-module, since every changed/added
      file was touched) scored 91.67%/93.22%, up from 91.53%/93.1%, with
      every mutant the new code introduced killed — the same 5
      pre-existing, already-documented survivors account for 100% of
      what's undetected; `mimaReportBinaryIssues` clean.
    - Full detail in docs/SPARK_ADAPTER.md's "Fail-closed on unverifiable
      writes" section, including the complete per-category reasoning for
      the safe list and what's deliberately left off it.

#### Sub-phase: Reusable process for adding a Spark connector (done)

Delta Lake support was built twice — once for `.save(...)` writes, again
separately for `.saveAsTable(...)` and the fail-closed policy — because
the first pass didn't survey the connector's full operation surface up
front. Rather than let the next connector (Iceberg, ClickHouse, Avro, ...)
repeat that, the investigation methodology that eventually got Delta
right (probe with `injectCheckRule`, reflectively survey the connector's
`Command` classes, classify every one found, verify rather than assert)
is now written up as a repeatable process, with an interactive Claude
Code skill that runs it.

- [x] **docs/ADDING_A_SPARK_CONNECTOR.md**: the durable design doc — a
      "Definition of done" checklist (every read/write path investigated,
      not assumed; every `Command` class the connector's jar defines
      classified, not guessed at; zero added dependency for non-users
      verified by jar inspection, not asserted; mutation testing/MiMa/
      `./dev/test`/`./dev/regression` all actually run), the exact
      investigation methodology (dependency scoping, dual-extension-point
      probing, reflective survey, classification rules), and a "Known
      limitations" section naming what this pattern doesn't yet solve for
      any connector (row-level DML has no IR representation yet;
      streaming is unexplored; DataSourceV2 catalog writes are a
      recurring gap worth solving once, not per-connector).
- [x] **`add-spark-connector` Claude Code skill**
      (`.claude/skills/add-spark-connector/SKILL.md`): the same process as
      an interactive, ordered workflow — 10 phases from scoping the
      connector through a final Definition-of-Done review, with explicit
      checkpoints (⏸) requiring user sign-off before the fail-closed
      classification is implemented and before the connector is called
      done. Deliberately does not duplicate the doc's prose — points back
      to the relevant section at each phase, per the progressive-
      disclosure pattern Claude Code skills use.
    - Cross-linked from CLAUDE.md's doc index and References section so
      "add support for X" doesn't get a one-off `translatePlan` case
      again instead of the full survey.

#### Sub-phase: Write command recognition, a single registry (done)

Before shipping the process above for a second/third connector, reviewed
the code it would actually walk a contributor through — and found the
process itself was compensating for a real structural problem: "is this
plan a write, and what does it mean" was implemented three separate
times (`SparkPlanAdapter.translatePlan`, `ContractEnforcementRule.verifyOrThrow`'s
output-schema derivation, `SparkAdapterListener.onSuccess`'s capture
check), independently kept in lockstep by hand. Both of this session's
real Delta bugs were exactly a write shape added to one match and missed
in another — not one-off mistakes, a hazard built into the duplication
itself. Fixed the foundation before layering more connectors onto it.

- [x] **New `WriteCommandSupport.scala`**: one
      `PartialFunction[LogicalPlan, WriteCommandInfo]` per recognized
      write shape (the same three as before — `InsertIntoHadoopFsRelationCommand`/
      `SaveIntoDataSourceCommand`/`CreateDataSourceTableAsSelectCommand`),
      combined via `orElse` into `WriteCommandSupport.combined`.
      `WriteCommandInfo` bundles location/query/format/saveMode/
      outputSchema in one value, so a write shape can no longer be added
      with its schema piece missing the way the original Delta bug did —
      the compiler enforces supplying it.
    - `SparkPlanAdapter.Translator.translatePlan`,
      `ContractEnforcementRule.verifyOrThrow`, and
      `SparkAdapterListener.onSuccess` all now consult exactly this
      registry (`.lift`/`.isDefinedAt`) instead of a match of their own.
      Adding a write shape (when one is actually needed — most connectors
      need none, per the Delta finding) now means: implement one
      `PartialFunction` here, chain it in. Nothing else changes.
- [x] **Verified behavior-preserving, not just re-tested**: full 59-test
      suite passed unchanged before and after (identical translation
      output for every existing case); `mimaReportBinaryIssues` stayed
      clean; `./dev/build`/`./dev/test`/`./dev/regression` all still pass
      against real `spark-submit`.
    - Mutation testing surfaced one real, new gap the refactor itself
      introduced: `SparkAdapterListener.onSuccess`'s `isDefinedAt` check
      surviving an "always capture" mutant, because no existing test
      asserted the *negative* case (a non-write action leaving
      `lastWrite` untouched — every prior listener test only checked "a
      write is captured"). Closed with a new test rather than left
      undetected. Final score 91.94%/93.44% (up from 91.53%/93.1% before
      any of this session's spark-adapter changes), same 5 pre-existing
      survivors as always, none in new code.
- [x] **docs/SPARK_ADAPTER.md, docs/ADDING_A_SPARK_CONNECTOR.md, and the
      `add-spark-connector` skill's Phase 6 all updated** to describe the
      one-file-one-list story instead of the old three-file one — the
      skill now explicitly says most connectors need zero
      `WriteCommandSupport` entries at all.

#### Sub-phase: Delta Lake reads (done)

Investigated whether reads shared the write side's "recognition
duplicated across independent sites" bug. They don't: both consumer
sites already match Delta's read shape (`LogicalRelation` wrapping
`HadoopFsRelation`) through the same existing code path, so they can't
disagree by construction — zero production code changed. Full detail:
docs/connectors/delta.md.

#### Sub-phase: Delta Lake operation-surface coverage ledger (done)

Prompted by a user question ("is Delta 100% supported?") that exposed a
real process gap: two prior Delta passes had each declared success on a
narrower scope than "does Delta work end to end." Fixed at the process
level first (docs/ADDING_A_SPARK_CONNECTOR.md's operation-surface
checklist and mandatory coverage ledger), then closed every row for
Delta specifically — V2 write commands confirmed to fail closed
correctly, time-travel and CDC reads translated, and a real gap found:
streaming writes had zero enforcement touchpoint at all (closed in the
sub-phase below). Full ledger: docs/connectors/delta.md.

#### Sub-phase: Streaming writes closed; fail-closed reframed as a
#### stopgap, not a verdict (done)

Two changes. Closed the streaming-write gap for real: `WriteToStream`
became a genuine `WriteCommandSupport` entry (translated and verified,
not merely gated), including a reflective fallback for `DeltaSink`'s
path accessor since its `name()`/`schema()` throw unconditionally.
Separately corrected the skill's own framing: a 🚫 "fails closed" ledger
row had been treated as an acceptable resting state rather than a safety
net for translation work not yet done — `docs/ADDING_A_SPARK_CONNECTOR.md`
and the skill now require every such row to state a concrete next step.
Full detail: docs/connectors/delta.md.

#### Sub-phase: Remaining Delta write-side gaps closed; streaming reads
#### recognized as contract inputs (done)

Closed every remaining 🚫 row the corrected ledger could state a next
step for: `AppendData`/`OverwriteByExpression`/`ReplaceTableAsSelect` all
became real `WriteCommandSupport` entries. Found and fixed a real
correctness trap along the way — atomic CTAS/RTAS issues a second,
nested write against a `StagedTable`, which reported no location,
producing two disagreeing locations for one commit — documented as a
shared pitfall for future connectors. Also recognized streaming reads
(`StreamingRelation`/`StreamingRelationV2`) as contract inputs, closing a
real false-positive `MISSING_INPUT`. Full detail: docs/connectors/delta.md.

#### Sub-phase: Row-level DML — structural verification, the last
#### coverage-ledger row closed (done)

Closed the final 🚫 row in the Delta ledger with a deliberately scoped
subset: `MergeIntoCommand`/`UpdateCommand`/`DeleteCommand` recognized via
reflection into Delta's internal classes, with structural-only
verification (target location/schema) — the merge condition, updated
columns, and delete predicate itself remain unchecked, since neither the
IR nor the contract model has vocabulary for them yet (tracked under
"Scope (Future)" below). Found and fixed a real second correctness trap:
MERGE's `source` wasn't reachable by the existing input-schema walk at
all, since Delta's DML commands don't expose it as a plan child. Full
detail: docs/connectors/delta.md.

#### Sub-phase: Delta feature-by-feature confidence pass (done)

Every write-command *shape* being covered left a separate, previously-
implicit gap: several Delta table *features* (schema evolution, generated
columns, deletion vectors, column mapping, liquid clustering, CHECK
constraints, identity columns) were "expected to work," never actually
exercised. Tested each for real: found and fixed two genuine
false-rejection bugs (schema evolution under `autoMerge`, generated
columns' values never appearing in any DataFrame-facing schema);
confirmed deletion vectors/column mapping/liquid clustering transparent
and CHECK constraints orthogonal; identity columns confirmed untestable
in this Spark version (parser rejects the syntax outright). Full detail:
docs/connectors/delta.md.

#### Sub-phase: Iceberg connector support (done)

Second connector onboarded via the `add-spark-connector` skill's
10-phase process. Found and closed two real, connector-agnostic gaps
that predate Iceberg entirely (batch `DataSourceV2Relation` reads had no
translation case at all; `CreateTableAsSelect`/`OverwritePartitionsDynamic`
had no write-recognition case). Row-level DML closed via Iceberg's
standard `SupportsRowLevelOperations` API — no reflection needed, unlike
Delta's proprietary classes, and connector-agnostic for any future
connector using the same API. `CALL` procedures deliberately left
unmodeled pending per-procedure classification (closed in later
sub-phases below). Full findings, ledgers, and mutation-testing
breakdown: docs/connectors/iceberg.md.

#### Sub-phase: Iceberg's own schema-evolution row closed, generalizing
#### the Delta generated-columns fix (done)

Follow-up on Iceberg's one remaining ❓ feature-surface row. The predicted
bug didn't exist; the real one was the opposite direction — a narrower
append accepted under `write.spark.accept-any-schema` wrongly rejected a
contract-satisfying write. Generalized Delta's Delta-specific reflective
generated-columns fix into a connector-agnostic
`outputSchemaWithTargetOnlyFields`, since both were the same underlying
situation (a target field the query doesn't supply) under two different
mechanisms. Also includes a mutation-testing wall-clock investigation
that tried two real Spark-config levers and found neither was the actual
bottleneck (genuine per-commit table I/O is). Full detail:
docs/connectors/iceberg.md.

#### Sub-phase: Iceberg's last two ❓ feature-surface rows closed —
#### deletion vectors, identity/generated columns (done)

Closed with real probes and permanent tests, zero production code
changes — both were genuine questions with genuine answers. Deletion
vectors (Iceberg's V3 merge-on-read spec) still produce a plain
`ReplaceData` node, already matched. Identity/generated columns have no
Spark-SQL-reachable equivalent in this Iceberg integration at all
(rejected outright by Spark's own analyzer). Both of Iceberg's
feature-surface ledger rows now closed. Full detail:
docs/connectors/iceberg.md.

#### Sub-phase: Iceberg CALL procedure classification — 10 of 20
#### procedures reclassified from wrongly-rejected to correctly-allowed
#### (done)

Closed the one remaining Iceberg operation-surface row
(`🚫 Fails closed, deliberately left unmodeled`). Enumerated all 20 real
procedure classes from the jar directly; 10 reclassified safe
(compaction/GC/stats/read-only, the same category as Delta's `OPTIMIZE`/
`VACUUM`), 10 stay deliberately unmodeled (change what's "current," or
add/reformat row content). Found and closed a real mutation-testing
coverage gap (the reflection fallback had zero coverage) and a real,
pre-existing CI failure unrelated to this pass's own code (Iceberg's jar
can't load under the JDK 11 CI leg — fixed by excluding the dependency
under JDK <17). Full detail: docs/connectors/iceberg.md.

#### Sub-phase: CI mutation-testing wall-clock cut via two real levers (done)

Found while the user watched CI's mutation-testing job run 35-40+
minutes on the CALL-classification PR - a different question from the
earlier *local* `sbt test` speed investigation (session reuse, shuffle
partitions, codegen), which found no real lever there.

- [x] **`--concurrency` works as an explicit CLI flag** (confirmed via
      the "Creating N test-runners" log line changing from 2 to 4), even
      though the equivalent `stryker4s.conf`/`build.sbt` settings don't
      take effect with this plugin version - the same category of quirk
      already documented for `--mutate`/`--thresholds`. Added to every
      `sbt stryker` invocation in CI.
- [x] **Split the single `mutation-testing` CI job into two parallel
      jobs** (`mutation-testing-ir`, `mutation-testing-spark-adapter`):
      independent modules, independent test suites, no reason to
      serialize them on one runner. Cuts wall-clock from
      `ir_time + spark_adapter_time` to roughly
      `max(ir_time, spark_adapter_time)`. `ir` has zero Spark dependency
      (confirmed via `ir/build.sbt`), so its job also drops the entire
      Spark cache/download/configure sequence `spark-adapter`'s job still
      needs. Updated `summary`'s `needs:` list and result-check condition
      to the two new job names.
- [x] **Honestly scoped what these levers don't fix**: `spark-adapter`'s
      whole-module run (~30-40 min) is still the real cost - genuine work
      (the full suite once per mutant against real Delta/Iceberg-backed
      Spark sessions), not overhead. Also corrected a stale
      ARCHITECTURE.md estimate (`~1-5 min`) that never matched the real
      observed time, unrelated to today's split.

#### Sub-phase: Verify `rollback_to_snapshot` against a contract — pilot,
#### with a mid-course design correction (done)

Piloted real contract verification for one of the 10 state-changing CALL
procedures left fail-closed above. The original design (checking the
rolled-back-to snapshot's own historical schema) was proven wrong by a
real end-to-end test, not just buggy — corroborated against Iceberg's own
open issue tracker (apache/iceberg#15165): rollback moves which
snapshot's data is current but never reverts the schema. Corrected to
check the table's *current* schema plus location as a scoping gate
instead. Full detail: docs/connectors/iceberg.md.

#### Sub-phase: Extend state-changing CALL verification to the 5
#### procedures sharing `rollback_to_snapshot`'s shape (done)

Investigated each procedure's real argument shape via `javap` and a live
probe before generalizing rather than assuming — most share
`rollback_to_snapshot`'s 2-arg shape, `set_current_snapshot` and
`fast_forward` declare 3. Generalized the pilot's single hardcoded
procedure-class check into a `Map[String, String]` rather than branching
per procedure; extraction/verification/wiring stayed unchanged. Full
detail: docs/connectors/iceberg.md.

#### Sub-phase: Verify the remaining 4 harder CALL procedures —
#### `add_files`, `migrate`, `snapshot`, `rewrite_table_path` (done)

Closed the last operation-surface gap in Iceberg's CALL coverage. Real
investigation found the original scoping assumption (all 4 need harder
argument-schema parsing) wrong for 3 of 4: `add_files`/`migrate` fit the
existing mechanism unchanged; `rewrite_table_path` needed no
verification at all and joined the safe list instead. `snapshot` is
genuinely different — it creates a new table from a different source
table — and needed real new catalog-identifier resolution. All 20
Iceberg CALL procedures now have a permanent, evidenced disposition.
Full detail: docs/connectors/iceberg.md.

#### Sub-phase: Parquet connector support (done)

Third connector onboarded — a different shape than Delta/Iceberg, since
Parquet is Spark's own built-in `FileFormat`: nothing added to
`build.sbt`. Found and fixed a real bug affecting every plain-`FileFormat`
streaming sink (Parquet, CSV, JSON, ORC, text): `FileStreamSink.path` is
a private field with no public accessor, so every such streaming write
fell through to an unreachable `toString` location, unconditionally
failing enforcement — caught by a mutation-testing "equivalent mutant"
signal prompting re-investigation of an initially-wrong first fix.
Confirmed several operations (time travel, row-level DML, DSv2-catalog
writes onto an existing table) are rejected by Spark itself for plain
Parquet — the first connector where 🚫 rows are genuinely N/A rather than
future work. Full findings and both ledgers: docs/connectors/parquet.md.

#### Sub-phase: Parquet's last two ❓ feature-surface rows closed —
#### rebase mode, storage optimizations (done)

Same-day follow-up. Both closed with real probes and permanent tests,
zero production code changes: legacy timestamp/date rebase mode never
affects a field's declared type or nullability; writer-side storage
optimizations (bloom filters, dictionary encoding) are resolved entirely
below the `LogicalPlan` level this adapter translates at. Both of
Parquet's coverage ledgers now fully closed. Full detail:
docs/connectors/parquet.md.

#### Sub-phase: CSV connector support (done)

Fourth connector onboarded — same shape as Parquet's (Spark's own
built-in `FileFormat`, nothing added to `build.sbt`). Confirmed CSV's
operation surface routes through the exact same generic mechanisms
Parquet's pass proved out, with zero bugs and zero `src/main/scala`
changes — a pure confirmatory audit. The real substance was the feature
surface: six CSV-specific behaviors (schema-inference default to
`StringType`, header handling, malformed-record modes, the corrupt-record
column, always-nullable read-back, custom date parsing) confirmed with
real probes and codified into permanent tests. Full findings and both
ledgers: docs/connectors/csv.md.

#### Sub-phase: Hive connector support (done)

Fifth connector onboarded, against a real Hive-enabled session (embedded
Derby metastore, no external Hive install). Unlike Parquet/CSV's
confirmatory passes, found the operation surface genuinely under-covered:
`HiveTableRelation` reads had no translation case at all (not just an
imprecise location — never counted as a read), fixed with zero new
dependency. A reflective jar scan found three genuinely new write
commands, one (`InsertIntoHiveDirCommand`) findable only by the scan, not
by exercising the standard operation-surface checklist. Found and fixed
a static-partition `INSERT` false-rejection bug (a third, independent
instance of the "target-only field" pattern). Left one real gap open,
not fixed, with a standing regression test proving it: a path-less new
Hive table's two nested write commands disagree on location. Full
findings and both ledgers: docs/connectors/hive.md.

#### Sub-phase: Avro connector support (done)

Sixth connector onboarded. `spark-avro` added as the first real
`% "test"` dependency since Delta/Iceberg/Hive (unlike Parquet/CSV, Avro
isn't bundled into `spark-sql`). A reflective jar scan found zero
`Command`-shaped classes — the whole operation surface routes through
existing generic mechanisms. Found and fixed a real, general
(not Avro-specific) `WriteCommandSupport` bug: path-less new-table
creates computed two disagreeing locations, benefiting every V1-format
connector. Also surfaced a real `contract`/`ir` type-vocabulary gap (no
parametrized decimal type) via Avro's decimal logical type — deliberately
left unfixed as out of scope for a spark-adapter connector pass,
documented with a next step. Full findings and both ledgers:
docs/connectors/avro.md.

#### Sub-phase: ClickHouse connector support (done, with real remaining
#### ❓ rows)

Seventh connector onboarded. Originally scoped as BigQuery; redirected to
ClickHouse at Phase 0 since BigQuery has no local/offline testing mode.
Built a novel test-infrastructure mechanism — a real standalone
`clickhouse` server binary launched as a subprocess, since Docker
couldn't run in the onboarding sandbox — Linux fully verified, macOS
provisioned but not runtime-verified, Windows excluded (no native
server). Fixed a JDK 21/Arrow 12.0.1 incompatibility (general beyond
ClickHouse) via a test-scope dependency override. Added one genuinely
new, connector-agnostic write case (`DeleteFromTable`, via Spark's
`SupportsDelete` mechanism). Found and documented a real read/write
location-format asymmetry as a next step rather than a bug to fix this
pass. Unlike every prior connector, left real ❓ rows honestly recorded
(streaming read, existing-table `.saveAsTable()`, maintenance operations,
richer type system) given ClickHouse's substantially larger surface. Full
findings and both ledgers: docs/connectors/clickhouse.md.

#### Sub-phase: ClickHouse's 4 remaining ❓ operation-surface rows closed (done)

Follow-up pass closing streaming read, existing-table `.saveAsTable()`,
maintenance operations, and the richer type system — zero
`spark-adapter/src/main/scala` changes, every finding a confirmed-
transparent behavior or a confirmed permanent connector limitation.
Streaming read and maintenance operations (`OPTIMIZE`/`ALTER TABLE ...
DELETE`/`VACUUM`) both confirmed genuinely unreachable through Spark SQL
with this connector — the latter fails at Spark's own parser, since the
connector installs no SQL extension for them, unlike Delta's. Existing-
table `.saveAsTable()` confirmed to produce the already-covered
`AppendData` shape. Type system split into three real findings:
`Array`/`Map`/`Struct` fully transparent (round-trip write/read/contract
verification all pass); `LowCardinality` confirmed transparent on read
and confirmed not requestable from Spark on write (verified against the
connector's own decompiled `SchemaUtils` class and the real server's
`system.columns`, not assumed); `Nested` — Spark's `ARRAY<STRUCT<...>>`
produces `Array(Tuple(...))`, a distinct type from ClickHouse's own
`Nested`, a real distinction worth documenting rather than a bug. A
bonus finding, deliberately left unfixed as out of scope for a connector
pass: a contract missing `outputs:` crashes `StructuralVerifier` with an
unguarded `NoSuchElementException` instead of failing cleanly —
root-caused to `ContractEnforcementRule`'s runtime path never calling
the `ContractValidator.validate` check that already exists and would
catch it, pinned with a permanent test in `StructuralVerifierSpec`
rather than left as conversation history. Operation-surface ledger now
fully closed (no ❓ rows remaining); feature surface has three remaining,
honestly-scoped ❓ rows (compression/`PARTITION BY`/replicated engines/
materialized views, TTL-driven expiry, whether reading a genuinely
pre-existing `Nested` column round-trips). `ClickHouseConnectorSpec`: 21
tests (up from 15), full `spark-adapter` suite 234/234 passing,
`mimaReportBinaryIssues` clean, `./dev/build`/`./dev/test` pass against
real `spark-submit`. Full findings and both ledgers:
docs/connectors/clickhouse.md.

#### Sub-phase: fixed the bonus contract-validation finding; closed ClickHouse's last 3 feature-surface ❓ rows (done)

Two follow-ups to the sub-phase above. **Fixed, not left pinned**: a
contract missing `outputs:` now fails cleanly instead of crashing
`StructuralVerifier` with an unguarded `NoSuchElementException` —
`ContractEnforcementRule.verifyOrThrow` now calls
`ContractValidator.validate` before checking a write or state-changing
`CALL`, throwing a clean `ContractViolationException` (new
`ViolationType.InvalidContract`) instead. Scoped to just the two
branches that actually consult the contract, not the top of the method:
`verifyOrThrow` runs for every plan `injectCheckRule` sees, not just
writes, so an unconditional guard broke ordinary reads/transformations
the moment an invalid contract was merely active — caught by a real test
failure before landing, not assumed safe. Mutation-tested scoped to both
changed files: 100% (45/45 non-excluded mutants killed).

**Closed ClickHouse's last 3 ❓ feature-surface rows**, zero further
`src/main/scala` changes: `PARTITION BY` confirmed real via Spark's
native `PARTITIONED BY` clause (not a `TBLPROPERTIES` key), with
`primary_key`/`sample_by` found and confirmed working the same way, both
decompiled directly from `ClickHouseCatalog` rather than guessed.
Replicated engines confirmed genuinely supported by the connector (a
dedicated `ReplicatedMergeTreeEngineSpec` class exists in its jar) but
not verifiable end to end in this single-node standalone-binary test
environment (`NO_ELEMENTS_IN_CONFIG` — no Keeper/shard-macro config) —
marked environment-limited, not forced into a false ✅ or 🚫. Materialized
views confirmed unreachable through Spark SQL, the same pattern as
maintenance operations. Compression clarified: the connector's
`spark.clickhouse.{read,write}.compression.codec` configs (decompiled
from `ClickHouseSQLConf`) control wire-transfer compression only, never
a column's real storage codec. TTL confirmed inert, the same
"accepted-but-not-applied" pattern as `LowCardinality`. Reading a
genuinely pre-existing `Nested` column (created via raw SQL passthrough)
confirmed to surface as two separate top-level dotted-name `ArrayType`
Spark columns (`"items.name"`/`"items.count"`), not a real nested
struct — ClickHouse's own internal Nested representation passing through
unflattened.

Both operation-surface and feature-surface ledgers are now **fully
closed** — no ❓ rows remaining in either, the only connector so far to
reach that state on both axes. `ClickHouseConnectorSpec`: 28 tests (up
from 21), full `spark-adapter` suite 241/241 passing,
`mimaReportBinaryIssues` clean, `./dev/build`/`./dev/test` pass against
real `spark-submit`. Full findings and both ledgers:
docs/connectors/clickhouse.md.

#### Sub-phase: Interpreting `rules` — first slice, DML rule verification
#### (done)

Closed the first item of the two the "Full semantic DML verification"
scope item below used to require together before either could start:
`rules` was recorded since Phase 1a but never interpreted. Rather than
build the full row-level-DML semantic model (merge condition predicate
logic, which specific rows an UPDATE touches) in one pass, this closes a
concrete, narrower slice: three rule types checked against a real Spark
MERGE/UPDATE/DELETE, chosen because the connector work above (Delta's row-
level DML, "structural verification, the last coverage-ledger row closed"
sub-phase) already left exactly this gap open, named and documented, not
discovered fresh.

- [x] **`contract`: typed rule vocabulary.** `RuleType`
      (`merge_condition`/`forbid_unconditional_delete`/
      `allowed_update_columns`) and `InterpretedRule`, plus
      `ContractRule.interpret: Option[InterpretedRule]` — decodes a raw
      `ContractRule`'s `properties` bag into one of the three shapes when
      well-formed, `None` for an unrecognized type or a known type with
      malformed properties (`ContractValidator` reports the latter as an
      `Error`, so an interpretable rule reaching enforcement is guaranteed
      well-formed). `Contract.rules: List[ContractRule]` itself is
      unchanged — additive, not a redesign.
    - Found and fixed a real bug before it ever shipped, not assumed
      correct: the rule vocabulary's own example (`on: [customer_id]`)
      hits YAML's "Norway problem" — SnakeYAML's default (YAML 1.1)
      resolver treats the bare key `on` as the boolean `true`, so
      `properties.get("on")` silently returned `None` for every contract
      authored with an unquoted `on:` key, confirmed by a real failing
      test. Fixed by using `columns` instead (matching
      `allowed_update_columns`'s own key), not by fighting the resolver.
    - Found and fixed a real MiMa binary-compatibility break, not
      pre-empted by guessing: giving `ContractRule` its first hand-written
      companion object (to hold `interpret`) silently dropped the
      compiler-synthesized `extends AbstractFunction2` — and with it,
      `tupled`/`curried` — that a case class with no user-written
      companion gets automatically. `mimaReportBinaryIssues` caught it
      against the 0.1.0 baseline; fixed by declaring that exact
      `extends AbstractFunction2[...]` explicitly, not a
      `ProblemFilters` exclusion.
    - 7 new tests (`ContractParserTest`/`ContractValidatorTest`, 45 total
      up from 38): a well-formed rule of each of the three types decodes
      correctly; a known type with malformed/missing properties decodes to
      `None` and is a `ContractValidator` `Error`; an unrecognized rule
      type is untouched (still recorded, not flagged).
- [x] **`ir`: `RowMutation`/`DeleteScope`, a wholly new standalone type,
      not a field on `Write`.** Captures exactly what the three rules
      need: a MERGE's `ON` condition as a full `Expr` (so a verifier can
      read the columns it references), whether/how an operation deletes
      rows (`DeleteScope`: `NotApplicable`/`Unconditional`/
      `Conditional(condition)` — a sealed trait, not `Option[Expr]`, since
      "no delete" and "deletes unconditionally" are both real states a
      bare `Option` can't distinguish), and a standalone UPDATE's assigned
      column names. Deliberately not a new field on the existing `Write`
      case class — adding one would change its constructor's arity, a
      binary-incompatible change to an already-published signature;
      `RowMutation` has no such history to break. 4 new tests
      (`RowMutationSpec`, `ir` module 42 total up from 38); 0 Stryker
      mutants generated for the file (pure data — no conditionals to
      mutate — not a coverage gap).
- [x] **`spark-adapter`: `RowMutationSupport` + `RuleVerifier`, wired into
      `ContractEnforcementRule`.** `RowMutationSupport` is a separate,
      parallel extractor over the same Delta
      `UpdateCommand`/`DeleteCommand`/`MergeIntoCommand` classes (plus
      DSv2's `DeleteFromTable`) `WriteCommandSupport` already recognizes —
      not a change to it, mirroring `StateChangingCallSupport`'s existing
      relationship to `WriteCommandSupport`, for the identical MiMa
      reason `RowMutation` itself is standalone. `RuleVerifier` checks the
      three rule types against the extraction; violations are appended to
      `StructuralVerifier`'s inside `ContractEnforcementRule.verifyOrThrow`,
      so a rule violation gets the same abort-before-any-data-is-written
      guarantee and four-part `explain()` treatment every other violation
      type gets.
    - `updatedColumns` detection is grounded in Delta 3.2.0's own source
      (`PreprocessTableUpdate.toCommand`/
      `UpdateExpressionsSupport.generateUpdateExpressions`), not assumed:
      confirmed that an untouched column's `updateExpressions` entry is
      literally Delta's own `target.output` attribute passed through
      unchanged, so comparing each pair via `semanticEquals` is exactly
      "did this column's value expression change."
    - `merge_condition`'s check is a documented structural approximation,
      not full predicate-logic verification: it checks that every
      declared column is *referenced* by the MERGE's `ON` condition, not
      that those are its only columns or that each forms a genuine
      `target.col = source.col` equality pair — enough to catch the real
      bug this rule guards against (a MERGE silently missing a match key)
      without false-rejecting a condition with additional legitimate
      predicate terms. **Superseded** — see "Sub-phase: Predicate-logic
      `merge_condition`" below, which replaces "referenced anywhere" with
      genuine equality-pairing detection.
    - Deliberately does **not** cover Iceberg's (or any DSv2
      `SupportsRowLevelOperations` connector's) MERGE/UPDATE/DELETE —
      `ReplaceData`/`WriteDelta` are Spark's own *rewritten* form of the
      operation, not a shape that still carries a clean condition/
      assigned-columns fact the way Delta's own command classes do;
      recovering those is real, unstarted work, not attempted here.
    - 18 new tests: `RuleVerifierSpec` (12, pure Scala, no Spark session —
      every rule type's inapplicable/PASS/FAIL case, plus an unrecognized/
      malformed rule contributing no violations) and 6 new
      `ContractEnforcementRuleSpec` cases (a real PASS and a real FAIL per
      rule type against a live Delta session — every FAIL case asserts
      the target table's rows are byte-identical before and after the
      aborted attempt, the same discipline every other enforcement test
      in this file uses). Full `spark-adapter` suite (all 7 connectors,
      including a real ClickHouse subprocess server): 259/259 passing, up
      from 241, zero regressions.
    - Mutation testing scoped to the 5 changed/added files
      (`RowMutationSupport.scala`, `RuleVerifier.scala`,
      `StructuralVerifier.scala`, `ContractEnforcementRule.scala`,
      `SparkPlanAdapter.scala`): **94.38%** (of total) / **95.45%** (of
      covered code) — 84/89 non-excluded mutants killed. All 5 survivors
      are the same pre-existing, already-documented `SparkPlanAdapter.scala`
      survivors this repo's mutation-testing history already names (JDBC
      near-equivalence, an unreachable branch under this Spark version,
      the no-metastore-available Hive fallback) — none in any new code
      this sub-phase added.
- [x] `mimaReportBinaryIssues` clean for all three modules (after the
      `ContractRule` fix above); `./dev/build`/`./dev/test`/
      `./dev/regression` all pass against real `spark-submit` — the demo
      pipeline's own `PASSED (invaract_demo_output@1.0.0)`, and the
      regression pack's 2/2 cases, both unaffected by this change.

Deliberately still open, not attempted here: the merge condition's actual
predicate logic (only *referenced* columns are checked, not a genuine
equality pairing) — since closed, see "Sub-phase: Predicate-logic
`merge_condition`" below — which specific rows an UPDATE touches, whether a
DELETE's predicate is trivially satisfiable, Iceberg/DSv2
`SupportsRowLevelOperations` connectors — since closed, see "Sub-phase:
Iceberg DML rule support" below — and any rule vocabulary beyond these
three types (governance, compatibility, richer transformation checks) —
all still tracked below, now with one fewer prerequisite blocking them.

#### Sub-phase: Iceberg DML rule support, and failing closed on an
#### unverifiable rule instead of silently skipping it (done)

Two follow-ups to "Interpreting `rules`" above, both prompted directly by
a user question about what the three DML rules could and couldn't cover
yet: Iceberg was a real, named gap (`RowMutationSupport` covered only
Delta's command classes and DSv2's plain `DeleteFromTable`), and the
existing design had a real silent-failure mode — a contract declaring a
DML rule against an operation this module recognized as a write but
couldn't extract DML-specific facts for would execute with no violation
and no protection, indistinguishable from "the rule doesn't apply here."

- [x] **Real investigation before writing extraction code, not
      assumption.** A throwaway probe (since deleted) against a live
      Iceberg 1.11.0 session, capturing real `ReplaceData`/`WriteDelta`
      plans for MERGE/UPDATE/DELETE under both copy-on-write and
      merge-on-read, plus reading Spark 3.5.1's own
      `RewriteDeleteFromTable`/`RewriteUpdateTable`/`RewriteMergeIntoTable`
      source. Found the original "not a form that carries a clean fact to
      extract" judgment was too pessimistic for two of the three rules:
      `RowLevelWrite.condition` (a real, stable, public field — no
      reflection needed, unlike Delta) is exactly the original predicate
      for both DELETE and MERGE, confirmed against real captured plans,
      not assumed from the doc comments. `RowLevelWrite.operation.command()`
      reliably reports MERGE/UPDATE/DELETE, letting classification stay
      accurate even where extraction isn't (see the fail-closed item
      below). UPDATE's assigned columns *are* extractable for
      copy-on-write's `ReplaceData` (every target column is wrapped
      `If(matchCondition, assignedExpr, originalAttr)`, confirmed via the
      same probe — a column changed iff its `If`'s branches aren't
      semantically equal) but genuinely not for merge-on-read's
      `WriteDelta` (an `Expand`-based rewrite with no equivalent
      per-column pairing) — a real, permanent gap, not a shortcut taken
      to save time.
- [x] **`merge_condition`/`forbid_unconditional_delete`: full Iceberg
      support, both write strategies.** `allowed_update_columns`:
      copy-on-write UPDATE supported; merge-on-read UPDATE deliberately
      unextractable — see the fail-closed item below for what happens
      instead of a silent pass.
- [x] **Fail closed on "recognized as DML, but unverifiable" instead of
      silently skipping the rule.** `RowMutationSupport.classify` now
      returns one of three things, not two: `None` (not row-level DML at
      all — a rule is genuinely inapplicable, the common case), `Extracted(kind,
      mutation)` (the existing path), or `Unverifiable(kind)` — genuinely
      `kind`-shaped DML this module couldn't extract facts for. Two real
      cases reach it: Delta's reflection failing on a hypothetical future
      version-renamed method (already existing, only now distinguished
      from "not a rule this module checks" instead of being silently
      swallowed alongside every other reason `combined` used to return
      `None`), and Iceberg's merge-on-read UPDATE, above. A new
      `RULE_UNVERIFIABLE_DML` violation type gets the same
      abort-before-any-data-is-written treatment as every other
      violation — `ContractEnforcementRule.verifyOrThrow` only raises it
      when `RuleVerifier.appliesTo(rule, kind)` says the active contract
      actually declares a rule relevant to `kind`, so an operation kind a
      contract simply doesn't constrain (e.g. a merge-on-read UPDATE under
      a contract that only declares `forbid_unconditional_delete`) is
      never spuriously rejected — proven by a real test, not just argued.
- [x] 11 new tests: 8 real-Iceberg PASS/FAIL pairs in
      `IcebergConnectorSpec` (`merge_condition`, `forbid_unconditional_delete`,
      `allowed_update_columns` against copy-on-write, plus the
      merge-on-read `RULE_UNVERIFIABLE_DML` case and its no-rule-declared
      counterpart proving no over-rejection) and 3 pure-Scala
      `RuleVerifier.appliesTo` truth-table tests in `RuleVerifierSpec`.
      Full `spark-adapter` suite: 270/270 passing (up from 259), zero
      regressions across all 7 connectors including a real ClickHouse
      subprocess server.
- [x] `mimaReportBinaryIssues` clean; `./dev/build`/`./dev/test`/
      `./dev/regression` all pass against real `spark-submit` (270/270
      spark-adapter tests, demo pipeline PASS, regression pack 2/2).
- [~] Mutation testing scoped to the 4 changed files
      (`RowMutationSupport.scala`, `RuleVerifier.scala`,
      `ContractEnforcementRule.scala`, `StructuralVerifier.scala`; 60
      non-excluded mutants) was started, not completed — stopped partway
      (~20/60 tested, zero survivors up to that point) at an explicit
      user decision to stop re-running the full check per change and
      instead write mutation-resistant tests up front (every PASS/FAIL
      pair above already asserts on both sides of each conditional this
      session's code introduces — kind dispatch, extraction success/
      failure, appliesTo's per-rule-type matrix), per CLAUDE.md's updated
      "Mutation Testing Requirement" section. Not run to a final score;
      recorded here rather than silently presented as a completed check.

#### Sub-phase: Predicate-logic `merge_condition` (done)

Closes the first of the three gaps `RuleVerifier`'s own doc comment and
ROADMAP.md's "Scope (Future)" had named since "Interpreting `rules`":
`merge_condition` checked that every declared column was *referenced*
somewhere in the MERGE's `ON` condition, not that it formed a genuine
equality match — a real false-negative gap, not a hypothetical one.
Prompted directly by a user follow-up ("what's up next on the
roadmap?").

- [x] **Three concrete false negatives the old "referenced anywhere"
      check missed, all closed by requiring genuine equality pairing:**
      a range/inequality check on a declared column (`t.customer_id > 0`)
      referenced it without matching on it; a literal comparison
      (`t.customer_id = 'ACME'`) referenced it while pinning to a
      constant, not the source side; and a declared column's only
      equality living inside an `OR` branch (`t.id = s.id OR t.region =
      s.region`) made the match strictly weaker than the contract
      intended, since only one side needs to hold.
- [x] **`RuleVerifier.equalityPairedColumns(expr)`**: recursively
      flattens top-level `&&` conjuncts (confirmed to be Catalyst's own
      `And.symbol`, `"&&"`, via `SparkPlanAdapter.translateExpr`'s
      `BinaryOperator` case — not the SQL keyword `"AND"`, a real mistake
      an early test draft made and a later read of the actual translator
      caught) and collects every column name appearing as a bare operand
      of a top-level `=`/`<=>` — the null-safe operator accepted
      alongside plain equality. Deliberately does not descend into `||`,
      `NOT`, or `CASE WHEN` — full De Morgan-aware predicate logic
      remains future work, not attempted here — and doesn't distinguish
      target- from source-side qualifiers (two target columns compared to
      each other would still count), a real remaining approximation.
      Cross-named pairings (`t.customer_id = s.cust_id`) are accepted
      either from the target's or the source's declared name, since a
      contract could reasonably be authored against either side's naming.
      An extra, non-equality conjunct beyond the declared columns (a
      partition-pruning predicate, say) is still tolerated, not flagged —
      checking more than required was never the failure this rule guards
      against.
- [x] 13 new/rewritten tests: `RuleVerifierSpec` gained 9 pure-Scala
      `merge_condition` cases (equality pairing PASS, null-safe `<=>`,
      missing pairing FAIL, an extra tolerated conjunct, cross-named
      pairing from either side, a range-only FAIL, a literal-only FAIL,
      an OR-only FAIL, and a nested three-way `AND`); `ContractEnforcementRuleSpec`
      gained 2 real-Delta cases (a range-check-only MERGE aborted with
      `RULE_MERGE_CONDITION_VIOLATION`; a cross-named-column MERGE
      executing normally) — both asserting on the target table's rows
      being byte-identical (FAIL) or correctly updated (PASS), the same
      discipline every enforcement test in this file uses. Full
      `spark-adapter` suite: 279/279 passing, zero regressions across all
      connectors (Delta, Iceberg both write strategies, Hive, Avro,
      ClickHouse via a real subprocess server, Parquet/CSV/JDBC).
- [x] Mutation testing scoped to `RuleVerifier.scala` (the only file this
      change touched): **100%** (15/15 non-excluded mutants killed, 39
      generated total) — written up front per CLAUDE.md's "write
      mutation-resistant tests the first time" guidance, run once to
      confirm rather than iteratively.
- [x] `mimaReportBinaryIssues` clean for `spark-adapter` (baseline
      published from this branch's prior HEAD via `git stash`, not a
      destructive checkout); `./dev/build`/`./dev/test`/`./dev/regression`
      all pass against real `spark-submit` — the demo pipeline's own
      `PASSED (invariant_demo_output@1.0.0)`, and the regression pack's
      2/2 cases, both unaffected by this change (neither uses a MERGE).

Deliberately still open, not attempted here: De Morgan-aware handling of
`NOT`/`CASE WHEN`, distinguishing target- from source-side qualifiers in
an equality pair, which specific rows an UPDATE touches, and whether a
DELETE's predicate is trivially satisfiable — all still tracked below.

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
- [x] Interpreting `rules` from the contract model — a first, narrow
      slice (three DML rule types) done; see the "Interpreting `rules`"
      sub-phase above. Everything beyond those three types (compatibility
      mode, quality expectations, and any richer rule vocabulary the
      other `Scope (Future)` items below would need) is still recorded
      only, not interpreted.
- [ ] **Full semantic DML verification** (row-level `MERGE`/`UPDATE`/
      `DELETE`), continued. Structural verification (target location/
      schema, MERGE's source as an input) and a rule-based slice covering
      both Delta and any DSv2 `SupportsRowLevelOperations` connector
      (Iceberg's mechanism) — merge match columns (now checked via
      genuine equality pairing, not just "referenced somewhere"),
      forbidding an unconditional delete, allowed update columns for
      copy-on-write UPDATE, failing closed rather than silently skipping
      the rule when this module recognizes an operation as DML but can't
      extract what a declared rule needs — are all done; see the "Delta
      Lake operation-surface coverage ledger", "Interpreting `rules`",
      "Iceberg DML rule support", and "Predicate-logic `merge_condition`"
      sub-phases above. What's still unverified, deliberately:
      - `equalityPairedColumns` doesn't reason about De Morgan
        equivalences, `NOT`, or `CASE WHEN` — only a flat top-level `AND`
        of equalities is recognized — and doesn't distinguish target- from
        source-side qualifiers (two same-side columns compared to each
        other would still count as a pairing).
      - Which specific rows an `UPDATE` touches, and whether a `DELETE`'s
        predicate (when present) is trivially satisfiable.
      - `allowed_update_columns` against an Iceberg merge-on-read
        UPDATE — genuinely unextractable from `WriteDelta`'s rewritten
        `Expand`-based plan (see "Iceberg DML rule support" above); fails
        closed (`RULE_UNVERIFIABLE_DML`) rather than silently passing,
        but isn't verified.
      A richer rule vocabulary (row-level conditions expressed as actual
      boolean logic against contract-declared fields, not just "which
      columns does it touch") would be needed for the De Morgan/`NOT`/
      `CASE WHEN` piece specifically — not started.

#### Sub-phase: Expression-algebra rework — a small, named vocabulary
for semantic lineage, not one `FunctionCall` bucket (done)

The first stage of Invaract's semantic-lineage capability: hardening the
transformation IR/translator so a Spark plan's real structure (which
source columns contribute to an output, whether it's a pure rename or a
derived computation, what a filter/join condition actually says) is
preserved with enough granularity for a future fingerprint/diff to reason
about *what kind* of expression changed, not just *that* one did.
Deliberately scoped to representation only — no Virtual Data Environment,
IO interception, snapshot persistence, fingerprinting, or diffing, and no
contract-format change.

- [x] **`ir.Expr` split into named categories** where the distinction is
      semantically load-bearing: `Cast`, `Arithmetic`, `Comparison`,
      `BooleanExpr` (`AND`/`OR`/`NOT`), `Conditional` (`CASE WHEN`/`IF`),
      and a new expression-level `Alias` for a rename occurring
      mid-expression (e.g. a struct field's name) — previously silently
      discarded by both the IR and the translator. `Function` remains the
      catch-all for everything else (string/date functions, null checks,
      non-aggregate window functions); `AggregateCall` unchanged.
- [x] **`UDF` as its own node**, never conflated with `Function`: carries
      the UDF's declared name (only when the engine exposes a real,
      non-generic identifier — Spark's own generic `"UDF"` placeholder is
      treated as "no name available"), its argument dependencies, and an
      `engineType` diagnostic (`ScalaUDF`/`PythonUDF`/Hive). Confirmed
      empirically against real Spark sessions that (a) a
      `spark.udf.register`-registered UDF's name genuinely round-trips,
      and (b) Spark's analyzer real-rewrites a UDF call in two ways this
      IR must preserve, not assume away: wrapping it in a null-propagation
      `CASE WHEN` when the UDF's parameter is a non-nullable Scala value
      type, and inserting an implicit argument `Cast` when an argument's
      runtime type doesn't match the UDF's declared parameter type.
- [x] **`ir.Plan` gained `Limit`** (`GlobalLimit`+`LocalLimit` collapse to
      one node, not the previous transparent pass-through) — a row cap is
      part of a transformation's observable semantics.
- [x] **`Unsupported`/`UnsupportedExpr` renamed to `UnknownPlan`/
      `UnknownExpression`**, each gaining a `sourceType` field (the
      front-end's own class name, e.g. Catalyst's `getClass.getSimpleName`
      — a plain string, never an engine type) alongside `description` and
      any still-resolvable `children` — never silently dropping structure
      a translator could still identify even when it couldn't interpret
      the construct as a whole.
- [x] **`ColumnRef` gained an optional `id: Option[Long]`**: a
      translator-assigned, opaque per-attribute identity (Spark's
      `exprId.id`, when a front end has one) that strengthens — never
      replaces — the existing qualifier-based disambiguation a self-join
      already relies on. Inert (`None`) for hand-constructed IR.
- [x] **`SparkPlanAdapter`/`RuleVerifier` rewritten** to produce and
      consume the new node types (`BinaryComparison`/`BinaryArithmetic`/
      `And`/`Or`/`Not`/`CaseWhen`/`If` matched directly instead of one
      generic `BinaryOperator` case; `RuleVerifier.equalityPairedColumns`
      now matches `BooleanExpr("AND", ...)`/`Comparison` instead of
      `FunctionCall("&&", ...)`/`FunctionCall("=", ...)`).
- [x] **A deliberate, documented MAJOR-version MiMa break** for `ir`
      (`mimaBinaryIssueFilters` in `ir/build.sbt`, each line the real
      output of `sbt mimaReportBinaryIssues` against a local
      `publishLocal` of the pre-change module) — see CLAUDE.md's "API
      Compatibility Requirement." `spark-adapter`'s own MiMa passed clean
      with no filters needed: none of the changed types are part of its
      own public surface.
- [x] **`ExpressionTranslationSpec`** (new, 17 tests) plus expanded
      `PlanSpec`/`LineageSpec`/`PlanPrinterSpec` (`ir`) and
      `SparkPlanAdapterSpec`/`RuleVerifierSpec` (`spark-adapter`) coverage
      for every new node type, real literal types, nested/chained
      expressions, multi-column and ambiguous joins (qualifier
      disambiguation verified through `Lineage.trace`, not just the raw
      `Project`), duplicate output names, and repeated column references
      — all against real analyzed Spark plans, structural assertions, not
      snapshots. `spark-adapter`'s full suite: 389 tests, all passing.

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
