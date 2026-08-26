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
- [x] **JSON Schema** — `contract/schema/invariant-contract.schema.json` (Draft 2020-12), the public, language-agnostic contract format spec for authoring/generating contracts outside Scala; validated against the same fixtures via `ContractSchemaSpec` so it can't silently drift from the parser/validator it documents. `demo/contracts/*.yaml` carry a `yaml-language-server` `$schema` hint for live editor validation. See docs/CONTRACT_MODEL.md's "JSON Schema" section for what it does and deliberately does not enforce.
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
- [x] Wired into `runner/DemoJobHarness.scala`: the contract is loaded and
      the check rule installed *before* the `SparkSession` is built (a
      check rule can't be added to an already-built session); the real
      write is now the verification gate itself, not a separate step
      after it.
- [x] Live-demonstrated against the real pipeline, not just unit tests:
      running `DemoJobHarness` with a deliberately-broken contract
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
(`df.write.format("delta").save(path)`) translated to `ir.Unsupported`,
not `ir.Write` — meaning `ContractEnforcementRule` silently treated it as
a no-op and let it through completely unverified, contract or no
contract. Found while answering a user question about what a Delta user
would need to add to their `spark-submit` to use Invariant.

- [x] **`SparkPlanAdapter` recognizes `SaveIntoDataSourceCommand`**, the
      plan node Delta (and any other `CreatableRelationProvider`-based
      `.save(...)` source) actually analyzes to, confirmed empirically
      against a real Delta-enabled `SparkSession` rather than assumed from
      documentation.
    - The originally-proposed approach (a `provided`-scope `delta-spark`
      dependency plus Delta-specific typed pattern matching, justified by
      an empirically-verified JVM lazy-classloading argument) turned out
      to be unnecessary: `SaveIntoDataSourceCommand` and
      `DataSourceRegister` are both plain, public `spark-sql` classes
      already on this module's existing `provided` Spark dependency.
      `formatOf` (previously typed to `FileFormat` for the
      `InsertIntoHadoopFsRelationCommand` case) was widened to `AnyRef`
      and reused for both cases — Delta's `DeltaDataSource` implements
      `DataSourceRegister` the same way every built-in format already
      does (`shortName() == "delta"`).
    - Net result: **zero added runtime or compile-time dependency** for
      non-Delta users. `delta-spark` (pinned to 3.2.0, not the latest 3.x
      — a confirmed real bug, `delta-io/delta#3737`, affects 3.2.1 on
      Scala 2.12 + Spark 3.5.1) is `% "test"` only, to spin up a real
      Delta-enabled session to test against. Confirmed via `unzip -l` on
      the assembled jar that it's unchanged in size and contains zero
      Delta classes.
    - `.saveAsTable`/DataFrameWriterV2/SQL `MERGE INTO` writes are a
      different, DataSourceV2-based plan shape, not covered by this and
      not yet investigated — documented as a known limitation.
- [x] **Two real bugs found and fixed, both via genuinely failing tests,
      not inspection:**
    - `ContractEnforcementRule.verifyOrThrow`'s output-schema derivation
      special-cased only `InsertIntoHadoopFsRelationCommand`
      (`cmd.query.schema`), falling back to the write command node's own
      `.schema` for everything else — which is empty for a `Command`.
      Caught by a Delta PASS test that instead threw
      `MISSING_OUTPUT_FIELD` on fields that were genuinely present. Fixed
      by adding the same `cmd.query.schema` handling for
      `SaveIntoDataSourceCommand`.
    - `SparkAdapterListener.onSuccess` had its own entirely independent
      "is this a write" filter, also hardcoded to
      `InsertIntoHadoopFsRelationCommand` only — meaning fixing
      translation and enforcement alone was insufficient. Caught by a
      test timeout (`eventually` never observed `listener.lastWrite`
      populate for a real Delta write). Fixed the same way. Left as an
      explicitly-documented design smell (three independent
      "is this a write" checks scattered across the module) rather than
      refactored now — tied to the still-outstanding fail-open-vs-closed
      question for unrecognized writes generally (see "Scope (Future)"
      below).
- [x] **Verified end to end**, not just unit-tested in isolation: new
      Delta translation test in `SparkPlanAdapterSpec` (via
      `SparkAdapterListener`) and a Delta PASS/FAIL enforcement pair in
      `ContractEnforcementRuleSpec` (mirroring the existing Parquet
      pair), all against a real Delta-enabled `SparkSession`. Full suite
      54/54 passing; mutation testing scoped to the 3 changed files
      (`SparkPlanAdapter.scala`, `ContractEnforcementRule.scala`,
      `SparkAdapterListener.scala`) scored 82.14%, clearing the 70% bar,
      with all 5 survivors pre-existing/already-documented, none in the
      new code; whole-module score unchanged at 91.53%; `mimaReportBinaryIssues`
      clean (only new pattern-match arms were added, no public signature
      changed); full local `./dev/build` + `./dev/test` + `./dev/regression`
      all pass.
    - Full detail, including the empirical investigation methodology
      (throwaway `QueryExecutionListener`-based probes against a real
      Delta session), in docs/SPARK_ADAPTER.md's "Delta Lake support"
      section.

#### Sub-phase: Fail-closed on unverifiable writes (done)

Resolves the fail-open-vs-closed question the Delta Lake sub-phase above
flagged and CLAUDE.md's "Mutation Testing Requirement" section referenced
as outstanding: should a write Invariant cannot translate/verify be
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

Asked directly after the write-side registry work: does the read side
have the same "recognition duplicated across independent sites" problem?
Investigated with the `add-spark-connector` skill rather than guessed at.

- [x] **Real investigation, not assumption**: probed `.load(path)` and a
      catalog table reference (`spark.table(...)`/`SELECT * FROM tbl`/
      `SELECT * FROM delta.\`path\``) against a real Delta-enabled session
      via `injectCheckRule` (the same mechanism `ContractEnforcementRule`
      uses). Both produce a `LogicalRelation` wrapping
      `org.apache.spark.sql.delta.DeltaLog$$anon$2` — confirmed to be an
      anonymous subclass of Spark's own `HadoopFsRelation`, not a distinct
      relation type. The existing `locationOf`/`translatePlan` branches
      already match it through ordinary subtyping and already extract the
      precise physical path, for both read shapes.
- [x] **Answer to the motivating question: no, not today, and here's
      why.** The write bug was three sites recognizing *different*
      concrete classes independently. Reads have exactly one type gate
      (`LogicalRelation`), reused identically by both consumer sites
      (`SparkPlanAdapter.translatePlan` and `ContractEnforcementRule.verifyOrThrow`'s
      input-schema collection) — they cannot disagree by construction.
      Explicitly *not* treated as "solved forever": a future connector
      whose read produces something other than `LogicalRelation` (most
      plausibly `DataSourceV2Relation`) would need a real second case in
      both sites, and that's the actual trigger for a
      `ReadRelationSupport`-style registry — building one now, for a
      shape that doesn't exist yet, would be premature.
- [x] **Zero production code changed.** Verified with tests, not left as
      an inspection claim: a translation test (`SparkPlanAdapterSpec`)
      confirms both read shapes produce a precise `ir.Read` with no
      fallback diagnostic; a PASS/FAIL enforcement pair
      (`ContractEnforcementRuleSpec`) confirms a contract's declared input
      schema is genuinely checked against a real Delta read's actual
      schema — surfacing a real, separate finding along the way (Delta
      reports every column nullable on read-back regardless of what was
      written), worked around in the test the same way a real contract
      author would need to. Full suite 63/63 passing; `mimaReportBinaryIssues`
      clean (no production code touched); full local
      `./dev/build`/`./dev/test`/`./dev/regression` all pass.
    - Full detail in docs/SPARK_ADAPTER.md's new "Delta Lake reads"
      section.

#### Sub-phase: Delta Lake operation-surface coverage ledger (done)

Prompted directly by a user question ("so is Delta 100% supported?") that
exposed a real process gap: `add-spark-connector` had been run for Delta
twice (write, then read), each time declaring success on a narrower scope
than "does Delta actually work end to end" — leaving several real
operations (V2 `.saveAsTable`/`.insertInto`/`.writeTo`, time travel,
streaming, CDC) never investigated at all, with no mechanism forcing that
gap to be stated explicitly. Fixed at the process level first
(docs/ADDING_A_SPARK_CONNECTOR.md's new "operation surface" checklist and
mandatory "coverage ledger" — see that doc and
`.claude/skills/add-spark-connector/SKILL.md`), then exercised against
Delta specifically to close it.

- [x] **Every row of the canonical operation surface probed empirically**,
      not assumed: `.format("delta").saveAsTable()` (new table),
      `.saveAsTable()`/`.insertInto()`/`.writeTo()` (existing table, every
      sub-op), time-travel reads, streaming reads, streaming writes, and
      CDC reads — all run against a real Delta-enabled `SparkSession` with
      an `injectCheckRule` probe, the exact mechanism
      `ContractEnforcementRule` uses.
- [x] **V2 write commands (`AppendData`/`OverwriteByExpression`/
      `ReplaceTableAsSelect`) confirmed to fail closed, not silently
      pass.** `.saveAsTable()`/`.insertInto()`/`.writeTo()` against an
      existing table, and `.format("delta").saveAsTable()` against a *new*
      table, all correctly throw `ContractViolationException` with
      `UnverifiableWrite` and write zero rows — verified with a new
      `ContractEnforcementRuleSpec` test that asserts row content is
      unchanged after every rejected attempt, not just that an exception
      was thrown.
- [x] **Time-travel reads need no new code**: `versionAsOf` produces the
      same `LogicalRelation`-wrapping-`HadoopFsRelation` shape as an
      ordinary read, already handled.
- [x] **CDC reads are translated, with a caveat**: `readChangeFeed`
      produces `LogicalRelation(relation=CDCReader$DeltaCDFRelation)`, a
      class distinct from `HadoopFsRelation` — but `translatePlan`'s
      generic `LogicalRelation` case (not a `HadoopFsRelation`-specific
      one) already covers it, producing a correct `ir.Read`. Because that
      relation has no populated `catalogTable` for a path-based read, it
      takes the existing "fallback" branch and emits a location
      diagnostic (uses the relation's `toString()` rather than a clean
      physical path) — a precision gap, not a correctness one; schema
      verification is unaffected.
- [x] **Streaming write found to have zero enforcement touchpoint — since
      closed (see the sub-phase immediately below).** At the time of this
      investigation, a real streaming write to Delta produced 9 distinct
      plans through `injectCheckRule`; confirmed via the probe's own
      `Command`-shaped filter that zero of them were `Command`-shaped.
      `WriteToStream`, the top-level plan, was separately confirmed via
      `javap` on Spark's catalyst jar to implement `LogicalPlan`/
      `UnaryNode` but not `Command` — so `ContractEnforcementRule`'s
      fail-closed policy, which only gates `Command`-shaped plans, could
      not structurally ever see it. Unlike every other row in this
      ledger, this was not "fails closed but unverified" — it was
      unenforced, full stop. A separate test confirmed the fail-closed
      policy did not *wrongly* block an unrelated streaming query, ruling
      out the opposite bug.
- [x] **Maintenance operations already have a reasoned classification**,
      not a gap: `FailClosedCommands.scala`'s `knownSafe` set already
      includes Delta's `VacuumTableCommand`/`OptimizeTableCommand`
      (file-level, doesn't change committed row content) and deliberately
      excludes `RestoreTableCommand`/`CloneTableCommand`/
      `ConvertToDeltaCommand` (row-content-changing) — built from a
      class-by-class enumeration of all 164 `Command` subclasses across
      Spark 3.5.1 + Delta 3.2.0, documented in that file's header. Not
      re-probed individually this pass; the classification stands.
- [x] **Full coverage ledger — all 5 read rows and 8 write rows disposed,
      not left implicit** — see docs/SPARK_ADAPTER.md's new "Delta Lake
      operation-surface coverage ledger" section for the complete table
      with evidence per row. Two probe specs
      (`RemainingDeltaOpsProbeSpec`, `StreamingWriteProbeSpec`,
      `CdcReadProbeSpec`) were investigation scaffolding, deleted once
      their findings were captured in tests/docs, per this repo's own
      established methodology.
- [x] Full suite passing; `./dev/build`/`./dev/test`/`./dev/regression`
      all pass; `mimaReportBinaryIssues` clean (no production API surface
      changed — only `ROADMAP.md`/`docs/SPARK_ADAPTER.md`/test files).

#### Sub-phase: Streaming writes closed; fail-closed reframed as a
#### stopgap, not a verdict (done)

Two changes, prompted by a single user question after the ledger above
shipped: "any way to close the streaming gap, and — the ledger's 🚫 rows
read like 'not supported' is an acceptable resting state; that was never
the point of fail-closed, was it?"

- [x] **Streaming writes: closed, not just documented.** `WriteToStream`
      reaches `injectCheckRule` (confirmed by the coverage-ledger pass
      above — it just isn't `Command`-shaped). Rather than special-casing
      it in `ContractEnforcementRule`'s fail-closed check, it was added as
      a real `WriteCommandSupport` entry — the same registry every other
      write shape goes through — so it's genuinely translated and
      verified, not merely gated. `inputQuery.schema` gives the output
      schema; location comes from a resolved `catalogTable`
      (`.toTable(...)`, confirmed to carry `storage.locationUri`/
      `provider`) or, for a path-based `.start(path)` write, from the
      sink's `name()` when that doesn't throw, or — since Delta's
      `DeltaSink` is a legacy V1 `Sink` wrapper whose `name()`/`schema()`
      unconditionally throw `IllegalStateException("should not be
      called")`, confirmed empirically via a real probe — a reflective
      call to its public `path()` accessor, the same
      no-compile-time-dependency reflection technique `jdbcLocationOf`
      already uses for `JDBCRelation`. Verified through real enforcement:
      a PASS/FAIL pair for `.start(path)`, a PASS test for `.toTable(...)`
      (the `catalogTable`-populated path), a direct-inspection test
      confirming format is detected as `"delta"` (StructuralVerifier only
      checks format when the contract also declares one, so this
      wouldn't otherwise surface as a PASS/FAIL failure), and a test
      confirming a streaming write to a location unrelated to the active
      contract is now correctly rejected (`OUTPUT_LOCATION_MISMATCH`) —
      the same behavior batch writes have always had, no longer
      special-cased by omission. Mutation testing scoped to
      `WriteCommandSupport.scala`: first run found two survived mutants in
      code added this pass (both in `streamSinkFormatOf`'s class-name
      string match — `StructuralVerifier` only compares format when the
      contract also declares one, so a wrong-format bug there wasn't
      guaranteed to surface through the PASS/FAIL pair alone); killed by
      adding the direct-inspection test above, confirmed by a second run:
      90.77% overall (92.19% of covered code) — the remaining survived/
      uncovered mutants are all in `unwrapWriteWrapper`/`SparkPlanAdapter`
      code this pass didn't touch. Full suite passing;
      `mimaReportBinaryIssues` clean; `./dev/build`/`./dev/test`/
      `./dev/regression` all pass.
- [x] **`add-spark-connector`'s "fails closed" framing corrected.** The
      coverage ledger's 🚫 disposition was written (and, in the Delta
      ledger above, applied) as if "not yet translated, verified to
      abort" were a complete, acceptable answer on its own — no different
      in spirit from ✅ Covered, just a different flavor of done. That's
      backwards: fail-closed exists to catch operations Invariant *hasn't
      gotten around to translating yet*, not to bless them as
      out-of-scope forever. Fixed at the process level:
      docs/ADDING_A_SPARK_CONNECTOR.md gained a "What 'fails closed'
      means (and doesn't)" section, and both it and
      `.claude/skills/add-spark-connector/SKILL.md` now require every 🚫
      ledger row to carry a next step — either the real translation work
      that would close it (the default assumption), or, rarely, a
      specific documented reason it should never be translated. The
      Delta ledger in docs/SPARK_ADAPTER.md was rewritten to match: every
      remaining 🚫 row (`AppendData`, `OverwriteByExpression`,
      `ReplaceTableAsSelect`, row-level DML) now states concretely what
      would close it, not just that it's currently rejected.

#### Sub-phase: Remaining Delta write-side gaps closed; streaming reads
#### recognized as contract inputs (done)

Follow-up to the sub-phase above, closing every 🚫 row the corrected
ledger could actually state a concrete next step for.

- [x] **`AppendData`/`OverwriteByExpression`/`ReplaceTableAsSelect`: all
      three now real `WriteCommandSupport` entries.** `AppendData` covers
      `.saveAsTable()` append, `.insertInto()`, and `.writeTo().append()`
      in one entry; `OverwriteByExpression` covers `.writeTo().overwrite(cond)`,
      mapped to the contract's `saveMode: overwrite` uniformly (the
      predicate itself needed no IR extension after all — contrary to
      what the previous sub-phase assumed, `StructuralVerifier`'s
      save-mode check never needed it); `ReplaceTableAsSelect` covers
      `.format("delta").saveAsTable()` on a new table and
      `.writeTo().createOrReplace()`. Location resolution for
      `AppendData`/`OverwriteByExpression` prefers a resolved
      `DataSourceV2Relation`'s `Table.properties()["location"]` (the
      physical warehouse path, confirmed empirically), shared via a new
      `SparkPlanAdapter.tableLocationAndFormat` helper. Verified via
      PASS/FAIL pairs in `ContractEnforcementRuleSpec` for both the new-
      and existing-table cases.
- [x] **Found and fixed a real correctness trap along the way: atomic
      CTAS/RTAS issues a second, nested write.** A single
      `.saveAsTable()` on a *new* table produces two write-shaped plans
      through `injectCheckRule` — the top-level `ReplaceTableAsSelect`
      and an internal `AppendData` against a `StagedTable` (Spark's own
      public 2-phase-commit protocol for atomic CTAS/RTAS) — both
      genuinely visible to `ContractEnforcementRule.verifyOrThrow`. A
      `StagedTable`'s `properties()` has no `"location"` yet, so the
      naive translation gave the two plans two *different* location
      strings for the same destination — a real PASS test failure, not
      caught by inspection. Fixed via a shared `qualifiedIdentifier`
      helper: `DataSourceV2Relation`'s own `catalog`/`identifier` fields
      (confirmed populated even for a staged table) now produce the exact
      same qualified form `ReplaceTableAsSelect`'s `ResolvedIdentifier`
      case does, so the two agree by construction. Documented in
      docs/SPARK_ADAPTER.md's new "A shared pitfall" subsection for
      whichever connector hits this next.
- [x] **Streaming reads recognized as contract inputs, closing a real
      false-positive.** Neither `StreamingRelation` (the legacy V1 path
      Delta's own streaming read uses, confirmed empirically — not
      `StreamingRelationV2`) nor `StreamingRelationV2` (the modern
      DataSourceV2 path — `rate`, Kafka, ...) was a `LogicalRelation`, so
      a contract declaring a streaming source as a required `input`
      always reported `MISSING_INPUT` even though data was genuinely
      being read. Closed in both `SparkPlanAdapter`'s translation and
      `ContractEnforcementRule`'s input-schema collection via two new
      shared helpers (`streamingRelationLocationOf`/
      `streamingRelationV2LocationOf`) rather than two independent
      matches — the exact duplication risk the write side already
      learned from. `StreamingRelation.dataSource.options`/`sourceName`
      need no reflection (plain public spark-sql classes, unlike
      `WriteToStream`'s sink); `StreamingRelationV2` reuses the same
      `Table.properties()` lookup as the write-side V2 cases above.
      Verified via a PASS/FAIL pair proving a contract's declared input
      schema is genuinely checked against a real streaming Delta source.
- [x] **Mutation testing scoped to all three changed files
      (`WriteCommandSupport.scala`/`SparkPlanAdapter.scala`/
      `ContractEnforcementRule.scala`): 88.57% overall (89.86% of covered
      code)**, up from 84.29%/85.51% on the first run — two direct
      translation-level tests added (asserting both location *and*
      diagnostics, mirroring this file's existing Delta-read-translation
      test) to kill mutants in the new `StreamingRelation`/
      `StreamingRelationV2` fallback-diagnostic conditions. Two mutants
      remain in new code, both in those same fallback-diagnostic
      conditions (whether a *diagnostic message* gets attached, not
      whether the translated location itself is correct — both directions
      of that are already asserted correct by the new tests): killing
      them fully would need a legacy V1 `StreamingRelation` source with no
      path option, which isn't realistically constructible from the
      sources available in this test environment (every built-in
      no-physical-location source, `rate` included, is natively V2).
      Left as an accepted mutant, the same category CLAUDE.md's own
      "Mutation Testing Requirement" already carves out (a StringLiteral
      mutant on human-readable message text) — this is a
      ConditionalExpression mutant on whether that same kind of message
      gets attached at all, not a correctness difference. Full suite
      passing (76/76); `mimaReportBinaryIssues` clean;
      `./dev/build`/`./dev/test`/`./dev/regression` all pass.

#### Sub-phase: Row-level DML — structural verification, the last
#### coverage-ledger row closed (done)

Closes the final 🚫 row in the Delta operation-surface ledger. Scoped
deliberately after discussion: the fuller version (verifying the actual
merge condition/update columns/delete predicate) needs both a new IR node
and contract `rules` interpretation, neither of which exist — see the
"Full semantic DML verification" item in "Scope (Future)" below for that
larger, explicitly-deferred design, kept there specifically so it isn't
lost. This sub-phase does the achievable, honest subset: structural
verification only.

- [x] **`MergeIntoCommand`/`UpdateCommand`/`DeleteCommand` recognized as
      real `WriteCommandSupport` entries**, matched by reflection (all
      three are Delta-internal classes, confirmed empirically via
      `injectCheckRule` — not generic Spark API the way `AppendData`/
      `OverwriteByExpression`/`ReplaceTableAsSelect` are), using their
      public `target()`/`catalogTable()`/`source()` methods (confirmed
      via `javap`, no `setAccessible` needed). Wrapped in `Try` (via
      `Function.unlift`), unlike the stable-API write cases: reflecting
      into undocumented, no-cross-version-guarantee Delta internals
      needs to degrade to the pre-existing fail-closed default if a
      future Delta version renames a method, not crash a real Spark job
      with a raw `ReflectiveOperationException`.
- [x] **What's checked, and what deliberately isn't, stated explicitly
      in code and docs, not left implicit.** Checked: the operation's
      *target* against the contract's declared output location and
      current schema (a `MERGE`/`UPDATE`/`DELETE`'s own `output` is a
      row-count summary, not data, confirmed empirically — there's no
      "new schema" to check the usual way, but the target's *existing*
      schema is still worth confirming still matches). Not checked, and
      not yet checkable: the merge condition, which columns an `UPDATE`
      touches, whether a `DELETE` is unconditional — no contract
      vocabulary exists for that (see the "Full semantic DML
      verification" item below).
- [x] **MERGE's `source` recognized as a contract input — found and fixed
      a real second correctness trap along the way.** Initially assumed
      (documented as such, before verifying) that
      `ContractEnforcementRule.verifyOrThrow`'s existing `plan.collect`
      input-schema collection would reach MERGE's source automatically,
      the same way it does for every other write shape. A real FAIL test
      (asserting `intercept[ContractViolationException]`) proved that
      assumption wrong: `plan.collect` walks `children`, and Delta's DML
      commands are effectively leaf nodes in the tree-traversal sense —
      `source`/`target` are ordinary case-class fields, never exposed as
      children — so a plain `plan.collect` on the command finds nothing
      inside it. Fixed by having `verifyOrThrow` also walk
      `WriteCommandSupport`'s already-extracted `query` field (MERGE's
      `source`) in addition to the raw plan. Documented in
      docs/SPARK_ADAPTER.md's new second "shared pitfall" subsection.
- [x] Verified through real enforcement, not translation in isolation: a
      PASS/FAIL pair for MERGE (rows genuinely merged on PASS; aborted,
      target genuinely untouched on FAIL, both for a target-schema
      violation and, separately, a MERGE-source-input violation), plus a
      PASS test each for `UPDATE`/`DELETE` (rows genuinely mutated), all
      in `ContractEnforcementRuleSpec`. A dedicated direct-inspection
      test also covers a path-based DML operation with no catalog table
      at all (`UPDATE delta.\`path\``, confirmed empirically to leave
      `catalogTable` as `None`, not just missing a location) — the
      fallback branch every other DML test's catalog-backed target
      doesn't reach.
- [x] Mutation testing scoped to `WriteCommandSupport.scala`/
      `ContractEnforcementRule.scala`: 85.71% overall (86.84%–89.19% of
      covered code across runs). Two mutants remain in new code, both in
      `deltaRowLevelDml`'s fallback-diagnostic-message branch
      (`catalogTable.isDefined`, deciding which of two message strings to
      use — the actual `location`/`format` computation doesn't re-branch
      there, it's already been computed above) — the same category of
      accepted mutant as this module's other message-wording-only cases
      (formally equivalent in spirit to the already-excluded
      `StringLiteral` mutator category). Full suite passing (81/81);
      `mimaReportBinaryIssues` clean;
      `./dev/build`/`./dev/test`/`./dev/regression` all pass.
- [x] **Every row of the Delta operation-surface ledger is now ✅
      Covered** — 5 read rows, 8 write rows, all 13. Full ledger in
      docs/SPARK_ADAPTER.md's "Delta Lake operation-surface coverage
      ledger" section.

#### Sub-phase: Delta feature-by-feature confidence pass (done)

The ledger above closing every write-command *shape* left a separate,
previously-implicit gap: "expected to work" against Delta table
*features* (schema evolution, generated columns, deletion vectors, column
mapping, liquid clustering, CHECK constraints, identity columns) is not
the same claim as "confirmed to work" — nothing had actually exercised
most of them. This sub-phase tried each one for real, not from
documentation, and turned every finding into either a fix or a permanent
regression test — no more relying on throwaway probe evidence.

- [x] **Schema evolution (`MERGE` + `autoMerge.enabled`) — real bug,
      found and fixed.** `target.schema` at analysis time is the
      *pre-merge* schema, confirmed empirically to not yet include
      columns evolution is about to add — a contract requiring such a
      field was wrongly `MISSING_OUTPUT_FIELD`-rejected. Fixed via
      `MergeIntoCommand.schemaEvolutionEnabled()` (public, confirmed via
      `javap`); the source's new fields are unioned into `target.schema`
      as a best-effort approximation, with a diagnostic. A second finding
      along the way: with `autoMerge` disabled, `INSERT *` silently drops
      a source column the target doesn't have (confirmed empirically,
      not assumed) — so the fix must gate strictly on
      `schemaEvolutionEnabled()`, not just "does the source have extra
      fields." Two PASS tests in `ContractEnforcementRuleSpec` cover both
      directions.
- [x] **Generated columns (`GENERATED ALWAYS AS (...)`) — real bug, found
      and fixed.** Same false-rejection class: Delta computes these at
      commit time, never supplied by the writer, so `AppendData`/
      `OverwriteByExpression`'s `outputSchema` never included them.
      Confirmed the hard way that no DataFrame-facing schema exposes the
      `delta.generationExpression` metadata key Delta itself sets on a
      generated column's `StructField` — not a read-back, not a catalog
      table, not even the DSv2 `Table` handle's own `.schema()`
      (`DeltaTableV2.schema()`, specifically) — only Delta's internal
      `Snapshot.schema()` (`DeltaTableV2.initialSnapshot()`) does. Fixed
      by reading that reflectively (`outputSchemaWithGeneratedColumns`/
      `deltaGeneratedFields`, same no-compile-time-dependency, `Try`-wrapped
      convention as `deltaRowLevelDml`) and unioning the target's
      generated-only columns in — checking the metadata key directly
      rather than reflecting into Delta's own
      `GeneratedColumn.isGeneratedColumn` helper (a `/simplify` pass
      finding: `StructField.metadata` needs no `Protocol` lookup or
      overload resolution, and is already a plain public Spark type).
      Verified with a PASS test (built via the
      `io.delta.tables.DeltaTable` builder API — raw SQL DDL for
      generated columns fails outright in this environment, confirmed
      empirically, even with explicit reader/writer-version
      `TBLPROPERTIES`).
- [x] **Deletion vectors, column mapping mode (`'name'`), liquid
      clustering (`CLUSTER BY`) — confirmed transparent.** Real writes/DML
      against tables with each enabled are recognized exactly as they
      would be without it — correct location, schema, no diagnostics. A
      permanent PASS test added for each in `ContractEnforcementRuleSpec`
      (previously only throwaway probe evidence existed).
- [x] **CHECK constraints — confirmed orthogonal.** Delta enforces these
      itself, independently, at commit time; Invariant has no rule
      vocabulary for a row-level condition. A permanent test asserts
      both halves: a violating write is recognized by `WriteCommandSupport`
      identically to a satisfying one (no diagnostic from Invariant), and
      is then rejected by Delta's own `DeltaInvariantViolationException`
      before commit.
- [x] **Identity columns (`GENERATED ALWAYS AS IDENTITY`) — confirmed
      untestable in this environment, documented as such rather than
      silently skipped.** Spark 3.5.1's own SQL parser rejects the syntax
      (`[PARSE_SYNTAX_ERROR] ... extra input 'IDENTITY'`), confirmed via a
      dedicated probe with no exception-masking `try`/`catch` — almost
      certainly a Databricks Runtime-only grammar extension not present
      in vanilla OSS Spark 3.5.1 at all, so there is no analyzed plan for
      `WriteCommandSupport` to ever see. Recorded as ❓ Not investigated
      in docs/SPARK_ADAPTER.md, not claimed as covered.
- [x] Mutation testing scoped to `WriteCommandSupport.scala` (the only
      file touched this sub-phase): **73.08%** (19/26 non-excluded
      mutants killed). Every real survivor investigated, not just cited —
      see docs/SPARK_ADAPTER.md's "Delta feature-by-feature confidence
      pass" mutation-testing subsection for the per-mutant breakdown (one
      killed with a new direct-inspection test, one accepted as
      near-equivalent given the surrounding `Try`'s safety net, the rest
      pre-existing from earlier sub-phases). Full suite passing (89/89);
      `mimaReportBinaryIssues` clean.
- [x] All throwaway probe specs used during this pass deleted once their
      findings were captured as permanent tests/docs — no probe evidence
      left standing in as a substitute for a real regression test.

#### Sub-phase: Iceberg connector support (done)

Second connector onboarded via the `add-spark-connector` skill's
10-phase process. Pinned `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0`
(test-scope only, after checking the connector's own issue tracker per
Phase 0 — 1.10.0 had a confirmed Avro/Spark-3.5 compatibility bug, fixed
before 1.11.0). Full findings, the operation-surface and feature-surface
coverage ledgers, and the per-mutant mutation-testing breakdown are in
docs/SPARK_ADAPTER.md's "Iceberg support" section — summary here:

- [x] **Two real, connector-agnostic gaps found and closed, that
      predate Iceberg entirely.** Batch `DataSourceV2Relation` reads had
      no `SparkPlanAdapter` translation case at all (fell to the generic
      `Unsupported` fallback, meaning no pure-DSv2 connector's reads
      could ever satisfy a contract's declared input) — Delta's own
      reads never hit this because Delta's batch reads happen to be
      `LogicalRelation`-wrapped V1 relations. `CreateTableAsSelect`
      (explicit-create V2 CTAS) and `OverwritePartitionsDynamic` had no
      `WriteCommandSupport` case — both Command-shaped, neither
      translated nor safe-listed, so both were already failing closed;
      closed with two new cases reusing existing helpers
      (`v2CreateOrReplaceLocation`, shared with `ReplaceTableAsSelect`;
      `namedRelationLocationAndFormat`, shared with `AppendData`/
      `OverwriteByExpression`).
- [x] **Row-level DML (`MERGE`/`UPDATE`/`DELETE`) closed via a
      genuinely different, more standard mechanism than Delta's.**
      Iceberg implements Spark's own `SupportsRowLevelOperations` API;
      `MERGE`/`UPDATE`/`DELETE` rewrite to stable, public Spark classes
      (`ReplaceData`/`WriteDelta`, sharing the `RowLevelWrite` trait) —
      no reflection needed, unlike Delta's proprietary command classes.
      New `dsv2RowLevelWrite` case is connector-agnostic: any future
      DSv2 connector using this standard API is covered automatically.
      Same structural-only scope as Delta's row-level DML (target schema
      checked, merge condition/predicate not).
- [x] **A second staged-table location trap found, in the opposite
      direction from Delta's — the fix from the row-level-DML sub-phase
      didn't generalize the way its own doc comment claimed.** Iceberg's
      `StagedTable` (unlike Delta's) reports a real `"location"`
      property pre-commit, so the existing "prefer physical location,
      fall back to qualified identifier" logic produced two
      *disagreeing* locations for one atomic CTAS/RTAS commit — caught
      by a real `OUTPUT_LOCATION_MISMATCH` test failure, not assumed to
      generalize from Delta. Fixed properly: `namedRelationLocationAndFormat`
      now keys the qualified-identifier fallback on Spark's own
      `StagedTable` marker interface, not on whether `properties()`
      happens to omit `"location"` — a staged table's reported location
      isn't trustworthy regardless of what a given connector populates.
- [x] **`CALL <catalog>.system.<proc>(...)` (Iceberg's maintenance
      mechanism) deliberately left unmodeled — a genuinely new
      classification problem, not solved by extending
      `FailClosedCommands`' existing per-class approach.** One shared
      Spark class (`Call`) represents every system procedure
      (`rewrite_data_files`/`expire_snapshots`/`rollback_to_snapshot`/...),
      spanning genuinely safe to genuinely row-content-mutating with no
      structural way to distinguish them by class identity. Safe-listing
      would silently pass all of them; left off both lists, so every
      `CALL` fails closed today (confirmed by a real test) — a
      documented limitation with a next step (procedure-name-aware
      classification), not an oversight. The other 13 Iceberg SQL-
      extension commands found via the same reflective jar-scan Delta's
      used (branch/tag ref management, partition-spec/identifier-field
      evolution, write-ordering config, view management) are genuinely
      metadata-only and are on the safe list.
- [x] **A real gap found in Iceberg's own published Maven artifact, not
      an Invariant bug.** `iceberg-spark-runtime-3.5_2.12:1.11.0`'s POM
      doesn't declare `scala-collection-compat`, which its SQL-extensions
      parser needs specifically for `CALL` syntax — confirmed via a real
      `NoClassDefFoundError` escaping an existing `Try` wrapper
      (`LinkageError` isn't `NonFatal`). Added as a `% "test"` dependency
      so this module's own suite can exercise `CALL`-based operations; a
      real user hitting this in their own job needs it on their runtime
      classpath too, independent of `spark-adapter`.
- [x] **Confirmed Iceberg's own path-based access pattern differs from
      Delta/Parquet's**, not a gap: a bare `.format("iceberg").save(path)`/
      `.load(path)` with no catalog qualification fails hard (defaults to
      an unreachable `HiveCatalog`); Iceberg's real mechanism is a path
      identifier under a named Hadoop-type catalog
      (`` local.`/abs/path` ``), confirmed working through the same
      `AppendData`/`DataSourceV2Relation` cases as any other table.
- [x] New `IcebergConnectorSpec.scala` (separate SparkSession from
      `ContractEnforcementRuleSpec` — Delta's and Iceberg's
      `spark.sql.extensions`/catalog configs can't coexist in one
      session): translation test for the new read case, PASS/FAIL pairs
      for every new write case, a direct-inspection test for
      `CreateTableAsSelect.saveMode`, a fail-closed test for `CALL`, a
      regression test proving the 13 safe-listed metadata commands
      aren't blocked, and a streaming-write test confirming
      `WriteToStream` needed no Iceberg-specific handling. Plus a new
      direct-construction test in `SparkPlanAdapterSpec.scala` proving
      the batch-read fallback diagnostic actually fires (not just that
      the happy path works).
- [x] Mutation testing scoped to all four changed files
      (`WriteCommandSupport.scala`, `SparkPlanAdapter.scala`,
      `ContractEnforcementRule.scala`, `FailClosedCommands.scala`):
      **80.65%** (of total) / **81.97%** (of covered code) — 50/61
      non-excluded mutants killed. Both real survivors in this pass's own
      new code (`CreateTableAsSelect.saveMode`'s `ignoreIfExists` branch;
      the new batch-read no-location diagnostic) were killed with new
      tests, not left — see docs/SPARK_ADAPTER.md's "Iceberg support"
      section for the per-mutant breakdown. The remaining 12 survivors
      are all pre-existing, already-documented from earlier sub-phases
      (Delta's `catalogTable.isDefined`/`DeltaTableV2` guards, the
      `WriteFiles`/`DeltaSink`/`JDBCRelation` near-equivalents, the
      Hive-metastore-unavailable gap) — none in code this pass touched.
      Full suite passing (104/104); `mimaReportBinaryIssues` clean;
      `./dev/build`/`./dev/test`/`./dev/regression` all pass, including a
      real jar inspection confirming zero Iceberg classes bundled for
      non-Iceberg users.
- [x] `docs/ADDING_A_SPARK_CONNECTOR.md`'s "Known limitations" section —
      found stale before starting (still described row-level DML/
      streaming writes/DSv2 catalog writes as unsolved, all closed by
      the prior Delta pass) — corrected first, so this pass's own
      classification work was grounded in accurate information rather
      than repeating a documentation-drift mistake.
- [x] Both required coverage ledgers (operation surface, feature
      surface) produced and written into docs/SPARK_ADAPTER.md, ROADMAP.md
      (this entry), and CHANGELOG.md, per the `add-spark-connector`
      skill's Phase 11.

#### Sub-phase: Iceberg's own schema-evolution row closed, generalizing the Delta generated-columns fix (done)

Follow-up investigation into the Iceberg feature-surface ledger's one
remaining ❓ row. The predicted bug (newly-added columns invisible to
`outputSchema`, the same shape as Delta's MERGE bug) turned out not to
exist — confirmed empirically that `AppendData`'s `query.schema` already
reflects a `mergeSchema`-evolving write's new columns correctly, since
(unlike Delta's MERGE) the query *is* the writer-supplied data, not a
re-derived plan that can go stale. The real bug was the *other*
direction: with Iceberg's `write.spark.accept-any-schema` table property,
a *narrower* append (missing a column the target already has) is
accepted and NULL-fills the omitted column — `outputSchema` omitted that
column entirely, wrongly `MISSING_OUTPUT_FIELD`-rejecting a write that
actually satisfies the contract.

- [x] **Found this generalizes Delta's existing generated-columns fix,
      not just adds a parallel Iceberg-specific one.** Both are the same
      underlying situation — a resolved target can have fields its
      `query` doesn't supply that still exist in the committed row —
      under two different connector-specific mechanisms. Confirmed
      `cmd.table.columns()` (plain public API, no reflection) already
      carries a Delta generated column's *name*, even without its
      generation metadata — detecting *which* target-only fields exist
      for a specific reason was never actually necessary. Replaced the
      Delta-specific reflective `outputSchemaWithGeneratedColumns`/
      `deltaGeneratedFields` outright with connector-agnostic
      `outputSchemaWithTargetOnlyFields`, used by `AppendData`/
      `OverwriteByExpression`/`OverwritePartitionsDynamic` alike.
- [x] **Verified the safety argument directly, not just asserted it**:
      a permanent test proves Spark's own analyzer rejects a genuinely-
      unsanctioned narrower write (`accept-any-schema` off) with
      `AnalysisException` before Invariant's check rule ever sees it —
      so unioning in every target-only field can never silence a
      genuinely-missing required field.
- [x] Mutation testing rescoped to `WriteCommandSupport.scala` after the
      simplification: **76.92%** (20/26 non-excluded mutants killed) —
      zero survivors in the new code; the 6 remaining are pre-existing
      and already documented from earlier sub-phases. Full suite passing
      (106/106); `mimaReportBinaryIssues` clean.
- [x] Removed a `Table.schema()` deprecation warning introduced along
      the way by switching to `Table.columns()` (the non-deprecated
      replacement) — "no new warnings" held, not just "tests pass."
- [x] **A mutation-testing speed investigation, prompted mid-session**:
      confirmed this module's ~5 test suites already share a single
      `SparkSession` bootstrap via Spark's own `getOrCreate()` semantics
      (config from later `.config(...)` calls merges into an already-active
      session rather than requiring a fresh one — confirmed empirically,
      not assumed, via log inspection: one `sparkDriver` service start
      for the whole suite, zero `SparkContext` stops), so repeated
      session bootstrap wasn't the mutation-testing bottleneck to begin
      with. Tried two real levers: Stryker4s's `concurrency` config (2→4
      test-runners on this environment's 4-core box) didn't take effect
      via `stryker4s.conf` with this plugin version (the same class of
      quirk already documented for `mutate`/`thresholds`, and no
      build.sbt-level setting exists in this plugin version either);
      `spark.sql.shuffle.partitions` (200 default, unset anywhere in this
      suite) tuned to `2` across all five test `SparkSession` builders —
      confirmed applied (`spark.conf.get` verified `"2"` directly) but
      measured *zero* wall-clock change on a full suite run (131s before,
      131s and 115s after, within normal variance) - the real cost is
      genuine per-test Spark/table-commit work, not shuffle overhead, at
      this suite's tiny data volumes. Kept the shuffle-partitions tuning
      anyway (correct, zero-downside best practice) despite it not being
      the fix; reverted nothing else, since neither lever cost anything
      to try. A third lever, `spark.sql.codegen.wholeStage=false` (the
      leading untried hypothesis — per-query-shape JIT/codegen
      compilation cost across this suite's ~100+ distinct query shapes),
      was tried and measured *worse*, not better: 106/106 tests still
      passed, but wall-clock went from 109s (sbt-reported total time) to
      149s — roughly 35-40s slower, real and reproducible, not noise.
      Reverted immediately; disabling codegen apparently costs more in
      slower interpreted execution than it saves in compilation time at
      this suite's scale. No further Spark-config levers were pursued —
      the honest remaining hypothesis is genuine Delta/Iceberg
      table-commit I/O (`_delta_log`/Iceberg metadata JSON per commit),
      which isn't a tunable config; it's the thing actually being tested.

#### Sub-phase: Iceberg's last two ❓ feature-surface rows closed - deletion vectors, identity/generated columns (done)

Targeted follow-up closing the two rows the initial Iceberg pass left
`❓ Not investigated`. Both closed with real probes and permanent tests,
**zero production code changes** — both were genuine questions with
genuine answers, not gaps needing fixes.

- [x] **Deletion vectors** (Iceberg's V3 merge-on-read spec): a real
      probe against a genuine `format-version = 3` table confirmed a
      `DELETE` still produces a plain `ReplaceData` node — the same
      class `dsv2RowLevelWrite` already matches via the shared
      `RowLevelWrite` trait. The deletion-vector-vs-position-delete-file
      distinction is below the `LogicalPlan` level this adapter
      translates at; nothing to special-case. Closed with one permanent
      test in `IcebergConnectorSpec`, no code change.
- [x] **Identity/generated columns**: two real probes (Spark's
      `GENERATED ALWAYS AS` syntax, and a column `DEFAULT` value —
      Iceberg V3's `initial-default`/`write-default` mechanism) both
      confirmed `AnalysisException` (`UNSUPPORTED_FEATURE.TABLE_OPERATION`)
      thrown by Spark's own analyzer before any plan is produced, and
      that `write.spark.accept-any-schema` doesn't change the outcome
      (tried explicitly). Unlike Delta, this Iceberg integration has no
      generated/default-column concept reachable through Spark SQL at
      all — nothing for `outputSchemaWithTargetOnlyFields` or anything
      else to need to handle. Closed with two permanent tests asserting
      the rejection.
- [x] `IcebergConnectorSpec`: 17 → 19 tests, all passing (19/19). No
      `spark-adapter` main source changed, so CLAUDE.md's
      mutation-testing requirement (scoped to changed/added
      `src/main/scala`) doesn't apply this pass; `mimaReportBinaryIssues`
      confirmed clean regardless.
- [x] Both of Iceberg's feature-surface ledger rows now closed — every
      row in both the Iceberg operation-surface and feature-surface
      ledgers has a disposition, none left `❓`.

#### Sub-phase: Iceberg CALL procedure classification - 10 of 20 procedures reclassified from wrongly-rejected to correctly-allowed (done)

Closed the one remaining Iceberg operation-surface row still marked
`🚫 Fails closed, deliberately left unmodeled`: `CALL system.*`
procedures. Every procedure shares the same Spark class (`Call`), so
class-name matching alone can't distinguish them - but `Call.procedure()`
is a real, distinct concrete class per procedure (confirmed via `javap`
on `SparkProcedures`' builder registry, no dynamic proxy), reachable via
reflection the same way `WriteCommandSupport`'s `deltaRowLevelDml`
reflects into Delta's MERGE/UPDATE/DELETE. `FailClosedCommands.isKnownSafe`
now special-cases `Call` and checks `procedure().getClass().getName()`
against a 10-procedure safe list.

- [x] Enumerated iceberg-spark-runtime 1.11.0's actual 20 concrete
      procedure classes from the jar directly (not guessed), classified
      each against its delegate action class (`javap`) and Iceberg's own
      documentation. **10 reclassified safe** (`rewrite_data_files`,
      `rewrite_manifests`, `rewrite_position_delete_files`,
      `remove_orphan_files`, `expire_snapshots`, `register_table`,
      `ancestors_of`, `compute_table_stats`, `compute_partition_stats`,
      `create_changelog_view` - compaction/GC/stats/read-only, same
      category as Delta's already-safe-listed `OPTIMIZE`/`VACUUM`).
      **10 stay unmodeled/fail-closed, deliberately** (`rollback_to_snapshot`/
      `rollback_to_timestamp`/`set_current_snapshot`/`cherrypick_snapshot`/
      `publish_changes`/`fast_forward` - change what's "current";
      `add_files`/`migrate` - genuinely add/reformat row content;
      `snapshot`/`rewrite_table_path` - produce new persisted content
      even though neither touches its *source* table).
- [x] Updated `IcebergConnectorSpec`'s CALL fail-closed test to use
      `rollback_to_snapshot` (genuinely unsafe) instead of
      `rewrite_data_files` (now correctly allowed) as the fail-closed
      proof - the reclassification changes what that test needed to
      demonstrate, not just add to it. Added a regression test sampling
      5 of the 10 newly-safe procedures, proving they run under a real,
      otherwise-checking contract.
- [x] Found and fixed a real coverage gap via mutation testing, not just
      cited a score: `isKnownSafeIcebergProcedureCall`'s reflection
      fallback (fails closed if `procedure()` reflection ever breaks -
      the single highest-stakes line in this change) had zero coverage,
      since a real Iceberg session's `Call` never actually fails that
      reflection. Closed with a focused, session-free unit test
      (`FailClosedCommandsSpec`) exercising the fallback directly.
      Mutation testing rescoped to `FailClosedCommands.scala`: **88.42%**
      (89.36% of covered code), zero survivors in the new code.
      `mimaReportBinaryIssues` clean; full suite passing.
- [x] **Explicitly scoped what this pass does NOT do**, per the user's
      own sequencing decision: verify the 10 unmodeled procedures'
      *actual effect* against a contract (e.g. checking a
      `rollback_to_snapshot` target's schema before allowing it, instead
      of rejecting outright). That needs new mechanisms this codebase
      doesn't have yet - reading a schema from the catalog with no Spark
      write involved, or from a table/path named in a CALL argument
      (argument parsing/binding, never done here before) - tracked as
      separate future work below, not attempted in this pass.
- [x] **Found and fixed a real, pre-existing CI failure while checking
      PR #3's checks**: `iceberg-spark-runtime-3.5_2.12:1.11.0`'s jar is
      compiled to Java 17 class file version and can't load under this
      repo's JDK-11 CI matrix leg - not caused by today's work, but
      present since Iceberg support first landed, and cascading into
      three unrelated `spark-adapter` suites sharing the same forked JVM.
      A first attempt (a `Tests.Filter` skipping only `IcebergConnectorSpec`
      at test-run time) turned out insufficient and was replaced before
      landing: merely having the jar on the classpath is enough to break
      JDK 11, since Spark's `ServiceLoader`-based `DataSourceRegister`
      lookup scans every registered provider for *any* format-based read,
      not just Iceberg's - confirmed via a real CI failure where a plain
      CSV read in an unrelated suite aborted the same way. The actual fix
      excludes the Iceberg dependency itself from `libraryDependencies`
      under JDK <17, and excludes `IcebergConnectorSpec.scala`'s own
      compilation under the same condition. Verified by simulating both
      branches locally (not just trusting the sbt syntax): with Iceberg
      forced out, all other suites compile and pass cleanly (91/91, zero
      `ServiceLoader` aborts); with it present, all 111 tests including
      `IcebergConnectorSpec` pass unchanged. Confirmed for real in CI
      after pushing: `Test on ubuntu-latest / Java 11` went from failing
      to passing.

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

#### Sub-phase: Verify `rollback_to_snapshot` against a contract - pilot, with a mid-course design correction (done)

Piloted contract verification for one of the 10 state-changing CALL
procedures left fail-closed by the classification pass above, per the
user's own decision to sequence this as a pilot rather than building all
mechanisms at once.

- [x] **Original design proven wrong, not just buggy, and corrected
      honestly rather than patched around.** The first design read the
      *target snapshot's own* historical schema (Iceberg's
      `SparkTable.copyWithSnapshotId(id).schema()`) and checked that
      against the contract. A real end-to-end test - not a mock - showed
      the post-rollback schema unchanged even after `refreshTable()`;
      investigated seriously rather than assumed to be a caching bug,
      and corroborated against Apache Iceberg's own issue tracker
      (apache/iceberg#15165, open/unresolved): `rollback_to_snapshot`
      moves which snapshot's *data* is current but never reverts
      `current-schema-id` - schema evolution and snapshot rollback are
      independent in Iceberg's model, so the original design was
      checking a question the operation can't actually answer.
- [x] **Corrected design**, prompted by the user's own reframing ("both
      file path and schema - not schema alone"): check the table's
      *current* schema (which a rollback provably cannot change) plus
      location as a scoping gate - a rollback on a table the active
      contract doesn't govern is allowed, not swept into an unrelated
      contract's rejection, mirroring how `StructuralVerifier.verify`'s
      `Write` case already treats out-of-scope locations. New
      `StateChangingCallSupport.extract` (2 reflection hops: `Call.procedure()`
      to the concrete `RollbackToSnapshotProcedure`, then
      `BaseProcedure.tableCatalog()` to a plain public `TableCatalog` -
      down from 3 hops in the original design, and no Iceberg-specific
      type needed at all). New `StructuralVerifier.verifyStateChange` and
      a `ContractEnforcementRule.verifyOrThrow` branch consulting it
      before falling through to `FailClosedCommands`' blanket rejection.
- [x] **Mutation testing caught a real gap in the pilot's own test
      rigor, not just the code.** The "unrelated table" scoping test's
      contract only required an already-present field, so it would have
      passed even with scoping completely broken (mutated to always
      check schema); fixed by requiring a field the unrelated table
      genuinely lacks, so a real schema check - if scoping had failed -
      would provably fail. Mutation score went from 96.08% to **100%**
      (51/51) as a result; `mimaReportBinaryIssues` clean.
- [x] `IcebergConnectorSpec` PASS/FAIL/scoping tests for
      `rollback_to_snapshot`, plus a new `cherrypick_snapshot` fail-closed
      test taking over `rollback_to_snapshot`'s old role as the "some CALL
      still fails closed" proof.

#### Sub-phase: Extend state-changing CALL verification to the 5 procedures sharing `rollback_to_snapshot`'s shape (done)

`rollback_to_timestamp`/`cherrypick_snapshot`/`publish_changes`/
`set_current_snapshot`/`fast_forward`, closing the "small, mechanical
extension" scope this work's original sequencing called out.

- [x] **Investigated each procedure's real argument shape before
      generalizing, not assumed from the pilot alone.** `javap` against
      the real `iceberg-spark-runtime-3.5_2.12:1.11.0` jar confirmed each
      procedure's declared parameters, followed by a real probe (since
      deleted) against a live Iceberg session. `rollback_to_timestamp`/
      `cherrypick_snapshot`/`publish_changes` share `rollback_to_snapshot`'s
      exact 2-arg (table, value) shape. `set_current_snapshot` declares
      **3** parameters (table, `snapshot_id`, `ref` - mutually exclusive,
      confirmed via probe that `Call.args` is always 3-wide with exactly
      one of `args(1)`/`args(2)` non-null). `fast_forward`
      (`FastForwardBranchProcedure`) also declares 3 (table, `branch`,
      `to`) and is genuinely different: confirmed via probe that
      fast-forwarding `"main"` changes the table's default read, while
      fast-forwarding any other named branch leaves it unchanged - looked
      like it might need branch-aware special-casing, but doesn't: the
      existing check only asserts an invariant (current schema can't
      move) that holds regardless of which branch a call targets, proven
      with a dedicated test rather than left as a documentation claim.
- [x] **Generalized cleanly instead of branching per procedure.**
      `StateChangingCallSupport`'s single hardcoded procedure-class check
      became a `Map[String, String]` (procedure class → CALL-syntax
      name, used only for the error message via a new
      `StateChangeInfo.callName` field); extraction, verification, and
      `ContractEnforcementRule` wiring stayed unchanged and already
      generic.
- [x] Nine new `IcebergConnectorSpec` tests: one PASS per newly-recognized
      procedure (including both `set_current_snapshot` arg forms), a
      `rollback_to_timestamp` FAIL proving the generalized FAIL path still
      works, and the `fast_forward`-on-a-non-`"main"`-branch FAIL test
      proving the branch-agnostic design concretely. Replaced the old
      "`cherrypick_snapshot` still fails closed" test (no longer true)
      with an `add_files` fail-closed test.
- [x] Mutation testing scoped to both changed files:
      `StateChangingCallSupport.scala` 80% (4/5 - the one survivor a
      genuinely equivalent `Call`-class-name guard mutant, same category
      already accepted for this pattern in the pilot);
      `ContractEnforcementRule.scala` 100% (10/10). `mimaReportBinaryIssues`
      clean (both touched types stay `private[sparkadapter]`). Full
      `./dev/build`/`./dev/test`/`./dev/regression` passing.

#### Sub-phase: Verify the remaining 4 harder CALL procedures - `add_files`, `migrate`, `snapshot`, `rewrite_table_path` (done)

Closed the last operation-surface gap in Iceberg's CALL procedure
coverage. The original scoping (above) assumed all 4 needed a materially
harder mechanism (CALL-argument schema parsing); real investigation via
`javap` on the real jar plus a live probe (since deleted) per procedure
found that assumption wrong for 3 of the 4, in both directions.

- [x] **`add_files`/`migrate` fit the existing 6-procedure mechanism
      unchanged - zero new code beyond a map entry each.** Confirmed via
      probe: `add_files` never changes its target's schema regardless of
      the source's shape (an extra source column is silently dropped, a
      missing one is NULL-filled - the same narrower-append behavior
      already handled elsewhere), so `source_table` is never read at all.
      `migrate` converts its table in place; probed using the actual
      production code path (`TableCatalog.loadTable`, not a
      `spark.table(...)` read) that it correctly resolves the
      *pre*-migration schema, which Iceberg always preserves unchanged
      anyway.
- [x] **`snapshot` is the one procedure that's genuinely different**,
      confirmed via probe: it creates a *new* table whose schema comes
      from a *different, existing* source table - the opposite
      schema/location pairing from every other procedure - and both
      arguments can be qualified with a catalog other than the CALL's
      own. Needed genuinely new resolution (`resolveIdentifier`,
      re-implementing Iceberg's own `Spark3Util.catalogAndIdentifier`
      algorithm) using `SparkSession.active`'s `CatalogManager` - fully
      public Spark APIs, not reflection, unlike every other procedure's
      `tableCatalogOf`. Hit one real Scala access-control surprise along
      the way (`CatalogManager`'s type can't be named directly in this
      module despite public bytecode - it's `private[sql]` at the Scala
      level) - worked around by passing `SparkSession` itself instead.
- [x] **`rewrite_table_path` needed no verification mechanism at all - a
      real positive finding.** Confirmed via probe: never touches the
      table's own catalog entry, schema, or snapshot, and registers no
      new catalog table itself - joined `FailClosedCommands`' safe list
      instead, the same disposition as the original 10 compaction/GC
      procedures.
- [x] **Solved a real environment obstacle without a new dependency.**
      Testing `migrate`/`snapshot` needs a default catalog that resolves
      both native and Iceberg tables; a Hadoop-type catalog rejects
      `migrate` unconditionally (any table with a pre-existing "custom"
      location, which every native table has - confirmed via probe, not
      a path-format bug). A real Hive metastore would need a new
      `spark-hive` test dependency; used Iceberg's `JdbcCatalog` against
      H2 instead, already transitively present on the test classpath.
- [x] Mutation testing found two real gaps (not just cited a score): a
      `resolveIdentifier` boundary condition (multi-part identifier
      without a catalog prefix) had no test distinguishing correct
      catalog-fallback behavior from a mutant that mishandled it - closed
      with a dedicated permanent test, which incidentally also proved
      that exact resolution path for real. Final score:
      `StateChangingCallSupport.scala` 85% (17/20 - 3 accepted
      near-equivalents, each documented inline at the survived mutant);
      `FailClosedCommands.scala` (`rewrite_table_path`'s new safe-list
      entry) 100% (4/4). `mimaReportBinaryIssues` clean. All 20 Iceberg
      CALL procedures now have a permanent, evidenced disposition - none
      left unmodeled.

#### Sub-phase: Parquet connector support (done)

Third connector onboarded via the `add-spark-connector` skill's process —
a different shape than Delta/Iceberg, since Parquet is Spark's own
built-in `FileFormat`, not a separate library: nothing added to
`build.sbt`, not even a `% "test"` dependency. Full findings and both
coverage ledgers are in docs/SPARK_ADAPTER.md's new "Parquet support"
section — summary here:

- [x] **A real bug found and fixed: streaming writes to any plain
      `FileFormat`-based sink (Parquet, but also CSV/JSON/ORC/text —
      they all share Spark's built-in `FileStreamSink`) resolved to a
      useless, non-matching location — plus a wrong first diagnosis,
      corrected by a mutation-testing "equivalent mutant" signal, not
      assumed correct on the first pass.** `WriteCommandSupport`'s
      `WriteToStream` case (built for Delta) tries `sink.name()`, then a
      reflective public `path()` method, then falls back to `toString`.
      The first fix assumed `FileStreamSink.name()` doesn't throw
      (unlike `DeltaSink.name()`) and returns a descriptive
      `"FileSink[<path>]"` string taken at face value — but a manually
      applied mutation of that guard produced no test failure, the
      documented Stryker4s "equivalent mutant" symptom, prompting
      re-investigation. A direct-construction probe (a real
      `FileStreamSink` built by hand, no live query) confirmed
      `FileStreamSink.name()` genuinely throws the same way `DeltaSink`'s
      does — the real bug was one tier later: the reflective `path()`
      lookup only ever tried a *public method*, and `FileStreamSink.path`
      is a `private final` field with no public accessor at all, so every
      plain-`FileFormat` streaming write fell through to `toString`,
      unconditionally failing `OUTPUT_LOCATION_MISMATCH` against any
      contract's real declared path. Caught by a real PASS test failing,
      not inspection. Fixed by extending the reflective lookup itself to
      also try the declared field, not by special-casing `FileStreamSink`
      in the tier-2 guard — a genuine bonus beyond the location fix:
      `streamSinkFormatOf` previously always returned `None` for any
      non-Delta sink; now every `FileFormat`-based streaming write gets a
      real, precise format too, via the same private-field reflection.
- [x] **A genuinely new operation-surface finding, not a bug:
      `.saveAsTable()` append onto an existing table is a third instance
      of the "one call, two nested Command-shaped plans" pattern**
      already known from Delta/Iceberg's `StagedTable` case, via a
      different mechanism this time (`CreateDataSourceTableAsSelectCommand.run()`'s
      own internal delegation to a nested `InsertIntoHadoopFsRelationCommand`
      when it detects the target already exists). Confirmed via
      `injectCheckRule` that `ContractEnforcementRule` runs twice for one
      logical write — and, unlike the `StagedTable` case, confirmed this
      needs **no fix**: both plans resolve to the identical physical
      location, so a satisfying write passes both and a violating one is
      rejected at the first, before the nested insert ever runs (verified
      via a real PASS/FAIL pair, the FAIL half asserting the row count is
      unchanged). A related, *unresolved* version of this same trap — a
      brand-new table created without an explicit path option, where the
      outer command's location falls back to the qualified catalog
      identifier while the nested insert's is a real physical path — was
      found but left out of scope for this pass (every existing
      precedent test, this pass's own included, sidesteps it with an
      explicit path) and documented as real next-step work, not silently
      left implicit.
- [x] **Confirmed, empirically, that Parquet's operation surface is
      architecturally smaller than Delta/Iceberg's, not just less
      explored.** Time travel, CDC/incremental reads, row-level DML
      (`MERGE`/`UPDATE`/`DELETE`), and DataSourceV2-catalog writes against
      an existing table (`.writeTo(...).append/overwrite/overwritePartitions()`)
      or via `createOrReplace()`/`.replace()` are all genuinely rejected
      by Spark itself for plain Parquet (`Cannot write into v1 table`,
      `does not support REPLACE TABLE AS SELECT`, `MERGE INTO TABLE is
      not supported temporarily`, or simply no SQL syntax exists) — real
      architectural constraints, not "Invariant hasn't translated this
      yet," the first connector where these rows are 🚫 **N/A** rather
      than future work. Confirmed via real probes and permanent
      regression tests, not assumed from the operations' absence.
- [x] **Feature surface: the "every column nullable on read-back"
      behavior this document previously attributed only to Delta (and
      separately rediscovered for Iceberg) is confirmed to originate from
      Parquet itself** — both connectors store data as Parquet under the
      hood, so this was always Parquet's own reader behavior, not
      something either connector does. Schema merging (`mergeSchema`) and
      `partitionBy` column discovery both confirmed transparent (no
      "generated columns"-style false-rejection gap); a corrupt file in
      the read path confirmed to fail entirely within Spark's own
      machinery, orthogonal to Invariant either way. Two feature-surface
      items (legacy timestamp/date rebase mode, writer-side storage
      optimizations) left ❓ **Not investigated**, honestly scoped out
      rather than assumed transparent by analogy.
- [x] Mutation testing scoped to the changed file
      (`WriteCommandSupport.scala`, the only `src/main/scala` file this
      pass touched): **80.0% (24/30 non-excluded mutants killed) — see
      docs/SPARK_ADAPTER.md's "Mutation testing" section for the full
      per-mutant reasoning.** All 6 survivors investigated, not just
      cited: 4 are pre-existing, unrelated to this pass (`unwrapWriteWrapper`'s
      already-documented no-wrapper branch, and two `deltaRowLevelDml`
      guards untouched by this change); the 2 in code this pass actually
      added are the `streamSinkFormatOf` `EqualityOperator`/`!=` mutant
      (killed) and a `ConditionalExpression`-to-`true` mutant on its
      `FileStreamSink` guard that survives because it's provably
      equivalent — confirmed by hand, not asserted: for a real
      `FileStreamSink`, forcing the guard to unconditionally `true`
      produces the identical result the real guard already does; for any
      other sink, the reflective `fileFormat` field lookup fails either
      way, producing `None` either way. `mimaReportBinaryIssues` clean
      (only private-method bodies changed, no public signature touched).
      `./dev/build`/`./dev/test`/`./dev/regression` all pass against real
      `spark-submit`. No new
      dependency to verify the absence of (Parquet needed none).

#### Sub-phase: Parquet's last two ❓ feature-surface rows closed - rebase mode, storage optimizations (done)

Same-day follow-up closing exactly the two rows the initial Parquet pass
left ❓. Both closed with real probes and permanent tests, **zero
production code changes** — both were real questions with real, testable
answers, not gaps needing a fix.

- [x] **Legacy timestamp/date rebase mode**: a pre-Gregorian-calendar
      date/timestamp written under `LEGACY` and read back under
      `CORRECTED` round-trips with its `DateType`/`TimestampType` schema
      completely unchanged — confirmed directly, not assumed: rebase mode
      only ever affects the encoded value, never a field's declared type
      or nullability. Also confirmed the strictest setting (`EXCEPTION`)
      never blocks analysis — it exists to guard genuinely ambiguous
      files (no rebase metadata tag, i.e. written by pre-2.4.6 Spark),
      never triggered by a file a modern Spark itself wrote.
- [x] **Writer-side storage optimizations** (bloom filters, dictionary
      encoding, summary metadata): zero effect on the analyzed column set
      or on `streamSinkFormatOf`/`formatOf`'s format detection —
      physical storage-layer decisions Parquet's reader resolves entirely
      below the `LogicalPlan` level this adapter translates at.
- [x] Both findings are new `ParquetConnectorSpec` tests (18 tests total
      in that suite after this pass, up from 16). No `spark-adapter` main
      source changed this pass, so CLAUDE.md's mutation-testing
      requirement (scoped to changed/added `src/main/scala` files)
      doesn't apply. Both of Parquet's coverage ledgers (operation
      surface and feature surface) are now fully closed — no ❓ rows
      remaining in either.

#### Sub-phase: CSV connector support (done)

Fourth connector onboarded via the `add-spark-connector` skill's process —
same shape as Parquet's, since CSV is also Spark's own built-in
`FileFormat`, not a separate library: nothing added to `build.sbt`. Full
findings and both coverage ledgers are in docs/SPARK_ADAPTER.md's new
"CSV support" section — summary here:

- [x] **Empirically confirmed, not assumed by analogy, that CSV's
      operation surface routes through the exact same generic mechanisms
      Parquet's pass already proved out** — `InsertIntoHadoopFsRelationCommand`,
      `CreateDataSourceTableAsSelectCommand`, and `WriteToStream`/
      `FileStreamSink` (including the private-field-reflection fix from
      Parquet's pass, confirmed to generalize to `CSVFileFormat`
      specifically via a real direct-construction test, not just taken on
      faith from that fix's own "connector-agnostic" doc comment). **No
      bugs found, no new structural pattern, zero `src/main/scala`
      changes** — a pure confirmatory audit on the operation surface.
      Added one permanent test Parquet's own pass never had:
      `.writeTo(...).createOrReplace()`/`.replace()` rejection, which
      Parquet's ledger row only ever rested on probe-phase confirmation.
- [x] **The real substance was the feature surface, not the operation
      surface** — CSV is a plain text format with no native schema,
      unlike self-describing Parquet. Six CSV-specific behaviors
      confirmed with a real probe and codified into a permanent test:
      schema-inference default (no `inferSchema` ⇒ every column reads
      back as `StringType`, and a contract declaring a numeric type
      against such a read is correctly rejected as
      `OUTPUT_FIELD_TYPE_MISMATCH`), header handling (`header=false`
      falls back to positional `_c0`/`_c1` names, ordinary column names
      either way), malformed-record modes (`FAILFAST` fails only at
      execution, never analysis — the same orthogonal pattern as
      Parquet's corrupt-file case; `DROPMALFORMED` silently drops bad
      rows without affecting the analyzed schema), `columnNameOfCorruptRecord`
      (an ordinary extra `StringType` column, no special handling),
      nullability on read-back (every field `nullable = true`
      regardless of declared schema — confirmed independently for CSV's
      own non-Parquet-based reader, not inherited from Parquet the way
      Delta's/Iceberg's version of this finding was), and date parsing
      with a custom `dateFormat` (an unparseable date becomes `null`
      under `PERMISSIVE`, not a failure, schema stays `DateType`).
- [x] **A real test-writing bug, caught and fixed before landing, not a
      product bug**: the first draft of the two streaming tests declared
      `type: long` for fields sourced from an `inferSchema=true` CSV
      read, but CSV's inference actually resolves small whole numbers to
      `IntegerType` — both tests correctly failed with a real
      `ContractViolationException` on first run (the engine doing its
      job), fixed by declaring `type: integer` to match CSV's real
      inferred schema.
- [x] No `spark-adapter` main source changed this pass, so CLAUDE.md's
      mutation-testing requirement (scoped to changed/added
      `src/main/scala` files) doesn't apply — confirmed via `git status`
      before closing, not just assumed. `mimaReportBinaryIssues` is
      unaffected for the same reason (no public signature, no signature
      of any kind, touched). Full `spark-adapter` suite (163 tests, all 8
      specs) passes; `CsvConnectorSpec` itself: 19 tests, all green.
      Throwaway probe scaffolding (`CsvConnectorProbeSpec.scala`) deleted
      before this pass ended, per the skill's own convention. Both of
      CSV's coverage ledgers (operation surface, feature surface) are
      fully closed — no ❓ rows remaining in either.

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
- [ ] Interpreting `rules` from the contract model — see the item
      immediately below for the concrete feature this unblocks first
- [ ] **Full semantic DML verification** (row-level `MERGE`/`UPDATE`/
      `DELETE`). Structural verification of these three (target location/
      schema, MERGE's source as an input) is done — see the "Delta Lake
      operation-surface coverage ledger" sub-phase below. What's still
      unverified, deliberately: the operation's actual row-level logic -
      the merge condition, which columns an `UPDATE` touches, whether a
      `DELETE` is unconditional. This needs **two** things together, not
      one:
      1. An IR extension modeling the operation itself (`ir.Write` only
         models "replace/append the output of a query" - something like
         `ir.Merge`/`ir.RowMutation` capturing condition/matched-clauses/
         not-matched-clauses would be needed).
      2. Interpreting contract `rules` (the item above) - without this,
         an `ir.Merge` node would hold structure nothing could check,
         since `StructuralVerifier` only compares schema/format/location/
         save-mode, and a contract has no vocabulary yet for constraining
         a merge condition or which columns an update may touch.
      Concrete example of the rule vocabulary this would need (not
      hypothetical - discussed and explicitly deferred, not forgotten):
      ```yaml
      rules:
        - type: merge_condition
          on: [customer_id]
        - type: forbid_unconditional_delete
        - type: allowed_update_columns
          columns: [status, updated_at]
      ```
      Building the IR node before the rule vocabulary exists to consume
      it would be speculative API surface in a MiMa-checked module - the
      two should be designed together, not the IR first. Not started;
      deliberately scoped out of the structural-DML pass below, per an
      explicit user decision to keep this session's DML work structural-
      only and document the fuller version here instead of losing it.

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
