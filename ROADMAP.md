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

#### Scope (Future)

- [ ] Contract verification algorithm (schema/dependency/transformation checks per [MISSION.md, §8](MISSION.md#8-contract-verification)), consuming `Lineage.trace` output and a parsed `Contract`
- [ ] Verification result format
- [ ] Integration tests against the `plugin`/`runner` demo pipeline verifying an actual contract (not just extracting lineage)

#### Dependencies

- Phase 1a completion (contract model)
- Phase 1b completion (transformation IR)
- Spark adapter completion (above)

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
