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
- **Structural contract verification (Phase 1c, first slice)**:
  `ContractVerifier.verify` checks a `Contract`'s declared output against
  a real Spark job's actual output schema and traced lineage — per-field
  presence, type compatibility, and lineage-traceability
  - `demo/contracts/invariant_output.yaml`: a real contract for the demo
    pipeline's actual output
  - Wired into `runner/PluginRunner.scala`: every `./dev/test` run now
    verifies the real output against the real contract, reported in its
    own `contractVerification` section (kept distinct from
    `ExecutionReport.status` — job success and contract compliance are
    different questions)
  - 4 tests: the real pipeline passing its own contract, and two
    deliberately broken contracts (missing required field; wrong
    declared type) genuinely failing against the same real output

### Fixed

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
