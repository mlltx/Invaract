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
