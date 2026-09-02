# Compatibility Matrix

Invaract maintains compatibility across multiple versions of Java, Scala, and Apache Spark.

## Quick Reference

| Component | Version | Status | Notes |
|-----------|---------|--------|-------|
| **Java** | 11, 17, 21 | ✓ Supported | 21 recommended (latest LTS) |
| **Scala** | 2.12.18 | ✓ Supported | Standard for Spark 3.5.x |
| **Apache Spark** | 3.5.7 | ✓ Primary | Latest stable, well-tested |
| **sbt** | 1.9+ | ✓ Supported | Build tool for plugin/runner |
| **Node.js** | 20 | ✓ Supported | For web UI development |

## Java/JDK Compatibility

### Supported Versions

| JDK | Status | Tested On | Notes |
|-----|--------|-----------|-------|
| 11 | ✓ Supported | Ubuntu, macOS, Windows | Oldest supported LTS |
| 17 | ✓ Supported | Ubuntu, macOS, Windows | Mid-cycle LTS |
| 21 | ✓ Supported | Ubuntu, macOS, Windows | Latest LTS (recommended) |

### Bytecode Target

- **Compile target**: Java 1.8 (via `-target:jvm-1.8` in scalacOptions)
- **Runtime requirement**: JDK 11+ (JVM 11+)
- **Recommendation**: JDK 21 for performance and security patches

### Version-Specific Notes

**JDK 11:**
- Oldest currently supported version
- End of life: September 2026
- Full Spark compatibility
- Use only if enterprise policy requires it

**JDK 17:**
- LTS release
- End of life: September 2029
- Full Spark compatibility
- Recommended for conservative deployments

**JDK 21:**
- Latest LTS (as of 2024)
- End of life: September 2031
- Full Spark compatibility
- **Recommended**: Best performance, longest support timeline

### Testing

CI/CD pipeline tests all supported JDK versions:
- Ubuntu: JDK 11, 17, 21
- macOS: JDK 17, 21 (11 excluded to reduce matrix)
- Windows: JDK 17, 21 (11 excluded to reduce matrix)

## Scala Compatibility

### Supported Versions

| Scala | Status | Spark Support | Binaries |
|-------|--------|---------------|----------|
| 2.12.18 | ✓ Primary | Spark 3.5.x | Included |
| 2.13.x | ◐ Planned (Phase 1) | Spark 3.5.x, 3.6.x | Future |

### Current Release (0.1.0)

- **Scala 2.12.18** (standard binary for Spark 3.5.7)
- Binary compiled once, works across Java 11+
- No cross-compilation needed for Phase 0

### Future Plans

- **0.2.0+**: Add Scala 2.13.x support via cross-compilation
- Build system will produce both 2.12 and 2.13 binaries
- CI tests on both versions
- Recommend Scala 2.13 for new projects (safer defaults)

## Apache Spark Compatibility

### Supported Versions

This is now a real, CI-enforced matrix, not a projection — CI's `spark-version-matrix` job
(`.github/workflows/test.yml`) runs `spark-adapter`'s full test suite against every row
marked "Verified" on every push, not a one-time spot check. See
`docs/SPARK_ADAPTER.md`'s "Spark version compatibility" section for how the job works, and
`docs-site`'s [Spark Version Support](https://github.com/mlltx/invaract/blob/main/docs-site/src/content/docs/reference/spark-version-support.mdx) page for the user-facing version.

| Spark | Status | Notes |
|-------|--------|-------|
| < 3.5.6 (incl. 3.5.1) | Not supported | Fails for real, confirmed by CI, not assumed: `delta-spark` 3.3.3 (this module's pinned Delta version) needs a class (`SupportsNonDeterministicExpression`) that doesn't exist before Spark 3.5.6, so any spec building a Delta-extended session aborts with `ClassNotFoundException`. |
| 3.5.6 | <!-- generate-version-docs:BEGIN spark 3.5.6 -->✓ Verified<!-- generate-version-docs:END --> | Floor of the supported range — the actual, CI-confirmed floor, not 3.5.1. |
| 3.5.7 | <!-- generate-version-docs:BEGIN spark 3.5.7 -->✓ Verified, Primary<!-- generate-version-docs:END --> | Current default (`spark-adapter/build.sbt`'s `sparkVersion`); what a real `./dev/test` run installs. |
| 3.5.9 | <!-- generate-version-docs:BEGIN spark 3.5.9 -->✓ Verified<!-- generate-version-docs:END --> | Newest verified patch. |
| Other 3.5.x ≥ 3.5.6 | Expected, unverified | Spark's own patch releases don't change Catalyst's plan shapes, but only the three rows above are actually CI-checked. |
| 3.4.x | Not supported | Never verified; not a claim this repo makes. |
| 4.x | Not supported | Requires Scala 2.13 (Spark 4.0 dropped 2.12); this repo has no Scala cross-build. A real project, not a CI-leg addition — see `docs/SPARK_ADAPTER.md`'s "Deferred: Spark 4.x" note. |

### Why Spark 3.5.x

- Long-term support line
- Excellent Scala 2.12 integration
- Rich DataFrame API
- Compatible with JDK 21

### Test Environment

```bash
# Current test environment
spark-submit --version
# Apache Spark 3.5.7
```

### Using a different Spark 3.5.x patch

`spark-adapter/build.sbt`'s `sparkVersion` reads an `INVARACT_TEST_SPARK_VERSION`
environment variable (falling back to `3.5.7` when unset), so testing against any 3.5.x
patch **at or above 3.5.6** — including ones outside the three CI-verified rows above —
doesn't require editing `build.sbt`:

```bash
cd spark-adapter
INVARACT_TEST_SPARK_VERSION=3.5.8 sbt test
```

A patch below 3.5.6 will fail the same way 3.5.1 did above — that's not a gap in this
override mechanism, it's a real Delta version floor.

Report any issues on a patch outside the verified set:
[GitHub Issues](https://github.com/mlltx/Invaract/issues).

### Spark 3.4.x and 4.x

Neither is supported today. 3.4.x has simply never been verified — there's no known
incompatibility, just no evidence either way. 4.x is a real gap: this repo compiles for
Scala 2.12 only, and Spark 4.0 requires Scala 2.13. Supporting it means a genuine
cross-compilation project (see `docs/SPARK_ADAPTER.md`), not an addition to the CI matrix
above.

## Connector Library Compatibility

`spark-adapter` also pins a single version of each connector library it's tested against
(`delta-spark`, `iceberg-spark-runtime`), test-scope only. The same "one version proves nothing
about another" gap the Spark-version matrix closed applied here too — CI's
`delta-version-matrix` and `iceberg-version-matrix` jobs close it now. See
`docs/connectors/delta.md` and `docs/connectors/iceberg.md`'s own "Version compatibility"
sections for the full mechanism and rationale.

### Delta Lake

| Version | Status | Notes |
|---------|--------|-------|
| 3.3.3 | <!-- generate-version-docs:BEGIN delta 3.3.3 -->✓ Verified, Primary and only supported release<!-- generate-version-docs:END --> | Current default (`spark-adapter/build.sbt`'s `deltaVersion`). Also the newest published release — confirmed via `delta-spark_2.12`'s own `maven-metadata.xml`. |
| 3.3.0 – 3.3.2 | Not supported | Confirmed by a real CI failure (the identical `TableCapabilityCheck` error 3.2.x hits), then root-caused: delta-io's own `LATEST_RELEASED_SPARK_VERSION` shows these three still target Spark 3.5.3 — only 3.3.3 moved to 3.5.6. |
| 3.2.x | Not supported | Same underlying reason as above — targets Spark 3.5.0/3.5.3, below this repo's Spark floor. See `docs/connectors/delta.md`. |

Unlike Iceberg below, Delta currently has **no working floor below the current pin** — `3.3.3`
is the only `delta-spark` release compatible with this repo's supported Spark range at all, not
just the newest. The `delta-version-matrix` CI job stays a single-leg job until delta-io
publishes a release that also targets Spark 3.5.6+.

### Iceberg

| Version | Status | Notes |
|---------|--------|-------|
| 1.10.0 | Not supported | Confirmed real bug inside this release (`apache/iceberg#14232`, Avro API mismatch) — this is why the repo never pinned it, not something re-tested by the matrix. |
| 1.10.2 | <!-- generate-version-docs:BEGIN iceberg 1.10.2 -->✓ Verified<!-- generate-version-docs:END --> | Latest patch of the previous minor line. |
| 1.11.0 | <!-- generate-version-docs:BEGIN iceberg 1.11.0 -->✓ Verified, Primary<!-- generate-version-docs:END --> | Current default (`spark-adapter/build.sbt`'s `icebergVersion`). Also the newest published release — confirmed via `iceberg-spark-runtime-3.5_2.12`'s own `maven-metadata.xml`. |

## Execution Environment

### Local Development

**GitHub Codespaces (Recommended):**
- Auto-provisioned: JDK 21, sbt, Spark 3.5.7, Node.js 20
- Works from any browser (including mobile)
- No local setup required
- Port forwarding available

**Local Machine:**
- Manual installation of: JDK 21, sbt, Spark 3.5.7, Node.js 20
- Supported on: Linux, macOS, Windows (with WSL2)
- See [CONTRIBUTING.md](../CONTRIBUTING.md) for setup

### CI/CD Environment

**GitHub Actions:**
- Runs on GitHub-hosted runners (Linux, macOS, Windows)
- Matrix testing: 3 OS × 3 JDK versions = 9 combinations
- Spark installed during workflow
- Tests run on real local Spark master

## Platform Compatibility

### Operating Systems

| OS | Status | Tested | Notes |
|----|---------| -------|-------|
| Linux (Ubuntu 22.04+) | ✓ Supported | CI | Primary development target |
| macOS (12+) | ✓ Supported | CI | Works with Apple Silicon (arm64) |
| Windows 11 | ✓ Supported | CI | Requires WSL2 for native build |

### Linux

- Ubuntu 20.04+, Debian 11+, CentOS 7+
- Both x86_64 and ARM64 architectures
- Recommended for servers and CI/CD

### macOS

- macOS 12.x and later
- Apple Silicon (M1/M2/M3) supported
- Intel (x86_64) supported
- Tested on latest 2-3 versions

### Windows

- Windows 11 recommended
- Requires WSL2 (Windows Subsystem for Linux) for best experience
- GitHub Codespaces recommended (no local setup)
- Native Windows support: sbt works, Spark may have path issues

## Dependency Compatibility

### Maven Central

Plugin and runner are built for Maven publication:

```xml
<dependency>
  <groupId>com.invaract</groupId>
  <artifactId>invaract-spark-plugin</artifactId>
  <version>0.2.0</version>
</dependency>
```

### Dependency Version Lock

**Plugin dependencies (build.sbt):**
```scala
"org.apache.spark" %% "spark-sql" % "3.5.7" % "provided"
"org.scalatest" %% "scalatest" % "3.2.18" % "test"
```

- Spark marked as "provided" (not bundled with plugin)
- Test dependencies locked to specific versions
- No transitive version conflicts

**Runner dependencies (build.sbt):**
```scala
"org.apache.spark" %% "spark-sql" % "3.5.7"
"org.apache.spark" %% "spark-core" % "3.5.7"
```

- Full Spark bundled with runner
- Used for `spark-submit` execution
- Self-contained distribution

## Upgrading

### Upgrading Java

```bash
# Check current version
java -version

# Install newer JDK
# macOS: brew install openjdk@21
# Ubuntu: sudo apt install openjdk-21-jdk
# Windows: Use Windows Installer or WSL2

# Set default
export JAVA_HOME=/path/to/jdk21
```

No changes needed to code; backward compatible.

### Upgrading Spark

For `spark-adapter`'s own suite against a different 3.5.x patch, see "Using a different
Spark 3.5.x patch" above (`INVARACT_TEST_SPARK_VERSION`, no `build.sbt` edit needed). To
exercise the full end-to-end harness (`./dev/test`, real `spark-submit`) against a
different Spark distribution:

1. Download from [archive.apache.org/dist/spark](https://archive.apache.org/dist/spark/)
2. Set environment: `export SPARK_HOME=/path/to/spark-<version>-bin-hadoop3`
3. Test: `./dev/test`
4. Report results if different from 3.5.7 (the current default both approaches share)

### Upgrading Dependencies

For security updates or bug fixes:

```bash
cd plugin
sbt dependencyUpdates  # See available upgrades
sbt update             # Download latest compatible

cd ../runner
sbt dependencyUpdates
sbt update

cd ../web
npm outdated          # See available upgrades
npm update            # Update in package.json
```

## Known Issues

### JSON Serialization

**Issue**: Report.json schema field shows Java object reference instead of actual schema

**Status**: Cosmetic (non-blocking)

**Impact**: Web UI falls back to displaying expected schema; data is correct

**Plan**: Fix in next patch release

### Windows Path Handling

**Issue**: spark-submit path handling may be different on Windows

**Workaround**: Use GitHub Codespaces or WSL2

**Status**: Will test natively in Phase 1

## Future Compatibility Changes

### Planned Additions

- **Scala 2.13** support (Phase 1)
- **Spark 3.6.x** support (Phase 2)
- **Spark 3.7.x** support (Phase 2+)

### Planned Removals

- **JDK 11** support likely dropped in 2.0.0 (2026)
- **Scala 2.12** support likely dropped in 2.0.0 (2026)
- **Spark 3.4.x** support never officially added

## Getting Help

**Compatibility questions:**
- Open GitHub Issue: "[COMPATIBILITY] Question"
- Post in Discussions with tag "compatibility"

**Report compatibility bugs:**
- Include: JDK version, OS, Spark version, exact error
- Attach: Output from `java -version`, `spark-submit --version`, `./dev/test`

---

**Last Updated:** 2024-08-20
**Next Review:** Phase 1 launch
