# Compatibility Matrix

Invaract maintains compatibility across multiple versions of Java, Scala, and Apache Spark.

## Quick Reference

| Component | Version | Status | Notes |
|-----------|---------|--------|-------|
| **Java** | 11, 17, 21 | ✓ Supported | 21 recommended (latest LTS) |
| **Scala** | 2.12.18 | ✓ Supported | Standard for Spark 3.5.x |
| **Apache Spark** | 3.5.1 | ✓ Primary | Latest stable, well-tested |
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

- **Scala 2.12.18** (standard binary for Spark 3.5.1)
- Binary compiled once, works across Java 11+
- No cross-compilation needed for Phase 0

### Future Plans

- **0.2.0+**: Add Scala 2.13.x support via cross-compilation
- Build system will produce both 2.12 and 2.13 binaries
- CI tests on both versions
- Recommend Scala 2.13 for new projects (safer defaults)

## Apache Spark Compatibility

### Supported Versions

| Spark | Status | Tested | Adapter |
|-------|--------|--------|---------|
| 3.5.1 | ✓ Primary | Ubuntu, macOS, Windows | Native |
| 3.5.x | ✓ Supported | Spot checks | Native (may differ in edge cases) |
| 3.4.x | ◐ Untested | Not yet | Manual testing required |
| 3.6.x+ | ◐ Planned (Phase 2) | Future | Adapter pattern |

### Why Spark 3.5.1

- Latest stable release (Aug 2024)
- Long-term support planned
- Excellent Scala integration
- Rich DataFrame API
- Performance improvements
- Compatible with JDK 21

### Test Environment

```bash
# Current test environment
spark-submit --version
# Apache Spark 3.5.1
```

### Using Older Spark Versions

If your environment requires Spark 3.4.x:

1. Test plugin locally: `./dev/test`
2. If tests pass, integration should work
3. Report any issues: [GitHub Issues](https://github.com/mlltx/Invaract/issues)
4. Phase 2 will add official multi-version support

### Newer Spark Versions (3.6.x+)

Spark 3.6.x is not yet released but will be tested when available.

Expected compatibility:
- Logical plan API should be stable
- May require adapter for schema handling
- Phase 2 introduces adapter pattern for multi-engine support

## Execution Environment

### Local Development

**GitHub Codespaces (Recommended):**
- Auto-provisioned: JDK 21, sbt, Spark 3.5.1, Node.js 20
- Works from any browser (including mobile)
- No local setup required
- Port forwarding available

**Local Machine:**
- Manual installation of: JDK 21, sbt, Spark 3.5.1, Node.js 20
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
  <groupId>com.mlltx</groupId>
  <artifactId>invaract-spark-plugin</artifactId>
  <version>0.1.0</version>
</dependency>
```

### Dependency Version Lock

**Plugin dependencies (build.sbt):**
```scala
"org.apache.spark" %% "spark-sql" % "3.5.1" % "provided"
"org.scalatest" %% "scalatest" % "3.2.18" % "test"
```

- Spark marked as "provided" (not bundled with plugin)
- Test dependencies locked to specific versions
- No transitive version conflicts

**Runner dependencies (build.sbt):**
```scala
"org.apache.spark" %% "spark-sql" % "3.5.1"
"org.apache.spark" %% "spark-core" % "3.5.1"
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

To test with Spark 3.5.0 or 3.6.0:

1. Download from [archive.apache.org/dist/spark](https://archive.apache.org/dist/spark/)
2. Set environment: `export SPARK_HOME=/path/to/spark-3.5.0-bin-hadoop3`
3. Test: `./dev/test`
4. Report results if different from 3.5.1

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
