# Versioning Strategy

Invaract follows [Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html).

## Version Format

```
MAJOR.MINOR.PATCH[-PRERELEASE][+BUILD]
```

Examples:
- `0.1.0` – Initial release
- `0.1.1` – Patch release (bug fixes)
- `0.2.0` – Minor release (new features, backward compatible)
- `1.0.0` – Major release (breaking changes)
- `1.0.0-alpha.1` – Pre-release
- `1.0.0+build.001` – Build metadata

## Versioning Rules

### MAJOR (X.0.0)

Increment when breaking changes are introduced:

- Removing public APIs
- Changing API signatures (methods, return types)
- Changing behavior in breaking ways
- Dropping support for old dependency versions
- Changing contract representation format (non-backward compatible)
- Changing verification algorithm results

**Process:**
- Must be approved by maintainers
- Requires migration guide in release notes
- Documented in CHANGELOG.md under "Breaking Changes"
- Spark/Scala/Java compatibility may change

### MINOR (X.Y.0)

Increment when adding features in backward-compatible way:

- Adding new public methods
- Adding new transformations
- Adding new contract validators
- Improving performance
- Adding documentation
- Adding optional parameters (with defaults)

**Process:**
- Peer reviewed
- Documented in CHANGELOG.md under "Added"
- No API removals
- Backward compatible with MAJOR.MINOR-1

### PATCH (X.Y.Z)

Increment for bug fixes and small improvements:

- Bug fixes (non-breaking)
- Documentation fixes
- Dependency updates (security patches, compatible upgrades)
- Performance improvements
- Refactoring (no behavior change)
- Test improvements

**Process:**
- Can be released quickly (same day)
- Documented in CHANGELOG.md under "Fixed"
- No API changes
- Backward compatible with X.Y.Z-1

## Pre-Release Versions

Format: `X.Y.Z-PRERELEASE`

Examples: `0.2.0-alpha.1`, `0.2.0-beta.2`, `1.0.0-rc.1`

**When to use:**
- Testing major features before release
- Getting community feedback
- Verifying breaking changes
- Alpha/beta/RC phases

**Process:**
- Not recommended for production use
- CI/CD tests run same as stable releases
- Documentation may be incomplete
- Can iterate rapidly (0.2.0-alpha.1 → 0.2.0-alpha.2 → 0.2.0-beta.1, etc.)

## Compatibility Guarantees

### Within Major Version (0.1.0 → 0.2.0 → 0.3.0)

- Public API is stable within MAJOR version
- New features will not break existing code
- Existing transformations will continue to work
- Verified lineage format may evolve (with migration guide)

### Across Major Versions (0.x → 1.0 → 2.0)

- Breaking changes explicitly called out
- Migration guide provided
- Old major versions no longer receive security updates (see Support Policy)

## Dependency Version Strategy

### Spark

| Invaract | Spark | Support Level |
|-----------|-------|---------------|
| 0.1.x     | 3.5.x | Primary       |
| 0.2.x     | 3.5.x | Supported     |
| 1.0.x     | 3.5.x, 3.6.x | Supported |

**Compatibility guarantee:** Plugin compiled for Spark 3.5.1 will work with 3.5.x versions. 3.6+ may require adapter pattern (Phase 2).

### Scala

| Invaract | Scala | Support Level |
|-----------|-------|---------------|
| 0.1.x     | 2.12.18 | Primary       |
| 0.2.x     | 2.12.18, 2.13.x | Supported |
| 1.0.x     | 2.12.18, 2.13.x | Supported |

**Compatibility guarantee:** Cross-compiled binaries provided for supported versions.

### Java

| Invaract | Java | Support Level |
|-----------|------|---------------|
| 0.1.x     | 11+  | Supported     |
| 1.0.x     | 11+  | Supported     |
| 2.0.x     | 17+  | Supported     |

**Compatibility guarantee:** Code compiles to Java bytecode 1.8 (compatible with Java 11+).

## Support Timeline

### End of Life Policy

| Version | Released | LTS? | End of Life |
|---------|----------|------|-------------|
| 0.1.x   | Aug 2024 | No   | Dec 2024    |
| 0.2.x   | TBD      | No   | TBD + 3 months |
| 1.0.x   | TBD      | Yes  | TBD + 2 years |
| 2.0.x   | TBD      | Yes  | TBD + 2 years |

**LTS (Long Term Support):** Receives security updates and critical bug fixes for extended period.

**Non-LTS:** Receives updates while current version; 3-month grace period after new version released.

## Deprecation Policy

### Deprecation Lifecycle

1. **Announced**: Feature marked deprecated in code + release notes + MIGRATION.md
2. **Supported**: Works but logs warning; can still be used for one major version
3. **Removed**: Feature deleted; must upgrade to use library

Timeline:
- Feature marked `@deprecated` in code (with migration path)
- Documented in `MIGRATION.md`
- At least 1 full minor version cycle before removal (typically 3-6 months)
- Removed in next major version

### Example: Deprecating a method

```scala
@deprecated("Use newMethod() instead. See MIGRATION.md", "0.3.0")
def oldMethod(): Unit = {
  logWarning("oldMethod() is deprecated. Use newMethod() instead.")
  newMethod()
}
```

Release notes in CHANGELOG.md:

```markdown
## [0.3.0] - 2024-12-15

### Deprecated
- `oldMethod()` deprecated in favor of `newMethod()`. Will be removed in 1.0.0.
  See MIGRATION.md for upgrade path.
```

Release notes in 1.0.0:

```markdown
## [1.0.0] - 2025-06-15

### Removed
- Removed `oldMethod()`. Use `newMethod()` instead (available since 0.3.0).
```

## Release Process

### Steps

1. **Prepare Release Branch**
   ```bash
   git checkout -b release/X.Y.Z
   # Update version numbers in build.sbt, package.json
   # Update CHANGELOG.md
   ```

2. **Run Full Test Suite**
   ```bash
   ./dev/test
   # Test on multiple JDKs/Spark versions
   ```

3. **Create Release Commit**
   ```bash
   git commit -m "Release version X.Y.Z"
   git tag -a vX.Y.Z -m "Release X.Y.Z"
   ```

4. **Open Release PR**
   - Merge to main branch
   - Maintainer approval required

5. **Publish Release**
   - Push tag: `git push origin vX.Y.Z`
   - Create GitHub Release with:
     - Release notes from CHANGELOG.md
     - JAR artifacts
     - Migration guide (if breaking changes)

6. **Announce**
   - Post to GitHub Discussions
   - Update README with new version
   - Update documentation links

### Version Number Updates

Files to update for release:

- `plugin/build.sbt`: `version := "X.Y.Z"`
- `runner/build.sbt`: `version := "X.Y.Z"`
- `web/package.json`: `"version": "X.Y.Z"`
- `CHANGELOG.md`: Add [X.Y.Z] section with date
- `ROADMAP.md`: Update timeline if relevant

## Communication

### Before Release

- Announce upcoming release in GitHub Discussions (1-2 weeks ahead)
- Point out breaking changes or major features
- Solicit final feedback

### Release Day

- Tag commit and push
- Create GitHub Release with notes
- Post announcement in Discussions
- Update website/docs (if applicable)

### Post-Release

- Monitor issues for regression reports
- Prepare patch release if critical bugs found
- Iterate on next version planning

## FAQ

**Q: Why start at 0.1.0 instead of 1.0.0?**
A: Indicates Phase 0 is foundational work; full contract verification (Phase 1) will be major feature work before 1.0.0.

**Q: What if we break API before 1.0.0?**
A: Bump MAJOR (0.2.0, 0.3.0, etc.). Each breaks compatibility but signals ongoing development.

**Q: When should we cut 1.0.0?**
A: After Phase 1 (core verification engine) is complete and stable for at least one quarter.

**Q: Can we patch old versions (0.1.1, 0.1.2)?**
A: Only for critical security issues. Standard policy: upgrade to latest.

---

**Last Updated:** 2024-08-20
**Next Review:** When Phase 1 begins
