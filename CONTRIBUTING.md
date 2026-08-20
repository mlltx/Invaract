# Contributing to Invariant

Thank you for your interest in contributing to Invariant! This document provides guidelines and instructions for contributing to the project.

## Code of Conduct

This project adheres to the Contributor Covenant [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to the project maintainers.

## Getting Started

### Development Environment

#### Prerequisites

- **JDK 21** (or later)
- **sbt 1.9+** (Scala Build Tool)
- **Git**
- **Apache Spark 3.5.1** (installed via Dev Container, or manually)
- **Node.js 20+** (for web UI development)

#### Setup in GitHub Codespaces (Recommended)

```bash
git clone https://github.com/mlltx/Invariant.git
cd Invariant
# Dev Container auto-provisions JDK, sbt, Spark, Node.js
# Wait for .devcontainer/post-create.sh to complete (~5 minutes on first launch)
```

#### Local Setup

```bash
# Install JDK 21
# Install sbt (via Coursier or native installer)
# Install Spark 3.5.1 to /opt/spark or set SPARK_HOME

# Clone repository
git clone https://github.com/mlltx/Invariant.git
cd Invariant

# Build plugin
cd plugin && sbt compile test assembly && cd ..

# Build runner
cd runner && sbt compile assembly && cd ..

# Verify Spark
spark-submit --version
```

### Running Tests

```bash
# Comprehensive test harness (7-step verification)
./dev/test

# Expected output: "All validation passed" with exit code 0
```

### Viewing Results

```bash
# Start web UI on http://localhost:3000
./dev/report

# Forward port to mobile device via Codespaces
```

## How to Contribute

### Reporting Bugs

1. **Search existing issues** to avoid duplicates
2. **Create a new issue** with:
   - Clear title describing the problem
   - Steps to reproduce
   - Expected vs. actual behavior
   - Environment (Spark version, Java version, OS)
   - Output from `./dev/test` if applicable
   - Relevant log excerpts or report.json

### Proposing Features

1. **Open a discussion** or issue before starting work on large features
2. **Describe the use case** and expected benefit
3. **Propose implementation approach** if you have one
4. **Wait for maintainer feedback** before implementing

### Making Code Changes

1. **Fork the repository**
2. **Create a feature branch** from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Make focused commits** with clear messages (see Commit Message Conventions below)
4. **Test thoroughly**:
   ```bash
   ./dev/test  # Must exit with code 0
   ```
5. **Verify test report**:
   ```bash
   ./dev/report  # Visually inspect results in web UI
   ```
6. **Push to your fork** and **create a pull request**

### Pull Request Process

1. **Fill out PR template** completely
2. **Link related issues** (e.g., "Fixes #123")
3. **Describe changes** in user-facing terms
4. **Reference ROADMAP.md** if relevant to phases
5. **Ensure CI passes** (GitHub Actions runs `./dev/test`)
6. **Request review** from maintainers
7. **Address review feedback** with new commits (do not force-push)
8. **Squash commits** (if requested) only after approval

### Code Style Guidelines

#### Scala Code

- **Formatting**: Follow [Scala style guide](https://docs.scala-lang.org/style/)
  - Indentation: 2 spaces
  - Line length: 100 characters preferred, 120 maximum
  - Package imports: Group stdlib, then third-party, then local

- **Naming**: Follow Scala conventions
  - Classes/objects: `PascalCase`
  - Methods/variables: `camelCase`
  - Constants: `UPPER_CASE`

- **Documentation**: Write scaladoc for public APIs
  ```scala
  /** Validates DataFrame schema against required columns.
    *
    * @param df Input DataFrame
    * @return Validated DataFrame
    * @throws IllegalArgumentException if required columns missing
    */
  def validate(df: DataFrame): DataFrame
  ```

#### TypeScript/React Code

- **Formatting**: Use ESLint config in repository
  ```bash
  cd web && npm run lint
  ```
- **Components**: Keep components small and focused
- **Styling**: Use CSS modules (`.module.css`)
- **Mobile-first**: Design for 375px+ width

### Testing Requirements

#### Unit Tests (Scala)

All Scala code must have corresponding unit tests:

```scala
test("feature should work") {
  // Arrange
  val input = ...
  
  // Act
  val result = InvariantPlugin.process(input)
  
  // Assert
  assert(result.count() == expected)
}
```

- Minimum coverage: 80% for new code
- Tests must pass: `sbt test` in `plugin/` and `runner/`
- No flaky tests (use fixed seeds, avoid timing dependencies)

#### Integration Tests

All changes must pass the comprehensive test harness:

```bash
./dev/test
# Exit code 0 = all integration tests passed
```

The harness verifies:
1. Plugin compiles
2. Plugin JAR created
3. Runner compiles
4. Spark available
5. Spark job executes
6. Report generated
7. Report status = PASS

#### Mobile UI Testing (if applicable)

- Test on multiple viewport widths: 375px, 768px, 1440px
- Test in light and dark modes
- Verify responsive layout (no horizontal scroll)
- Test on actual mobile device if possible

### Commit Message Conventions

Use clear, descriptive commit messages:

```
Short summary (50 chars max)

Longer explanation of the change (if needed).
Wrapped at 72 characters. Explain the "why" not the "what".

- Bullet point details if applicable
- Another detail

Fixes #123
Related: #456
```

Examples:

```
Add Apache license and SPDX headers

Add Apache 2.0 LICENSE file to repository root.
Add SPDX headers to all source files.
Update copyright to include Invariant Contributors.

Fixes #45
```

```
Improve plugin schema validation error messages

Change validation error to include actual schema alongside
required columns. Helps users debug configuration issues faster.
```

### Documentation Requirements

- **Public APIs**: Scaladoc comments required
- **Complex logic**: Inline comments explaining "why" (not "what")
- **New features**: Update relevant .md files
- **Examples**: Add to README if feature is user-facing

## Development Workflow Example

```bash
# 1. Create feature branch
git checkout -b feature/add-new-transformation

# 2. Make changes
# Edit plugin/src/main/scala/com/example/plugin/InvariantPlugin.scala
# Add test to plugin/src/test/scala/.../InvariantPluginTest.scala

# 3. Test locally
./dev/test
# Verify exit code 0 and PASS status

# 4. View results
./dev/report
# Inspect in browser at http://localhost:3000

# 5. Commit
git add plugin/
git commit -m "Add new-transformation to plugin

Adds new transformation that processes X, producing Y.
Improves pipeline performance by 50% on typical workloads.

Tests: 2 new unit tests, integration test passes
Fixes #234"

# 6. Push and create PR
git push origin feature/add-new-transformation
# Go to GitHub and open PR
```

## Review Process

1. **Automated checks** (GitHub Actions)
   - Compilation: must pass
   - Tests: must pass
   - Coverage: should not decrease

2. **Code review** (maintainers)
   - Architecture: aligns with ROADMAP
   - Testing: comprehensive coverage
   - Documentation: updated if needed
   - Performance: no regressions

3. **Approval & merge**
   - Maintainer approval required
   - CI must be green
   - PR author should squash if history is messy

## Release Process

See [ROADMAP.md](ROADMAP.md#6-release-and-versioning) for versioning strategy.

Contributors are credited in:
- CHANGELOG.md release notes
- CONTRIBUTORS.md file
- GitHub release descriptions

## Getting Help

- **Questions**: Open a discussion in GitHub Discussions
- **Issues**: Open an issue on GitHub Issues
- **Security concerns**: See [SECURITY.md](SECURITY.md)
- **General questions**: Ask in Discussions, not PRs

## Project Roadmap

See [ROADMAP.md](ROADMAP.md) for:
- Current phase and work items
- Upcoming priorities
- Long-term vision

Contributions aligned with the roadmap are encouraged.

## License

By contributing to Invariant, you agree that your contributions will be licensed under the Apache 2.0 License. See [LICENSE](LICENSE) for details.

---

Thank you for contributing to Invariant!
