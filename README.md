# Invariant - Spark Plugin Mobile Development Environment

A complete, production-ready development environment for building and testing Apache Spark plugins from a mobile device using GitHub Codespaces and Claude Code.

## Quick Start

### Prerequisites

- GitHub account with Codespaces access
- Modern web browser (Safari, Chrome, Firefox)
- No local development setup required

### Setup

1. **Clone and open in Codespaces**:
   ```bash
   git clone https://github.com/mlltx/Invariant.git
   # Open in GitHub Codespaces (Dev Container auto-provisions everything)
   ```

2. **Wait for setup** (~5 minutes first time):
   - JDK 21
   - sbt
   - Apache Spark 3.5.1
   - Node.js 20

3. **Run tests**:
   ```bash
   ./dev/test
   ```

4. **View results**:
   ```bash
   ./dev/report
   ```

5. **Forward port 3000 to your phone and open `http://localhost:3000`**

## Development Workflow

```
Phone Browser
    ↓
Claude Code (Claude.ai/code)
    ↓
Edit plugin source
    ↓
./dev/test
    ↓
Spark executes real JAR
    ↓
Results → demo/output/report.json
    ↓
./dev/report (port 3000)
    ↓
View on phone
```

## Key Features

✅ **Real Spark Execution**: Packages plugin into JAR, runs via `spark-submit`  
✅ **Mobile-First UI**: Fully responsive, 375px+ screens  
✅ **One Command Testing**: `./dev/test` = full pipeline  
✅ **GitHub Codespaces**: Complete environment in cloud  
✅ **CI/CD Pipeline**: GitHub Actions mirrors local testing  
✅ **Deterministic Results**: Fixed demo data, reproducible outputs  
✅ **Structured Reports**: JSON output, machine-readable  
✅ **Event Logging**: Track plugin execution steps  

## File Structure

```
plugin/              # Scala/Spark plugin source
  └── src/main/...   # InvariantPlugin.scala
demo/
  ├── input/         # Deterministic test data (CSV)
  └── output/        # Generated results (parquet, report.json)
runner/              # Spark job executor (reads plugin JAR)
web/                 # Next.js + TypeScript results viewer
dev/
  ├── test           # Main test harness (7-step verification)
  └── report         # Start web UI (port 3000)
.devcontainer/       # Codespaces configuration
.github/workflows/   # GitHub Actions CI/CD
CLAUDE.md            # Complete development guide
```

## Commands

| Command | Purpose |
|---------|---------|
| `./dev/test` | Build, test, package, execute plugin, generate report |
| `./dev/report` | Start web UI on localhost:3000 |
| `cd plugin && sbt test` | Run unit tests only |
| `cd plugin && sbt assembly` | Build JAR only |

## Example Plugin

The included example plugin demonstrates:

- **Schema Validation**: Ensures required columns exist
- **Transformation**: Adds `value_squared` computed column
- **Event Logging**: Records execution timeline
- **Error Handling**: Validates input before processing

See `plugin/src/main/scala/com/example/plugin/InvariantPlugin.scala`

## Test Results

Each run of `./dev/test` generates:

1. **report.json** - Structured test results
   - Status (PASS/FAIL)
   - Build info (versions, duration)
   - Test counts (unit, integration)
   - Input/output schema and sample data
   - Plugin events and diagnostics

2. **result.parquet** - Output data from plugin execution

3. **Web UI** - Mobile-friendly visualization of results

## Mobile Access

1. Run `./dev/test` to generate report
2. Run `./dev/report` to start web UI
3. Codespaces forwards port 3000
4. Open forwarded URL on phone (375px+ responsive)
5. UI polls every 2 seconds for new reports

## Architecture

```
Local Spark Master (local[*])
    ↓
Plugin JAR (sbt assembly)
    ↓
Spark Job Runner (Scala executor)
    ↓
Input: demo/input/sample.csv
    ↓
Plugin: InvariantPlugin.scala
    ↓
Output: demo/output/result.parquet + report.json
    ↓
Web UI: Next.js (typescript + CSS)
```

## CI/CD

GitHub Actions workflow (`.github/workflows/test.yml`):
- Runs on every push/PR
- Sets up JDK 21 + Spark 3.5.1
- Executes `./dev/test`
- Uploads report artifact
- Fails job if tests fail

## Important Notes

- **Real Spark Execution**: Plugin runs via actual `spark-submit`, not unit test mocks
- **Exit Code 0**: Success means true plugin execution passed
- **Mobile-First**: Optimized for 375-430px screens
- **No Infrastructure**: Uses local Spark master, no cloud required
- **Deterministic**: Same demo data every run, reproducible results

## Documentation

See [CLAUDE.md](CLAUDE.md) for:
- Complete development guide
- Building and modifying plugins
- Report format specification
- Troubleshooting failures
- Performance expectations
- Future extensibility

## References

- [Apache Spark](https://spark.apache.org/) - Data processing framework
- [Scala 2.12](https://docs.scala-lang.org/2.12/) - Programming language
- [sbt](https://www.scala-sbt.org/) - Build tool
- [Next.js 14](https://nextjs.org/) - React framework
- [GitHub Codespaces](https://github.com/features/codespaces) - Cloud development

---

**Status**: Ready for production  
**Last Updated**: 2024-08-20 Spark Plugin

A complete, mobile-first Apache Spark plugin development environment designed for GitHub Codespaces and mobile device development with Claude Code.

## Quick Start

### 1. Open in GitHub Codespaces

Click the button or visit:
```
https://github.com/mlltx/Invariant/codespaces
```

The Dev Container will auto-provision all dependencies (JDK 21, Scala 2.12, Spark 3.5.1, sbt, Node.js).

### 2. Test the Plugin

```bash
./dev/test
```

This runs a complete test suite:
- Builds the plugin
- Runs unit tests
- Packages the plugin JAR
- Executes a real Spark job
- Generates a structured JSON report

Exit code `0` = success, non-zero = failure.

### 3. View Results

```bash
./dev/report
```

Opens a mobile-responsive web UI at `http://localhost:3000` showing:
- Overall PASS/FAIL status
- Build information (versions, duration)
- Test results
- Input/output data and schemas
- Plugin event logs
- Error details (if any)

### 4. Modify the Plugin

Edit `plugin/src/main/scala/com/example/plugin/InvariantPlugin.scala`

Run `./dev/test` to verify your changes.

## Repository Structure

```
.
├── .devcontainer/           # GitHub Codespaces configuration
├── plugin/                  # Spark plugin source (Scala)
├── runner/                  # Spark job executor
├── demo/                    # Test data and results
├── web/                     # Mobile-friendly results UI (Next.js)
├── dev/                     # Development scripts
├── .github/workflows/       # CI/CD pipeline
├── CLAUDE.md               # Comprehensive development guide
└── README.md               # This file
```

## Key Features

✓ **Real Spark Execution** - Plugin runs via `spark-submit` with packaged JAR
✓ **Fast Feedback Loop** - Complete test in ~30 seconds
✓ **Mobile-First UI** - Fully responsive results viewer
✓ **Deterministic Testing** - Committed demo data for reproducibility
✓ **Structured Reports** - Machine-readable JSON output
✓ **One-Command Verification** - `./dev/test` covers everything
✓ **CI/CD Ready** - GitHub Actions mirrors local test harness
✓ **Extensible Architecture** - Easy to add new transformations and tests

## Technology Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| Apache Spark | 3.5.1 | Distributed processing framework |
| Scala | 2.12.18 | Plugin implementation language |
| Java | 21 | Runtime environment |
| sbt | Latest | Build and test automation |
| Next.js | 14.1.0 | Results web UI |
| Node.js | 20 | Web server runtime |

## Development Workflow

```
Edit plugin code
       ↓
./dev/test (15-30 seconds)
       ↓
Exit code 0? → demo/output/report.json generated
       ↓
./dev/report
       ↓
View results on phone (mobile browser)
       ↓
Iterate or commit
```

## Test Execution Flow

The `./dev/test` command:

1. **Build**: Compiles plugin, runs unit tests, assembles JAR
2. **Verify**: Checks JAR exists and contains expected classes
3. **Setup**: Configures local Spark master
4. **Execute**: Runs plugin on demo data via `spark-submit`
5. **Capture**: Records output data, schema, events
6. **Report**: Generates `demo/output/report.json`
7. **Validate**: Confirms report status is PASS

## Example Report

```json
{
  "status": "PASS",
  "timestamp": "2024-08-20T15:30:45.123Z",
  "pluginVersion": "0.1.0",
  "sparkVersion": "3.5.1",
  "durationMs": 2345,
  "tests": {
    "unit": { "passed": 4, "failed": 0 },
    "integration": { "passed": 1, "failed": 0 }
  },
  "input": {
    "rowCount": 10,
    "schema": [
      { "name": "id", "type": "integer" },
      { "name": "value", "type": "integer" }
    ]
  },
  "output": {
    "rowCount": 10,
    "schema": [
      { "name": "id", "type": "integer" },
      { "name": "value", "type": "integer" },
      { "name": "value_squared", "type": "integer" }
    ],
    "sample": [
      { "id": 1, "value": 10, "value_squared": 100 },
      { "id": 2, "value": 20, "value_squared": 400 }
    ]
  },
  "plugin": {
    "events": [
      "[123456789] Validating DataFrame...",
      "[123456790] Schema validation passed. Row count: 10",
      "[123456791] Adding computed column: value_squared"
    ]
  }
}
```

## Critical Notes

⚠️ **Plugin changes are not verified by unit tests alone.** Always run `./dev/test` and examine the report before considering a change complete.

✓ Real Spark execution via packaged JAR is the source of truth.

## Accessing Results on Mobile

1. Run `./dev/report` in Codespace terminal
2. Look for the Codespaces port forwarding notification
3. Copy the forwarded URL for port 3000
4. Open URL on your phone
5. Results UI auto-updates every 2 seconds

## Troubleshooting

### `./dev/test` fails

1. Check the error message in the terminal
2. Review `demo/output/report.json` for details
3. Check plugin events in web UI (if report generated)
4. Verify Spark environment: `spark-submit --version`
5. Review plugin code for errors

### Web UI shows "No Report Available"

1. Run `./dev/test` first to generate a report
2. Ensure `demo/output/report.json` exists
3. Restart web UI: `./dev/report`

### Plugin compilation fails

1. Check Scala syntax in `plugin/src/main/scala/`
2. Verify sbt is installed: `sbt --version`
3. Check Java version: `java -version`
4. Try `cd plugin && sbt clean compile`

### Performance is slow

First build caches dependencies (~2 min), subsequent builds are ~15-30 seconds.
On mobile network, port forwarding may add latency.

## CI/CD

Push to GitHub to trigger `.github/workflows/test.yml`:

- Runs on `main` and `develop` branches
- Executes same `./dev/test` logic in CI
- Posts results as PR comment
- Uploads artifacts (report, output data)

## Next Steps

1. **Understand the plugin**: Read `plugin/src/main/scala/com/example/plugin/InvariantPlugin.scala`
2. **Learn the test harness**: Read `dev/test` script
3. **Modify the plugin**: Add new transformations or validations
4. **Run the full test**: `./dev/test && ./dev/report`
5. **Extend the UI**: Add new visualizations in `web/app/page.tsx`

## Documentation

For comprehensive development guide, see **CLAUDE.md**:

- Detailed workflow explanation
- Version compatibility matrix
- Architecture and extensibility
- Debugging strategies
- Mobile development tips
- Performance expectations

## License

This project is provided as a demonstration of Spark plugin development with remote mobile access.

## Support

For questions or issues:

1. Review CLAUDE.md
2. Check GitHub Actions workflow logs
3. Inspect `demo/output/report.json` and plugin events
4. Examine plugin source code and tests

---

**Status**: Ready for development
**Last Updated**: 2024-08-20
