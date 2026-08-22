# Invariant Spark Plugin - Development Guide

This repository provides a complete, mobile-first development environment for building and testing Apache Spark plugins using Claude Code.

## Overview

The **Invariant Spark Plugin** is a demonstration Spark plugin designed to run entirely in a GitHub Codespace, accessible and testable from a mobile device.

### Quick Summary

- **Plugin Type**: Apache Spark data processing plugin (Scala)
- **Plugin Version**: 0.1.0
- **Spark Version**: 3.5.1
- **Scala Version**: 2.12.18
- **Java Version**: 21
- **Build System**: sbt
- **Test Execution**: Local Spark master (`local[*]`)
- **Results Viewer**: Next.js web UI, mobile-responsive

## Critical Requirement

**NEVER** consider a plugin change complete solely because compilation or unit tests succeed.

You MUST:

1. Run `./dev/test`
2. Verify the exit code is `0`
3. Examine the generated `demo/output/report.json`
4. Open the results in the web UI via `./dev/report`
5. Confirm the **Status** field is **PASS**
6. Visually inspect input/output data and schema

Real Spark execution is the source of truth. Unit test passing ≠ plugin working.

## Mutation Testing Requirement

`ir` and `spark-adapter` are mutation-tested with Stryker4s (see
docs/TRANSFORMATION_IR.md and docs/SPARK_ADAPTER.md's "Mutation testing"
sections). CI blocks on each module's *whole-module* score staying above
its `break` threshold (see `strykerThresholdsBreak` in each module's
`build.sbt` — currently 50% for `ir`, 70% for `spark-adapter`, the latter
after `strykerExcludedMutations` was set to disclose-and-exclude the
`StringLiteral` mutator category there; see docs/SPARK_ADAPTER.md), but
that only catches an aggregate regression. It does not prove new code is
well-tested — a large, well-tested module can absorb a weakly-tested new
file and still clear its module's break threshold.

So: when a feature adds or changes code in `ir/src/main/scala/...` or
`spark-adapter/src/main/scala/...`, passing tests are **not** enough to
call it done. Before considering such a feature complete, you MUST:

1. From inside the module directory, run mutation testing scoped to just
   the file(s) you touched, e.g. `sbt stryker --mutate "src/main/scala/com/example/ir/YourFile.scala"`.
2. Confirm the score for those file(s) is at least **70%**.
3. For every real Survived/NoCoverage mutant in the code you added or
   changed, either strengthen an assertion to kill it, add a test that
   reaches it, or note explicitly why it's being left (e.g. a genuinely
   equivalent mutant, or a `StringLiteral` mutant on human-readable
   message text — see docs/SPARK_ADAPTER.md's "Mutation testing" section
   for what's already been judged not worth chasing).

This is a manual, PR-scoped check — Stryker4s has no incremental/diff
mode, so CI cannot enforce "the new code specifically" on its own.

## Repository Structure

```
.
├── .devcontainer/
│   ├── devcontainer.json        # Dev Container configuration
│   └── post-create.sh           # Setup script (JDK, sbt, Spark)
│
├── plugin/                       # Spark plugin source code
│   ├── src/
│   │   ├── main/scala/com/example/plugin/
│   │   │   └── InvariantPlugin.scala
│   │   └── test/scala/com/example/plugin/
│   │       └── InvariantPluginTest.scala
│   ├── build.sbt                # Plugin build configuration
│   └── project/assembly.sbt     # sbt-assembly plugin
│
├── demo/
│   ├── input/sample.csv         # Deterministic test data
│   └── output/                  # Generated results (not in git)
│       ├── report.json
│       └── result.parquet
│
├── runner/                       # Spark job executor
│   ├── src/main/scala/com/example/runner/
│   │   └── PluginRunner.scala   # Executes plugin, generates report
│   ├── build.sbt
│   └── project/assembly.sbt
│
├── web/                          # Mobile-friendly results UI
│   ├── app/
│   │   ├── layout.tsx
│   │   ├── page.tsx             # Main report viewer component
│   │   ├── page.module.css      # Mobile-first styling
│   │   ├── globals.css
│   │   └── api/report/route.ts  # API endpoint for report JSON
│   ├── package.json             # Next.js + TypeScript
│   ├── tsconfig.json
│   ├── next.config.js
│   └── .eslintrc.json
│
├── dev/                          # Development scripts
│   ├── test                     # Main test harness (7-step verification)
│   └── report                   # Launch web UI
│
├── .github/workflows/
│   └── test.yml                 # CI/CD pipeline (mirrors local test)
│
├── CLAUDE.md                     # This file
└── README.md
```

## Development Workflow

### 1. Initial Setup

When opening the repository in GitHub Codespaces:

```bash
# Dev Container auto-runs post-create.sh, which installs:
# - JDK 21
# - sbt
# - Scala 2.12.18
# - Apache Spark 3.5.1
# - Node.js 20

# After container is ready, nothing else is needed
```

### 2. Make Plugin Changes

Edit files under `plugin/src/main/scala/com/example/plugin/`.

Example: Add a new transformation to `InvariantPlugin.scala`.

### 3. Test the Plugin

Run the comprehensive test harness:

```bash
./dev/test
```

This single command:

1. ✓ Cleans and compiles the plugin
2. ✓ Runs unit tests
3. ✓ Packages plugin into JAR: `plugin/target/scala-2.12/invariant-spark-plugin-0.1.0.jar`
4. ✓ Builds the runner application
5. ✓ Verifies Spark environment
6. ✓ Prepares demo data (loads `demo/input/sample.csv`)
7. ✓ Executes real Spark job with packaged JAR via `spark-submit`
8. ✓ Captures execution results to `demo/output/result.parquet`
9. ✓ Captures schema and diagnostics
10. ✓ Generates machine-readable report: `demo/output/report.json`
11. ✓ Validates report status
12. ✓ Returns exit code `0` on success, non-zero on failure

**Example output:**

```
======================================
Invariant Spark Plugin Test Suite
======================================

Step 1/7: Building plugin...
✓ Plugin built successfully

Step 2/7: Verifying plugin JAR...
  JAR size: 45M
  Main classes:
    com/example/plugin/InvariantPlugin.class
    com/example/plugin/InvariantPlugin$

Step 3/7: Building runner...
✓ Runner built successfully

Step 4/7: Verifying Spark environment...
  Spark: version 3.5.1

Step 5/7: Preparing output directory...

Step 6/7: Executing Spark integration test...
  Input: demo/input/sample.csv
  Output: demo/output/result.parquet
  Report: demo/output/report.json
  [Spark job runs here...]

Step 7/7: Validating execution report...
  Status: PASS
  Plugin Version: 0.1.0
  Duration: 2345ms

✓ All validation passed
✓ Plugin JAR is ready: plugin/target/scala-2.12/invariant-spark-plugin-0.1.0.jar
✓ Execution report: demo/output/report.json

To view results in web UI:
  ./dev/report
```

### 4. View Results

Start the mobile-friendly web UI:

```bash
./dev/report
```

The UI will start on `http://localhost:3000` and show:

- **Status Badge**: ✓ PASS or ✕ FAIL (large, mobile-visible)
- **Build Information**: Plugin/Spark/Java versions, duration
- **Test Results**: Unit and integration test pass/fail counts
- **Input Data**: Row count, schema, sample rows
- **Output Data**: Row count, schema, sample rows
- **Plugin Events**: Execution timeline and diagnostics
- **Errors**: Full error messages if execution failed

Forward the Codespaces port to your phone and open the URL in a mobile browser. The UI is fully responsive for screens as narrow as 375px.

### 5. Iterate

If `./dev/test` fails:

1. Examine the error output
2. Check `demo/output/report.json` for diagnostics
3. Review plugin events in the web UI
4. Fix the plugin code
5. Run `./dev/test` again
6. Repeat until exit code is `0`

## Build Artifacts and Outputs

### Plugin JAR

- **Location**: `plugin/target/scala-2.12/invariant-spark-plugin-0.1.0.jar`
- **Size**: ~45 MB (includes Spark dependencies)
- **Purpose**: Packaged plugin for submission to Spark
- **Created by**: `sbt assembly` (in step 1 of `./dev/test`)
- **Used by**: Spark via `spark-submit --jars <JAR>`

### Execution Report

- **Location**: `demo/output/report.json`
- **Format**: Structured JSON
- **Schema**:
  ```json
  {
    "status": "PASS" | "FAIL",
    "timestamp": "ISO8601",
    "pluginVersion": "0.1.0",
    "sparkVersion": "3.5.1",
    "scalaVersion": "...",
    "javaVersion": "...",
    "durationMs": 1234,
    "buildInfo": { ... },
    "tests": {
      "unit": { "passed": 42, "failed": 0 },
      "integration": { "passed": 1, "failed": 0 }
    },
    "input": {
      "rowCount": 10,
      "schema": [ { "name": "id", "type": "integer" }, ... ],
      "sample": [ ... ]
    },
    "output": {
      "rowCount": 10,
      "schema": [ ... ],
      "sample": [ ... ]
    },
    "plugin": {
      "events": [ "...", "..." ],
      "diagnostics": [ ... ]
    },
    "error": null
  }
  ```

### Demo Output Data

- **Location**: `demo/output/result.parquet`
- **Format**: Apache Parquet
- **Content**: Output of plugin processing on `demo/input/sample.csv`
- **Lifecycle**: Regenerated on each `./dev/test`

## Execution Model

### Local Spark Master

All plugin execution uses a **local Spark master**:

```scala
spark.builder()
  .master("local[*]")  // Uses all available cores
  .getOrCreate()
```

This provides:

- ✓ Fast execution (milliseconds to seconds)
- ✓ Deterministic results
- ✓ No remote infrastructure
- ✓ Full diagnostic access

### JAR Submission

The plugin is executed **via real Spark submission**, not unit test mocking:

```bash
spark-submit \
  --class com.example.runner.PluginRunner \
  --master local[*] \
  --jars plugin/target/scala-2.12/invariant-spark-plugin-0.1.0.jar \
  runner/target/scala-2.12/invariant-spark-runner.jar \
  demo/input/sample.csv \
  demo/output/result.parquet \
  demo/output/report.json
```

This ensures:

- ✓ Real classloading behavior
- ✓ Spark serialization/deserialization of plugin objects
- ✓ Accurate performance characteristics
- ✓ True integration testing

## Test Data

**Input File**: `demo/input/sample.csv`

```csv
id,value
1,10
2,20
...
10,100
```

- **Size**: 10 rows
- **Format**: CSV
- **Deterministic**: Yes (committed to Git)
- **Purpose**: Exercise plugin transformation
- **Processing Time**: <1 second

## Plugin Implementation

The example plugin (InvariantPlugin.scala) demonstrates:

1. **Schema Validation**: Checks for required columns
2. **Transformation**: Adds a computed column (`value_squared`)
3. **Event Logging**: Records execution steps
4. **Error Handling**: Validates input before processing

To modify the plugin:

1. Edit `plugin/src/main/scala/com/example/plugin/InvariantPlugin.scala`
2. Add or update tests in `plugin/src/test/scala/com/example/plugin/InvariantPluginTest.scala`
3. Run `./dev/test`
4. Verify the report

## Versions and Compatibility

| Component | Version | Reason |
|-----------|---------|--------|
| JDK       | 21      | Latest LTS, Spark 3.5 compatible |
| Scala     | 2.12.18 | Spark 3.5.1 standard binary |
| Spark     | 3.5.1   | Latest stable, well-supported |
| sbt       | Latest  | Via Coursier (no manual install needed) |
| Next.js   | 14.1.0  | Latest stable, Vercel-maintained |
| Node.js   | 20      | LTS, stable |

### Java Compatibility

- Plugin code targets JVM 1.8 (via `-target:jvm-1.8` scalacOption)
- Runtime JDK 21 fully supports 1.8 bytecode
- Forward compatible to future JDK versions

## CI/CD Pipeline

GitHub Actions workflow (`.github/workflows/test.yml`) runs on every push/PR:

1. Checkout code
2. Setup JDK 21
3. Build plugin (compile, test, assembly)
4. Build runner
5. Setup Spark 3.5.1
6. Run Spark integration test
7. Validate report
8. Upload test report artifact
9. Fail job if tests fail

Exit code determines PR check status: ✓ for pass, ✗ for fail.

## Inspecting Failures

If `./dev/test` fails, debug in order:

### 1. Check exit code and output

```bash
./dev/test
echo $?  # Non-zero indicates failure
```

### 2. Review the report

```bash
cat demo/output/report.json | jq .
```

Look for:
- `"status"` field (should be `"PASS"`)
- `"error"` field (contains error message)
- `"plugin.events"` array (execution timeline)

### 3. View in web UI

```bash
./dev/report
# Open in browser and inspect Plugin Events section
```

### 4. Check Spark logs

If the report indicates Spark execution failed:

```bash
# Look for Spark logs in runner output
# Check that input CSV is readable
file demo/input/sample.csv
head -5 demo/input/sample.csv
```

### 5. Review plugin code

Check `plugin/src/main/scala/com/example/plugin/InvariantPlugin.scala` for:
- Null pointer exceptions
- Schema assumptions
- Column name case sensitivity
- Type mismatches

### 6. Run unit tests in isolation

```bash
cd plugin
sbt test
cd ..
```

This helps isolate whether the problem is in:
- Plugin code itself
- Plugin/Spark integration
- Test data
- Report generation

## Future Extensibility

### Adding a Real Spark Cluster

The current architecture uses `local[*]` Spark master. To use a real cluster later:

1. Modify `runner/src/main/scala/com/example/runner/PluginRunner.scala`
2. Change `.master("local[*]")` to `.master("spark://cluster:7077")` or YARN/Kubernetes
3. Update `.github/workflows/test.yml` to provision cluster
4. Report format remains unchanged

### Extending the Report Format

The report JSON structure is extensible. To add new fields:

1. Update `ExecutionReport` case class in `PluginRunner.scala`
2. Add corresponding fields to `reportToJson` serialization
3. Update `web/app/page.tsx` to display new fields
4. Update `web/app/page.module.css` for styling

Example additions:
- SQL/DataFrame API calls executed by plugin
- Spark stages and task execution timings
- Memory usage and garbage collection stats
- Plugin-specific metrics
- Before/after schema comparison

### Supporting Multiple Spark Versions

Currently pinned to Spark 3.5.1. To support multiple versions:

1. Create matrix in `.github/workflows/test.yml` (e.g., Spark 3.4, 3.5, 3.6)
2. Update `.devcontainer/post-create.sh` to accept version parameter
3. Pin compatible Scala/Java versions per Spark version
4. Report should include Spark version in output (already does)

## Common Development Tasks

### Add a new column transformation

```scala
// In InvariantPlugin.scala
def addNewColumn(df: DataFrame): DataFrame = {
  logEvent("Adding new_column")
  df.withColumn("new_column", col("value") + 100)
}
```

Then add test:

```scala
// In InvariantPluginTest.scala
test("addNewColumn should add column") {
  val df = spark.createDataFrame(...)
  val result = InvariantPlugin.addNewColumn(df)
  assert(result.columns.contains("new_column"))
}
```

Run `./dev/test` to verify.

### Change demo data

Edit `demo/input/sample.csv` and run `./dev/test`. The plugin will process new data.

### Update plugin version

1. Edit version in `plugin/build.sbt` (e.g., `version := "0.2.0"`)
2. Update JAR name in `plugin/build.sbt` assembly config
3. Update runner's `pluginVersion` in `PluginRunner.scala`
4. Run `./dev/test`

### Troubleshoot Spark locally

```bash
# Start Spark shell with plugin JAR
spark-shell --jars plugin/target/scala-2.12/invariant-spark-plugin-0.1.0.jar

# Then in shell:
// scala> val df = spark.read.csv("demo/input/sample.csv", header=true, inferSchema=true)
// scala> val result = com.example.plugin.InvariantPlugin.process(df)
// scala> result.show()
```

## Mobile Development Tips

- **Port Forwarding**: Codespaces auto-forwards ports 3000 and 4040. Open the forwarded URL on your phone.
- **Browser Compatibility**: Works in Safari, Chrome, Firefox on iOS and Android.
- **Screen Width**: UI optimized for 375–430px (iPhone SE to Pro Max).
- **Offline**: Web UI requires connection to Codespace; cannot work offline.
- **Real-time Updates**: Web UI polls for new reports every 2 seconds while open.

## Typical Development Session

```bash
# 1. Clone and open in Codespaces (Dev Container auto-provisions)
git clone https://github.com/mlltx/Invariant.git
# Wait for post-create.sh to finish (~5 min first time)

# 2. Make a change to the plugin
edit plugin/src/main/scala/com/example/plugin/InvariantPlugin.scala

# 3. Test
./dev/test
# Wait for result (~10-20 seconds)

# 4. View results on phone
./dev/report
# Open http://localhost:3000 on phone (via forwarded Codespaces port)

# 5. Iterate
# Make more changes, run ./dev/test, check results

# 6. When satisfied
git add .
git commit -m "Add feature X to plugin"
git push
# CI/CD runs the same ./dev/test suite
```

## Support and Debugging

For issues:

1. Check that `./dev/test` produces non-zero exit code
2. Review `demo/output/report.json` for error details
3. Examine `plugin/src/test/scala/...` tests
4. Check plugin event logs in web UI
5. Verify Spark is running: `spark-submit --version`

## Performance Expectations

- First build (cold sbt cache): ~2 minutes
- Subsequent builds: ~15-30 seconds
- Spark job execution: ~5-10 seconds
- Report generation: ~1 second
- **Total ./dev/test time**: ~30-60 seconds (after first build)

On mobile network, UI may be slower due to data volume (~100KB report).

## References

- [Apache Spark](https://spark.apache.org/)
- [Scala 2.12](https://docs.scala-lang.org/2.12/)
- [sbt](https://www.scala-sbt.org/)
- [Next.js](https://nextjs.org/)
- [GitHub Codespaces](https://github.com/features/codespaces)

---

**Last Updated**: 2024-08-20
**Status**: Ready for development
