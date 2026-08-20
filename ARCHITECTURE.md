# Invariant Architecture

## Overview

Invariant is built as a modular system for verifying data transformations against machine-readable contracts. This document describes the current architecture, design decisions, and how components interact.

## System Architecture

### High-Level Design

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Contracts (ODCS)                    │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                  Transformation Logic                        │
│  (Spark SQL, dbt, SQL, etc. → Transformation IR)            │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              Invariant Verification Engine                   │
│  ┌─────────────────────────────────────────────────────────┐│
│  │ Contract Parser  │ Transformation Analyzer  │ Verifier   ││
│  └─────────────────────────────────────────────────────────┘│
└──────────────────────────┬──────────────────────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
   VERIFIED            REJECTED          UNDETERMINED
   (with metadata)     (with reasons)    (needs analysis)
        │                  │
        ├──────────────────┴──────────────────┐
        │                                     │
        ▼                                     ▼
   Verified Lineage                    Machine-Readable
   (for downstream use)                 Error Report
```

## Component Breakdown

### 1. Plugin (Scala/Spark)

**Location:** `plugin/src/main/scala/com/example/plugin/InvariantPlugin.scala`

**Purpose:** Implements data transformation logic that processes DataFrames.

**Key Classes:**
- `InvariantPlugin` (object): Entry point for plugin logic
  - `validate(df: DataFrame): DataFrame` - Validates input schema
  - `addComputedColumn(df: DataFrame): DataFrame` - Adds derived columns
  - `process(df: DataFrame): DataFrame` - Orchestrates full transformation
  - `logEvent(msg: String)` - Records execution events
  - `getEvents(): List[String]` - Retrieves execution log

**Responsibilities:**
- Accept Spark DataFrames as input
- Perform validation (schema, column existence)
- Apply transformations (column addition, filtering, etc.)
- Log events for diagnostic purposes
- Return transformed DataFrame

**Design Decision:** Plugin logic is implemented as pure Spark SQL transformations, not as a Spark extension. This allows it to be tested independently and composed with other transformations.

### 2. Runner (Scala/Spark)

**Location:** `runner/src/main/scala/com/example/runner/PluginRunner.scala`

**Purpose:** Test harness that executes the plugin, captures results, and generates machine-readable reports.

**Key Classes:**
- `ExecutionReport` (case class): Structured representation of execution results
  - `status`: PASS or FAIL
  - `timestamp`: ISO 8601 instant
  - `pluginVersion`, `sparkVersion`, `scalaVersion`, `javaVersion`: Environment info
  - `durationMs`: Execution time
  - `buildInfo`: Build metadata
  - `tests`: Unit/integration test counts
  - `input`: Input DataFrame metadata (row count, schema, sample)
  - `output`: Output DataFrame metadata (row count, schema, sample)
  - `plugin`: Plugin-specific metrics (events, diagnostics)
  - `error`: Error message if execution failed

- `PluginRunner` (object): Main entry point
  - `main(args: Array[String])`: Orchestrates test execution
  - `reportToJson(report: ExecutionReport): String`: Serializes report

**Responsibilities:**
- Accept CLI arguments (input path, output path, report path)
- Load input data from CSV
- Execute plugin via `InvariantPlugin.process()`
- Capture output to Parquet
- Generate structured JSON report
- Return exit code (0 for success, 1 for failure)

**Design Decision:** Runner is separate from plugin to maintain clean separation of concerns. Plugin handles transformation; runner handles testing infrastructure. This pattern scales to multiple runners (CI/CD, local dev, cloud execution).

### 3. Web UI (Next.js/React)

**Location:** `web/`

**Purpose:** Mobile-responsive results viewer for execution reports.

**Key Components:**
- `web/app/page.tsx`: Main React component
  - Fetches JSON from `/api/report` endpoint
  - Renders status badge (✓ PASS or ✕ FAIL)
  - Displays build info, test results, schema, sample data
  - Shows plugin events timeline
  - Auto-refreshes every 2 seconds

- `web/app/api/report/route.ts`: API endpoint
  - Reads `demo/output/report.json` from filesystem
  - Returns parsed JSON to client

- `web/app/page.module.css`: Styling
  - Mobile-first responsive design
  - CSS custom properties for light/dark modes
  - Works on screens 375px–1440px+

**Responsibilities:**
- Fetch and parse execution report
- Display results in human-readable format
- Handle light/dark mode preferences
- Provide mobile-responsive layout
- Show diagnostic information for troubleshooting

**Design Decision:** Next.js provides serverless functions (API routes) and React components with built-in SSR, eliminating need for separate backend. CSS modules keep styling scoped to components. Mobile-first design ensures usability on phones.

## Data Flow

### Execution Flow (./dev/test)

```
1. Clean & Compile Plugin
   └─> plugin/src/**/*.scala → plugin/target/scala-2.12/invariant-spark-plugin-0.1.0.jar

2. Verify Plugin JAR
   └─> Check file exists and has expected size

3. Build Runner
   └─> runner/src/**/*.scala → runner/target/scala-2.12/invariant-spark-runner.jar

4. Verify Spark Environment
   └─> spark-submit --version (must succeed)

5. Prepare Output Directory
   └─> mkdir -p demo/output

6. Execute Spark Job
   ├─> Load demo/input/sample.csv into DataFrame
   ├─> Call InvariantPlugin.process(inputDf)
   ├─> Write output to demo/output/result.parquet
   ├─> Capture schema, sample rows, duration
   └─> Generate ExecutionReport

7. Validate Report
   ├─> Check report.json exists
   ├─> Parse JSON
   ├─> Verify status == "PASS"
   └─> Return exit code 0 (success) or 1 (failure)
```

### Report Generation Flow

```
ExecutionReport (Scala case class)
    │
    ├─> status: "PASS"
    ├─> timestamp: "2026-08-20T16:56:47Z"
    ├─> pluginVersion: "0.1.0"
    ├─> tests: {unit: {passed: 4, failed: 0}, ...}
    ├─> input: {rowCount: 10, schema: [...], sample: [...]}
    ├─> output: {rowCount: 10, schema: [...], sample: [...]}
    └─> plugin: {events: [...], diagnostics: [...]}
            │
            ▼
        reportToJson()
            │
            ▼
        JSON string (demo/output/report.json)
            │
            ▼
        java.nio.file.Files.write()
            │
            ▼
        Filesystem (demo/output/report.json)
            │
            ▼
        Web UI fetches via /api/report
            │
            ▼
        React component renders results
```

### Results Viewing Flow

```
./dev/report
    │
    ▼
Next.js dev server starts on :3000
    │
    ├─> App component mounts
    │   └─> useEffect() called
    │
    ▼
fetch("/api/report")
    │
    ▼
API route reads demo/output/report.json
    │
    ▼
parse JSON & return to client
    │
    ▼
React re-renders with results
    │
    ├─> Status badge (✓ PASS)
    ├─> Build info section
    ├─> Tests section
    ├─> Input section (schema, sample)
    ├─> Output section (schema, sample)
    └─> Plugin events timeline
```

## Architectural Decisions

### ADR-001: Separation of Plugin and Runner

**Decision:** Keep plugin and runner as separate JARs.

**Rationale:**
- Plugin is a reusable transformation library
- Runner is a testing harness (could be replaced with other test frameworks)
- Follows Unix philosophy: do one thing well
- Allows plugin to be consumed by other test harnesses (CI/CD, cloud platforms, etc.)
- Makes versioning and dependency management clearer

**Alternative considered:** Single JAR with embedded plugin and runner
- **Rejected:** Harder to reuse plugin independently, couples business logic to test infrastructure

### ADR-002: Real Spark Execution vs. Unit Test Mocking

**Decision:** Test harness uses real `spark-submit` with local master, not mocked Spark.

**Rationale:**
- Verifies real classloading and serialization behavior
- Tests actual Spark API usage (not mocking quirks)
- Captures realistic performance characteristics
- Builds confidence that code works in production
- Deterministic with local[*] master (no network issues)

**Alternative considered:** Mock Spark in unit tests
- **Rejected:** Doesn't verify real Spark behavior; false confidence in passing tests

### ADR-003: Scala/Spark for Plugin Implementation

**Decision:** Use Scala 2.12 and Apache Spark 3.5.1

**Rationale:**
- Spark is idiomatic in Scala (DataFrame API designed for Scala)
- Scala's type system catches errors at compile time
- Scala collections integrate seamlessly with Spark
- Spark 3.5.1 is latest stable with long-term support
- Scala 2.12 is standard binary for Spark 3.5

**Alternative considered:** Java or Python
- **Rejected:** Java less idiomatic for Spark; Python harder to deploy with Spark on CI/CD

### ADR-004: Next.js for Web UI

**Decision:** Use Next.js 14 with TypeScript for results viewer.

**Rationale:**
- Built-in API routes eliminate separate backend
- React component model scales with complexity
- Server-side rendering improves mobile performance
- CSS modules keep styles scoped and maintainable
- Vercel deployment (optional future step) is straightforward
- TypeScript catches UI bugs at build time

**Alternative considered:** Plain React + Express.js
- **Rejected:** More infrastructure to manage; Next.js does both better out-of-box

### ADR-005: JSON for Machine-Readable Reports

**Decision:** Use JSON (not XML, YAML, or binary) for execution reports.

**Rationale:**
- Language-agnostic and widely supported
- Human-readable (can inspect in browser)
- Efficient parsing in JavaScript (Web UI native)
- Good for version control (Git diffs are clean)
- Integrates with any downstream system (CI/CD, data platforms)
- No custom parsing logic needed

**Alternative considered:** Protocol Buffers or Avro
- **Rejected:** Adds complexity; JSON is sufficient for structured reports

### ADR-006: Local Spark Master for Determinism

**Decision:** Use `spark.master("local[*]")` for all test execution.

**Rationale:**
- Deterministic results (no network, no other jobs interfering)
- Fast (uses all available cores on single machine)
- Works offline in Codespaces
- Sufficient for unit/integration testing on demo data
- Easy to upgrade to cluster later (just change master URL)

**Alternative considered:** YARN or Kubernetes cluster
- **Rejected for Phase 0:** Adds infrastructure complexity; local master is adequate for development

## Module Dependencies

### Plugin Dependencies

```
plugin/
├─ org.apache.spark:spark-sql_2.12:3.5.1 (provided)
└─ org.scalatest:scalatest_2.12:3.2.18 (test)
```

All Spark dependencies marked as "provided" (not bundled) so plugin can run with any Spark 3.5.x runtime.

### Runner Dependencies

```
runner/
├─ org.apache.spark:spark-sql_2.12:3.5.1 (compile)
├─ com.example:invariant-spark-plugin:0.1.0 (unmanaged JAR)
└─ org.scala-lang:scala-library:2.12.18 (transitive)
```

Runner pulls in full Spark dependencies for `spark-submit` execution.

### Web UI Dependencies

```
web/
├─ next:14.1.0
├─ react:18.2.0
├─ typescript:5.3.3
└─ eslint:8.55.0 (dev)
```

Lightweight dependencies; no heavy state management needed for report viewing.

## API Contracts

### CLI Arguments (PluginRunner)

```bash
spark-submit \
  --class com.example.runner.PluginRunner \
  --master local[*] \
  --jars plugin.jar \
  runner.jar \
  [input_path] [output_path] [report_path]
```

- `input_path` (optional): Path to input CSV (default: `demo/input/sample.csv`)
- `output_path` (optional): Path to output Parquet (default: `demo/output/result.parquet`)
- `report_path` (optional): Path to output JSON report (default: `demo/output/report.json`)

### ExecutionReport JSON Schema

```json
{
  "status": "PASS" | "FAIL",
  "timestamp": "2026-08-20T16:56:47.025820609Z",
  "pluginVersion": "0.1.0",
  "sparkVersion": "3.5.1",
  "scalaVersion": "2.12.18",
  "javaVersion": "21.0.10",
  "durationMs": 7879,
  "buildInfo": {
    "pluginName": "invariant-spark-plugin",
    "pluginVersion": "0.1.0"
  },
  "tests": {
    "unit": {"passed": 4, "failed": 0},
    "integration": {"passed": 1, "failed": 0}
  },
  "input": {
    "rowCount": 10,
    "schema": [
      {"name": "id", "type": "integer"},
      {"name": "value", "type": "integer"}
    ],
    "sample": [...]
  },
  "output": {
    "rowCount": 10,
    "schema": [...],
    "sample": [...]
  },
  "plugin": {
    "events": ["[timestamp] message", ...],
    "diagnostics": [...]
  },
  "error": null
}
```

### Web API Endpoint

```
GET /api/report
200 OK
Content-Type: application/json

{ ExecutionReport JSON }
```

Or:

```
404 Not Found
{ "error": "Report not found" }
```

## Testing Strategy

### Unit Tests

Located in `plugin/src/test/scala/com/example/plugin/InvariantPluginTest.scala`

- Test individual methods in isolation
- Run via `sbt test` in plugin directory
- Use local Spark session with `master("local[*]")`
- Currently: 4 tests (all passing)

### Integration Tests

Executed via `./dev/test` (steps 6-7)

- Full end-to-end Spark job execution
- Real JAR compilation and `spark-submit`
- Real CSV input and Parquet output
- Report generation and validation
- Exit code determines success/failure

### Test Data

**Location:** `demo/input/sample.csv`

```csv
id,value
1,10
2,20
...
10,100
```

- 10 rows, 2 columns (id: Int, value: Int)
- Deterministic (committed to Git)
- Small enough for fast execution (~100ms processing)
- Representative of real pipeline inputs

## Performance Characteristics

| Component | Timing | Notes |
|-----------|--------|-------|
| Plugin compilation | 5-10s | First build cold cache |
| Plugin compilation | 2-3s | Subsequent builds (incremental) |
| Runner compilation | 10-15s | Includes Spark dependency resolution |
| Spark job execution | 3-5s | Local master, demo data |
| Report generation | <100ms | JSON serialization |
| Report parsing (UI) | <50ms | JSON.parse in browser |
| **Total ./dev/test** | **30-60s** | After first build |

## Future Architecture Directions

### Phase 1: Verification Engine

Add a contract analyzer that:
- Parses ODCS contract definitions
- Analyzes Spark logical plans
- Verifies transformation conforms to contract
- Produces verified lineage metadata

```
Contract (ODCS YAML)
    │
    ▼
┌────────────────────────────┐
│ Contract Parser & Analyzer │
└──────────┬─────────────────┘
           │
           ├─> Identifies inputs/outputs
           ├─> Validates data types
           ├─> Checks required fields
           └─> Extracts constraints
           │
           ▼
    Transformation IR
    (abstraction over Spark logical plan)
           │
           ▼
    Verification Engine
           │
    ┌──────┴──────┐
    ▼             ▼
 VERIFIED     REJECTED
 (with proof) (with reasons)
```

### Phase 2: Multi-Engine Support

Extend to support multiple transformation engines:

```
Spark → Spark IR
dbt   → SQL IR     → Generic Transformation IR → Verifier
SQL   → SQL IR
```

Abstract IR allows same verification logic across engines.

### Phase 3: Contract Registry

Version contracts as Git artifacts:

```
Git/Registry
    │
    ├─ customer_orders@1.0.yaml
    ├─ customer_orders@2.0.yaml (breaking change)
    └─ payments@1.0.yaml
         │
         ▼
    Implementations (Spark, dbt, SQL)
         │
         ▼
    Verified
```

### Phase 4: AI Integration

Expose machine-readable queries for AI agents:

```python
# Python API
from invariant import Verifier, Lineage

lineage = Verifier.verify(contract, transformation)
# Returns: Verified Lineage with column-level provenance

# AI agent use cases:
# - Find implementations of a contract
# - Assess pipeline impact of contract change
# - Compose transformations that satisfy contracts
# - Recommend schema migrations
```

## References

- [Apache Spark Documentation](https://spark.apache.org/docs/3.5.1/)
- [Scala Language Documentation](https://docs.scala-lang.org/2.12/)
- [sbt Documentation](https://www.scala-sbt.org/)
- [Next.js Documentation](https://nextjs.org/docs)
- [ODCS Specification](https://github.com/opendatadiscovery/open-data-contracts-standard)
- [OpenLineage Specification](https://openlineage.io/docs/)

---

**Last Updated:** 2024-08-20
**Architecture Version:** 0.1.0
