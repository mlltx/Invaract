# Invaract Architecture

## Overview

Invaract is a framework for verifying data transformations against
machine-readable data contracts. This document describes the current
architecture: what's actually built, how the pieces interact, and the
design decisions behind them.

**The product is the verification engine — `contract`, `ir`, and
`spark-adapter`.** Everything else in this repository (`plugin`, `runner`,
`demo`, `web`) is an example integration and test harness built to prove
the engine works against a real Spark job, not something a real Invaract
user would import. See "Two halves of this repository" below before
reading further — conflating the two is the most common way to misjudge
where a change belongs.

## System Architecture

### High-Level Design

```
┌─────────────────────────────────────────────────────────────┐
│                 Data Contract (contract/, ODCS-shaped)       │
└──────────────────────────┬──────────────────────────────────┘
                           │ ContractParser / ContractValidator
                           ▼
┌─────────────────────────────────────────────────────────────┐
│         A Spark job's real Catalyst logical plan             │
└──────────────────────────┬──────────────────────────────────┘
                           │ spark-adapter: SparkPlanAdapter.translate
                           ▼
┌─────────────────────────────────────────────────────────────┐
│     Transformation IR (ir/) — engine-independent Plan/Expr   │
└──────────────────────────┬──────────────────────────────────┘
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
   spark-adapter: StructuralVerifier   ir: Lineage.trace
   (contract vs. IR, before write)     (column-level provenance)
              │
              ▼
   spark-adapter: ContractEnforcementRule
   (SparkSessionExtensions check rule —
    aborts the write if verification fails)
              │
    ┌─────────┴─────────┐
    ▼                    ▼
 VERIFIED             REJECTED
 (write proceeds)   (ContractViolationException,
                      write never happens)
```

This is a real, exercised path, not a design sketch: `ContractEnforcementRule`
runs on the analyzed plan of every query a `SparkSession` executes once
installed, and a violation throws before Spark writes anything. See
[docs/SPARK_ADAPTER.md](docs/SPARK_ADAPTER.md) for the mechanism, and
[docs/CONTRACT_MODEL.md](docs/CONTRACT_MODEL.md) /
[docs/TRANSFORMATION_IR.md](docs/TRANSFORMATION_IR.md) for the two things
that feed it.

## Two halves of this repository

### The verification engine (the product)

| Module | Package | Purpose |
|---|---|---|
| `contract/` | `com.invaract.contract` | Parses and validates ODCS-shaped YAML contracts; classifies compatibility between two contract versions. No Spark dependency — a contract is a plain data structure. |
| `ir/` | `com.invaract.ir` | An engine-independent `Plan`/`Expr` algebra (`Read`, `Write`, `Project`, `Join`, `Aggregate`, ...), plus `Lineage.trace` (structural column-level provenance) and `PlanPrinter` (human-readable rendering). No Spark dependency, no dependency on `contract` — this is meant to be the thing any engine's plan gets translated *into*. |
| `spark-adapter/` | `com.invaract.sparkadapter` | Translates a real Spark Catalyst `LogicalPlan` into the IR (`SparkPlanAdapter`), verifies it against a contract (`StructuralVerifier`), and enforces that verification inside Spark's own execution lifecycle (`ContractEnforcementRule`, a `SparkSessionExtensions` check rule) or observes it after the fact (`SparkAdapterListener`, a `QueryExecutionListener`). Depends on `ir` and `contract`, and on Spark (`provided`). |

This is where a feature request almost always belongs, and where the
regression-testing guardrails (property-based fuzzing, mutation testing —
see CLAUDE.md's "Mutation Testing Requirement" — and the ones still
outstanding: a multi-Spark-version compatibility matrix, coverage gating,
API-compatibility checking) are scoped: against these three modules, not
against `plugin`/`runner`.

### The example integration & test harness

| Module | Package | Purpose |
|---|---|---|
| `plugin/` | `com.invaract.plugin` | `InvaractPlugin`: a small, illustrative Spark transformation (validate a schema, add a computed column) standing in for "some real job's transformation logic." Not part of the engine — it's what the engine is demonstrated against. |
| `runner/` | `com.invaract.runner` | `DemoJobHarness`: an example Spark job, run as a test harness. It builds a real `SparkSession` with the verification engine installed exactly the way a real user's job would, drives `InvaractPlugin`'s transformation through it, and captures the outcome as `demo/output/report.json`. Despite the historical directory name `runner/`, this is not "the thing that runs the engine" — it's one example caller of it. |
| `demo/` | — | Deterministic fixtures (`demo/input/sample.csv`), example contracts (`demo/contracts/*.yaml`, including a deliberately-broken one used to prove rejection works), and generated, gitignored output (`demo/output/`). |
| `web/` | — | A Next.js viewer for `demo/output/report.json` — lets a human (on a phone, via forwarded Codespaces ports) see a harness run's PASS/FAIL status, schemas, and diagnostics without reading raw JSON. |

`./dev/test` running this harness end-to-end via real `spark-submit` (not
mocked Spark) is the project's actual source of truth that the engine
works — see CLAUDE.md's "Critical Requirement." A change to `spark-adapter`
is not done until this harness proves it against a real Spark execution,
even though the harness itself is not the thing being changed.

## Data Flow

### Execution flow (`./dev/test`)

```
1. Build contract, ir, plugin (independent — built concurrently)
   └─> each module's target/scala-2.12/*.jar

2. Build spark-adapter (needs contract + ir)
   └─> spark-adapter/target/scala-2.12/invaract-spark-adapter-0.2.0.jar

3. Build runner (needs contract, ir, plugin, spark-adapter)
   └─> runner/target/scala-2.12/invaract-spark-runner.jar

4. Verify Spark environment
   └─> spark-submit --version (must succeed)

5. Run the demo job (DemoJobHarness, via spark-submit)
   ├─> ContractParser loads demo/contracts/invaract_output.yaml
   ├─> SparkSession built with ContractEnforcementRule installed
   │   (from the same contract) and SparkAdapterListener registered
   ├─> Load demo/input/sample.csv into a DataFrame
   ├─> InvaractPlugin.process(inputDf) — the example transformation
   ├─> outputDf.write(...) — ContractEnforcementRule verifies the
   │   real Catalyst plan here, before any bytes are written; a
   │   violation raises ContractViolationException and no output
   │   file is created
   ├─> SparkAdapterListener's translation is rendered (PlanPrinter)
   │   and traced (Lineage) for the report
   └─> Capture schema, sample rows, duration, contract verification
       outcome, and Transformation IR into an ExecutionReport

6. Validate the report
   ├─> Check report.json exists
   ├─> Parse JSON, verify status == "PASS"
   └─> Return exit code 0 (success) or 1 (failure)
```

`./dev/regression` runs the same harness twice more against
`demo/contracts/` — once against a passing contract, once against
`invaract_output_broken_example.yaml` — asserting the write *succeeds* in
the first case and is *aborted* in the second. That pass/fail pair, not
just a single green run, is what actually demonstrates
`ContractEnforcementRule` enforces anything. See "Contract regression
pack" in [docs/SPARK_ADAPTER.md](docs/SPARK_ADAPTER.md).

### Report generation flow

```
ExecutionReport (Scala case class, runner/DemoJobHarness.scala)
    │
    ├─> status, timestamp, versions, durationMs
    ├─> input / output: {rowCount, schema, sample}
    ├─> plugin: {events, diagnostics}
    ├─> transformationIR: {renderedPlan, lineage, diagnostics}
    │       (from spark-adapter's translation + ir's PlanPrinter/Lineage)
    └─> contractVerification: {status, contract, violations}
            │
            ▼
        reportToJson() — hand-rolled serializer, no JSON library dep
            │
            ▼
        demo/output/report.json
            │
            ▼
        Web UI fetches via GET /api/report, renders it
```

## Architectural Decisions

### ADR-001: The verification engine is Spark-specific by adapter, not by design

**Decision:** `contract` and `ir` have zero Spark dependency; only
`spark-adapter` does.

**Rationale:**
- `ir`'s `Plan`/`Expr` algebra deliberately does not mirror Catalyst's
  expression class hierarchy one-for-one: it distinguishes node kinds
  (`Arithmetic`, `Comparison`, `BooleanExpr`, `Cast`, `Conditional`,
  `UDF`, ...) only where the distinction is semantically load-bearing,
  collapsing everything else into one `Function` catch-all — see
  docs/TRANSFORMATION_IR.md's "Critical principle: semantics, not
  syntax."
- This is what makes Phase 2 (a SQL or dbt adapter, per ROADMAP.md)
  additive rather than a rewrite: `contract` and `ir` don't change, only a
  new adapter module translating into the same IR.

**Alternative considered:** Build the IR as a thin Catalyst wrapper.
**Rejected:** Ties lineage/verification logic to one engine's optimizer
internals, defeating the point of having an IR at all.

### ADR-002: Enforcement via `SparkSessionExtensions` check rule, observation via `QueryExecutionListener`

**Decision:** `spark-adapter` uses two different Spark extension points for
two different jobs, not one mechanism for both.

**Rationale:**
- A `QueryExecutionListener` callback fires only *after* Spark has already
  executed a query — sufficient to observe and report a plan, useless to
  stop a bad write.
- A `SparkSessionExtensions` check rule runs on the analyzed plan *before*
  execution and can throw to abort it — the only mechanism of the two
  capable of actually gating a write.
- Both are genuinely needed: `DemoJobHarness` registers a listener purely
  for reporting (`SparkAdapterListener`) and installs the check rule
  purely for enforcement (`ContractEnforcementRule`) — see
  docs/SPARK_ADAPTER.md's "Integration point" for the empirical comparison
  of all three mechanisms Spark exposes, including why `.analyzed` rather
  than `.optimizedPlan` was chosen as the plan to translate.

### ADR-003: Never throw — degrade to `UnknownPlan`/`Diagnostic`

**Decision:** `SparkPlanAdapter.translate` always returns a `Plan`, never
an exception, even for a Catalyst construct it doesn't recognize.

**Rationale:** A verification engine that crashes on an unrecognized plan
node is less useful than one that verifies what it can and flags what it
can't. Every degradation carries a `Diagnostic`; property-based fuzzing
(`SparkPlanAdapterFuzzSpec`) exists specifically to keep this promise
honest across combinations the hand-written suite doesn't reach. See
docs/SPARK_ADAPTER.md's "Never throws" section.

### ADR-004: Separation of the example plugin and the example harness

**Decision:** Keep `plugin` and `runner` as separate modules/JARs.

**Rationale:**
- `plugin` stands in for "a user's real transformation"; `runner` stands
  in for "the job wiring that installs Invaract and drives that
  transformation." Keeping them separate keeps the harness honest — a real
  user's job looks like `runner`'s shape wrapped around their own
  transformation, not like `plugin` plus something engine-specific baked
  in.
- Lets the demo pipeline exercise the *installation* pattern (build a
  `SparkSession` with `ContractEnforcementRule` injected, register
  `SparkAdapterListener`) independently of the transformation itself.

**Alternative considered:** A single combined demo JAR.
**Rejected:** Would blur exactly the "user code vs. engine installation"
boundary a real integration needs to get right.

### ADR-005: Real Spark execution vs. unit test mocking

**Decision:** The harness (`./dev/test`) runs a real `spark-submit` against
a local master, not mocked Spark, and this is treated as the actual source
of truth — see CLAUDE.md's "Critical Requirement."

**Rationale:**
- Verifies real classloading, serialization, and `SparkSessionExtensions`/
  `QueryExecutionListener` registration behavior — none of which a mock
  reproduces faithfully.
- `contract`/`ir` are pure Scala and unit-tested directly; `spark-adapter`
  has its own real-Spark unit suite (`SparkPlanAdapterSpec`, a real H2
  JDBC read, etc.) *and* is proven against `local[*]` end-to-end through
  the harness — passing unit tests alone were explicitly judged
  insufficient for either.

### ADR-006: JSON for the harness's execution report

**Decision:** `demo/output/report.json` is plain JSON, hand-serialized
(no library dependency).

**Rationale:** Human-readable for local debugging, trivial for the web UI
to consume, clean in Git-tracked golden fixtures if/when golden-file
regression testing is added (ROADMAP.md, future scope). This format is
specific to the demo harness — it is not a public API of the verification
engine, and nothing outside this repository is expected to parse it.

### ADR-007: Local Spark master for determinism

**Decision:** `DemoJobHarness` always uses `spark.master("local[*]")`.

**Rationale:** Deterministic, fast, no external infrastructure, adequate
for proving the engine's behavior against known input. Swapping to a real
cluster later needs only a `.master(...)` change — see ROADMAP.md's
"Future Extensibility" notes.

## Module Dependencies

```
contract/        no internal deps; org.scalatest (test)
ir/               no internal deps; org.scalatest (test)
spark-adapter/    depends on: contract, ir
                  org.apache.spark:spark-sql (provided)
                  org.scalatestplus:scalacheck (test, property-based fuzzing)
plugin/           org.apache.spark:spark-sql (provided)
runner/           depends on: contract, ir, plugin, spark-adapter
                  org.apache.spark:spark-sql/spark-core (compile — needed
                  for spark-submit execution, not just provided)
web/              next, react, typescript — independent of every Scala module
```

Cross-module references go through `unmanagedJars` pointing at a sibling
module's assembled jar (no aggregating root `build.sbt`), so
`dev/build`'s build order — `contract`/`ir`/`plugin` concurrently, then
`spark-adapter`, then `runner` — is load-bearing, not incidental. See
`dev/build`'s own comments for the exact dependency graph.

## API Contracts

### CLI arguments (`DemoJobHarness`)

```bash
spark-submit \
  --class com.invaract.runner.DemoJobHarness \
  --master local[*] \
  --jars plugin.jar \
  runner.jar \
  [--dry-run] [input_path] [output_path] [report_path] [contract_path]
```

- `--dry-run` (optional flag, recognized anywhere in the argument list): run with no
  contract at all — `contract_path` is ignored entirely — and infer/print one from this
  run's actual inputs/outputs instead of enforcing one. See
  `ContractEnforcementRule.dryRun` (`spark-adapter`) and docs-site's "Dry-run mode" guide.
- `input_path` (optional): input CSV — default `demo/input/sample.csv`
- `output_path` (optional): output Parquet — default `demo/output/result.parquet`
- `report_path` (optional): output JSON report — default `demo/output/report.json`
- `contract_path` (optional): contract YAML to enforce — default
  `demo/contracts/invaract_output.yaml`; unused in `--dry-run` mode

### `ExecutionReport` shape (harness report, not an engine API)

```json
{
  "status": "PASS" | "FAIL",
  "timestamp": "2026-08-22T16:00:00Z",
  "pluginVersion": "0.1.0",
  "sparkVersion": "3.5.1",
  "scalaVersion": "2.12.18",
  "javaVersion": "21.0.10",
  "durationMs": 7879,
  "buildInfo": { "pluginName": "invaract-spark-plugin", "pluginVersion": "0.1.0" },
  "tests": { "unit": {"passed": 4, "failed": 0}, "integration": {"passed": 1, "failed": 0} },
  "input": { "rowCount": 10, "schema": [{"name": "id", "type": "integer"}, ...], "sample": [...] },
  "output": { "rowCount": 10, "schema": [...], "sample": [...] },
  "plugin": { "events": ["...", ...], "diagnostics": [...] },
  "transformationIR": { "captured": true, "renderedPlan": "...", "lineage": [...], "diagnostics": [...] },
  "contractVerification": { "status": "PASSED", "contract": "invaract_demo_output@1.0.0", "violations": [] },
  "error": null
}
```

In `--dry-run` mode, `contractVerification` instead looks like:

```json
{ "status": "DRY_RUN", "inferredContractYaml": "id: inferred_contract\nversion: \"0.1.0\"\n..." }
```

(`status` stays `"PASS"`/`"FAIL"` at the report's top level either way — it reflects
whether the job itself ran successfully, not whether a contract was enforced.)

### Web API endpoint

```
GET /api/report          200 OK  { ExecutionReport JSON }
                          404 Not Found  { "error": "Report not found" }
```

## Testing Strategy

Full detail lives in each module's own docs
([docs/CONTRACT_MODEL.md](docs/CONTRACT_MODEL.md),
[docs/TRANSFORMATION_IR.md](docs/TRANSFORMATION_IR.md),
[docs/SPARK_ADAPTER.md](docs/SPARK_ADAPTER.md)) and CLAUDE.md's "Mutation
Testing Requirement." Summary:

- **`contract`/`ir`**: pure Scala unit tests (`sbt test` in each module),
  plus whole-module mutation testing (Stryker4s) blocking CI.
- **`spark-adapter`**: unit tests against real Spark (`local[*]`,
  including a real H2 JDBC read — no Spark behavior is mocked), a
  property-based fuzz suite (`SparkPlanAdapterFuzzSpec`, random chains of
  operations asserting the adapter never throws), and whole-module
  mutation testing.
- **The harness (`plugin`/`runner`) via `./dev/test`**: real end-to-end
  `spark-submit`, exercising the whole engine as installed, per ADR-005.
- **`./dev/regression` (Docker, CI's `docker-regression` job)**: the
  pass/fail pair proving `ContractEnforcementRule` actually enforces
  something, not just that a harness run completes.

Guardrails still outstanding (ROADMAP.md, scoped to `contract`/`ir`/
`spark-adapter`): a multi-Spark-version compatibility matrix, coverage
gating, and API-compatibility checking.

## Performance Characteristics

| Step | Timing | Notes |
|---|---|---|
| `contract`/`ir`/`plugin` build | ~15-30s | Concurrent, incremental after first build |
| `spark-adapter` build | ~10-20s | Depends on contract+ir jars existing |
| `runner` build | ~10-15s | Depends on all four other modules |
| Demo job execution | ~5-10s | Local master, 10-row demo data |
| Report generation | <100ms | JSON serialization |
| **Total `./dev/test`** | **~30-60s** | After first (cold-cache) build |
| Mutation testing `ir` (CI) | ~1-2 min | Separate parallel CI job, not part of `./dev/test`; no Spark dependency |
| Mutation testing `spark-adapter` (CI) | ~30-40 min | Separate parallel CI job, not part of `./dev/test` — the actual bottleneck, not the ~1-5 min this table previously (and wrongly) estimated. Runs the whole-module suite once per generated mutant against Delta+Iceberg-backed Spark sessions; see docs/SPARK_ADAPTER.md's mutation-testing-speed investigation for what was and wasn't a real lever here |

## Future Architecture Directions

See [ROADMAP.md](ROADMAP.md) for the authoritative, maintained plan —
Phase 1 (this document's "verification engine" above) is done; the
sections below are Phase 2+ at a glance.

### Phase 2: Multi-engine support

```
Spark → Spark adapter (done)
dbt   → dbt adapter (future)   → same Transformation IR → same Verifier
SQL   → SQL adapter (future)
```

The IR's engine-independence (ADR-001) is what makes this additive rather
than a rewrite.

### Phase 3: Contract registry & governance

Version contracts as Git/registry artifacts (`contract`'s
`ContractCompatibility` already classifies MAJOR/MINOR/PATCH changes
between two contract *versions* — the registry is what would host and
diff them at scale).

### Phase 4: AI & platform integration

Expose the engine's machine-readable output (`VerificationResult`, IR
lineage) for programmatic / agent use — e.g. "find every implementation of
this contract," "assess the blast radius of a contract change,"
"recommend a compatible schema migration."

## References

- [ROADMAP.md](ROADMAP.md) — authoritative phase-by-phase plan and status
- [docs/CONTRACT_MODEL.md](docs/CONTRACT_MODEL.md)
- [docs/TRANSFORMATION_IR.md](docs/TRANSFORMATION_IR.md)
- [docs/SPARK_ADAPTER.md](docs/SPARK_ADAPTER.md)
- [Apache Spark Documentation](https://spark.apache.org/docs/3.5.1/)
- [Scala Language Documentation](https://docs.scala-lang.org/2.12/)
- [sbt Documentation](https://www.scala-sbt.org/)
- [Next.js Documentation](https://nextjs.org/docs)
- [ODCS Specification](https://github.com/opendatadiscovery/open-data-contracts-standard)
- [OpenLineage Specification](https://openlineage.io/docs/)

---

**Last Updated:** 2026-08-22
**Architecture Version:** 0.2.0 — reflects Phase 1 (contract/ir/spark-adapter)
as built, not as planned.
