# Invaract Architecture

## Overview

Invaract is a framework for verifying data transformations against
machine-readable data contracts. This document describes the current
architecture: what's actually built, how the pieces interact, and the
design decisions behind them.

**The product is the verification engine — `contract`, `ir`,
`spark-adapter`, and `fingerprint`.** Everything else in this repository
(`plugin`, `runner`, `demo`, `web`) is an example integration and test
harness built to prove the engine works against a real Spark job, not
something a real Invaract user would import. See "Two halves of this
repository" below before reading further — conflating the two is the
most common way to misjudge where a change belongs.

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
              │ (opt-in: VerificationOptions.computeFingerprint)
              ▼
   fingerprint: TransformationFingerprinter
   (canonicalizes the same IR Plan into a
    deterministic SHA-256 hash — never itself
    a pass/fail signal, see docs/
    SEMANTIC_LINEAGE_FINGERPRINTING.md)
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
that feed it. Fingerprinting is a separate concern layered on top, not a
verification step itself — it runs (when enabled) regardless of whether
the check passes or fails, since a rejected write's fingerprint is just as
useful a diagnostic as a passing one's; see
[docs/SEMANTIC_LINEAGE_FINGERPRINTING.md](docs/SEMANTIC_LINEAGE_FINGERPRINTING.md)
§14 for exactly where in `ContractEnforcementRule` this is computed.

## Two halves of this repository

### The verification engine (the product)

| Module | Package | Purpose |
|---|---|---|
| `contract/` | `com.invaract.contract` | Parses and validates ODCS-shaped YAML contracts; classifies compatibility between two contract versions. No Spark dependency — a contract is a plain data structure. |
| `ir/` | `com.invaract.ir` | An engine-independent `Plan`/`Expr` algebra (`Read`, `Write`, `Project`, `Join`, `Aggregate`, ...), plus `Lineage.trace` (structural column-level provenance) and `PlanPrinter` (human-readable rendering). No Spark dependency, no dependency on `contract` — this is meant to be the thing any engine's plan gets translated *into*. |
| `spark-adapter/` | `com.invaract.sparkadapter` | Translates a real Spark Catalyst `LogicalPlan` into the IR (`SparkPlanAdapter`), verifies it against a contract (`StructuralVerifier`), and enforces that verification inside Spark's own execution lifecycle (`ContractEnforcementRule`, a `SparkSessionExtensions` check rule) or observes it after the fact (`SparkAdapterListener`, a `QueryExecutionListener`). Depends on `ir` and `contract`, and on Spark (`provided`). |
| `fingerprint/` | `com.invaract.fingerprint` | Canonicalizes an `ir.Plan`/`ir.Expr`/`ir.Lineage` value into a deterministic, versioned SHA-256 hash (`Canonicalizer`, `FingerprintHasher`, `TransformationFingerprinter`) — see [docs/SEMANTIC_LINEAGE_FINGERPRINTING.md](docs/SEMANTIC_LINEAGE_FINGERPRINTING.md). Detects a business-logic change (`amount * 1.20` → `amount * 1.25`) that leaves the output schema identical, something schema verification alone cannot. Depends only on `ir`, no Spark dependency — surfaced through `spark-adapter` opt-in (`VerificationOptions.computeFingerprint`), not a required part of verification itself. |

This is where a feature request almost always belongs, and where the
regression-testing guardrails (property-based fuzzing, mutation testing —
see CLAUDE.md's "Mutation Testing Requirement" — and the ones still
outstanding: a multi-Spark-version compatibility matrix, coverage gating)
are scoped: against these four modules, not against `plugin`/`runner`.
API-compatibility checking (MiMa) covers all four too, though only
`contract`/`ir`/`spark-adapter` are (so far) published to Maven Central —
see "Module Dependencies" below.

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

2. Build fingerprint (needs ir published locally — not contract/plugin)
   └─> fingerprint/target/scala-2.12/invaract-fingerprint-0.1.0.jar

3. Build spark-adapter (needs contract, ir, fingerprint published locally)
   └─> spark-adapter/target/scala-2.12/invaract-spark-adapter-0.4.0.jar
       (already bundles fingerprint's compiled classes via sbt-assembly —
       a consumer installing only this jar gets fingerprinting for free)

4. Build runner (needs contract, ir, plugin, spark-adapter)
   └─> runner/target/scala-2.12/invaract-spark-runner.jar

5. Verify Spark environment
   └─> spark-submit --version (must succeed)

6. Run the demo job (DemoJobHarness, via spark-submit)
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

7. Validate the report
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

### ADR-008: Optional capabilities read Spark configuration, not only constructor arguments

**Decision:** An opt-in capability layered onto `ContractEnforcementRule.forContract`
— location resolution's `spark.invaract.locationMap` is the first instance —
reads its configuration from the real `SparkSession`'s own `conf` inside
`forContract`'s outer closure, in addition to (never instead of) accepting
it as an explicit constructor/method argument in code. See CLAUDE.md's
"External Attachability Requirement" for the rule this codifies, and
docs/SPARK_ADAPTER.md's "Location resolution" section for the worked
example.

**Rationale:**
- ADR-002's check rule already receives the live `SparkSession` once, in its
  own outer `SparkSession => LogicalPlan => Unit` closure, before any plan
  is checked (`VersionCompatibilityGuard.check(session)` already runs
  there, for the same reason) — that is both the correct moment (Spark
  configuration is fully assembled from `spark-submit --conf` by then) and
  an already-available one, needing no new extension point.
- The alternative — a capability configurable only via an explicit Scala
  argument a developer writes into their own job — means only that job's
  author can turn it on. A platform team, an orchestration framework, or a
  CI pipeline operating a job whose source they don't control can't attach
  it at all, which defeats the point of a job-external mechanism (a
  location registry, a governance policy, a rollout flag) in the first
  place. Real deployments are exactly the case where the team operating a
  job and the team that wrote it are different people.
- This mirrors how established Spark plugins (Delta Lake's own
  `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`,
  configured entirely through `--conf`) already expect to be attached —
  not a novel pattern invented for this engine.

**What this ADR alone does not cover:** installing `ContractEnforcementRule.forContract`
itself was, at the time this ADR was written, still one line of code every
job wrote — there was no `spark.sql.extensions`-based, fully code-free way
to install the check rule the way Delta's own extension is installed. **This
is now resolved, by ADR-009 below** (`InvaractSparkSessionExtension`) — a
materially larger change than any single optional capability, which is why
it's its own ADR rather than folded into this one, but everything this ADR
describes (optional capabilities reading `SparkConf`) composes with it for
free, since ADR-009's extension class calls the very same `forContract`.

**Alternative considered:** Configuration only via explicit method
arguments (`ContractLocationResolution.resolve(contract, resolver)`,
called in the job's own code), as `com.invaract.sparkadapter.location`
first shipped with. **Rejected as the only mechanism**, not removed: it
remains available for a resolver `SparkConf` can't express (a mapping
built at runtime, a future `HttpLocationResolver`'s endpoint/auth), and
composes freely with the conf-driven path — a location already resolved
to a literal is simply left alone by the conf-driven pass. But it cannot
be the *only* mechanism a capability offers, per this ADR.

### ADR-009: A `spark.sql.extensions`-based, fully code-free install path

**Decision:** `InvaractSparkSessionExtension`
(`spark-adapter/src/main/scala/com/invaract/sparkadapter/InvaractSparkSessionExtension.scala`)
installs Invaract with no code at all in the job it's attached to — named
via `--conf spark.sql.extensions=com.invaract.sparkadapter.InvaractSparkSessionExtension`,
the exact mechanism Delta Lake's own
`spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension` uses.
`ContractEnforcementRule.forContract`/`.dryRun`, called directly in code,
remain available for anything the conf-driven path can't express. See
CLAUDE.md's "External Attachability Requirement" for the rule this
codifies at the baseline-installation level, closing the one gap ADR-008
above left open when it was written.

**Rationale:**
- Spark's `spark.sql.extensions` loader (`SparkSession$.applyExtensions`)
  instantiates each named class via `getConstructor().newInstance()` — a
  **public no-arg constructor**, never one taking `SparkSession`. That
  means `InvaractSparkSessionExtension.apply(extensions: SparkSessionExtensions)`
  itself never has a `SparkSession` to read `spark.invaract.contract` (or
  any other conf key) from directly, confirmed against Spark's own source
  before assuming otherwise.
- `SparkSessionExtensions.injectCheckRule` is the escape hatch, not a new
  mechanism: it registers a `SparkSession => LogicalPlan => Unit` builder
  that Spark itself invokes once a real session using these extensions is
  being built — the exact same shape, and the exact same "session
  materializes here" moment, `ContractEnforcementRule.forContract` already
  returns and relies on (ADR-002, ADR-008). `apply` just calls
  `extensions.injectCheckRule(InvaractSparkSessionExtension.checkRuleFor)`;
  `checkRuleFor` is where `spark.invaract.contract`/`.dryRun`/
  `.notifyConfig` are actually read, receiving the real session Spark hands
  it, needing no mechanism beyond what ADR-002 already established.
- Auto-wiring a notification sink needed one further step beyond what
  ADR-008's conf keys do: `SparkAdapterListener` (for `WriteEvent`s) is
  registered via `session.listenerManager.register(...)`, a call
  independent of `injectCheckRule` entirely. Since `checkRuleFor` already
  has the real `session` in hand at the same moment, it registers the
  listener itself when `spark.invaract.notifyConfig` names a sink that
  builds successfully — no second Spark extension point (e.g. Spark's own
  `spark.sql.queryExecutionListeners` conf key, a plausible alternative)
  needed for this.
- Dry-run mode's `onInferred: Contract => Unit` callback has no code to
  call in the conf-driven path — `spark.invaract.dryRun=true` logs the
  inferred contract at `WARN` instead of invoking a caller-supplied
  callback, a real (if less rich) default rather than declaring dry-run
  mode conf-inexpressible. The callback-based `ContractEnforcementRule.dryRun`
  stays available in code for anything wanting to do more than log.

**Alternative considered:** A `SparkSession`-arg constructor (e.g.
`class InvaractSparkSessionExtension(session: SparkSession)`), reading conf
directly in `apply`. **Rejected**: not merely a style choice — Spark's
loader only ever calls `getConstructor()` (no-arg), so a class requiring a
`SparkSession` argument fails to instantiate via `spark.sql.extensions` at
all, throwing at session-construction time. Confirmed against Spark's own
`applyExtensions` source, not assumed from the constructor signature Delta
happens to use.

## Module Dependencies

```
contract/        no internal deps; org.scalatest (test)
ir/               no internal deps; org.scalatest (test)
fingerprint/      depends on: ir
                  org.scalatestplus:scalacheck (test, property-based fuzzing)
                  no Spark dependency — usable by any future front end that
                  produces ir.Plan, not just spark-adapter
spark-adapter/    depends on: contract, ir, fingerprint
                  org.apache.spark:spark-sql (provided)
                  org.scalatestplus:scalacheck (test, property-based fuzzing)
plugin/           org.apache.spark:spark-sql (provided)
runner/           depends on: contract, ir, plugin, spark-adapter
                  org.apache.spark:spark-sql/spark-core (compile — needed
                  for spark-submit execution, not just provided)
web/              next, react, typescript — independent of every Scala module
```

Cross-module references go through real `libraryDependencies` against each
module's own coordinate, resolved from the local Ivy cache via
`publishLocal` (`contract`/`ir`/`spark-adapter` are the three modules
also published to Maven Central, see "API Contracts" below and
docs/RELEASING.md; `fingerprint` resolves the same way but isn't published
to Central yet — see `fingerprint/build.sbt`'s own FOLLOW-UP comment),
except `plugin` (harness-only, never published), which stays on
`unmanagedJars` pointing at its assembled jar directly. Either way, there is still no aggregating root `build.sbt` —
each module remains an independent sbt project — so `dev/build`'s build
order —
`contract`/`ir`/`plugin` concurrently (with `contract`/`ir` also
`publishLocal`ed), then `fingerprint` (also `publishLocal`ed), then
`spark-adapter` (also `publishLocal`ed), then
`runner` — is load-bearing, not incidental. See `dev/build`'s own comments
for the exact dependency graph.

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

**Last Updated:** 2026-09-06
**Architecture Version:** 0.3.0 — reflects Phase 1 (contract/ir/spark-adapter/
fingerprint) as built, not as planned.
