# Invaract — Development Guide

This repository builds a framework for verifying data transformations
against machine-readable data contracts, with a mobile-first Codespace
development environment for exercising it against a real Spark job.

## What's the product, and what's the test harness

**The product is the verification engine: `contract/`, `ir/`, and
`spark-adapter/`.** Together they parse a data contract, translate a real
Spark job's Catalyst logical plan into an engine-independent IR, verify it
against the contract, and — via a `SparkSessionExtensions` check rule
installed in the `SparkSession` — abort the write if it fails. This is
what a real user of Invaract would depend on.

**`plugin/`, `runner/`, `demo/`, and `web/` are an example integration and
test harness, not the product.** `plugin/` is a small illustrative Spark
transformation (`InvaractPlugin`) standing in for "some real job's
logic." `runner/` is `DemoJobHarness` — an example Spark job that installs
the verification engine and drives `InvaractPlugin` through it, exactly
the way a real user's job would install it, then captures the outcome as
`demo/output/report.json`. `web/` is a mobile-friendly viewer for that
report. None of `InvaractPlugin`, `DemoJobHarness`, or `report.json` is
something a real Invaract user imports or depends on — they exist so
`./dev/test` can prove the engine works against a real Spark execution,
not just unit tests.

Full architecture, component breakdown, and data flow:
[ARCHITECTURE.md](ARCHITECTURE.md). Phase-by-phase status:
[ROADMAP.md](ROADMAP.md). Module-level design docs:
[docs/CONTRACT_MODEL.md](docs/CONTRACT_MODEL.md),
[docs/TRANSFORMATION_IR.md](docs/TRANSFORMATION_IR.md),
[docs/SPARK_ADAPTER.md](docs/SPARK_ADAPTER.md). Adding support for a new
Spark data connector (Iceberg, ClickHouse, Avro, ...) has its own
reusable process — full read/write investigation, fail-closed
classification, verification — documented in
[docs/ADDING_A_SPARK_CONNECTOR.md](docs/ADDING_A_SPARK_CONNECTOR.md) and
runnable as the `add-spark-connector` Claude Code skill
(`.claude/skills/add-spark-connector/`); use it rather than adding a
one-off `translatePlan` case, since skipping the survey it requires is
exactly how the Delta Lake gaps happened.

Keep this distinction in mind before proposing a testing or tooling
addition: something that protects the engine's real behavior (fuzzing,
mutation testing, a compatibility matrix) belongs against `contract`/`ir`/
`spark-adapter`. Something that only formalizes the *demo harness's own
output shape* (e.g. a schema for `report.json`) is protecting a
CI-internal artifact, not a public API — right-size it accordingly, and
don't present it as something external consumers would bind to.

### Quick Summary

- **Verification engine**: `contract` (parser/validator/compatibility),
  `ir` (engine-independent transformation IR + lineage), `spark-adapter`
  (Spark → IR translation, contract enforcement)
- **Example harness**: `plugin` (demo transformation), `runner` (demo job
  — `DemoJobHarness`), `demo` (fixtures + generated output), `web` (report
  viewer)
- **Optional extension**: `notification-kafka` (a `NotificationSink`
  publishing to Kafka — see "Notification sinks" in
  docs/SPARK_ADAPTER.md; not part of the engine's own dependency
  footprint, not built by `./dev/build`, opt-in like `plugin`/`runner`)
- **Spark Version**: 3.5.1
- **Scala Version**: 2.12.18
- **Java Version**: 21 (sbt 1.9.8 for `contract`/`plugin`/`runner`/
  `notification-kafka`; sbt 1.11.7 for `ir`/`spark-adapter`, required by
  Stryker4s — see "Mutation Testing Requirement")
- **Build System**: sbt (5 independent modules `./dev/build` builds, plus
  the standalone opt-in `notification-kafka` — no aggregating root
  `build.sbt` — see `dev/build`'s comments for the cross-module dependency
  graph)
- **Test Execution**: Local Spark master (`local[*]`)
- **Results Viewer**: Next.js web UI, mobile-responsive

## Critical Requirement

**NEVER** consider a change to the verification engine — or to the demo
harness — complete solely because compilation or unit tests succeed.

You MUST:

1. Run `./dev/test`
2. Verify the exit code is `0`
3. Examine the generated `demo/output/report.json`
4. Open the results in the web UI via `./dev/report`
5. Confirm the **Status** field is **PASS**
6. Visually inspect input/output data and schema

Real Spark execution is the source of truth. Unit tests passing ≠ the
engine actually verifying anything inside a real Spark job. If your change
is meant to affect *enforcement* specifically (does a bad write actually
get blocked, not just reported), also run `./dev/regression` — it's the
only thing that proves `ContractEnforcementRule` rejects a violation, as
opposed to a harness run merely completing.

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

**Write mutation-resistant tests the first time — don't use repeated
Stryker runs as your discovery process.** A full `spark-adapter` run
against real Delta/Iceberg/Hive/etc. sessions takes tens of minutes; a
scoped run against a handful of files still takes several minutes to
tens of minutes, since Stryker reruns the real suite per mutant. Treating
that loop ("run it, see what survived, patch a test, run it again") as
the normal way to reach 70% burns that wall-clock repeatedly for
something a careful first pass avoids. Before writing a test, look at the
logic you just wrote and ask what Stryker will actually try — every
comparison flipped (`<` ↔ `<=`, `==` ↔ `!=`), every boolean negated or
forced to a constant, every boundary shifted by one, every `&&`/`||`
swapped, every `exists`/`forall`/`isEmpty` inverted — and write assertions
that would fail under each mutation, not just assertions that exercise
the happy path. Concretely: for an `if (x > 0)`, assert behavior on both
sides *and* at the boundary (`x == 0`); for a filter/exists over a
collection, assert on an empty collection and a collection where no/all
elements match, not just a typical one; for a conjunction, assert a case
where only one side is true. Run the scoped Stryker check once near the
end to confirm the bar is met and to catch anything genuinely
non-obvious — not as the first draft of your test suite. Only iterate
further if that single run turns up a real survivor.

This is a manual, PR-scoped check — Stryker4s has no incremental/diff
mode, so CI cannot enforce "the new code specifically" on its own. (It
*is* automated for PRs: see `.github/workflows/test.yml`'s
`mutation-testing` job, which diffs each PR against its previous push and
reruns `sbt stryker` scoped to just the changed files — see
docs/SPARK_ADAPTER.md's "Incremental checking in CI.")

This bar — and every other regression-testing guardrail in this repo
(property-based fuzzing, mutation testing, API-compatibility checking, and
the still-outstanding compatibility matrix / coverage gating) — is scoped
to `contract`/`ir`/`spark-adapter`. It does not apply to `plugin`/`runner`,
which are example/test code, not the engine.

## API Compatibility Requirement

`contract`, `ir`, and `spark-adapter` are checked for binary compatibility
with [MiMa](https://github.com/lightbend/mima) (`sbt-mima-plugin`) — the
same tool Apache Spark and Akka use to gate their own public API
compatibility release-to-release. It answers a different question than
mutation testing or fuzzing: not "does my code work," but "does this
change silently break everyone who already depends on the previous
version's compiled jar" — a real risk for `contract`/`ir`/`spark-adapter`
specifically, since those are what a real Invaract user would depend on,
and Scala case classes (`Contract`, `Dataset`, `Field`, `Plan`, `Expr`,
...) make this easy to break by accident (adding a field, reordering a
constructor parameter) without it ever showing up as a compile error in
this repository itself.

There is no Maven Central release yet to compare against, so each
module's `mimaPreviousArtifacts` (in its `build.sbt`) points at its own
`com.example %% <module> % 0.1.0` coordinate, and CI's
`api-compatibility` job (`.github/workflows/test.yml`) publishes the PR's
base branch to the runner's local Ivy cache under that exact coordinate
before running `sbt mimaReportBinaryIssues` against the PR's head — "did
this PR, as a whole, break compatibility with what existed before it."
The base is the PR's actual base commit
(`github.event.pull_request.base.sha`), a fixed anchor for the PR's
lifetime, deliberately **not** the previous push's HEAD — a sliding
baseline like that can never durably catch a regression that's
introduced and then never fixed (push N breaks something and fails
correctly; push N+1, even one that touches nothing relevant, diffs
against N, where the break already looks like the status quo, so it
passes clean without anything having been fixed). A module that doesn't
exist yet at the base commit is skipped gracefully — normal for this
repo's own PR #1, whose base predates `contract`/`ir`/`spark-adapter`
entirely (introducing them *is* what that PR does) — rather than
special-cased by changing which commit counts as the base. This job is
part of the `summary` gate like every other CI job here, so a real binary
break fails the PR's overall status — the same "mandatory, automatic"
enforcement every other guardrail in this repo gets, not a separate
opt-in check.

When it fails, you have two honest options, not a third one where you
just silence it:

1. **The break was accidental** — restore the old signature, or find a
   binary-compatible way to make the same change (e.g. a new overload
   instead of changing an existing method's signature; a default
   parameter added at the end of a case class, not the middle).
2. **The break is deliberate** — add the exact `ProblemFilters.exclude[...]`
   line MiMa's own failure output suggests to that module's
   `mimaBinaryIssueFilters` setting, with a comment explaining why, and
   treat it as a MAJOR version change per `ContractCompatibility`'s own
   versioning semantics (docs/CONTRACT_MODEL.md's "Version Compatibility"
   section) once this project starts tagging real releases.

Never react to a MiMa failure by loosening `mimaPreviousArtifacts` or
disabling the check — that defeats the entire point of running it.

## Documentation Policy

**Documentation is a first-class part of the product, not an afterthought bolted on
later.** User-facing documentation lives in `docs-site/` — an Astro Starlight site built
from `docs-site/src/content/docs/`. It documents Invaract from the perspective of
someone *using* the verification engine (writing contracts, installing the enforcement
rule, running the demo harness) — never Invaract's own internals. See
`docs-site/DOCUMENTATION.md` for the full writing playbook (information architecture,
style, when to use MDX/Starlight components, what belongs here vs. in `docs/`/
`ARCHITECTURE.md`/`ROADMAP.md`) before writing or editing a page there.

Whenever a change adds or changes **user-facing** behavior — anything in
`contract`/`ir`/`spark-adapter`'s public surface, the contract format, a `dev/` script, or
how a user installs/configures/runs the engine — you MUST determine whether
`docs-site/` needs updating, as part of the same task, not a follow-up. A change scoped
entirely to `plugin`/`runner`/`demo`/`web` internals with no effect on how a real
Invaract user would install, configure, or use the engine does not require a docs-site
update — but check that assumption before skipping it, since a change to the example
harness sometimes does change what a guide demonstrates (e.g. the actual console output
quoted in a guide).

### When adding a user-facing feature

1. Identify the user-visible behavior introduced — what can a user now do, configure, or
   observe that they couldn't before?
2. Find the appropriate existing documentation page (`docs-site/src/content/docs/`) —
   check Guides, Concepts, and Reference for the closest fit before assuming none exists.
3. Update it if the feature extends something already documented (e.g. a new connector
   joins the table in `reference/connector-support.md`; a new violation type joins
   `reference/violation-types.md`).
4. Create a new guide (under `guides/`) if the feature introduces a genuinely new
   workflow a user would follow — not a page per source file, a page per user goal.
5. Update `getting-started/` if the feature affects the initial install/quick-start
   experience.
6. Update `reference/` if it changes commands, contract-format fields, or other public,
   stable behavior.
7. Add or update examples — real ones, derived from an actual test, fixture, or a real
   run's output, never invented. See `docs-site/DOCUMENTATION.md`'s "How to write an
   example."
8. Check existing pages for contradictions the change introduces (a guide describing the
   old behavior, a reference table now missing a row, a "not yet supported" note that's
   now stale).

### When changing existing behavior

- Find the documentation describing the old behavior and update it — don't leave it
  describing something no longer true.
- Check related guides, concept pages, and reference tables that assumed the old
  behavior.
- Check every example that exercises the changed behavior; re-verify its output against
  a real run rather than editing it by eyeball.
- Remove obsolete instructions rather than leaving them alongside the new ones with a
  caveat — stale-but-present docs are worse than a clean cut.

### When fixing a user-facing bug

Determine whether:

- The documentation was itself incorrect (described behavior that never actually
  existed) — fix it.
- The correct behavior was under-explained and contributed to the bug being filed —
  clarify it.
- A [Troubleshooting](docs-site/src/content/docs/troubleshooting/) entry would help the
  next person hit the same thing — add one, but only for a problem that's actually real
  and reproducible, never a hypothetical.

### Definition of done

A user-facing feature or fix is not complete until:

- [ ] Implementation complete
- [ ] Tests complete (per this file's other requirements — mutation testing, API
      compatibility, `./dev/test`/`./dev/regression` where applicable)
- [ ] User documentation updated (`docs-site/`), per the checklist above
- [ ] Examples updated where the change affects one
- [ ] Existing documentation checked for accuracy against the change
- [ ] `cd docs-site && npm run build` succeeds

CI enforces the last item automatically (`.github/workflows/deploy-docs.yml` builds
`docs-site/` on every push touching it); the rest is a manual check, the same way the
Mutation Testing Requirement above is manual-but-mandatory. A merged PR that changes user-
facing behavior without a corresponding `docs-site/` update should be treated as
incomplete, the same way a PR that changes `ir`/`spark-adapter` without mutation testing
would be.

## Repository Structure

```
.
├── .devcontainer/
│   ├── devcontainer.json        # Dev Container configuration
│   └── post-create.sh           # Setup script (JDK, sbt, Spark)
│
├── contract/                     # Verification engine: contract model
│   ├── src/main/scala/com/example/contract/
│   │   ├── ContractModel.scala
│   │   ├── ContractParser.scala       # YAML → object model
│   │   ├── ContractValidator.scala    # structural validation
│   │   └── ContractCompatibility.scala # version-diff classification
│   └── src/test/scala/com/example/contract/
│
├── ir/                            # Verification engine: transformation IR
│   ├── src/main/scala/com/example/ir/
│   │   ├── Expr.scala, Plan.scala, Identifiers.scala  # engine-independent algebra
│   │   ├── Lineage.scala              # column-level provenance tracing
│   │   └── PlanPrinter.scala          # human-readable rendering
│   └── src/test/scala/com/example/ir/
│
├── spark-adapter/                 # Verification engine: Spark integration
│   ├── src/main/scala/com/example/sparkadapter/
│   │   ├── SparkPlanAdapter.scala     # Catalyst LogicalPlan → ir.Plan
│   │   ├── StructuralVerifier.scala   # IR vs. contract verification
│   │   ├── ContractEnforcementRule.scala # SparkSessionExtensions check rule (gates writes)
│   │   ├── ContractInference.scala    # dry-run mode: infers a Contract from a real write
│   │   ├── SparkAdapterListener.scala # QueryExecutionListener (observes writes)
│   │   └── notification/              # Notification sinks (opt-in event publishing)
│   │       ├── NotificationEvent.scala      # ContractValidationEvent / WriteEvent
│   │       ├── NotificationSink.scala       # trait + Logging/File/Http/HadoopFs built-ins
│   │       ├── NotificationConfig.scala     # .properties-based sink configuration
│   │       └── NotificationSinkFactory.scala # reflective sink loading
│   └── src/test/scala/com/example/sparkadapter/
│
├── plugin/                       # Example harness: demo transformation
│   ├── src/
│   │   ├── main/scala/com/example/plugin/
│   │   │   └── InvaractPlugin.scala
│   │   └── test/scala/com/example/plugin/
│   │       └── InvaractPluginTest.scala
│   ├── build.sbt
│   └── project/assembly.sbt
│
├── demo/                          # Example harness: fixtures + output
│   ├── input/sample.csv         # Deterministic test data
│   ├── contracts/               # Example contracts (incl. a deliberately-broken one)
│   └── output/                  # Generated results (not in git)
│       ├── report.json
│       └── result.parquet
│
├── runner/                       # Example harness: demo job
│   ├── src/main/scala/com/example/runner/
│   │   └── DemoJobHarness.scala # Runs InvaractPlugin through the engine, generates report
│   ├── build.sbt
│   └── project/assembly.sbt
│
├── notification-kafka/           # Optional extension: Kafka NotificationSink
│   ├── src/main/scala/com/example/sparkadapter/notification/kafka/
│   │   └── KafkaNotificationSink.scala # real, unscoped kafka-clients dependency —
│   │                                    # of this module only, not spark-adapter's
│   ├── src/test/scala/com/example/sparkadapter/notification/kafka/
│   └── build.sbt                # unmanagedJars against spark-adapter's assembly jar
│
├── web/                          # Example harness: mobile-friendly results UI
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
│   ├── build                    # Builds all 5 modules in dependency order
│   ├── test                     # End-to-end harness run (7-step verification)
│   ├── regression                # Docker-based pass/fail enforcement proof
│   └── report                   # Launch web UI
│
├── docs/                         # Module-level design docs (developer-facing)
│   ├── CONTRACT_MODEL.md
│   ├── TRANSFORMATION_IR.md
│   └── SPARK_ADAPTER.md
│
├── docs-site/                     # User documentation: Astro Starlight site
│   ├── src/content/docs/
│   │   ├── index.mdx
│   │   ├── introduction/
│   │   ├── getting-started/
│   │   ├── guides/
│   │   ├── concepts/
│   │   ├── reference/
│   │   └── troubleshooting/
│   ├── public/images/            # Screenshots/diagrams (empty until real ones exist)
│   ├── DOCUMENTATION.md          # Documentation playbook — read before editing pages
│   └── astro.config.mjs
│
├── .github/workflows/
│   ├── test.yml                 # CI: OS/JDK test matrix, docker-regression,
│   │                             # mutation-testing, summary gate
│   └── deploy-docs.yml          # Builds and deploys docs-site/ to GitHub Pages
│
├── ARCHITECTURE.md               # Full architecture, ADRs, data flow
├── ROADMAP.md                    # Phase-by-phase plan and status
├── CLAUDE.md                     # This file
└── README.md
```

## Development Workflow

### Working on the verification engine (`contract`/`ir`/`spark-adapter`)

1. Edit source under the relevant module's `src/main/scala/...`
2. Add/update tests under that module's `src/test/scala/...` (for
   `spark-adapter`, prefer a real `local[*]` `SparkSession` over mocking —
   see ARCHITECTURE.md's ADR-005)
3. `cd <module> && sbt test`
4. If you touched `ir` or `spark-adapter`: run mutation testing per the
   "Mutation Testing Requirement" above before calling it done
5. Run `./dev/test` (and `./dev/regression` if the change affects
   enforcement) to prove it against a real Spark job — per "Critical
   Requirement," this is not optional

### Working on the example harness (`plugin`/`runner`/`demo`/`web`)

Useful for demonstrating new engine behavior, adding fixtures, or
improving the results viewer — not for engine logic itself.

#### 1. Initial Setup

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

#### 2. Make Harness Changes

Edit files under `plugin/src/main/scala/com/example/plugin/` (the demo
transformation) or `runner/src/main/scala/com/example/runner/` (the demo
job / `DemoJobHarness`).

#### 3. Test the Harness

Run the comprehensive test harness:

```bash
./dev/test
```

This single command:

1. ✓ Builds all 5 modules (`contract`, `ir`, `plugin` concurrently, then
   `spark-adapter`, then `runner`) via `./dev/build`
2. ✓ Verifies the plugin JAR
3. ✓ Verifies the Spark environment
4. ✓ Prepares the demo output directory
5. ✓ Runs the demo job (`DemoJobHarness`) via real `spark-submit`,
   installing the verification engine and driving `InvaractPlugin`
   through it
6. ✓ Captures results to `demo/output/result.parquet`
7. ✓ Generates `demo/output/report.json`
8. ✓ Validates report status
9. ✓ Returns exit code `0` on success, non-zero on failure

**Example output:**

```
======================================
Invaract Test Suite
======================================

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
✓ Execution report: demo/output/report.json

To view results in web UI:
  ./dev/report
```

#### 4. View Results

Start the mobile-friendly web UI:

```bash
./dev/report
```

The UI will start on `http://localhost:3000` and show:

- **Status Badge**: ✓ PASS or ✕ FAIL (large, mobile-visible)
- **Build Information**: Plugin/Spark/Java versions, duration
- **Test Results**: Unit and integration test pass/fail counts
- **Input/Output Data**: Row count, schema, sample rows
- **Transformation IR**: The translated plan, rendered
- **Contract Verification**: PASSED/FAILED, with violation detail
- **Plugin Events**: Execution timeline and diagnostics
- **Errors**: Full error messages if execution failed

Forward the Codespaces port to your phone and open the URL in a mobile
browser. The UI is fully responsive for screens as narrow as 375px.

#### 5. Iterate

If `./dev/test` fails:

1. Examine the error output
2. Check `demo/output/report.json` for diagnostics
3. Review plugin events and contract verification detail in the web UI
4. Fix the code (engine or harness, depending on where the failure is)
5. Run `./dev/test` again
6. Repeat until exit code is `0`

## Build Artifacts and Outputs

### Engine and plugin JARs

- `plugin/target/scala-2.12/invaract-spark-plugin-0.1.0.jar`
- `contract/target/scala-2.12/invaract-contract-0.1.0.jar`
- `ir/target/scala-2.12/invaract-ir-0.1.0.jar`
- `spark-adapter/target/scala-2.12/invaract-spark-adapter-0.1.0.jar`
- `runner/target/scala-2.12/invaract-spark-runner.jar` — the demo job,
  bundling `DemoJobHarness` plus the engine jars via `unmanagedJars`

All created by `sbt assembly` (via `./dev/build`); used by Spark through
`spark-submit --jars <plugin jar> <runner jar>`.

- `notification-kafka/target/scala-2.12/invaract-notification-kafka-0.1.0.jar`
  — not built by `./dev/build` (opt-in, like `plugin`/`runner`): a user who
  wants `KafkaNotificationSink` runs `cd notification-kafka && sbt assembly`
  themselves and adds the resulting jar to their own `--jars` list. See
  docs/SPARK_ADAPTER.md's "Notification sinks" section.

### Execution Report (harness artifact, not an engine API)

- **Location**: `demo/output/report.json`
- **Format**: Structured JSON — full shape in ARCHITECTURE.md's "API
  Contracts" section
- **Generated by**: `DemoJobHarness.reportToJson` (`runner/`) — a
  hand-rolled serializer, no JSON library dependency
- This format is internal to the demo harness. It is not published or
  versioned as something external tooling binds to — if you need a
  machine-readable output *from the engine itself*, that's
  `VerificationResult`/`Diagnostic` (see `spark-adapter`), not this file.

### Demo Output Data

- **Location**: `demo/output/result.parquet`
- **Format**: Apache Parquet
- **Content**: Output of `InvaractPlugin` processing `demo/input/sample.csv`
- **Lifecycle**: Regenerated on each `./dev/test`

## Execution Model

### Local Spark Master

The demo job always uses a **local Spark master**:

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

The demo job runs **via real Spark submission**, not unit test mocking
(see ARCHITECTURE.md's ADR-005):

```bash
spark-submit \
  --class com.example.runner.DemoJobHarness \
  --master local[*] \
  --jars plugin/target/scala-2.12/invaract-spark-plugin-0.1.0.jar \
  runner/target/scala-2.12/invaract-spark-runner.jar \
  demo/input/sample.csv \
  demo/output/result.parquet \
  demo/output/report.json
```

This ensures:

- ✓ Real classloading behavior
- ✓ Real `SparkSessionExtensions`/`QueryExecutionListener` registration
  (the actual mechanisms the verification engine hooks into)
- ✓ Accurate performance characteristics
- ✓ True integration testing of the engine, not a mock of it

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
- **Purpose**: Exercise the demo transformation and the engine translating it
- **Processing Time**: <1 second

## Example Plugin (demo transformation, not the engine)

`InvaractPlugin.scala` illustrates:

1. **Schema Validation**: Checks for required columns
2. **Transformation**: Adds a computed column (`value_squared`)
3. **Event Logging**: Records execution steps
4. **Error Handling**: Validates input before processing

To modify it:

1. Edit `plugin/src/main/scala/com/example/plugin/InvaractPlugin.scala`
2. Add or update tests in `plugin/src/test/scala/com/example/plugin/InvaractPluginTest.scala`
3. Run `./dev/test`
4. Verify the report

## Versions and Compatibility

| Component | Version | Reason |
|-----------|---------|--------|
| JDK       | 21      | Latest LTS, Spark 3.5 compatible |
| Scala     | 2.12.18 | Spark 3.5.1 standard binary |
| Spark     | 3.5.1   | Latest stable, well-supported |
| sbt       | 1.9.8 (`contract`/`plugin`/`runner`), 1.11.7 (`ir`/`spark-adapter`) | Stryker4s 1.1.1 needs sbt ≥ 1.11.2; the other three modules stayed on the older, more broadly-tested version |
| Next.js   | 14.1.0  | Latest stable, Vercel-maintained |
| Node.js   | 20      | LTS, stable |

### Java Compatibility

- All modules target JVM 1.8 bytecode (via `-target:jvm-1.8` scalacOption)
- Runtime JDK 21 fully supports 1.8 bytecode
- Forward compatible to future JDK versions

## CI/CD Pipeline

GitHub Actions workflow (`.github/workflows/test.yml`) runs on every push/PR:

- **`test`**: OS × Java matrix (ubuntu/macos/windows × 11/17/21, with
  exclusions) — builds all 5 modules and runs `./dev/test`
- **`docker-regression`**: runs `./dev/regression`, proving
  `ContractEnforcementRule` actually blocks a bad write, not just that a
  harness run completes
- **`mutation-testing`**: whole-module Stryker4s for `ir`/`spark-adapter`
  (blocking at each module's `break` threshold), plus the incremental
  changed-files check on PRs (70% bar) — see "Mutation Testing
  Requirement" above
- **`summary`**: gates on all of the above

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
- `"contractVerification"` (status, violations — is this an engine
  rejection, or something else?)
- `"error"` field (contains error message)
- `"plugin.events"` array (execution timeline)

### 3. View in web UI

```bash
./dev/report
# Open in browser and inspect the Contract Verification and Plugin Events sections
```

### 4. Check Spark logs

If the report indicates Spark execution failed:

```bash
# Look for Spark logs in the demo job's output
# Check that input CSV is readable
file demo/input/sample.csv
head -5 demo/input/sample.csv
```

### 5. Narrow down where the problem is

- Engine translation/verification issue → check `spark-adapter`'s own
  unit tests (`cd spark-adapter && sbt test`) before assuming it's the
  demo harness
- Demo transformation issue → check
  `plugin/src/main/scala/com/example/plugin/InvaractPlugin.scala` for
  null pointer exceptions, schema assumptions, case sensitivity, type
  mismatches
- Report/harness wiring issue → check `runner/src/main/scala/com/example/runner/DemoJobHarness.scala`

### 6. Run unit tests in isolation

```bash
cd spark-adapter   # or contract, ir, plugin
sbt test
cd ..
```

This helps isolate whether the problem is in the engine itself, the demo
transformation, test data, or report generation.

## Future Extensibility

See [ROADMAP.md](ROADMAP.md) for the maintained, authoritative plan. A few
harness-specific notes:

### Adding a Real Spark Cluster

The harness uses `local[*]` Spark master (see ARCHITECTURE.md's ADR-007).
To use a real cluster later:

1. Modify `runner/src/main/scala/com/example/runner/DemoJobHarness.scala`
2. Change `.master("local[*]")` to `.master("spark://cluster:7077")` or YARN/Kubernetes
3. Update `.github/workflows/test.yml` to provision the cluster
4. Report format remains unchanged

### Extending the Report Format

`demo/output/report.json` is extensible (it's a harness artifact, not a
versioned API — see "Build Artifacts and Outputs" above). To add fields:

1. Update `ExecutionReport` case class in `DemoJobHarness.scala`
2. Add corresponding fields to `reportToJson` serialization
3. Update `web/app/page.tsx` to display new fields
4. Update `web/app/page.module.css` for styling

### Supporting Multiple Spark Versions

Currently pinned to Spark 3.5.1, only in `spark-adapter` (and thus the
harness). This is one of the outstanding regression-testing guardrails
(ROADMAP.md) — a matrix run of `spark-adapter`'s suite against several
Spark versions would catch Catalyst plan-shape drift the adapter would
otherwise silently mistranslate.

## Common Development Tasks

### Add a new column transformation (demo harness)

```scala
// In InvaractPlugin.scala
def addNewColumn(df: DataFrame): DataFrame = {
  logEvent("Adding new_column")
  df.withColumn("new_column", col("value") + 100)
}
```

Then add test:

```scala
// In InvaractPluginTest.scala
test("addNewColumn should add column") {
  val df = spark.createDataFrame(...)
  val result = InvaractPlugin.addNewColumn(df)
  assert(result.columns.contains("new_column"))
}
```

Run `./dev/test` to verify.

### Change demo data

Edit `demo/input/sample.csv` and run `./dev/test`.

### Update plugin version

1. Edit version in `plugin/build.sbt` (e.g., `version := "0.2.0"`)
2. Update JAR name in `plugin/build.sbt` assembly config
3. Update the harness's `pluginVersion` in `DemoJobHarness.scala`
4. Run `./dev/test`

### Troubleshoot Spark locally

```bash
# Start Spark shell with the engine + plugin JARs
spark-shell --jars plugin/target/scala-2.12/invaract-spark-plugin-0.1.0.jar,spark-adapter/target/scala-2.12/invaract-spark-adapter-0.1.0.jar

# Then in shell:
// scala> val df = spark.read.csv("demo/input/sample.csv", header=true, inferSchema=true)
// scala> val result = com.example.plugin.InvaractPlugin.process(df)
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
git clone https://github.com/mlltx/Invaract.git
# Wait for post-create.sh to finish (~5 min first time)

# 2. Make a change — engine (contract/ir/spark-adapter) or harness (plugin/runner)
edit spark-adapter/src/main/scala/com/example/sparkadapter/SparkPlanAdapter.scala

# 3. Test
./dev/test
# Wait for result (~30-60s)

# 4. View results on phone
./dev/report
# Open http://localhost:3000 on phone (via forwarded Codespaces port)

# 5. Iterate
# Make more changes, run ./dev/test, check results
# If you touched ir/ or spark-adapter/, also run mutation testing (see above)

# 6. When satisfied
git add .
git commit -m "Add feature X"
git push
# CI runs the same checks: test matrix, docker-regression, mutation-testing
```

## Support and Debugging

For issues:

1. Check that `./dev/test` produces non-zero exit code
2. Review `demo/output/report.json` for error details
3. Examine each module's `src/test/scala/...` tests
4. Check plugin events and contract verification detail in the web UI
5. Verify Spark is running: `spark-submit --version`

## Performance Expectations

See ARCHITECTURE.md's "Performance Characteristics" for the full
per-module build breakdown. Headline numbers:

- First build (cold sbt cache): ~2 minutes
- Subsequent builds: ~15-30s per module (incremental)
- **Total `./dev/test`**: ~30-60s (after first build)
- Mutation testing (`ir`/`spark-adapter`, CI-only, not part of `./dev/test`): ~1-5 min

On mobile network, the web UI may be slower due to data volume (~100KB report).

## References

- [ARCHITECTURE.md](ARCHITECTURE.md) — full architecture, ADRs, data flow
- [ROADMAP.md](ROADMAP.md) — phase-by-phase plan and status
- [docs/ADDING_A_SPARK_CONNECTOR.md](docs/ADDING_A_SPARK_CONNECTOR.md) —
  reusable process for adding a new Spark connector to `spark-adapter`
- [docs/CVE_REMEDIATION.md](docs/CVE_REMEDIATION.md) — process for
  triaging and fixing Dependabot/CVE alerts without regressing the
  guardrails above
- [Apache Spark](https://spark.apache.org/)
- [Scala 2.12](https://docs.scala-lang.org/2.12/)
- [sbt](https://www.scala-sbt.org/)
- [Next.js](https://nextjs.org/)
- [GitHub Codespaces](https://github.com/features/codespaces)
- [ODCS Specification](https://github.com/opendatadiscovery/open-data-contracts-standard)

---

**Last Updated**: 2026-08-23
**Status**: Phase 1 (verification engine) complete; example harness and
web UI stable. See ROADMAP.md for what's next.
