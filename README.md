# Invariant

A framework for verifying data transformations against machine-readable
data contracts — parse a contract, translate a real Spark job's logical
plan into an engine-independent IR, verify it against the contract, and
abort the write if it fails. Ships with a mobile-first Codespaces
environment for exercising the whole thing against a real Spark job from
a phone.

**The product is the verification engine — `contract/`, `ir/`, and
`spark-adapter/`.** `plugin/`, `runner/`, `demo/`, and `web/` are an
example integration and test harness that prove the engine works against
a real Spark job, not something a real user imports. See
[ARCHITECTURE.md](ARCHITECTURE.md) and [CLAUDE.md](CLAUDE.md) for the full
distinction — it matters for where a change belongs.

## Quick Start

### Prerequisites

- GitHub account with Codespaces access
- Modern web browser (Safari, Chrome, Firefox)
- No local development setup required

### Setup

1. **Clone and open in Codespaces**:
   ```bash
   git clone https://github.com/mlltx/Invariant.git
   # Open in GitHub Codespaces — the Dev Container auto-provisions
   # JDK 21, sbt, Apache Spark 3.5.1, and Node.js 20 (~5 min first time)
   ```

2. **Run the test harness**:
   ```bash
   ./dev/test
   ```
   Builds all 5 modules, runs a real Spark job (`DemoJobHarness`) with the
   verification engine installed, and validates the result. Exit code `0`
   = pass.

3. **View results**:
   ```bash
   ./dev/report
   ```
   Forward port 3000 to your phone and open `http://localhost:3000`.

## Key Features

- **A real verification engine, not a linter**: parses an ODCS-shaped
  contract, translates a Spark job's actual Catalyst logical plan into an
  engine-independent IR, and verifies it structurally against the
  contract
- **Enforcement, not just reporting**: a `SparkSessionExtensions` check
  rule (`ContractEnforcementRule`) aborts a write that violates its
  contract *before* Spark executes it — proven by `./dev/regression`'s
  pass/fail pair, not just a happy-path run
- **Real Spark execution as the test harness**: the demo job runs via
  actual `spark-submit`, not mocked Spark (see ARCHITECTURE.md's ADR-005)
- **Never throws**: the Spark adapter degrades to `Unsupported`/
  `Diagnostic` on constructs it doesn't recognize instead of crashing —
  checked by property-based fuzzing across random operation chains
- **Mutation-tested**: `ir` and `spark-adapter` are mutation-tested with
  Stryker4s, blocking CI below each module's threshold (see CLAUDE.md's
  "Mutation Testing Requirement")
- **Mobile-first results UI**: fully responsive down to 375px screens,
  auto-refreshing
- **One-command verification**: `./dev/test` covers build → real Spark
  execution → report validation

## Repository Structure

```
.
├── contract/                # Verification engine: contract model
├── ir/                      # Verification engine: transformation IR
├── spark-adapter/           # Verification engine: Spark integration
│                             (translation, verification, enforcement)
├── plugin/                  # Example harness: demo transformation
├── runner/                  # Example harness: demo job (DemoJobHarness)
├── demo/                    # Example harness: fixtures + generated output
├── web/                     # Example harness: mobile-friendly results UI
├── dev/                     # Development scripts (build, test, report, regression)
├── docs/                    # Module-level design docs
├── .devcontainer/           # GitHub Codespaces configuration
├── .github/workflows/       # CI/CD pipeline
├── ARCHITECTURE.md          # Full architecture, ADRs, data flow
├── ROADMAP.md               # Phase-by-phase plan and status
├── CLAUDE.md                # Comprehensive development guide
└── README.md                # This file
```

## Commands

| Command | Purpose |
|---------|---------|
| `./dev/test` | Build every module, run the demo job on a real Spark session, generate and validate a report |
| `./dev/build` | Build every module's jar, in dependency order, without running the demo job |
| `./dev/regression` | Contract regression pack: proves a satisfied contract executes and a violated one is aborted before any output is written (see below) |
| `./dev/regression-docker` | Same regression pack, in a self-contained Docker image — no local JDK/sbt/Spark needed |
| `./dev/report` | Start the web UI on localhost:3000 |
| `cd spark-adapter && sbt test` | Run one module's unit tests only (works for `contract`/`ir`/`spark-adapter`/`plugin`) |
| `cd spark-adapter && sbt stryker` | Run mutation testing for one module (`ir`/`spark-adapter` only) |

## Contract Regression Pack

`ContractEnforcementRule` (see [docs/SPARK_ADAPTER.md](docs/SPARK_ADAPTER.md))
gates every Spark write against a contract before it executes: a write
that violates its contract is aborted, and no output is ever created.
`./dev/regression` re-runs that guarantee as a script instead of a
transcript, with real `spark-submit` invocations and real assertions —
not mocks:

```bash
./dev/regression
```

It checks two real cases and exits non-zero if either behaves unexpectedly:

1. **Contract satisfied** (`demo/contracts/invariant_output.yaml`) — the
   job exits 0, the report says `PASS`, and the output file exists.
2. **Contract violated**
   (`demo/contracts/invariant_output_broken_example.yaml`, which requires a
   `customer_name` column the demo transformation never produces) — the
   job exits non-zero, the report says `FAIL` with a `MISSING_OUTPUT_FIELD`
   violation, and — the core guarantee — the output file is never created.

The only requirement is a working `./dev/test` environment (Codespaces
already provides one). With just Docker installed and nothing else,
`./dev/regression-docker` builds a self-contained image and runs the same
pack inside it — useful for verifying the guarantee on a machine that
hasn't set up JDK/sbt/Spark at all. This same pack runs in CI on every
push, so the abort path is checked automatically, not just the happy path.

## Example Integration

`plugin/`'s `InvariantPlugin` is a small, illustrative transformation
(schema validation, a computed column, event logging) standing in for
"some real job's logic" — it's what the engine is demonstrated against,
not part of the engine itself. `runner/`'s `DemoJobHarness` is the example
Spark job: it installs the verification engine into a real `SparkSession`
exactly the way a real user's job would, drives `InvariantPlugin` through
it, and captures the outcome.

See `plugin/src/main/scala/com/example/plugin/InvariantPlugin.scala` and
`runner/src/main/scala/com/example/runner/DemoJobHarness.scala`.

## Test Results

Each run of `./dev/test` generates (harness artifacts, not engine APIs —
see ARCHITECTURE.md's "API Contracts"):

1. **`demo/output/report.json`** — status, build info, test counts,
   input/output schema and sample data, the translated Transformation IR,
   contract verification outcome, and demo-plugin events/diagnostics
2. **`demo/output/result.parquet`** — output data from the demo run
3. **Web UI** — mobile-friendly visualization of both

## Mobile Access

1. Run `./dev/test` to generate a report
2. Run `./dev/report` to start the web UI
3. Codespaces forwards port 3000
4. Open the forwarded URL on your phone (375px+ responsive)
5. The UI polls every 2 seconds for new reports

## CI/CD

GitHub Actions workflow (`.github/workflows/test.yml`) runs on every push/PR:

- **`test`**: OS × Java matrix (ubuntu/macos/windows × 11/17/21) — builds
  all 5 modules and runs `./dev/test`
- **`docker-regression`**: runs `./dev/regression`, proving enforcement
  actually blocks a bad write
- **`mutation-testing`**: Stryker4s for `ir`/`spark-adapter`, whole-module
  and incremental per-PR
- **`summary`**: gates on all of the above

Exit code determines PR check status.

## Important Notes

- **Real Spark execution**: the demo job runs via actual `spark-submit`,
  not unit test mocks
- **Exit code 0**: success means the engine actually verified a real
  Spark write, not just that code compiled
- **Mobile-first**: optimized for 375–430px screens
- **No infrastructure**: uses a local Spark master, no cloud required
- **Deterministic**: same demo data every run, reproducible results

## Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) — component breakdown, data flow, ADRs
- [ROADMAP.md](ROADMAP.md) — phase-by-phase plan and status
- [CLAUDE.md](CLAUDE.md) — complete development guide, troubleshooting,
  performance expectations
- [docs/CONTRACT_MODEL.md](docs/CONTRACT_MODEL.md),
  [docs/TRANSFORMATION_IR.md](docs/TRANSFORMATION_IR.md),
  [docs/SPARK_ADAPTER.md](docs/SPARK_ADAPTER.md) — module-level design docs

## References

- [Apache Spark](https://spark.apache.org/) — data processing framework
- [Scala 2.12](https://docs.scala-lang.org/2.12/) — programming language
- [sbt](https://www.scala-sbt.org/) — build tool
- [Next.js 14](https://nextjs.org/) — React framework
- [GitHub Codespaces](https://github.com/features/codespaces) — cloud development
- [ODCS Specification](https://github.com/opendatadiscovery/open-data-contracts-standard) — the contract format this project's contracts are shaped after

## License

Apache License 2.0 — see [LICENSE](LICENSE).

## Support

For questions or issues:

1. Review [CLAUDE.md](CLAUDE.md)
2. Check GitHub Actions workflow logs
3. Inspect `demo/output/report.json` and the web UI's diagnostics
4. Examine the relevant module's source and tests

---

**Status**: Phase 1 (verification engine) complete; example harness and
web UI stable. See [ROADMAP.md](ROADMAP.md) for what's next.
**Last Updated**: 2026-08-22
