<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/logo-dark.svg">
  <img src="assets/logo-light.svg" alt="Invaract" height="56">
</picture>

**Your Spark job says it writes X. Does it actually write X?**

Invaract checks a Spark job's real write — the actual Catalyst plan, not
what the code claims — against a machine-readable data contract, and
aborts the write before it happens if they don't match. No silent bad
data. No finding out from a downstream dashboard.

1. **Add a contract** — declare the schema a write is supposed to produce.
2. **Run the check** — Invaract's enforcement rule verifies the job's real
   Spark plan against it, before Spark executes the write.
3. **Get a useful failure** — a violating write is aborted, nothing is
   written, and you get an exact diagnosis: which field, why, how to fix it.

📖 **[Full documentation →](https://mlltx.github.io/Invaract/)** — install,
write your first contract, guides, reference.

## See it fail, for real

This is the actual output of this repository's own example job — no
setup beyond `./dev/build`, no wrappers, just `spark-submit`.

**1. The contract** (`demo/contracts/invaract_output_broken_example.yaml`)
declares an output field the job doesn't produce yet:

```yaml
outputs:
  - name: result
    location: demo/output/result_broken_example.parquet
    format: parquet
    schema:
      fields:
        - name: id
          type: integer
          required: true
        - name: value
          type: integer
          required: true
        - name: value_squared
          type: integer
          required: true
        - name: customer_name
          type: string
          required: true
```

**2. Run it** — Invaract's `ContractEnforcementRule` is installed in the
job's `SparkSession` the same way any real job would install it; this just
invokes the resulting jars directly:

```bash
spark-submit \
  --class com.example.runner.DemoJobHarness \
  --master local[*] \
  --jars plugin/target/scala-2.12/invaract-spark-plugin-0.1.0.jar \
  runner/target/scala-2.12/invaract-spark-runner.jar \
  demo/input/sample.csv \
  demo/output/result_broken_example.parquet \
  demo/output/report.json \
  demo/contracts/invaract_output_broken_example.yaml
```

**3. The failure** — `spark-submit` exits non-zero, and the real exception
message (trimmed here to the useful part — the full message also shows
what the contract expects and the full translated plan) reads:

```
Exception in thread "main" com.example.sparkadapter.ContractViolationException:
Contract violation: 'invaract_demo_output@1.0.0' rejected this transformation. Write aborted.

What the plan contains:
  Write(demo/output/result_broken_example.parquet, format=parquet, saveMode=Overwrite)
  [...]

Why it violates the contract (1 violation):
  1. [MISSING_OUTPUT_FIELD] required field 'customer_name' is absent from the actual OUTPUT schema

How to correct it:
  1. Add a 'customer_name' column (type 'string') to the output, or mark it
     optional in the contract if it isn't always produced.
```

`demo/output/result_broken_example.parquet` is never created. Nothing
partial gets committed — the write is stopped before Spark touches disk.
This is the exact mechanism `./dev/regression` checks in CI on every push
(both directions: a satisfying write goes through, a violating one is
aborted) — see [Prove Enforcement with the Regression
Pack](https://mlltx.github.io/Invaract/guides/running-the-regression-pack/).

## What's the product

**The verification engine — `contract/`, `ir/`, `spark-adapter/`.**
Together they parse a contract, translate a real Spark job's logical plan
into an engine-independent IR, verify one against the other structurally,
and — via a `SparkSessionExtensions` check rule — abort the write if it
fails. This is what a real user of Invaract depends on.

**`plugin/`, `runner/`, `demo/`, `web/` are the example above and its test
harness**, not the product: a small illustrative transformation, the demo
job that installs the engine and drives it (exactly like a real job
would), fixtures, and a mobile-friendly results viewer. See
[ARCHITECTURE.md](ARCHITECTURE.md) for the full breakdown.

## Try the full demo

```bash
git clone https://github.com/mlltx/Invaract.git
cd Invaract
./dev/test      # build everything, run the passing case, validate the report
./dev/report    # results UI on :3000
```

Needs JDK 21, sbt, and Spark 3.5.1 on your `PATH` — see
[Installation](https://mlltx.github.io/Invaract/getting-started/installation/).
Don't want to install those locally? [GitHub
Codespaces](https://github.com/features/codespaces) is a supported,
zero-setup alternative — its dev container provisions all three for you.
Or skip Spark/sbt entirely and just prove the abort path:

```bash
./dev/regression-docker
```

## Contract rules — beyond schema

A contract can also constrain a table's row-level `MERGE`/`UPDATE`/`DELETE`,
checked against the real Spark plan the same way, aborting the write if
violated:

```yaml
rules:
  - type: merge_condition
    columns: [customer_id, region]
  - type: forbid_unconditional_delete
  - type: allowed_update_columns
    columns: [status, updated_at]
```

Catches real bugs: a `MERGE` silently missing a match key, a `DELETE` with
no `WHERE`, an `UPDATE` touching a column it shouldn't. Works today against
Delta and Iceberg. Where a rule genuinely can't be checked (e.g. Iceberg
merge-on-read `UPDATE`), the write is aborted with
`RULE_UNVERIFIABLE_DML` rather than silently let through unverified. Full
details: [Enforce Row-Level DML
Rules](https://mlltx.github.io/Invaract/guides/enforcing-dml-rules/).

## Commands

| Command | Purpose |
|---------|---------|
| `./dev/test` | Build every module, run the demo job on a real Spark session, generate and validate a report |
| `./dev/build` | Build every module's jar, in dependency order, without running the demo job |
| `./dev/regression` | Proves a satisfied contract's write executes and a violated one is aborted before any output is written |
| `./dev/regression-docker` | Same regression pack, in a self-contained Docker image — no local JDK/sbt/Spark needed |
| `./dev/report` | Start the web results UI on `localhost:3000` |
| `cd spark-adapter && sbt test` | Run one module's unit tests (`contract`/`ir`/`spark-adapter`/`plugin`) |
| `cd spark-adapter && sbt stryker` | Mutation testing for one module (`ir`/`spark-adapter` only) |

## Repository structure

```
.
├── contract/                # Verification engine: contract model
├── ir/                      # Verification engine: transformation IR
├── spark-adapter/           # Verification engine: Spark integration
│                             (translation, verification, enforcement)
├── plugin/                  # Example: demo transformation
├── runner/                  # Example: demo job (DemoJobHarness)
├── demo/                    # Example: fixtures + generated output
├── web/                     # Example: mobile-friendly results UI
├── dev/                     # Development scripts
├── docs/                    # Module-level design docs (developer-facing)
├── docs-site/               # User documentation (Astro Starlight)
├── ARCHITECTURE.md          # Full architecture, ADRs, data flow
├── ROADMAP.md               # Phase-by-phase plan and status
└── CLAUDE.md                # Comprehensive development guide
```

## Documentation

- **[Full user documentation](https://mlltx.github.io/Invaract/)** —
  install, write your first contract, install the enforcement rule in
  your own job, guides, concepts, and reference.
- [ARCHITECTURE.md](ARCHITECTURE.md) — component breakdown, data flow, ADRs
- [ROADMAP.md](ROADMAP.md) — phase-by-phase plan and status
- [CLAUDE.md](CLAUDE.md) — development guide, testing requirements,
  troubleshooting

## License

Apache License 2.0 — see [LICENSE](LICENSE).

---

**Status**: Phase 1 (verification engine) complete; example harness and
web UI stable. See [ROADMAP.md](ROADMAP.md) for what's next.
**Last Updated**: 2026-08-29
