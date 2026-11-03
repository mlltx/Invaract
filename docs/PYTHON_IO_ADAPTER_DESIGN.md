# Python I/O adapter: design

> **Status: design, decisions settled 2026-11-01 (section 13). No code exists.** This is the design for checking Python data jobs against a data contract by having
> Invaract do the job's reading and writing. It follows two surveys:
> - docs/PYTHON_ADAPTER_SURVEY.md, which found that watching an unchanged Python job from inside is possible but costly
>   to maintain;
> - docs/SINK_RECORDS_SURVEY.md, whose section 6 proposed this design and tested it as a toy (probes 13 and 14).
>
> It needs a go-ahead before `dev/new-adapter` is run. Once it is, the write-up that generator creates becomes the adapter's
> design record and takes over from this document. Written: 2026-11-01.

## Summary

**What it is.** A Python package, `invaract`. A team makes a small, one-time change to its job:
- it puts `@invaract.job` on the entry point;
- it reads and writes its datasets *by name*, with `invaract.read("orders")` and `invaract.write(df, "report")`.

Invaract then does the reading and the writing:
- it takes the locations from the contract, so no paths remain in the job;
- it checks each output against the contract when it is written, and holds it in staging;
- when the run ends, it publishes all the outputs together, and only if every check passed.

**Why this design.** Earlier approaches watched an unchanged job from inside it: hooks, wrappers, tracers, the syntax tree,
operating-system watches. Each depends on things that keep changing: library internals, coding styles, privileges. This
design depends only on stable, public interfaces:
- the Arrow stream protocol, which pandas, Polars, DuckDB and pyarrow all implement (probe 15);
- pyarrow's Parquet and CSV reading and writing;
- Invaract's existing checking code.

**What it guarantees.** For everything a job reads and writes through the package:
- it reads only declared inputs and writes only declared outputs, at the contract's locations;
- every declared output is written, with the declared columns, types and required columns, in the declared format and
  save mode;
- a run that breaks any of this publishes nothing.

What it does not see is reading or writing done *outside* the package. A lint guards against that (section 7).

**Decisions** (settled 2026-11-01; section 13):
1. Adopting Invaract for Python is a one-time code change per job. `config.zeroCodeInstall` is declared unsupported: a
   deliberate exception to the install half of the External Attachability Requirement in CLAUDE.md. Every setting stays
   configurable from outside the job.
2. **No Java where jobs run.** The existing checker is compiled ahead of time into a native program (GraalVM native image)
   and shipped inside the Python package. A Python re-implementation is the fallback only if that fails (section 4).
3. "Required" means no empty values in what was written, checked exactly on the data.
4. The first version covers Parquet and CSV files on local and mounted file systems. Object storage, Delta Lake and Iceberg
   follow **immediately after, as committed work tracked until delivered** (section 12; ROADMAP.md).
5. The lint is the main guard against reads and writes outside the package; the run-time strict mode is optional and off
   by default.
6. The package is `invaract` on PyPI, with `invaract-lint` beside it. Both names were free on 2026-11-01 and should be
   registered early.

## 1. Goals and non-goals

**Goals:**
- Confirm a job wrote where its contract says, and nowhere else, for everything it writes through the package.
- Confirm each output is what the contract declares: its columns, types, required columns, format and save mode.
- Confirm a job read its declared inputs, and nothing undeclared, through the package.
- Make a failing run publish nothing.
- Keep adoption to a small, mechanical change.
- Keep maintenance low: no dependence on library internals, no operating-system privileges.
- Make every setting configurable without touching the job (contract, location map, registry, organisational policy,
  notifications, dry run).

**Non-goals for the first version:**
- Readiness signals. Decided 2026-11-01: every output is a data asset.
- Transformation-shape rules, lineage and the analyses. They are refused safely; a later version could take them from a
  Polars lazy plan.
- Row-level changes (update, delete, merge) and streaming.
- Seeing reads and writes made outside the package. The lint and the destination's own records are the guard.

## 2. How a team uses it

**The contract** holds the locations; the job holds names:

```yaml
id: orders_report
version: "1.0.0"
inputs:
  - { name: orders,    location: data/in/orders,        format: parquet }
  - { name: customers, location: data/in/customers.csv, format: csv }
outputs:
  - name: report
    type: DATA_ASSET
    location: data/out/report
    format: parquet
    saveMode: overwrite
    schema:
      fields:
        - { name: region, type: string, required: true }
        - { name: total,  type: long,   required: true }
```

**The API** has three pieces. There are optional decorator forms of the last two.

| Piece | Where | What it does |
|---|---|---|
| `@invaract.job()` or `@invaract.job("contract.yaml")` | once, on the entry point. Or `with invaract.run():` in a script without one | starts a run, and at its end checks and publishes, or publishes nothing |
| `invaract.read("orders", as_="pandas")` | wherever the job reads | reads a declared input as pandas (the default), `"polars"`, `"polars-lazy"` or `"arrow"` |
| `invaract.write(frame, "report")` | wherever the job writes | checks the table now and stages it; publishes at the run's end |
| `@invaract.reads("customers")` | on a function | the input is passed in as a parameter |
| `@invaract.writes("report")` | on a function | what the function returns is written |

`write` accepts any object that offers an Arrow stream: a pandas or Polars DataFrame, a DuckDB result, a pyarrow Table or
RecordBatchReader. A Polars LazyFrame is written by streaming it into staging.

**A job over three files.** Before, it read and wrote with pandas, with the paths in the code:

```python
# load.py
def paid_orders():
    orders = invaract.read("orders")                  # was pd.read_parquet("data/in/orders")
    return orders[orders["status"] == "paid"]

@invaract.reads("customers")                          # was pd.read_csv("data/in/customers.csv")
def customers(customers):
    return customers

# build.py: the transformation is unchanged
def report(paid, customers):
    return paid.merge(customers, on="customer_id").groupby("region", as_index=False).agg(total=("amount", "sum"))

# main.py
@invaract.job()
def main():
    paid = load.paid_orders()
    invaract.write(build.report(paid, load.customers()), "report")    # was report.to_parquet("data/out/report/...")
```

**Migrating in two steps, with no contract to begin with.**
1. **Replace the reads and writes, keeping the paths.**
   - The calls take a `location=` while no contract exists: `invaract.read("orders", location="data/in/orders")`.
   - Run the job with `INVARACT_DRY_RUN=true`. Invaract reads and writes as before, and publishes a draft contract built
     from the names, locations, formats and schemas it saw.
2. **Review the draft, commit it, and remove the `location=` arguments.**
   - With a contract in force, a `location=` that disagrees with the contract is refused.
   - So a forgotten argument cannot point a write elsewhere.

## 3. What happens in a run

```
@invaract.job starts ─► the checker helper starts; it resolves the contract (file or registry://), the location map and the
                        organisational policy, and refuses a contract it cannot check (UNSUPPORTED_CONTRACT_FEATURE) before
                        the job's code runs
invaract.read(name)   ─► undeclared name: refused at once. Otherwise read from the contract's location (only the declared
                        columns, if the contract lists them), and the input's schema checked
invaract.write(f, n)  ─► undeclared name: refused at once. Otherwise the Arrow stream's schema and null counts checked by the
                        checker; on a pass, written to staging with the contract's types and required-ness
the job returns       ─► the checker checks every declared output was written and every declared input read
                        pass: publish every staged output (overwrite, append or error, per the contract's save mode)
                        fail: discard staging; nothing published
                        either way: one validation event, through the configured notification sink
the job raises        ─► discard staging; nothing published; a FAILED event saying the job itself failed
```

The run ends with exit status 0 (published), 1 (refused) or 2 (the check could not run, for example no Java). A refusal
raises `invaract.ContractViolation`, a subclass of `PermissionError`, with Invaract's standard explanation.

## 4. Architecture

```
team's job (Python) ──► invaract package (Python, thin)                   ──► invaract checker (Scala, verification-core)
                         reads and writes data with pyarrow                     resolves and validates the contract
                         turns frames into Arrow streams                        checks each write's schema, format, save mode
                         stages and publishes files                             checks outputs written and inputs read
                         sends descriptions (no data) over a pipe               publishes events; drafts contracts (dry run)
```

**The package does no checking.** It sends the checker a description of each read and write: its name, its format, the
Arrow schema with each column's null count, and the number of rows. The verdict comes back. Data never crosses to the
checker.

**One helper process per run.** It is started by `@invaract.job` and talks over JSON lines on standard input and output:
- `open`: the settings and the job's details; returns the resolved contract (names, locations, formats, schemas, save
  modes), or the refusal;
- `read` and `write`: one description each; returns the verdict. A write is described twice: by `write` before any of it
  is staged (columns and types, so a write that cannot pass is refused early), and by `written` once it is (the empty values
  each column holds, for decision 3);
- `finish`: what was read and written, and whether the job completed; returns the final verdict and publishes the event.

**Why the existing checker, not Python.** Contract parsing, validation, organisational policy, location maps, the registry,
notification sinks, dry-run inference and the checks themselves already exist, held to the repository's coverage,
mutation and API-compatibility gates. A Python copy would drift from them.

**No Java where jobs run (decision 2).** Most users' Python workers have no Java. So the checker is compiled ahead of time
with GraalVM `native-image` into a standalone program, one per platform, shipped inside the Python package's wheels. The
package starts it exactly as it would start Java: same protocol, same process model. Milestone 0 measured it: a whole run
of a recorded case, start-up included, takes 6 to 15 ms natively, against about 0.45 s on the JVM.

What compiling ahead of time costs:
- **Plug-ins loaded by class name.**
  - Invaract loads several things by the class named in configuration: notification sinks (even the built-in ones,
    `NotificationSinkFactory`), custom rule verifiers, custom policy evaluators, type-guarantee checks, and registry
    clients (`ReflectivePluginResolver`, `ContractReference`).
  - A native program can load only classes listed when it is built. The built-in ones are listed in its reflection
    configuration.
  - A team's *own* plug-in classes cannot be loaded. Those teams need a build that includes them, or the Java build. The
    declaration and the user guide must say so.
- **Libraries that use reflection.** Very little was needed. GraalVM's tracing agent, run over the recorded protocol cases,
  found only the file sink's constructor, two `java.sql` classes SnakeYAML looks up, and the capability declaration as a
  resource. The configuration committed in the jar (`META-INF/native-image/`) lists those and the other built-in sinks.
- **A build per platform:** Linux x86_64 and aarch64, macOS arm64 and x86_64, and Windows if users need it. Each is built in
  CI with GraalVM's GitHub Action; Linux x86_64 is built today (CI's `pythonio-native-checker` job).
- **Wheel size.** 41 MB for Linux x86_64, 14 MB compressed, which is roughly what a wheel carries.

**The fallback, only if the native image fails the first milestone's criteria.** Re-implement in Python the narrow set of
checks this adapter needs: contract parsing, schema, location, format, save mode, outputs written and inputs read. Hold
it to the Scala implementation with a shared set of recorded cases, run against both in CI, so a difference fails the
build. Organisational policy, the registry and plug-ins would then be unavailable to Python jobs until ported.

**Type mapping.** Arrow types map to the contract's types through one mapping shared with the Beam Python adapter:
`ArrowSchemas` in `verification-core` (moved there from the Beam adapter's `PyArrowSchemas` in milestone 1). It covers
`large_string`, `string_view`, dictionaries, lists, structs and maps; the package sends each column's `str(field.type)`, which
`ArrowSchemas.dataType` reads.

## 5. Data handling

- **Accepting tables.** Anything with `__arrow_c_stream__` is accepted; probe 15 showed pandas 2.3.3, Polars 2.1.0, DuckDB
  2.0.0 and pyarrow 25.0.1 all offer it. A Polars LazyFrame's schema is known before it runs (`collect_schema()`) and it
  streams into staging. Anything else is refused as `UNVERIFIABLE_WRITE`.
- **Large data.** Writes stream batch by batch. The schema is checked from the stream's schema before any batch is written.
  Null counts are summed per batch, and a required column's first empty value fails the write.
- **Required columns (decision 3).** "Required" means no empty values in what was written. pandas never marks a column as
  "cannot be empty" (the native Python survey, probe 8), so checking the schema's flag would refuse every pandas job. The
  written file then carries `not null` for each required column, so consumers see the contract's schema exactly.
- **Formats.** Parquet and CSV in the first version.
  - CSV inputs are read with a comma, not detected, since detection failed in the sink records survey's probe 6. (A
    contract has no delimiter field; milestone 2 found this.)
  - Types are given to the reader from the contract's input schema where it lists them.
- **Save modes**, from the contract, not chosen by the job:
  - `overwrite` replaces the output's folder at publish;
  - `append` adds files named after the run, never replacing;
  - `error` refuses if the output already has data.
- **Staging and publishing.** Each output is written to a staging folder beside its location. At publish, the previous
  version is moved aside for rollback, then the staging folder is renamed into place. On a local or mounted file system a
  rename is atomic per output. Across several outputs, publishing is ordered, not atomic: a crash between two renames
  leaves the first published. The run's event records exactly what was published.
- **Object storage** (S3, GCS) has no rename. Publishing would be a copy, then a delete, which readers can see half done.
  So object storage waits for a design of its own. Writing a Delta or Iceberg table is the likely answer: its commit is
  atomic, and both were probed in the sink records survey.

## 6. Configuration and external attachability

Every setting can be given without touching the job, as environment variables spelled `INVARACT_<NAME>`. The decorator's
argument is only a default, and `INVARACT_CONTRACT` overrides it.

| Setting | Variable |
|---|---|
| `contract` (a file or `registry://id@version`) | `INVARACT_CONTRACT` |
| `dryRun` | `INVARACT_DRY_RUN` |
| `notifyConfig` | `INVARACT_NOTIFY_CONFIG` |
| `registryUrl`, `registryClientClass` | `INVARACT_REGISTRY_URL`, `INVARACT_REGISTRY_CLIENT_CLASS` |
| `locationMap` | `INVARACT_LOCATION_MAP` |
| `orgPolicy`, `orgPolicyOverlays` | `INVARACT_ORG_POLICY`, `INVARACT_ORG_POLICY_OVERLAYS` |
| `rejectUndeclaredInputs`, `rejectUndeclaredFields` | `INVARACT_REJECT_UNDECLARED_INPUTS`, `INVARACT_REJECT_UNDECLARED_FIELDS` |
| `computeFingerprint`, `staticDataQuality`, `roleConsistency` | the same spelling; each declared unsupported, so a run with one is refused |
| `failOnMissingOutput`, `trustedSteps` | not applicable: a missing output always fails the run, and there are no steps to trust |

The package checks its list against `InvaractConf.All` in its tests, as the Beam Python adapter does, so a new neutral
setting cannot be missed.

**The exception (decision 1).** Adopting the package is a one-time change to the job, so this adapter declares
`config.zeroCodeInstall` unsupported, with that reason. CLAUDE.md's External Attachability Requirement asks that the
baseline installation work without the job's code. This design trades that away, after the two surveys found the zero-change
routes too costly to maintain. The maintainers should accept the exception explicitly.

## 7. Guarding against reads and writes outside the package

The package sees only what goes through it. Three guards, in order of cost:
1. **`invaract-lint`.** A check of the job's code, run in the team's CI or as a pre-commit hook. It flags calls that read or
   write data outside the package, such as:
   - `pd.read_*`, `DataFrame.to_*`;
   - `pq.read_table`, `pq.write_table`;
   - Polars' `read_*`, `scan_*`, `write_*`, `sink_*`;
   - `open(..., "w")`, `duckdb` `COPY`, database connections.

   It checks only that such calls are *present*, which a syntax tree answers reliably. It does not try to work out what
   they do (the sink records survey, probe 9, showed that is where the syntax tree fails). A deliberate exception is
   marked in the code (`# invaract: allow`) and shows up in review. Upkeep: a short list of function names.
2. **The destination's own records**, for databases, once they are supported (the sink records survey, section 3).
3. **An optional strict mode at run time.** Python's audit hook refuses file writes outside staging (the native Python
   survey, probe 3). It is off by default, because it is the kind of mechanism this design exists to avoid relying on.

## 8. Capability declaration and conformance forecast

The draft declaration:
- **supported:**
  - `read.batch`, `write.batch`;
  - `check.location`, `check.outputsWritten`, `check.schema`, `check.nestedTypes`, `check.format`, `check.saveMode`;
  - `policy.organizational`;
  - `config.locationRefs`, `config.contractRegistry`;
  - `reporting.notifications`, `reporting.dryRunInference`.
- **partial:**
  - `check.inputExistence`: reads are exact, but which reads fed which write is not known;
  - `failClosed.unverifiableWrites`: writes outside the package are not seen.
- **unsupported:**
  - streaming reads and writes;
  - `check.catalogRegistration`, since the first version writes files only;
  - `rules.planShape`, `rules.custom`;
  - every analysis and column lineage;
  - `config.zeroCodeInstall`.
- **not applicable:** row-level changes and their rules, schema-only changes, boundary resolution.

As built (`invaract-capabilities-pythonio.yaml`, milestone 1), the declaration matches this draft, except that the engine is
labelled "in development" on the user pages until the first release (milestone 5 removed the label). `failClosed.unverifiableWrites` is partial because a
table the package cannot describe is refused (`UNVERIFIABLE_WRITE`), which is what the kit's scenario for it checks. A write
made around the package is never refused, because it is not seen; the lint is the guard.

`./dev/adapter-survey` with that declaration says the kit would expect, of its 55 scenarios:
- **27 to be checked:**
  - locations, inputs and outputs;
  - columns, types, nesting and required columns;
  - formats and save modes;
  - an invalid contract, an untranslatable write.
- **15 to be refused safely:**
  - catalog requirements;
  - the transformation-shape rules;
  - static data quality;
  - role consistency.
- **13 skipped:**
  - row-level changes;
  - fingerprints and lineage;
  - boundaries;
  - streaming.

Three of the 27 carry known risks:
- **Scenario 12** (a required column the job can leave empty). The kit's job must actually produce an empty value, or a
  check on the written values passes. The conformance adapter generates the input data, so it should include one.
  Decision 3 is the underlying choice.
- **Scenario 18** (a write that skips its own input). It conforms only because of how the kit's job is laid out.
- **Scenario 20** (an undeclared second write). The stray write is refused, but the kit also expects `MISSING_INPUT`, and
  the package cannot know which reads fed that write. This is the open kit decision from the native Python survey (D4).

**How CI proves it.** A conformance adapter writes each scenario as a small Python job that uses the package, and runs it
with the real interpreter, package and checker, as `BeamPythonConformanceAdapter` does. No credentials are needed:
everything is local files.

## 9. Errors and reporting

- `ContractViolation(PermissionError)` carries the violations and Invaract's standard four-part explanation.
- `CheckNotRun` (exit status 2) means the checker could not start or answer, for example no build for this platform. The
  job's data is never published unchecked.
- The validation event carries:
  - the engine `pythonio`;
  - the Python version;
  - the versions of pyarrow and of each frame library used;
  - the job's entry point;
  - the run id.

  It does not carry the command line, which can hold secrets.

## 10. Repository layout, build, CI and release

- **The module.** The sbt module `pythonio-adapter`, generated by `dev/new-adapter pythonio` (an adapter's name has no
  hyphen). It holds:
  - the Scala checker (`PythonIoCheck`, the helper's entry point), its declaration, and its conformance and attestation
    tests;
  - the Python package, in `pythonio-adapter/python/invaract/`, with pytest tests;
  - the lint, in `pythonio-adapter/lint/invaract_lint/` (its own package root; milestone 4).
- **Repository registration.** `modules.json` gets the module, and `check_modules.py` names every file that must list it.
  The user documentation gets a guide, "Using Invaract with Python jobs".
- **Gates.**
  - The Scala side gets the usual ones: coverage, mutation testing, and API compatibility after the first release.
  - The Python side gets pytest, a lint, and mutation testing with `mutmut`, since the repository's mutation testing is
    Scala-only.
  - A CI job runs the conformance suite with real Python, over a version matrix.
- **Release.** The package on PyPI as one wheel per platform, each carrying the native checker, plus a pure-Python wheel
  that starts a Java checker for platforms without a native build. `invaract-lint` is a separate pure-Python package. The
  checker jar is also published, for the Java route and for teams with their own plug-ins.

## 11. Versions and compatibility

- Platforms with a native checker: Linux x86_64 and aarch64, macOS arm64 and x86_64. Windows only if users need it.
- Python 3.10 to 3.13. (3.9 reached its end of life in October 2025, and CI's runners no longer offer it; milestone 2.)
- pyarrow 15, the first with `RecordBatchReader.from_stream` (14 lacks it; milestone 2).
- pandas 2.2, which added `__arrow_c_stream__`; the package converts pandas with `pa.Table.from_pandas` in any case, to leave
  out the row index (milestone 2).
- Polars 1.0 and DuckDB 1.0: neither has the stream yet, so the package uses their own conversion; the tests pass on both
  (milestone 2).

An older frame library falls back to its own conversion to Arrow (`pa.Table.from_pandas`, Polars' `to_arrow`), which are
long-standing public functions. The CI matrix tests the oldest and newest supported release of each.

## 12. Milestones

0. **The native checker, first and gating** (medium). Build `PythonIoCheck` with GraalVM `native-image` in CI, for Linux
   x86_64 first. It passes if:
   - it answers the protocol with the same verdicts as on the JVM, over the recorded cases;
   - it parses contracts, applies organisational policy and publishes to the built-in sinks;
   - it starts in well under a second;
   - its size suits a wheel.

   If it fails, the Python fallback of section 4 replaces it, and this design is revised.

   **Result (2026-11-01): passed.** GraalVM CE 21.0.2 builds it in about 75 s. It gives the same replies as the JVM on 23
   recorded protocol cases (`pythonio-adapter/src/test/resources/protocol`). The cases cover a pass, each kind of refusal,
   organisational policy, `ref://` locations, the file, HTTP, logging and fan-out sinks, a bad contract and protocol
   misuse. Each whole run takes 6 to 15 ms. The program is 41 MB, 14 MB compressed. One case is JVM-only: a registry
   client is named by class, and a native program loads only the classes built into it (section 4's plug-in limit).
   The fallback is not needed. Details are in docs/PYTHONIO_ADAPTER.md.
1. **The checker protocol** (small to medium). `PythonIoCheck`, with `open`, `write`, `read` and `finish`, over the shared
   Arrow mapping. Tested with recorded descriptions; no Python needed.

   **Result (2026-11-02): done.** `read`, the two-step write (`write`, then `written` with null counts), Arrow's type names
   through `ArrowSchemas` (now in `verification-core`), unverifiable writes, and dry-run mode with drafts merged at the end.
   40 recorded cases; the native program gives the JVM's replies on the 39 it can run. 42 of the kit's scenarios conform.
   The protocol is listed in docs/PYTHONIO_ADAPTER.md.
2. **The package** (medium). The three pieces, the decorator forms, staging and publishing, the configuration, and the
   dry-run path with `location=`. pytest tests, and the end-to-end example of the sink records survey's probe 14 made
   permanent.

   **Result (2026-11-02): done.** `pythonio-adapter/python/invaract`, with 91 pytest tests against the real checker (the jar
   and the native program), and probe 14's job in `examples/orders_job` with a test for each of its bugs. The checker gained
   the inputs' declared columns in `open`'s reply and a check of `location=` with a contract in force. Details, and what the
   build found, are in docs/PYTHONIO_ADAPTER.md.
3. **Conformance** (medium). The conformance adapter, the declaration, the baseline file, the capability matrix row.
   Started with milestone 0: the kit's scenarios already run through the checker, as the package will drive it, and 41
   conform. What remains is running them as real Python jobs, and a `staged-publish` enforcement point in
   `verification-core`. The user pages' wording for `in-engine-blocking` says a job's earlier writes "have already
   happened", which staging makes untrue.

   **Result (2026-11-02): done.** All 42 conforming scenarios run as real Python jobs through the package
   (`PythonIoPackageConformanceSpec`, strict in CI), alongside the in-process run, against the one baseline. The declaration's
   enforcement point is the new `staged-publish`. The conformance run uses one Python release (3.13, current pyarrow); the
   version matrix of section 10 runs the package's own tests (`pythonio-python`). Details are in docs/PYTHONIO_ADAPTER.md.
4. **The lint** (small).

   **Result (2026-11-02): done.** `invaract-lint` in `pythonio-adapter/lint`: section 7's checks, with `# invaract: allow`
   exceptions, a command line for CI and pre-commit, and 71 tests. Run over the example job before it adopted the package, it
   finds its four reads and writes, and none after. Mutation testing with `mutmut`: 87.9%. Details are in
   docs/PYTHONIO_ADAPTER.md.
5. **The user guide, and the first release** (small).

   **Result (2026-11-03): built; the release itself waits on PyPI's one-time setup.** The docs-site guide "Use Invaract
   with Python Jobs", from real runs of the example job. `release-python.yml` builds the native checker and a wheel on
   each platform of section 11, tagged from the program itself (its glibc version, or its minimum macOS version), and a
   pure-Python wheel carrying the checker jar, which the package runs with Java (11 or later) where no native wheel
   matches; each wheel is installed and runs the example job before anything is published. `invaract-lint` is
   released with them, under one version. Publishing is PyPI's trusted publishing on a `python-v<version>` tag; the
   names, the trusted publishers and the `pypi` environment are a maintainer's one-time setup (docs/RELEASING.md).
   Details are in docs/PYTHONIO_ADAPTER.md.
6. **Immediately after the first release, committed and tracked until delivered (decision 4):**
   - **Object storage** (S3, GCS, Azure), for reading and for writing.
   - **Delta Lake and Iceberg outputs.** Their commits are atomic, which is what makes publishing safe on object storage.
     The sink records survey, probe 3, showed both record operation, mode, rows and schema per commit, through delta-rs and
     pyiceberg.

   The rule for this work:
   - it is listed in ROADMAP.md as open items;
   - it is not displaced by other Python I/O work;
   - each release note until it lands states what is still missing.
7. **Later, each on its own:** partitioned outputs, Polars lazy plans for the transformation-shape rules, databases.

## 13. Decisions (settled 2026-11-01)

| # | Decision | Answer |
|---|---|---|
| 1 | Adoption is a one-time code change per job; `config.zeroCodeInstall` unsupported; every setting configurable from outside | **Yes** |
| 2 | Where the checks run | **No Java where jobs run.** The existing checker compiled to a native program, shipped in the package; a Python re-implementation only if milestone 0 fails |
| 3 | "Required" means no empty values in what was written; the file then states `not null` | **Yes** |
| 4 | First version: Parquet and CSV on local and mounted file systems | **Yes, then object storage, Delta Lake and Iceberg immediately after, committed and tracked until delivered** |
| 5 | The lint is the main guard; the run-time strict mode optional and off | **Yes** |
| 6 | `invaract` on PyPI (and `invaract-lint`) | **Yes**; both free on 2026-11-01, to be registered early |

## 14. Open questions and unverified points

| Question | How to settle it |
|---|---|
| ~~The oldest pandas, Polars, DuckDB and pyarrow releases with the Arrow C stream protocol~~ | settled by milestone 2 (section 11): pyarrow 15, pandas 2.2, Polars 1.0 and DuckDB 1.0, held by CI's `pythonio-python` job |
| ~~Whether the checker builds as a native image, with which reflection configuration, at what size and start-up~~ | settled by milestone 0 (section 12): it does, with a few lines of configuration, at 41 MB and 6 to 15 ms a run |
| How many teams use their own plug-in classes (sinks, rules, policies, registry clients), which a native checker cannot load | ask the teams; check the deployed notification and policy configurations |
| How small the change is on real jobs | convert a sample of real jobs |
| How often real jobs read or write outside the package | run the lint over the same sample |
| ~~The kit decision on per-write read attribution (scenario 20)~~ | settled as D4 recommends: the kit's `Scenarios.partialExcuses` holds an adapter that declares `check.inputExistence` `partial` to scenario 20's other violation only |
| Whether the PyPI names stay free until registered | register placeholder releases of `invaract` and `invaract-lint` |

## Evidence

- **Probe 15 (2026-11-01).**
  - `pa.RecordBatchReader.from_stream` read an Arrow stream from a pandas 2.3.3 DataFrame, a Polars 2.1.0 DataFrame, a
    DuckDB 2.0.0 result and a pyarrow 25.0.1 Table. All four offer `__arrow_c_stream__`; Polars gave `string_view` for text.
  - `LazyFrame.collect_schema()` returned a lazy query's output schema before it ran.
- **The toys this design is built from:** docs/SINK_RECORDS_SURVEY.md, section 6, probes 13 (a decorator over one function,
  including a Polars lazy job) and 14 (the three pieces over a three-file job, correct and with four mistakes).
- **The conformance counts:** `./dev/adapter-survey` with the draft declaration of section 8, run 2026-11-01.
