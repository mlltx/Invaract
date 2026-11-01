# Python I/O adapter: design

> **Status: design. No code exists.** This is the design for checking Python data jobs against a data contract by having
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

**Decisions needed** (section 13):
1. Accept a one-time code change per job as the way to adopt Invaract for Python. This means declaring
   `config.zeroCodeInstall` unsupported: a deliberate exception to the install half of the External Attachability
   Requirement in CLAUDE.md. Every setting stays configurable from outside the job.
2. Run the checks in Invaract's existing Java checker, as one helper process per run, rather than re-implementing them in
   Python.
3. Treat "required" as "no empty values in what was written" (checked exactly on the data) rather than "can never be
   empty" (which needs the transformation).
4. Start with Parquet and CSV files on local and mounted file systems. Object storage, partitioned outputs, table formats
   and databases come later.

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
- `read` and `write`: one description each; returns the verdict;
- `finish`: what was read and written, and whether the job completed; returns the final verdict and publishes the event.

**Why the existing checker, not Python.** Contract parsing, validation, organisational policy, location maps, the registry,
notification sinks, dry-run inference and the checks themselves already exist, held to the repository's coverage,
mutation and API-compatibility gates. A Python copy would drift from them. The cost:
- Java 11 or later wherever the job runs;
- about a second of start-up per run, measured for the Beam Python adapter's checker. It starts in parallel with the job's
  own start.

A GraalVM native image of the checker would remove the Java requirement; it is a later step (section 12), as it was for
Beam Python.

**Type mapping.** Arrow types map to the contract's types through one mapping shared with the Beam Python adapter. That
mapping is `PyArrowSchemas`, which already covers `large_string`, `string_view`, dictionaries, lists, structs and maps; it
moves into `verification-core` and gains a structured input form.

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
  - CSV inputs are read with the contract's delimiter (default `,`), not detected, since detection failed in the sink
    records survey's probe 6.
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
- `CheckNotRun` (exit status 2) means the checker could not start or answer. The job's data is never published unchecked.
- The validation event carries:
  - the engine `python-io`;
  - the Python version;
  - the versions of pyarrow and of each frame library used;
  - the job's entry point;
  - the run id.

  It does not carry the command line, which can hold secrets.

## 10. Repository layout, build, CI and release

- **The module.** A new sbt module, `python-io-adapter`, generated by `dev/new-adapter python-io` once approved. It holds:
  - the Scala checker (`PythonIoCheck`, the helper's entry point), its declaration, and its conformance and attestation
    tests;
  - the Python package, in `python-io-adapter/python/invaract/`, with pytest tests;
  - the lint, in `python-io-adapter/python/invaract_lint/`.
- **Repository registration.** `modules.json` gets the module, and `check_modules.py` names every file that must list it.
  The user documentation gets a guide, "Using Invaract with Python jobs".
- **Gates.**
  - The Scala side gets the usual ones: coverage, mutation testing, and API compatibility after the first release.
  - The Python side gets pytest, a lint, and mutation testing with `mutmut`, since the repository's mutation testing is
    Scala-only.
  - A CI job runs the conformance suite with real Python, over a version matrix.
- **Release.** The checker as a runnable jar; the package on PyPI, versioned with the jar.

## 11. Versions and compatibility

- Python 3.9 to 3.13.
- pyarrow: the oldest release with `RecordBatchReader.from_stream` and the C stream protocol (UNVERIFIED which one).
- pandas: the release that added `__arrow_c_stream__` (UNVERIFIED, believed 2.2).
- Polars and DuckDB: the releases that added it (UNVERIFIED).

An older frame library falls back to its own conversion to Arrow (`pa.Table.from_pandas`, Polars' `to_arrow`), which are
long-standing public functions. The CI matrix tests the oldest and newest supported release of each.

## 12. Milestones

1. **The checker protocol** (small to medium). `PythonIoCheck`, with `open`, `write`, `read` and `finish`, over the shared
   Arrow mapping. Tested with recorded descriptions; no Python needed.
2. **The package** (medium). The three pieces, the decorator forms, staging and publishing, the configuration, and the
   dry-run path with `location=`. pytest tests, and the end-to-end example of the sink records survey's probe 14 made
   permanent.
3. **Conformance** (medium). The conformance adapter, the declaration, the baseline file, the capability matrix row.
4. **The lint** (small).
5. **The user guide, and the first release** (small).
6. **Later, each on its own:**
   - object storage through Delta or Iceberg;
   - partitioned outputs;
   - a native image of the checker (no Java);
   - Polars lazy plans for the transformation-shape rules;
   - databases.

## 13. Decisions for you

1. Adoption is a one-time code change per job, with `config.zeroCodeInstall` declared unsupported and every setting
   configurable from outside. *Recommend yes.*
2. The checks run in the existing Java checker, as one helper process per run. *Recommend yes; a native image later.*
3. "Required" means no empty values in what was written, and the published file then states `not null`. *Recommend yes.*
4. The first version covers Parquet and CSV files on local and mounted file systems. *Recommend yes.*
5. The lint is the main guard against reads and writes outside the package, and the run-time strict mode is optional and
   off by default. *Recommend yes.*
6. The package's import name is `invaract`, and its PyPI name `invaract`. *Recommend yes, if the name is free.*

## 14. Open questions and unverified points

| Question | How to settle it |
|---|---|
| The oldest pandas, Polars, DuckDB and pyarrow releases with the Arrow C stream protocol | their changelogs, then the CI matrix |
| Java on the machines that run Python jobs (decision 2) | `java -version` on the platforms' worker images |
| How small the change is on real jobs | convert a sample of real jobs |
| How often real jobs read or write outside the package | run the lint over the same sample |
| The kit decision on per-write read attribution (scenario 20) | the open decision D4 of docs/PYTHON_ADAPTER_SURVEY.md |
| Whether `invaract` is free on PyPI | pypi.org |

## Evidence

- **Probe 15 (2026-11-01).**
  - `pa.RecordBatchReader.from_stream` read an Arrow stream from a pandas 2.3.3 DataFrame, a Polars 2.1.0 DataFrame, a
    DuckDB 2.0.0 result and a pyarrow 25.0.1 Table. All four offer `__arrow_c_stream__`; Polars gave `string_view` for text.
  - `LazyFrame.collect_schema()` returned a lazy query's output schema before it ran.
- **The toys this design is built from:** docs/SINK_RECORDS_SURVEY.md, section 6, probes 13 (a decorator over one function,
  including a Polars lazy job) and 14 (the three pieces over a three-file job, correct and with four mistakes).
- **The conformance counts:** `./dev/adapter-survey` with the draft declaration of section 8, run 2026-11-01.
