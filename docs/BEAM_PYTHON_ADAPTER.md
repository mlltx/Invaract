# Beam Python adapter

> **Status: built.** Checks Apache Beam pipelines written with the Python SDK against a data contract before they are
> submitted. Survey: [docs/BEAM_PYTHON_ADAPTER_SURVEY.md](BEAM_PYTHON_ADAPTER_SURVEY.md). The Java adapter it shares its
> translation with: [docs/BEAM_ADAPTER.md](BEAM_ADAPTER.md). User guide: docs-site `guides/using-invaract-with-beam-python`.

## In one picture

```
team's Python job (unchanged)
   │  --runner=invaract_beam.InvaractRunner --invaract_contract=...
   ▼
invaract_beam (Python, beam-adapter/python)          ← thin: no checking logic at all
   │  pipeline.to_runner_api()  →  pipeline.pb (a temporary file)
   │  java -jar invaract-beam-checker.jar --pipeline=pipeline.pb --invaract_<setting>=...
   ▼
PortableCheck (Scala, beam-adapter)                  ← the checker: exit 0 pass, 1 refused, 2 not checked
   │  PortablePipeline: proto → BeamStep tree (Java coders rehydrated, display data with namespaces)
   │  BeamTranslator(steps, PythonSteps)             ← the same translator as Java pipelines, with Python's IO catalogue
   │  BeamCheck(..., BeamCapabilities.python)        ← the same check, with the Python adapter's declaration
   ▼
exit 0 → the package hands the pipeline to --invaract_delegate (DataflowRunner, DirectRunner, ...)
else  → ContractViolation / CheckNotRun is raised; nothing runs
```

## The decisions, and why they keep it easy to maintain

| | Decision | Why |
|---|---|---|
| D1 | **A Python wrapper runner** (`--runner=invaract_beam.InvaractRunner`) is the hook, a pre-submit gate. | The Java adapter's design. It sees every pipeline, whatever the runner (Dataflow's Python runner does not go through a job service). |
| D2 | **All checking is in Scala; the Python package only bridges.** | One place for the logic, held to the module's coverage, mutation and MiMa gates. The Python package is ~150 lines with no decision in it: it forwards flags, writes a file, runs a program and reads its exit status. |
| D3 | **One translator for both SDKs** (`BeamTranslator`, parametrised by a `StepCatalogue`). A Python pipeline becomes the same `BeamStep` tree a Java one does. | The walk, what fails closed, unknown steps keeping the reads beneath them, steps that only report on writes (D14 of the Java adapter): written and tested once. Only "which steps are reads and writes" differs, and that is the catalogue (`JavaSteps`, `PythonSteps`). |
| D4 | **The bridge is a separate program per submission** (`java -jar`), not a service or a native image. | The least machinery. A check takes under a second (0.9 s measured on a recorded pipeline). Java 11+ where jobs are submitted is the cost, stated in the declaration. |
| D5 | **The Python catalogue keys on the source and sink classes**, not on transform names: every Python read is an `iobase.Read` whose `source` item names its source class, and every file read or write states `file_pattern` published by that class. | A short table (`PythonSteps.FileSources`, `FileSinks`) covers every file format; a new SDK transform built on a known source or sink needs no change. |
| D6 | **Read a write's columns from what it states**: a Parquet write's pyarrow schema (`PyArrowSchemas`), an Avro write's schema, a BigQuery load's schema, else the data's row schema (`SchemaCoder`, rehydrated from the proto). No Python-side helper. | Everything the checks need is in the portable form; a helper reading private Python attributes would be a second, untested place that breaks on SDK releases. |
| D7 | **Transformation-shape rules are unsupported.** | Python filters and joins are code. A contract with such a rule is refused with `UNSUPPORTED_CONTRACT_FEATURE` before anything else (`VerificationPipeline.requireSupported`, which both Beam adapters now call first). |
| D8 | **The settings are spelled `--invaract_<snake_case>`** and the package forwards each one unchanged; the checker refuses one it does not know (exit 2). | A setting means the same on every engine. `InvaractConf.All` is the list; three tests tie the Java options, the checker and the Python flags to it (see "Maintaining it"). |
| D9 | **CI runs real Python pipelines** through the real package and checker (`beam-python` job), and the translator's unit tests run on **recorded protos**, so `sbt test` needs no Python. | Fast unit tests everywhere; the end-to-end proof where Python is installed. |
| D10 | **Release later**: the checker jar is `sbt assembly` in `beam-adapter`; the package installs from the repository. | No release steps to maintain before there is a user; PyPI and a jar asset are a follow-up once the shape has settled. |
| D11 | **A check-only mode for CI** (`--invaract_check_only=true`): the pipeline is checked and handed to no runner; `run()` returns `CheckedOnly` (finished, no metrics), the delegate is not created, and the checker is told (`--check_only=true`) so events carry `engineDetails.checkOnly`. | The Java adapter's D22, in the bridge's one place for runner behaviour. It is a runner flag, not a setting forwarded to the checker, because it decides what the runner does after the verdict. |

## Where things are

| What | Where |
|---|---|
| The translator, shared with Java pipelines | `beam-adapter/src/main/scala/.../BeamTranslator.scala` |
| The step catalogues | `StepCatalogue.scala` (`JavaSteps`), `PythonSteps.scala` |
| Reading the portable form | `PortablePipeline.scala` |
| pyarrow schemas | `PyArrowSchemas.scala` |
| The checker program | `PortableCheck.scala` (`PythonConfigSource` spells the settings) |
| What differs from a Java pipeline | `BeamFrontEnd.scala` (`BeamFrontEnd.Python`: the engine name, the step catalogue, the declaration and the advice, kept together) |
| What a refusal advises | `BeamAdvice.scala` (`BeamAdvice.Python`: the settings as Python spells them, the Python guide) |
| The declaration | `beam-adapter/src/main/resources/invaract-capabilities-beam-python.yaml` |
| The Python package | `beam-adapter/python/invaract_beam/` (`runner.py`, `options.py`), tests in `beam-adapter/python/tests/` |
| Recorded pipelines (test fixtures) | `beam-adapter/src/test/resources/portable/*.pb`, made by `beam-adapter/python/tests/record_fixtures.py` |
| Conformance | `BeamPythonConformanceAdapter`/`Spec` (Scala) running `python/tests/conformance_job.py`; baseline `conformance-baseline-beam-python.txt` |
| Attested claims | `BeamPythonAttachSpec`, `BeamPythonAttestedClaimsSpec`, the team's job `python/tests/example_job.py` |

## Maintaining it

- **A new neutral setting** (added to `InvaractConf` and `InvaractConf.All`): `ConfigSourceSpec` fails until it is in `All`;
  then `PortableCheckSpec` fails until it is a Beam option (`InvaractBeamOptions` and `BeamConfigSource`) and a flag of the
  Python package (`SETTINGS` in `python/invaract_beam/options.py`, each with its help text), naming each file. A setting that only an adapter checking
  after the job reads (`failOnMissingOutput`) is left out of `BeamSettings.Names`.
- **Python SDK releases tested**: `beam.python.verified` (2.68.0, 2.71.0, 2.74.0 and 2.77.0). 2.68.0 is the floor: the
  first release whose `WriteToParquet` takes `triggering_frequency`, which the streaming scenarios need.
- **A new Python SDK release**: add it to the `beam-python` job's `beam-python-version` matrix in
  `.github/workflows/test.yml`. Each run there records the test pipelines afresh with its release (`record_fixtures.py`)
  before the tests read them, so a renamed source or sink class, display key or coder shows up as a failing
  `PythonStepsSpec` assertion on that release. Once a run passes, add the release to `beam.python.verified` in
  `supported-beam-versions.properties` (version-matrix-drift-check fails if a listed release is not in the matrix). To move
  the committed recordings to it, re-record them (the script's docstring has the command), run `sbt test` and set
  `beam.python.primary`. Recordings are not byte-for-byte reproducible (the SDK makes up new ids each time), so a re-recording
  always shows as changed files.
- **The checker's own Beam release** is `beam.primary`, like the Java adapter's; it reads the portable form whichever Python
  SDK made it, which is what the matrix above checks.
- **A new IO**: if it is built on a file source or sink, add the class to `PythonSteps.FileSources`/`FileSinks`; otherwise add a
  case to `sourceRead` or `write`, record a fixture for it in `record_fixtures.py`, and assert its translation in
  `PythonStepsSpec`. Until then it fails closed: a read is not located, a write ending the pipeline is refused.
- **A change to the translator** is a change to both adapters: run both conformance suites (`BeamConformanceSpec`, and
  `BeamPythonConformanceSpec` with `INVARACT_TEST_PYTHON` set).
- **Running the Python suites locally**: `INVARACT_TEST_PYTHON=<python with apache-beam[gcp] and pytest> sbt "testOnly *BeamPython*"`;
  without it they are canceled. That interpreter must not have `invaract_beam` installed (the suites put it on `PYTHONPATH`,
  and one shows a job where it is missing); CI builds the wheel to check the packaging instead of installing it. `cd beam-adapter/python && python -m pytest` runs the package's own tests.

## Engine investigation

The survey (docs/BEAM_PYTHON_ADAPTER_SURVEY.md, sections 2, 3 and 10) is the investigation; the probes it lists were made
permanent as the recorded fixtures. What building it added:

- The portable form a `Pipeline.run()` hands the runner has been round-tripped through the proto by Beam itself
  (`test_runner_api`), so a runner sees exactly what the checker reads.
- A coder Java does not know is rehydrated as `UnknownCoderWrapper` rather than failing; the adapter treats it as no coder.
- A protocol buffer map has no order, so a step's inputs and outputs are sorted by tag.
- The Python SDK's root step wraps the whole pipeline; its children are not named under it (Beam names a part
  `<step>/<part>`), which is how it is told from a Java composite with no URN.
- A file stream (`fileio.MatchContinuously`) keeps its pattern in pickled code: it is a read that is not located.

## Capability ledger

| Capability | Declared | Evidence |
|---|---|---|
| `read.batch` | supported | conformance scenarios (`BeamPythonConformanceSpec`); `PythonStepsSpec` |
| `read.streaming` | partial | streaming scenarios pass with the read unlocated (`MatchContinuously`); Pub/Sub located (`PythonStepsSpec`) |
| `write.batch` | supported | conformance scenarios; `PythonStepsSpec` |
| `write.streaming` | supported | streaming scenarios (windowed `WriteToText`/`WriteToParquet`) |
| `write.rowLevelDml` | not-applicable | Beam has none |
| `write.stateChange` | not-applicable | Beam has none |
| `check.inputExistence` | supported | scenarios 15, 16, 22, 23 |
| `check.location` | supported | scenarios 2, 20, 51 |
| `check.outputsWritten` | supported | scenario 21 |
| `check.schema` | partial | scenarios 3, 4, 12-14; dictionaries and untyped fields have none (`PythonStepsSpec`) |
| `check.nestedTypes` | supported | scenarios 28, 29; `PyArrowSchemasSpec` |
| `check.format` | partial | scenarios 24, 25 |
| `check.saveMode` | partial | scenarios 26, 27 (BigQuery) |
| `check.catalogRegistration` | partial | scenarios 10, 11 (BigQuery) |
| `rules.dml` | not-applicable | — |
| `rules.planShape` | unsupported | scenarios 30-37 refused safely |
| `rules.custom` | unsupported | — |
| `analysis.staticDataQuality` | unsupported | scenarios 40-42 refused safely |
| `analysis.roleConsistency` | unsupported | scenarios 43, 44 refused safely |
| `analysis.fingerprint` | unsupported | scenarios 38, 39 skipped |
| `analysis.functionCatalog` | unsupported | — |
| `analysis.sensitivityPropagation` | unsupported | scenario 47 skipped |
| `lineage.columnLevel` | unsupported | scenarios 45, 46 skipped |
| `lineage.boundaryResolution` | not-applicable | scenarios 48, 49 skipped |
| `policy.organizational` | supported | `BeamPythonAttachSpec` |
| `config.zeroCodeInstall` | partial | `BeamPythonAttachSpec` (a real Python job; the limits too) |
| `config.locationRefs` | supported | `BeamPythonAttachSpec` |
| `config.contractRegistry` | supported | `BeamPythonAttachSpec` |
| `reporting.notifications` | supported | every scenario reads the checker's events |
| `reporting.dryRunInference` | partial | `BeamPythonAttachSpec` (a real Python job) |
| `failClosed.unverifiableWrites` | partial | scenario 52; `PythonStepsSpec` |

## Operation surface

| Step | Treated as |
|---|---|
| `ReadFromParquet`, `ReadFromText`, `ReadFromAvro`, `ReadFromTFRecord` | a located read (the pattern's folder) |
| `ReadFromBigQuery` (table, either method) | a located read in the `bigquery` catalog |
| `ReadFromBigQuery` (query) | a read that is not located |
| `ReadFromPubSub` | a located read (topic or subscription) |
| `ReadFromCsv`/`ReadFromJson` (pandas), `fileio.MatchFiles`/`MatchContinuously`, `beam.Impulse`, an unknown source, a Java transform used from Python | a read that is not located |
| a template parameter as a path | a read that is not located; a write that is refused |
| `WriteToParquet`, `WriteToText`, `WriteToAvro`, `WriteToTFRecord` | a located write (pattern prefix) |
| `WriteToBigQuery` (a table) | a located write with its disposition and, for file loads, its schema |
| `WriteToBigQuery` (a computed table) | refused |
| `WriteToPubSub` | a located write |
| `WriteToCsv`/`WriteToJson` (pandas), `fileio.WriteToFiles`, a function with side effects | a step that ends the pipeline: refused unless trusted or reading only what writes report |
| `beam.Create`, `TestStream`, `PeriodicImpulse` | data the job makes |
| `beam.Flatten` | a union (not a combine) |
| `CoGroupByKey`, a side input | a combine the shape rules cannot see into (they are unsupported anyway) |

## Type mapping

| Source | Contract type |
|---|---|
| pyarrow `int8/16/32/64`, `float`, `double`, `bool`, `string`, `binary`, `date32/64`, `time32/64` | `byte/short/integer/long`, `float`, `double`, `boolean`, `string`, `binary`, `date`, `time` |
| pyarrow `timestamp[unit, tz=...]` / `timestamp[unit]` | `timestamp` / `timestamp_ntz` |
| pyarrow `decimal128(p, s)` (p ≤ 38) | `decimal(p,s)` |
| pyarrow `list<item: T>`, `struct<...>`, `map<K, V>`, `dictionary<values=T, ...>` | `array<T>`, `struct<...>`, `map<K,V>`, `T` |
| pyarrow `uint*`, `halffloat`, `duration`, a wider decimal | their own name (equal only to themselves) |
| Beam row schemas, Avro and BigQuery schemas | as the Java adapter (`BeamSchemas`) |

A column is nullable unless pyarrow says `not null`, a Beam row field is not nullable, or a BigQuery field is `REQUIRED`.

## Friction log

1. A dynamically created `NamedTuple` does not survive the round trip `Pipeline.run()` makes through the portable form, so a
   job that types rows built at run time should use `RowTypeConstraint` (the conformance jobs do).
2. The core's remediation text for `UNVERIFIABLE_WRITE` named Spark's `FailClosedCommands` and `SparkPlanAdapter`, which
   misled both Beam adapters. Fixed: the adapter supplies it (`UnverifiableOperation`); `BeamAdvice` holds the Beam advice,
   with the setting spelled and the guide named for the SDK the pipeline was written with.
3. Python's in-process runner is now Prism, which it downloads once (`~/.apache_beam/cache/prism`); a machine without network
   access needs it there before the conformance run.
4. BigQuery has no in-memory stand-in in Python, so the BigQuery scenarios hand the checked pipeline to a runner that runs
   nothing (`conformance_job.CheckOnlyRunner`); the check precedes the hand-over either way, as it does for streams.

## Unverified items

| Item | Cheapest check |
|---|---|
| Dataflow Flex Template Python launcher images have Java 11+ | `java -version` in the launcher image |
| Beam YAML pipelines launched by `apache_beam.yaml.main` honour `--runner` | run one with the runner |
