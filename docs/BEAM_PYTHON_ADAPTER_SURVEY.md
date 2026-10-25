# Beam Python adapter survey

> **Status: survey, followed by the adapter** ([docs/BEAM_PYTHON_ADAPTER.md](BEAM_PYTHON_ADAPTER.md)). The forecasts below
> are as written before the adapter existed; the scenario tables' **Actual** column and the Retrospective at the end compare
> them with what the conformance run showed. This document helps decide whether and how to let Invaract check Apache Beam
> pipelines written in Python. It builds on the Java Beam adapter (`docs/BEAM_ADAPTER.md`), which exists, and on its survey
> (`docs/BEAM_ADAPTER_SURVEY.md`).
> Engine surface surveyed: the Beam Python SDK 2.77.0 on Python 3.11, its portable pipeline description, Beam YAML, and Java
> steps used from Python ("cross-language"), checked on Beam's in-process runner.
> Written: 2026-10-18. Evidence checked: 2026-10-18. Source of each claim: section 10, "Evidence log".

## The short version

**What it would be.** Before a Python Beam job starts, Invaract would take the job's description in Beam's language-neutral
form (the *portable pipeline*, defined in section 2). It would hand that to the checking code that already exists in Java,
compare it with the data contract, and refuse to start the job if it breaks the contract. As on the Java adapter, it checks
the *shape* of a job: what it reads, where it writes, and which columns and types come out. It does not check what the team's
own Python functions do to individual values.

**How much of the suite it is expected to pass.** Of the suite's 53 scenarios: 24 by doing the check, 5 partly, 13 by refusing
safely, and 11 skipped because they do not apply. "Partly" means BigQuery in CI and streaming runs, neither proven yet. Details
in section 7.

**Can it be switched on without changing the job's code?** *Partly*, in the same way as on Java. A platform team adds
`--runner=invaract_beam.InvaractRunner` and the Invaract settings to the job's launch command, and installs one Python package
where the job is submitted. The job's code stays as it is, provided it passes its command line to Beam and does not choose its
own runner. Tested on a real Python pipeline (probe 4). Section 5.

**The biggest risks.**
1. **The checking code runs in Java, so the machine that submits the job needs Java.** A Python job is often launched from a
   Python-only environment, such as a scheduler worker or a Dataflow Flex Template launcher. Whether those have Java is not yet
   checked (UNVERIFIED). The fallback is to ship the checker as a ready-to-run program that needs no Java (D3).
2. **Much of what a Python job states about itself is "display data"**: labels each step publishes for people (`File Pattern`,
   `Table`), not a promised interface. A Beam release could rename them. The Java adapter has the same exposure through class
   names, and the answer is the same: a version matrix in CI, and refusing what can't be read.
3. **Python steps carry their logic as pickled Python** (Python's own way of saving an object, readable only by Python). Java
   cannot read it, so whatever is known only inside a Python object (a grouping's keys, a Parquet read's columns when the job
   did not ask for rows) is invisible unless a small Python-side helper copies it out (D2).

**Decisions I need from you.** (Each is explained in section 4; a one-line answer is enough.)
1. Build it, as a Python package plus the existing Java checker reading the portable pipeline? *Recommend yes.*
2. Translate in Java from the portable pipeline (one translator for Python, YAML and any other SDK), with a small Python
   helper that adds the facts only Python can read? *Recommend yes.*
3. Call the Java checker as a separate program per job submission at first, needing Java on the submitting machine, and move
   to a no-Java build only if that proves a problem? *Recommend yes.*
4. Declare the transformation-shape rules unsupported at first (refused safely), and add group-by from the helper later?
   *Recommend yes.*
5. For the BigQuery scenarios in CI, check the pipeline without running it, since Python Beam has no in-memory BigQuery?
   *Recommend yes, if the kit can be given a "check only" mode; otherwise declare those partial with a note.*
6. Publish the Python package to PyPI with its own release step, versioned with the Java adapter? *Recommend yes.*

## How to read this document

- **What "conforming" means here.** Invaract has a *conformance suite*: 53 small, engine-neutral jobs (*scenarios*) every
  adapter is run against. An *adapter* is the part of Invaract that knows one engine. It declares what it can and cannot do
  (its *declaration*), and each scenario must come out the way that declaration promises. **Refusing safely is conforming.** If
  an adapter says "I cannot check this", the suite expects it to refuse any contract that depends on it, rather than let the
  job through unchecked. So a ❌ below is a limit to understand, not a failing grade.
- **Forecast marks.**
  - ✅ expected to do the check and produce what the suite expects.
  - ⚠️ only partly, or with a workaround, or depends on something not yet verified.
  - ❌ cannot do the check, so the suite expects a safe refusal.
  - ➖ does not apply.
- **Confidence.** **High**: shown by a probe I ran. **Medium**: each part was probed, or the source says so, but the whole has
  not been assembled. **Low**: inferred. Nothing here has been run as an adapter.
- **"Probe".** A throwaway program I wrote and ran against real Python Beam 2.77.0 to see what it reveals. The programs are not
  committed; what they printed is quoted below.

## 1. What Invaract asks of an engine

### The contract, and the check

A *data contract* is a small YAML file saying what a job may read, where it must write, and what the written data must look
like:

```yaml
id: orders_report
version: "1.0.0"
inputs:
  - { name: orders, location: data/in/orders, format: parquet }
outputs:
  - name: report
    location: data/out/report
    format: parquet
    schema:
      fields:
        - { name: id, type: long, required: true }
        - { name: amount, type: long }
```

Invaract checks a job against it **before the job writes anything**. If the job reads something undeclared, writes somewhere
else, or produces the wrong columns, the write is refused with a message saying what was checked, what failed and how to fix
it. On Spark the check runs inside Spark just before each write. On Beam (Java today, Python here) it runs once, just before
the whole pipeline is handed to the engine.

### The engine-neutral model (the "IR")

Every engine describes a job in its own way. Invaract translates that description into one small engine-independent
description of reads, filters, joins, groupings and writes, called the *IR* (intermediate representation). All the checking
code works on the IR, so it is written once for every engine:

```
read data/in/orders  ->  (an opaque step: the team's filter)  ->  write data/out/report (parquet: id long, amount long)
```

The Spark adapter builds the IR from Spark's query plan. The Java Beam adapter builds it from the Java pipeline in memory. A
Python adapter would build it from the portable pipeline, the description every Beam SDK can produce (section 3).

## 2. The engine in plain words

Apache Beam is a way of writing data-processing jobs ("pipelines") once and running them on several engines, called *runners*:
Google Dataflow, Flink, Spark, or Beam's own in-process runner (in Python 2.77 that is "Prism", a small runner written in Go).
A pipeline is a chain of steps (*transforms*: a read, a filter, a grouping, a write) with collections of data flowing between
them.

Beam has an SDK (a library for writing pipelines) per language: Java, Python, Go, and a YAML format that the Python SDK runs.
Python is the most used of these on Dataflow. A Python job looks like this:

```python
with beam.Pipeline(options=PipelineOptions(argv)) as p:
    (p
     | "ReadOrders" >> beam.io.ReadFromParquet("data/in/orders/*.parquet", as_rows=True)
     | "Positive" >> beam.Filter(lambda r: r.amount > 0)
     | "WriteReport" >> beam.io.WriteToParquet("data/out/report/part", schema))
```

When the `with` block ends, Beam asks the chosen runner to run the pipeline. Before a runner submits a Python pipeline, Beam
turns it into the **portable pipeline**: a language-neutral description of every step and collection, written in Protocol
Buffers (a compact, typed data format that any language can read; "proto" for short). That moment, after the pipeline is
built and before anything runs, is where Invaract can stop a job.

**Why the Java adapter does not cover this.** The Java adapter is a Java runner that reads Java objects. A Python job never
reaches it: Python picks a Python runner, and the Python objects are not Java objects.

## 3. How the adapter would get hold of a job

### The portable pipeline (the proto)

- **What it is.** The pipeline written out as data: every step with its name, the kind of step it is, its inputs and outputs,
  a few labels it publishes about itself, and every collection with the encoding its items travel in. It is like a shipping
  manifest for the job: it lists what goes where, but not what is inside each parcel.
- **How we get hold of it.** `pipeline.to_runner_api()` in Python returns it. A wrapper runner gets the pipeline in
  `run_pipeline(pipeline, options)`, after the job has built it and before any step has run (probe 4), so a job can still be
  refused. The proto can be written to bytes and read in Java with Beam's own Java classes (`RunnerApi.Pipeline.parseFrom`,
  probe 7).
- **What it contains.**
  - Every step's unique name.
  - Its kind (a `urn`: `beam:transform:read:v1`, `beam:transform:pardo:v1`, ...).
  - The Python class it was built from, in an annotation (`python_type=apache_beam.io.parquetio.ReadFromParquet`).
  - Its inputs and outputs.
  - Its *display data*: labels such as `File Pattern=...`, `Table=proj:ds.report`, `schema=id: int64 ...`.
  - For each collection, whether it is finite or a stream, and its *coder* (how items are encoded). When items are Beam rows,
    the coder carries the row *schema*: the column names and types.
- **What it leaves out.**
  - Anything inside a step's own Python code. Functions are stored pickled (Python's own way of saving an object, readable only
    by Python).
  - The columns of plain Python dictionaries, which travel pickled too.
  - A grouping's keys (`beam.GroupBy("id")` states no key in the proto).
  - The columns of a Parquet read that did not ask for rows.

  The cost: those checks are either done from what the write states, done by a small Python helper (D2), or refused.
- **A real example.** Captured by probe 1 and probe 2 (`pipeline.to_runner_api()`, Beam 2.77.0), trimmed to the steps the
  job wrote:

```
ReadOrders  urn=beam:transform:generic_composite:v1
    python_type = apache_beam.io.parquetio.ReadFromParquet
    File Pattern = .../data/in/orders/*.parquet
    out coder = beam:coder:row:v1   SCHEMA [('id', INT64, nullable), ('amount', INT64, nullable)]
Positive    urn=beam:transform:pardo:v1
    python_type = apache_beam.transforms.core.ParDo
    Transform Function = <lambda>
WriteReport urn=beam:transform:generic_composite:v1
    python_type = apache_beam.io.parquetio.WriteToParquet
    File Pattern = .../data/out/report/part-%(shard_num)05d-of-%(num_shards)05d.parquet
    schema = id: int64
             amount: int64
```

Reading it line by line:
- **`ReadOrders`** is a Parquet read (its `python_type`) of the folder `data/in/orders` (its `File Pattern`). Its output has
  the columns `id` and `amount`, both 64-bit integers that may be empty (the row schema).
- **`Positive`** is the team's own function (`<lambda>`). Invaract sees that a step is there, not what it does, as with Java.
- **`WriteReport`** writes Parquet files under `data/out/report`, with exactly the columns `id` and `amount`, both 64-bit
  integers (the `schema` label).

Those are the facts the contract above needs.

### The same pipeline, read in Java

- **What it is.** The bytes of the proto above, parsed by Beam's Java classes inside the existing `beam-adapter` test setup.
- **How we get hold of it.** The Python side writes the bytes; the Java side reads them (`RunnerApi.Pipeline.parseFrom`).
  Display data decodes with `RunnerApi.LabelledPayload.parseFrom`, and row schemas with
  `SchemaTranslation.schemaFromProto` (probe 7).
- **What it contains.** Everything above. Probe 7 printed, from Java:
  `ReadOrders urn=beam:transform:generic_composite:v1 dd=[File Pattern=.../data/in/orders/*.parquet, Compression Type=auto]
  ann=[python_type=apache_beam.io.parquetio.ReadFromParquet] out=[{ id: <INT64> amount: <INT64> }]`.
- **What it leaves out.** One surprise: a `beam.Select` with a computed field has fields with no type, and Beam's Java decoder
  rejects such a schema (`Failed to decode Schema`). The translator must read the field names itself and treat their types as
  unknown.
- **A real example.** Above. The annotation Invaract's helper added in Python (`invaract:group_keys=id`) also arrived intact
  (probe 5, read back in probe 7).

### What each IO step states (probe 3, probe 8)

| Python step | Where it reads or writes | Columns | Other |
|---|---|---|---|
| `ReadFromParquet` | `File Pattern` | only with `as_rows=True` (a row schema) | read patterns are checked to exist when the pipeline is built |
| `WriteToParquet` | `File Pattern` (prefix plus shard template) | the `schema` label (pyarrow text, nested types included) | compression |
| `ReadFromText` / `WriteToText` | `File Pattern` (on the inner `Read` / `Write` step) | none (lines of text) | |
| `ReadFromAvro` / `WriteToAvro` | `File Pattern` | write: the Avro schema label; read: none | |
| `ReadFromBigQuery` | `table_reference` or `query` (on the inner `Read`) | none, unless rows are asked for (UNVERIFIED) | `method=DIRECT_READ` |
| `WriteToBigQuery` | `Table=proj:ds.report` | `schema={'fields': [...]}` (inside the write) | `write_disposition=WRITE_TRUNCATE`, `create_disposition` |
| `ReadFromPubSub` / `WriteToPubSub` | `Pubsub Topic` | none (bytes) | the read's collection is marked a stream |
| a classic-template parameter | `RuntimeValueProvider(option: input, ...)` in place of a path | | the same as Java's D21: a read is unlocated, a write is refused |
| a Java step used from Python (SQL, Kafka, JDBC) | the Java step's own display data, under Java class names | as on Java | runs in a separate Java environment (probe 6) |
| Beam YAML | the same Python steps, plus `yaml_type` and `yaml_args` annotations (the step's YAML configuration as JSON, e.g. `"keep": "amount > 0"`) | as above | probe 9 |

## 4. Key design decisions

### D1. Where does the check run?

- **Options.**
  - (a) A Python wrapper runner named by `--runner`, which checks and then hands the pipeline to the real runner.
  - (b) A proxy in front of a "job service" (the server portable runners such as Flink and Spark accept jobs through).
  - (c) Inside each runner.
- **Recommendation.** (a).
- **Why.** It is the same design as the Java adapter. It sees every pipeline whatever the runner, Dataflow included (Python's
  Dataflow runner does not go through a job service, so (b) would miss it). And probe 4 showed it works: the wrapper got the
  whole pipeline before anything ran, refused it with exit status 1, and nothing was written.
- **What it costs.** A job that picks its runner in code bypasses it, exactly as on Java.
- **What would change my mind.** A large share of jobs choosing their runner in code. Then (b) for the portable runners, plus
  a one-line call for the rest.

### D2. Where is the pipeline translated into the IR?

- **Options.**
  - (a) In Java, from the proto alone.
  - (b) In Python, from the live Python objects, sending the IR to Java.
  - (c) In Java from the proto, with a small Python helper that first copies into the proto, as annotations, the few facts
    only Python can read.
- **Recommendation.** (c), starting with an empty helper (pure (a)) and adding facts one at a time.
- **Why.** One translator in Java serves Python, Beam YAML and any later SDK (Go), and reuses the Java adapter's rules: how
  locations become contract locations, what fails closed, which steps only report (D14). The helper is the escape hatch for
  facts that live only in Python objects. Probe 5 read a `GroupBy`'s keys from the live object (`_key_fields`) and added them
  to the proto as `invaract:group_keys=id`, and Java read them back (probe 7).
- **What it costs.** A second "IO catalogue", from Python step types and display labels to reads and writes, next to the Java
  one. The translation walk (the graph, unknown steps, D14, failing closed) should be shared, which means a small refactor of
  `BeamTranslator` to work on steps from either source.
- **What would change my mind.** If most checks needed Python-only facts, (b) would be simpler. Probes 1 to 3 say they don't:
  locations, write schemas, BigQuery tables and dispositions are all in the proto.

### D3. How does Python reach the Java checker?

- **Options.**
  - (a) Run `java -jar invaract-beam-checker.jar` once per submission, passing the proto as a file and reading the verdict.
  - (b) A long-running local service.
  - (c) The checker compiled ahead of time into a program that needs no Java (GraalVM "native image"), shipped inside the
    Python package.
- **Recommendation.** (a) first; (c) if Java on the submitting machine turns out to be a real obstacle.
- **Why.** (a) is the least new machinery. Submission happens once per job, so a JVM start-up of a second or two
  (UNVERIFIED, to be measured) does not matter. Jobs that use Java steps from Python (SQL, Kafka) already need Java on the
  submitting machine for Beam's own expansion service (probe 6 started one).
- **What it costs.** Java 11+ wherever jobs are submitted, and two artefacts to install (the Python package and the jar). The
  package can download the jar the way Beam downloads its expansion service.
- **What would change my mind.** Dataflow Flex Template launchers or common schedulers lacking Java (UNVERIFIED).

### D4. How do Python's names become contract locations?

- **Recommendation.** Reuse the Java adapter's rules:
  - A read pattern is its folder (`data/in/orders/*.parquet` is `data/in/orders`).
  - A write is its prefix's folder, ignoring the shard template (`part-%(shard_num)05d-...`).
  - A BigQuery `proj:ds.table` is `proj/ds/table` in the `bigquery` catalog.
  - A Pub/Sub topic is its path.
- **Why.** One contract then describes the same dataset whichever SDK a job uses.
- **What it costs.** Parsing the Python write's file-pattern template, which Java writes do not have.

### D5. How do Python's types map to the contract's?

- **Recommendation.**
  - Row schemas use Beam's own type names, mapped exactly as on Java (`BeamSchemas`).
  - Write schemas arrive as pyarrow text (`id: int64`, `tags: list<item: string>`); the helper should send them as structured
    data instead of the text being parsed.
  - A field with no type (`beam.Select` with a computed field) is a column whose type is unknown, so a contract that declares
    its type has the write refused, never passed.
- **Why.** Parsing pyarrow's printed form is fragile (it is meant for people), while the helper can read the pyarrow schema
  object directly.
- **What it costs.** A small part of the helper, and a test per pyarrow type.

### D6. Transformation-shape rules (required filter, join or group-by columns; no cross join)

- **Options.**
  - (a) Unsupported, refused safely.
  - (b) Group-by from the helper (`GroupBy` keys); filters and joins stay opaque.
  - (c) Also read Beam YAML's filter expressions.
- **Recommendation.** (a) at first, then (b) and (c).
- **Why.** Python has no column-naming filter or join: `beam.Filter` takes a function, and joins are `CoGroupByKey`. The suite
  judges a capability as a whole, so declaring it "partial" with only group-by would make the filter and join scenarios fail,
  instead of refusing safely as they do when it is declared unsupported. (b) needs the capability split, or a per-rule
  declaration in the kit.
- **What it costs.** Teams that rely on these rules get a clear refusal, not a check, at first.

### D7. What cannot be seen, and the policy

- **Recommendation.** As on Java:
  - A step that ends the pipeline without being a recognised write is refused, unless it only reads what writes report or the
    platform trusts it (D14, `--invaract_trusted_steps`).
  - A read Invaract cannot locate makes a declared input "unverifiable", not "missing".
  - A write whose columns cannot be told is refused when its output declares columns.
  - A function that writes in the middle of a pipeline cannot be seen by any check made before the run.
- **Why.** Failing closed (refusing what cannot be checked) is Invaract's rule everywhere.

### D8. How results are reported

- **Recommendation.** The Java checker publishes the same validation events as every adapter, through the same sinks. The
  engine is named `beam-python`, and the Python SDK version and the delegate runner are passed by the helper. The Python side
  prints the verdict and raises, so the job's submit command fails with Invaract's message.

### D9. How CI proves it without secrets

- **Recommendation.**
  - Python jobs generated from each scenario and run on Beam's in-process runner (Prism), the way the Java adapter's
    `BeamScenarioJobs` builds Java jobs.
  - Recorded protos as fast unit-test fixtures for the Java translator.
  - A Python version matrix like the Java one.
- **The open part.** Python Beam ships no in-memory BigQuery (Java's `FakeBigQueryServices` has no Python counterpart in the
  2.77.0 package). The BigQuery scenarios, save mode and catalog, need either a "check only, do not run" mode in the kit or a
  stand-in. Streaming scenarios need a Python stream that ends by itself on Prism (UNVERIFIED).

### D10. Language, packaging and release

- **Recommendation.**
  - A Python package (`invaract-beam` on PyPI: the runner, the options and the helper) with its own tests (pytest), lint, and
    a release step.
  - The Java checker as a runnable jar (the existing `beam-adapter`, plus a small command-line entry point), published with the
    other modules.
  - Versions kept in step.
- **What it costs.** A new toolchain in the repository's guardrails: Python tests, coverage and a release job. The repository's
  mutation testing is Scala-only, so the Python helper would need its own (`mutmut`) or be kept small enough not to.

## 5. Switching it on without touching the job: external attachability

**Verdict: Partly**, exactly as on Java Beam.

**The mechanism.** Python Beam accepts a runner by dotted module path (`--runner=module.Class`; `apache_beam/runners/runner.py`,
`create_runner`). The wrapper's options are an ordinary `PipelineOptions` subclass, parsed from the same command line. Probe 4
ran a job that knows nothing about Invaract like this:

```
pip install invaract-beam        # where the job is submitted (PREDICTED name)
python job.py --runner=invaract_beam.InvaractRunner \
    --invaract_delegate=DataflowRunner --invaract_contract=gs://contracts/orders_report.yaml \
    --project=... --region=... --temp_location=...
```

The probe's version printed `INVARACT: contract=contracts/report.yaml delegate=DirectRunner ... nothing has run yet`, ran the
job, and wrote its output. With a refusing contract it exited with status 1 and wrote nothing. Without the package installed,
Beam stopped with `ModuleNotFoundError: No module named 'invaract_probe'`, exit status 1, nothing written. A missing install
fails closed.

A Beam YAML pipeline is launched by Beam's own Python entry point (`python -m apache_beam.yaml.main --yaml_pipeline_file=...`),
so the same flags apply (PREDICTED; the proto it produces was probed, the launch was not).

**The settings.** Python options are spelled in snake case:

| Setting | What it does | How it is given on Python Beam | Possible? |
|---|---|---|---|
| `contract` | which contract to enforce (a file, or `registry://id@version`) | `--invaract_contract` | Yes |
| `dryRun` | observe and infer a contract instead of enforcing | `--invaract_dry_run=true` | Yes |
| `notifyConfig` | where validation events are sent | `--invaract_notify_config` | Yes |
| `registryUrl` | where `registry://` contracts are fetched | `--invaract_registry_url` | Yes |
| `locationMap` | maps `ref://` names in a contract to real locations | `--invaract_location_map` | Yes |
| `orgPolicy` | organisation-wide rules layered on the contract | `--invaract_org_policy` | Yes |
| `rejectUndeclaredInputs` / `rejectUndeclaredFields` | stricter checking | `--invaract_reject_undeclared_inputs` / `..._fields` | Yes |
| `computeFingerprint` / `staticDataQuality` / `roleConsistency` | optional analyses | `--invaract_compute_fingerprint` ... | Yes, but each is declared unsupported, so a run with one is refused |
| `trustedSteps` | steps the platform trusts to end the pipeline | `--invaract_trusted_steps` | Yes |

The helper passes them to the Java checker unchanged, so the Java side reads them through the same engine-neutral settings
code (`ConfigSource`) as every adapter.

**How a job can get around it, and what is not possible.**
- A job that sets its runner in code (`PipelineOptions(runner="DataflowRunner")`, or `options.view_as(StandardOptions).runner = ...`)
  or ignores its command line is not checked. This is the same as Java, and stated in the declaration.
- A job submitted from a machine without the package fails to start. That fails closed, but means an install step for the
  platform.
- A job submitted from a machine without Java cannot be checked (D3).

## 6. What it would look like to a user

**The contract** is the one in section 1.

**The job** (what the team wrote; unchanged):

```python
import sys, apache_beam as beam, pyarrow as pa
from apache_beam.options.pipeline_options import PipelineOptions

schema = pa.schema([("id", pa.int64()), ("amount", pa.int64())])
with beam.Pipeline(options=PipelineOptions(sys.argv[1:])) as p:
    (p | "ReadOrders" >> beam.io.ReadFromParquet("data/in/orders/*.parquet", as_rows=True)
       | "Positive" >> beam.Filter(lambda r: r.amount > 0)
       | "WriteReport" >> beam.io.WriteToParquet("data/out/report/part", schema))
```

**Turning it on** (what the platform team does): the command in section 5.

**A passing run.** The job runs as before. One INFO line from Invaract says the pipeline was checked against
`orders_report@1.0.0` and passed, and a PASSED validation event goes to the configured sink.

**A blocked run (PREDICTED).** If the team changed the write to `data/out/report_v2/part`, the submit command would fail
before anything ran, with the message every adapter prints:

```
Invaract: contract violation: 'orders_report@1.0.0' rejected this transformation. Write aborted.

What the contract expects:
  output 'report' at data/out/report: id: long (required), amount: long (optional)
What the plan contains:
  Write(data/out/report_v2, parquet) <- (Positive: a function) <- Read(data/in/orders)
Why it violates the contract (1 violation):
  1. [OUTPUT_LOCATION_MISMATCH] the job writes to data/out/report_v2, but the contract's output 'report' is at data/out/report
How to correct it:
  1. Write to data/out/report, or update the contract if the output has moved.
```

**Dry-run mode (PREDICTED).** With `--invaract_dry_run=true` and no contract, the job runs unchanged and Invaract publishes a
draft contract for each write, plus a job summary that merges them. Each draft is self-checked against its write, as on Java,
and is marked degraded where a column could not be told (a Parquet read without `as_rows=True`).

**Where the events go.** The sink named by `--invaract_notify_config`, as for every adapter. Events name the engine
(`beam-python`), its version, the job name and the delegate runner.

## 7. How much of the suite it is expected to pass

### 7.1 The capability declaration, forecast

| Capability | What it means | Would declare | Why | Confidence |
|---|---|---|---|---|
| `read.batch`, `write.batch` | reads and writes of finite data | supported | Each IO step states its pattern, table or topic in display data (probe 3) | High |
| `read.streaming`, `write.streaming` | the same for streams | supported | A Pub/Sub read is marked a stream in the proto (probe 8) | Medium |
| `write.rowLevelDml`, `write.stateChange`, `rules.dml` | row-level changes, schema-only changes | not-applicable | Beam has neither | High |
| `check.inputExistence`, `check.location`, `check.outputsWritten` | declared inputs read, writes in the right place, every declared output written | supported | The whole pipeline, with every read and write, is in the proto before it runs | High |
| `check.schema` | the written columns and types match | partial | Known from row schemas and write schemas, not from plain dictionaries | Medium |
| `check.nestedTypes` | arrays, structs and maps | supported | Row schemas and pyarrow schemas carry them (probe 8) | Medium |
| `check.format` | the file format | partial | The IO step names it; a text write cannot tell CSV from JSON | Medium |
| `check.saveMode` | append, overwrite, error | partial | Only BigQuery writes state a disposition | Medium |
| `check.catalogRegistration` | the output is a registered table | partial | BigQuery tables only | Medium |
| `rules.planShape` | required filter, join, group-by columns; no cross join | unsupported | Filters and joins are code; group-by keys need the helper (D6) | High |
| `rules.custom` | an organisation's own rule types | unsupported | They judge row-level changes, which Beam has none of | High |
| `analysis.*`, `lineage.columnLevel` | static data-quality proofs, role consistency, fingerprints, function catalog, sensitivity, column lineage | unsupported | They need to see inside functions, which are pickled Python | High |
| `lineage.boundaryResolution` | seeing past a checkpoint or cache | not-applicable | A pipeline has neither | High |
| `policy.organizational`, `config.locationRefs`, `config.contractRegistry`, `reporting.notifications` | organisation policy, `ref://` locations, a contract registry, events | supported | Shared code in Java, configured through the options | Medium |
| `config.zeroCodeInstall` | switched on without changing the job | partial | `--runner` plus an installed package; a job that picks its runner in code escapes (probe 4) | High |
| `reporting.dryRunInference` | draft contracts from real jobs | partial | Drafts are only as complete as the columns the proto states | Medium |
| `failClosed.unverifiableWrites` | refusing what cannot be checked | partial | Unknown terminal steps are refused; a write inside a function cannot be seen | Medium |

**Enforcement point:** `pre-submit-gate`. The whole pipeline is checked once, after it is built and before it is submitted, so
any write can be blocked. Nothing can be stopped once the job is running.

### 7.2 Scenario by scenario

#### Structural checks: strong. Where data is read and written, and which datasets feed which write, are all in the proto.

| # | Scenario | What it means for a user | What the kit expects | Forecast | Why | Confidence | Actual |
|---|---|---|---|---|---|---|---|
| 1 | `conforming-write` | A team's job reads the declared input and writes the declared output with the declared columns. Invaract lets it run and records that the check passed. | does the checks, write allowed | ✅ | Locations, input columns and output columns are all in the proto: the columns written come from the schema the Parquet write is given, which the proto carries as text (probe 1); the read's columns from `ReadFromParquet(as_rows=True)`'s row schema (probe 2). | Medium | ✅ checked |
| 2 | `wrong-output-location` | A job writes its report to a different folder than the contract promises, say a test folder in production. Invaract stops it before anything is written. | does the checks, write blocked: `OUTPUT_LOCATION_MISMATCH` | ✅ | A wrong destination is visible before the job runs: the read's file pattern and the write's destination are in the proto's display data (probe 1, 3). | High | ✅ checked |
| 15 | `declared-input-not-read` | The contract says the job is built from the orders data, but the job never reads it. Invaract reports that the job is not using what the contract says it uses. | does the checks, write blocked: `MISSING_INPUT` | ✅ | Every read's pattern is in the proto, so a declared input nobody reads is seen to be missing. | High | ✅ checked |
| 16 | `undeclared-input-rejected` | With the strict option on, a job reads a dataset the contract never mentions. Invaract stops it, so nobody quietly pulls in an unapproved source. | does the checks, write blocked: `UNDECLARED_INPUT` | ✅ | Every read is visible with its pattern, so a read the contract does not declare is caught. | High | ✅ checked |
| 17 | `derived-from-excuses-an-input-the-output-is-not-built-from` | A contract has two outputs built from different inputs. The job writes both; the write of the first does not read the second one's input, and that is fine: Invaract does not demand it of that write. | does the checks, write allowed | ✅ | The proto states which step feeds which (inputs and outputs per step), so each write's reads are known. | High | ✅ checked |
| 18 | `derived-from-requires-every-source-of-the-output` | The job's write of one output skips the input that output is built from and reads the other output's source instead. Invaract still reports the missing input. | does the checks, write blocked: `MISSING_INPUT` | ✅ | As above: a write that does not read a source it is declared to be built from is caught. | High | ✅ checked |
| 19 | `derived-from-rejects-a-read-of-another-outputs-source` | With the strict option on, a job's write of one output reads an input that belongs to the other output. Invaract stops it as an unapproved source for this output. | does the checks, write blocked: `UNDECLARED_INPUT` | ✅ | As above, per write. | High | ✅ checked |
| 20 | `an-undeclared-second-write-is-blocked` | A job writes its report and its audit as the contract says, then also copies the customers somewhere else. Invaract allows the declared writes and stops the extra one: a job writes only where its contract says. | does the checks, write blocked: `MISSING_INPUT`, `OUTPUT_LOCATION_MISMATCH` | ✅ | Each write is a separate step with its own destination; the second is checked like the first. | Medium | ✅ checked |
| 21 | `declared-output-never-written` | A contract promises two outputs, a report and an audit. The job writes the report, correctly, but never the audit. Invaract refuses the job: an output the contract says the job writes, it must write. | does the checks, write blocked: `MISSING_OUTPUT` | ✅ | The whole pipeline is known before it runs, as on Java Beam. | High | ✅ checked |
| 22 | `unrecognized-step-keeps-the-reads-beneath-it-visible` | A job passes its data through a step Invaract cannot interpret (a custom or unusual operation). The data still came from the declared input, and Invaract still sees that. | does the checks, write allowed | ✅ | A step Invaract does not recognise still lists its inputs in the proto, so the reads beneath it stay visible. | Medium | ✅ checked |
| 23 | `unrecognized-step-does-not-hide-an-undeclared-read` | A job hides an unapproved source behind a step Invaract cannot interpret, hoping it will not be noticed. Invaract still finds it and stops the write. | does the checks, write blocked: `UNDECLARED_INPUT` | ✅ | As above: an undeclared read beneath an unknown step is still seen. | Medium | ✅ checked |
| 52 | `untranslatable-write-fails-closed` | A job does something that changes data but that Invaract cannot understand. Invaract assumes the worst and stops it, rather than letting an unchecked change through. | does the checks, write blocked: `UNVERIFIABLE_WRITE` | ✅ | A step that ends the pipeline and is not a recognised write (a function pushing data out) is visible in the proto and refused, as on Java Beam. | Medium | ✅ checked |
| 53 | `invalid-contract` | The contract itself is broken, for example it contradicts itself. Invaract rejects it up front rather than checking jobs against nonsense. | does the checks, write blocked: `INVALID_CONTRACT` | ✅ | An invalid contract is refused by the shared core before any pipeline is looked at. | High | ✅ checked |

#### Columns and types: good where the proto carries a schema (row schemas, or the schema a write is given), absent for plain Python dictionaries.

| # | Scenario | What it means for a user | What the kit expects | Forecast | Why | Confidence | Actual |
|---|---|---|---|---|---|---|---|
| 3 | `missing-output-field` | The contract promises a customer_name column but the job's output no longer has it, which would break whoever reads the data. Invaract stops the write. | does the checks, write blocked: `MISSING_OUTPUT_FIELD` | ✅ | The write's schema names every column the files will have, so a declared column it lacks is seen (the text form must be parsed). | Medium | ✅ checked |
| 4 | `output-type-mismatch` | The contract says id is a number but the job turns it into text, which would break whoever reads the report. Invaract stops the write. | does the checks, write blocked: `OUTPUT_FIELD_TYPE_MISMATCH` | ✅ | The write's schema gives each column's type (`id: int64`), so a column cast to text shows up as `string`. | Medium | ✅ checked |
| 5 | `extended-type-not-produced` | The contract promises a special type (time, JSON, interval or geography) but the job produces something else, perhaps because its engine has no such type. Invaract treats that as a mismatch and never waves it through. | does the checks, write blocked: `OUTPUT_FIELD_TYPE_MISMATCH` | ✅ | pyarrow has time types but no JSON or geography; a declared type the job cannot produce is a mismatch either way, which is the expected result. Not probed. | Low | ✅ checked |
| 12 | `output-nullability-mismatch` | The contract says a column must always have a value, but the job can produce empty values there. Invaract stops it. | does the checks, write blocked: `OUTPUT_FIELD_NULLABILITY_MISMATCH` | ✅ | Row schemas carry nullability (probe 2), and pyarrow prints `not null` for a required column. The write side is not yet probed. | Medium | ✅ checked |
| 13 | `undeclared-output-column-allowed-by-default` | A job adds an extra column the contract does not list. By default Invaract allows extras, because most teams add columns over time. | does the checks, write allowed | ✅ | An extra column in the write's schema is allowed unless the contract says otherwise; the core decides this. | Medium | ✅ checked |
| 14 | `undeclared-output-column-rejected` | A strict team turns on the no-extra-columns option. A job that adds an unlisted column, perhaps by accident or perhaps personal data, is stopped. | does the checks, write blocked: `UNDECLARED_OUTPUT_COLUMN` | ✅ | The same extra column, with `rejectUndeclaredFields`, is blocked by the core. | Medium | ✅ checked |
| 28 | `nested-type-matches` | A column holding a list or a record (a list of tags, say) has the structure the contract describes. Invaract lets it through. | does the checks, write allowed | ✅ | Nested types are in row schemas and in the write's schema text (`list<item: string>`, `struct<name: string>`, probe 8). | Medium | ✅ checked |
| 29 | `nested-type-mismatch` | The list of tags the job produces holds numbers where the contract says text. Invaract looks inside the structure, not just at the word list, and stops it. | does the checks, write blocked: `OUTPUT_FIELD_TYPE_MISMATCH` | ✅ | As above: a struct where an array is declared is a mismatch. | Medium | ✅ checked |

#### Formats, save modes and catalogs: as on Java Beam, BigQuery is the one write that states a save mode and a catalog table.

| # | Scenario | What it means for a user | What the kit expects | Forecast | Why | Confidence | Actual |
|---|---|---|---|---|---|---|---|
| 10 | `catalog-required-output-unregistered` | The contract says the output must be a registered table other teams can find, but the job writes to a bare folder. Invaract stops it. | does the checks, write blocked: `MISSING_OUTPUT_CATALOG_REGISTRATION` | ✅ | A file write names no catalog table, so a contract that requires one is refused, as on Java Beam. | Medium | ✅ checked |
| 11 | `catalog-required-output-registered` | The job writes to the registered table the contract requires. Invaract lets it through. | does the checks, write allowed | ⚠️ | A BigQuery write names its table (`Table=proj:ds.report`, probe 3); but the kit runs jobs for real, and Python Beam has no in-memory BigQuery like Java's, so CI needs a stand-in (D9). | Medium | ✅ checked |
| 24 | `output-format-matches` | The contract says the output is Parquet and the job writes Parquet. Invaract lets it through. | does the checks, write allowed | ✅ | The format is the IO step itself (`python_type` names `ReadFromParquet`, `WriteToParquet`, probe 7). | Medium | ✅ checked |
| 25 | `output-format-mismatch` | The contract says Parquet but the job writes CSV, which readers cannot parse the same way. Invaract stops it. | does the checks, write blocked: `OUTPUT_FORMAT_MISMATCH` | ✅ | As above: a Parquet write where CSV is declared is a mismatch. | Medium | ✅ checked |
| 26 | `output-save-mode-matches` | The contract says the job must add to the table (append) and it does. Invaract lets it through. | does the checks, write allowed | ⚠️ | A BigQuery write states its write disposition (`WRITE_TRUNCATE`, probe 3, inside the write); the same CI stand-in question as row 11 applies. | Medium | ✅ checked |
| 27 | `output-save-mode-mismatch` | The contract says append, but the job overwrites the table and wipes its history. Invaract stops it. | does the checks, write blocked: `OUTPUT_SAVE_MODE_MISMATCH` | ⚠️ | As row 26. | Medium | ✅ checked |

#### Streaming: the proto marks streams, but a stream the kit can run in-process is not yet proven.

| # | Scenario | What it means for a user | What the kit expects | Forecast | Why | Confidence | Actual |
|---|---|---|---|---|---|---|---|
| 50 | `streaming-write-is-checked-like-a-batch-write` | A continuously running job (a stream) is checked like a one-off job: its source is the declared input and its destination the declared output. | does the checks, write allowed | ⚠️ | A Pub/Sub read is marked unbounded (probe 8), but the kit needs a stream it can run to completion in-process; Python's file-watching reads on the Prism runner are not yet tried. | Low | ✅ checked |
| 51 | `streaming-write-to-the-wrong-location` | A streaming job writes to the wrong destination. Invaract stops it before it starts. | does the checks, write blocked: `OUTPUT_LOCATION_MISMATCH` | ⚠️ | As row 50. | Low | ✅ checked |

#### Transformation-shape rules and analyses: refused safely, as declared.

| # | Scenario | What it means for a user | What the kit expects | Forecast | Why | Confidence | Actual |
|---|---|---|---|---|---|---|---|
| 30 | `required-filter-column-present` | The contract requires every job to filter on a date column so nobody scans the whole history. The job filters on it. Invaract lets it through. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | Declared unsupported, so the shared core refuses the contract with `UNSUPPORTED_CONTRACT_FEATURE` before the job starts: Python filters and joins are code, and a grouping's keys are only in pickled Python (D6). | High | ✅ refused safely |
| 31 | `required-filter-column-missing` | Same rule, but the job never filters on the date column. Invaract stops it. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | Declared unsupported, so the shared core refuses the contract with `UNSUPPORTED_CONTRACT_FEATURE` before the job starts: Python filters and joins are code, and a grouping's keys are only in pickled Python (D6). | High | ✅ refused safely |
| 32 | `forbid-cross-join-allows-a-keyed-join` | The contract bans accidental all-pairs joins, which can explode a table. The job joins on a matching column. Invaract lets it through. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | Declared unsupported, so the shared core refuses the contract with `UNSUPPORTED_CONTRACT_FEATURE` before the job starts: Python filters and joins are code, and a grouping's keys are only in pickled Python (D6). | High | ✅ refused safely |
| 33 | `forbid-cross-join-blocks-a-cartesian-product` | The job joins two tables with no condition, pairing every row with every row. Invaract stops it. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | Declared unsupported, so the shared core refuses the contract with `UNSUPPORTED_CONTRACT_FEATURE` before the job starts: Python filters and joins are code, and a grouping's keys are only in pickled Python (D6). | High | ✅ refused safely |
| 34 | `required-join-columns-present` | The contract requires joins to use customer_id. The job joins on it. Invaract lets it through. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | Declared unsupported, so the shared core refuses the contract with `UNSUPPORTED_CONTRACT_FEATURE` before the job starts: Python filters and joins are code, and a grouping's keys are only in pickled Python (D6). | High | ✅ refused safely |
| 35 | `required-join-columns-missing` | The job joins on some other column, so the result could mix up customers. Invaract stops it. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | Declared unsupported, so the shared core refuses the contract with `UNSUPPORTED_CONTRACT_FEATURE` before the job starts: Python filters and joins are code, and a grouping's keys are only in pickled Python (D6). | High | ✅ refused safely |
| 36 | `required-group-by-present` | The contract requires totals per customer. The job groups by customer. Invaract lets it through. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | Declared unsupported, so the shared core refuses the contract with `UNSUPPORTED_CONTRACT_FEATURE` before the job starts: Python filters and joins are code, and a grouping's keys are only in pickled Python (D6). | High | ✅ refused safely |
| 37 | `required-group-by-wrong-keys` | The job groups by region, then renames the column to customer so the output looks right. Invaract notices the real grouping and stops it. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | Declared unsupported, so the shared core refuses the contract with `UNSUPPORTED_CONTRACT_FEATURE` before the job starts: Python filters and joins are code, and a grouping's keys are only in pickled Python (D6). | High | ✅ refused safely |
| 40 | `static-data-quality-proves-a-constant` | The contract says total must be between 0 and 100 and the job sets it to a fixed 1. Invaract can prove that from the job alone, and says so. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | Declared unsupported (functions are pickled Python code), so the contract is refused safely. | High | ✅ refused safely |
| 41 | `static-data-quality-cannot-prove-a-passthrough` | The contract gives a range for a column that is simply copied from an input. Invaract cannot know the input's values, so it reports not guaranteed, and does not block. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | Declared unsupported (functions are pickled Python code), so the contract is refused safely. | High | ✅ refused safely |
| 42 | `static-data-quality-violation-blocks` | The job sets a column to a fixed value the contract's range rules out, such as -5 where 0 to 100 is allowed. Invaract proves it can never be valid and stops it. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | Declared unsupported (functions are pickled Python code), so the contract is refused safely. | High | ✅ refused safely |
| 43 | `role-source-contributing-conforms` | A dataset the contract calls a source feeds the output, as expected. Invaract finds that consistent. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | Declared unsupported (needs column-level usage), so the contract is refused safely. | High | ✅ refused safely |
| 44 | `role-control-contributing-contradicts` | A dataset the contract calls a control (a lookup or switch that should not supply data) ends up in the output. Invaract stops it. | must refuse safely (`UNSUPPORTED_CONTRACT_FEATURE`) | ❌ | Declared unsupported (needs column-level usage), so the contract is refused safely. | High | ✅ refused safely |

#### Skipped: not applicable, or depends on an analysis declared out of reach.

| # | Scenario | What it means for a user | What the kit expects | Forecast | Why | Confidence | Actual |
|---|---|---|---|---|---|---|---|
| 6 | `row-level-delete-checked-as-write` | A job deletes rows from a table the contract does not mention. Invaract treats the delete as a write to that table and stops it. | skipped: 'write.rowLevelDml' does not apply to this engine: Beam has no row-level MERGE, UPDATE or DELETE. | ➖ | Skipped: Beam has no row-level DML. | High | ➖ skipped |
| 7 | `row-level-delete-own-table-is-not-an-input` | A job deletes rows from its own output table. Invaract does not complain that the table counts as an unapproved input, because a delete has to look at the rows it changes. | skipped: 'write.rowLevelDml' does not apply to this engine: Beam has no row-level MERGE, UPDATE or DELETE. | ➖ | Skipped: Beam has no row-level DML. | High | ➖ skipped |
| 8 | `unconditional-delete-forbidden` | The contract forbids deleting every row at once, and a job runs a delete with no condition. Invaract stops it. | skipped: 'write.rowLevelDml' does not apply to this engine: Beam has no row-level MERGE, UPDATE or DELETE. | ➖ | Skipped: Beam has no row-level DML. | High | ➖ skipped |
| 9 | `filtered-delete-allowed` | Same contract, but the job deletes only the rows matching a condition. Invaract lets it through. | skipped: 'write.rowLevelDml' does not apply to this engine: Beam has no row-level MERGE, UPDATE or DELETE. | ➖ | Skipped: Beam has no row-level DML. | High | ➖ skipped |
| 38 | `fingerprint-flags-nothing-for-a-deterministic-job` | With fingerprinting on, a job that gives the same answer on every run is reported as repeatable. | skipped: 'analysis.fingerprint' is declared unsupported: Pickled functions cannot be fingerprinted meaningfully. | ➖ | Skipped: the analysis it needs is declared unsupported. | High | ➖ skipped |
| 39 | `fingerprint-flags-a-generated-id` | A job adds a random unique id to every row. Invaract reports that column as different on every run, so teams know a re-run will not match. | skipped: 'analysis.fingerprint' is declared unsupported: Pickled functions cannot be fingerprinted meaningfully. | ➖ | Skipped: the analysis it needs is declared unsupported. | High | ➖ skipped |
| 45 | `lineage-passthrough-cast-and-constant` | For each output column Invaract can say where it came from: copied from this input column, converted from that one, or a fixed value that came from nowhere. | skipped: 'lineage.columnLevel' is declared unsupported: Functions are opaque. | ➖ | Skipped: the analysis it needs is declared unsupported. | High | ➖ skipped |
| 46 | `lineage-through-join-and-filter` | A column that came through a join still traces back to the right input, and filtering rows does not add false sources. | skipped: 'lineage.columnLevel' is declared unsupported: Functions are opaque. | ➖ | Skipped: the analysis it needs is declared unsupported. | High | ➖ skipped |
| 47 | `sensitivity-follows-a-cast` | An input column is tagged sensitive, say personal data. Output columns built from it, even converted to another type, carry the tag; unrelated ones do not. | skipped: 'analysis.sensitivityPropagation' is declared unsupported: Needs column-level lineage. | ➖ | Skipped: the analysis it needs is declared unsupported. | High | ➖ skipped |
| 48 | `boundary-filter-is-seen-through` | A job saves intermediate results midway (a checkpoint or cache). A filter applied before that point still counts for the rules. | skipped: 'lineage.boundaryResolution' does not apply to this engine: A pipeline is an immutable graph. | ➖ | Skipped: a pipeline has no checkpoint or cache. | High | ➖ skipped |
| 49 | `boundary-does-not-excuse-a-missing-input` | Saving intermediate results midway does not let a job get away with never reading a declared input. | skipped: 'lineage.boundaryResolution' does not apply to this engine: A pipeline is an immutable graph. | ➖ | Skipped: a pipeline has no checkpoint or cache. | High | ➖ skipped |

### 7.3 Claims no scenario can check

| Capability | What a test must show | How this adapter's test would show it | Forecast |
|---|---|---|---|
| `policy.organizational` | a contract that breaks an organizational policy is rejected when enforcement is set up | a Python job run with `--invaract_org_policy` and a breaking contract fails before it starts | ✅ Medium |
| `config.zeroCodeInstall` | enforcement installed through configuration alone blocks a violating write | probe 4, made permanent: an unchanged job, run with `--runner=invaract_beam.InvaractRunner`, refused | ✅ High |
| `config.locationRefs` | a `ref://` location is resolved through configuration and checked | a job run with `--invaract_location_map` | ✅ Medium |
| `config.contractRegistry` | a `registry://` contract is fetched and enforced | a job run against the stub registry the Java adapter's tests use | ✅ Medium |
| `reporting.dryRunInference` | dry-run lets a real write through and reports an inferred contract | a job run with `--invaract_dry_run=true` | ✅ Medium |
| `write.stateChange` | a schema-only change is checked | not applicable | ➖ |
| `rules.custom` | a custom rule type blocks a write | declared unsupported | ❌ |

### 7.4 Totals

**24 of 53 by doing the check · 13 by refusing safely · 5 partly · 11 skipped.** For a user, the checks that matter most hold
from the first version:
- reads and writes in the declared places;
- every declared output written;
- the written columns and types, for Parquet, Avro and BigQuery writes;
- streams treated like batches.

The transformation-shape rules and the analyses are refused clearly rather than checked. The "partly" rows are about proving
BigQuery and streaming in CI, not about what the adapter can see.

## 8. What it would take

1. **Settle the unverified items** (small). Java on Flex Template launchers; a Python stream that ends on Prism; a
   BigQuery read with rows; JVM start-up time. This unlocks the D3 and D9 choices.
2. **A Java translator from the proto** (medium). Share the walk in `BeamTranslator` between Java steps and proto steps; add a
   Python IO catalogue; add a command-line checker (`invaract-beam-check --pipeline=x.pb --setting=...`). Proven by recorded
   protos as fixtures, covered by the repository's coverage and mutation gates. This is the first useful piece: it already
   checks Beam YAML and any SDK whose proto can be saved.
3. **The Python package** (medium). The runner, the options and the bridge to the checker, with pytest tests and the attach
   test of probe 4 made permanent.
4. **Conformance** (medium). A conformance adapter that writes each scenario as a Python job and runs it on Prism; a baseline
   file; the declaration; the capability matrix row. This is where the forecast is checked.
5. **The helper's facts** (small each): structured write schemas (D5), group-by keys (D6), YAML filter expressions.
6. **Release** (small). The PyPI package and the runnable jar.

## 9. Risks and open questions

| Risk or question | Why it matters | How to settle it |
|---|---|---|
| Java on the submitting machine | without it, nothing can be checked | try a Dataflow Flex Template Python launcher image and the common schedulers; if missing, D3 (c) |
| Display-data labels are not a promised interface | a rename hides a location, which the adapter then refuses | a Python version matrix in CI, as for Java; fail closed on anything unreadable |
| Pickled Python holds some facts | group-by keys, unrequested read columns | the helper (D2), one fact at a time |
| No in-memory BigQuery in Python | the save-mode and catalog scenarios can't run as real jobs | a "check only" mode in the kit, or a stand-in (D9) |
| Untyped fields from `beam.Select` | Java's decoder rejects the schema | read field names directly; unknown types fail closed (probe 7) |
| Jobs that choose their runner in code | they escape the check | stated in the declaration; offer a one-line `invaract_beam.verify(pipeline)` call too |

## 10. Evidence log

### Probes I ran

All on 2026-10-18, Python 3.11.15, `apache-beam[gcp,yaml,dataframe]==2.77.0` in a scratch virtual environment. They are not
committed.

1. **Probe 1, the proto of a Parquet job.** `ReadFromParquet` → `beam.Filter` → `WriteToParquet`, then `p.to_runner_api()`,
   printing every step, its URN, display data, and every collection's coder. Output quoted in section 3. Python Beam first
   failed with `OSError: No files found based on the file pattern ...`: file reads check their pattern when the pipeline is
   built.
2. **Probe 2, schemas and Python's schema transforms.** `ReadFromParquet(as_rows=True)` gave `beam:coder:row:v1` with
   `[('id', INT64, nullable), ('amount', INT64, nullable)]`. `beam.GroupBy("id").aggregate_field(...)` gave a
   `generic_composite` with no display data and pickled output. `beam.Select("id", amount2=lambda r: r.amount)` gave a row
   schema with untyped fields. `beam.Filter` kept the row schema. `CoGroupByKey` gave a `kv` coder.
3. **Probe 3, IO display data.** Text, Avro, BigQuery (table and query reads, a write with schema and dispositions) and
   Pub/Sub. Summarised in the table in section 3. The BigQuery write's disposition and schema are on inner steps
   (`.../TriggerLoadJobsWithoutTempTables/ParDo(TriggerLoadJobs)`).
4. **Probe 4, the wrapper runner.** A probe package with a `PipelineRunner` subclass and a `PipelineOptions` subclass. `python
   job.py --runner=invaract_probe.ProbeRunner --invaract_contract=...` printed the contract and delegate, delegated to the
   DirectRunner (Prism), and wrote the output. A refusing contract gave exit status 1 with nothing written. Without the
   package: `ModuleNotFoundError`, exit status 1, nothing written. (A first attempt wrongly found the package because Python
   puts the script's own folder on its import path; rerun from a separate folder.)
5. **Probe 5, a Python-side helper.** A `PipelineVisitor` found the `GroupBy`'s `_key_fields` (`[('id', ...)]`); an annotation
   `invaract:group_keys=id` written into the proto was kept. The proto already annotates each step with `python_type`.
6. **Probe 6, a Java step from Python.** `SqlTransform("SELECT id, SUM(amount) AS total FROM PCOLLECTION GROUP BY id")`.
   Beam downloaded and started the Java SQL expansion service (the first download, 324 MB, arrived corrupt and was fetched
   again and checked against its SHA-1). The proto shows Java steps (`Group.CombineFieldsByFields`, `ParMultiDo`) in a
   `beam:env:docker:v1` environment, with Java display data and no key fields.
7. **Probe 7, the proto in Java.** A throwaway ScalaTest in `beam-adapter` parsed the saved protos with
   `RunnerApi.Pipeline.parseFrom`, decoded display data and row schemas, and read the `invaract:group_keys` annotation. The
   untyped `Select` schema failed `SchemaTranslation.schemaFromProto` with `Failed to decode Schema due to an error decoding
   Field proto`.
8. **Probe 8, nested schemas, streams, templates.** A `WriteToParquet` with a list and a struct column printed
   `schema=id: int64 / tags: list<item: string> / cust: struct<name: string>`. `ReadFromPubSub`'s collection had
   `is_bounded=UNBOUNDED`. A `ReadFromText` of a value-provider option showed `File Pattern=RuntimeValueProvider(option:
   input, type: str, default_value: None)`.
9. **Probe 9, Beam YAML.** A `ReadFromParquet` → `Filter` (`keep: "amount > 0"`) → `WriteToParquet` chain, expanded with
   `yaml_transform.expand_pipeline`. The proto holds the same Python steps, annotated with `yaml_type`, `yaml_provider` and
   `yaml_args` (the configuration as JSON, filter expression included).

### Sources

- `apache_beam/runners/runner.py`, `create_runner` (lines 58 to 88 in 2.77.0): a runner name with a dot is imported as
  `module.Class`.
- `apache_beam/io/filebasedsource.py`, `_validate` (line 191): file patterns are checked when the pipeline is built.
- Beam's portable pipeline definition (`beam_runner_api.proto`), as shipped in `apache_beam.portability.api` and in the Java
  `beam-model-pipeline` jar (`RunnerApi`, `LabelledPayload`, `SchemaApi`).
- The Java adapter and its survey: `docs/BEAM_ADAPTER.md` (D12 unlocated reads, D14, D18, D21), `docs/BEAM_ADAPTER_SURVEY.md`.

### Unverified

| Claim | The cheapest check |
|---|---|
| Dataflow Flex Template Python launchers and common schedulers have Java 11+ | run `java -version` in the Flex Template Python base image |
| The JVM checker starts in a second or two | time `java -jar` of the assembled checker on a recorded proto |
| A Python stream can run to completion in-process on Prism (for the streaming scenarios) | a `MatchContinuously` read with a stop condition, run on Prism |
| `ReadFromBigQuery(output_type="BEAM_ROW")` gives a row schema when the pipeline is built | a probe with a fake table schema (it may need a live table) |
| A Beam YAML pipeline launched by `apache_beam.yaml.main` honours `--runner` | run it with the probe runner |
| The display labels are the same on Python Beam 2.60 to 2.77 | run probes 1 and 3 on each release |
| A nullable pyarrow write field prints without `not null` and a required one with it | a probe with `pa.field("id", pa.int64(), nullable=False)` |
| `--invaract_trusted_steps` and the other options parse when given before the wrapper's module is imported | a probe passing them with the runner given by short name |

## Glossary

- **Adapter**: the part of Invaract that knows one engine and translates its jobs into the IR.
- **Annotation**: a key/value note attached to a step in the portable pipeline (`python_type=...`).
- **Attach**: switch Invaract on for a job, ideally without changing the job's code.
- **Capability / declaration**: one thing an adapter can or cannot do; the declaration is its full list, with a status for each.
- **Coder**: how a collection's items are encoded; a row coder carries the column names and types.
- **Composite step**: a step made of smaller steps (a Parquet read is a read plus a conversion).
- **Conformance / scenario**: the suite of small engine-neutral jobs; an adapter conforms when each comes out as its declaration says.
- **Cross-language**: a Java step used inside a Python pipeline, run by Beam in a Java environment.
- **Display data**: labels a step publishes about itself, meant for people (`File Pattern`, `Table`).
- **Dry run**: run the job unchanged and infer a draft contract from its writes, instead of enforcing one.
- **Enforcement point**: where the check runs; here `pre-submit-gate`, before the job is handed to the engine.
- **Expansion service**: the Java server Beam starts so a Python pipeline can include Java steps.
- **Fail closed**: refuse what cannot be checked, rather than let it through.
- **Graph / pipeline**: the job as a chain of steps and the collections between them.
- **IR**: Invaract's engine-neutral description of a job's reads, filters, joins, groupings and writes.
- **Lineage**: which input columns each output column comes from.
- **Pickled**: saved with Python's own object format, readable only by Python.
- **Portable pipeline / proto**: the language-neutral description of a pipeline, in Protocol Buffers.
- **Prism**: Beam's in-process runner in Python 2.77, a small Go program.
- **Runner**: the engine a Beam pipeline is handed to (Dataflow, Flink, Spark, Prism).
- **Schema**: the column names and types of a dataset.
- **Sink / source**: where a write lands / where a read comes from.
- **Transform**: one step of a pipeline.
- **Wrapper runner**: a runner that checks the pipeline and then hands it to the real one.
- **`OUTPUT_LOCATION_MISMATCH`**: a write lands somewhere other than the contract's output.
- **`MISSING_OUTPUT_FIELD`** / **`OUTPUT_FIELD_TYPE_MISMATCH`** / **`OUTPUT_FIELD_NULLABILITY_MISMATCH`**: a declared output column is missing, has the wrong type, or the wrong nullability.
- **`UNDECLARED_OUTPUT_COLUMN`**: with `rejectUndeclaredFields`, the output has a column the contract does not declare.
- **`MISSING_INPUT`** / **`UNDECLARED_INPUT`**: a declared input is not read / an undeclared one is.
- **`MISSING_OUTPUT`**: a declared output is never written.
- **`OUTPUT_FORMAT_MISMATCH`** / **`OUTPUT_SAVE_MODE_MISMATCH`**: the write's format or save mode differs from the contract's.
- **`MISSING_OUTPUT_CATALOG_REGISTRATION`**: the contract requires a registered table and the write is to a bare path.
- **`UNVERIFIABLE_WRITE`**: a step changes data in a way Invaract cannot check, so it is refused.
- **`UNSUPPORTED_CONTRACT_FEATURE`**: the contract relies on something this adapter declares it cannot check, so it is refused.
- **`INVALID_CONTRACT`**: the contract itself is malformed.

## Retrospective

The adapter was built the same day (docs/BEAM_PYTHON_ADAPTER.md) and its conformance run, real Python pipelines through the
real package and checker, gave: **42 of 53 scenarios conform (29 by doing the check, 13 by refusing safely), 11 skipped**,
against a forecast of 24 checked, 5 partly, 13 refused, 11 skipped. Every ⚠️ became a ✅, and no ✅ or ❌ changed.

What the forecast got wrong, and why:

- **BigQuery (rows 11, 26, 27) did not need a stand-in.** The check happens before the pipeline is handed to a runner, so the
  conformance job hands a checked BigQuery pipeline to a runner that runs nothing. The survey assumed the kit had to run the
  write; it only has to reach the hand-over.
- **Streaming (rows 50, 51) works, but not the way forecast.** A file stream (`fileio.MatchContinuously`) keeps its pattern in
  pickled code, so the stream's read is not located: the declared input is reported unverifiable rather than read. That is
  why `read.streaming` is declared partial, a limit the survey did not foresee. Pub/Sub reads are located.
- **No Python-side helper was needed (D2).** Every fact the checks use is in the portable form; the helper the survey kept
  as an escape hatch was never written, which leaves the Python package with no logic to maintain.
- **A refusal could hide the reason that mattered.** With shape rules unsupported, a pipeline combining collections with
  `CoGroupByKey` was refused as unverifiable before the core said the rule was unsupported. Both Beam adapters now refuse an
  unsupported contract first (`VerificationPipeline.requireSupported`).
- **Unknown coders do not fail; their order is not fixed.** Java rehydrates a pickled Python coder as `UnknownCoderWrapper`,
  and a proto map has no order, so a step's inputs are sorted by tag. Neither was foreseen; both were found by the first tests.
- **Resolved unverified items.** The JVM checker answers in about 0.9 s. Python's in-process runner (Prism) runs the
  conformance jobs to completion. A required pyarrow column prints `not null`.
