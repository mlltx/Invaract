# Apache Beam adapter survey

> **Status: survey. No adapter code exists.** This document helps decide whether and how to build one.
> Engine surface surveyed: the Beam Java SDK, version 2.77.0, run on Beam's in-process test runner (the `DirectRunner`).
> Written: 2026-10-13. Evidence checked: 2026-10-13. Source of each claim: section 10, "Evidence log".

## The short version

**What it would be.** Before a Beam job starts, Invaract would read the job's description (Beam calls it the
*pipeline*: the list of steps the job will run and how data flows between them), compare it with the data contract,
and refuse to start the job if it breaks the contract. It cannot stop a job that is already running, and it cannot
look inside the code a team writes for its own steps, so it checks the *shape* of a job (what it reads, where it
writes, what columns and types come out) and not what the team's own code does to individual values.

**How much of the suite it is expected to pass.** Of the suite's 51 scenarios: 21 by doing the check, 2 partly
(streaming, not yet proven possible), 17 by refusing safely, and 11 skipped because they do not apply or are
declared out of reach. Details in section 7.

**Can it be switched on without changing the job's code?** *Partly.* A platform team adds one flag
(`--runner=...`) and one jar to the job's launch command; the job's source stays as it is, provided the job passes its
command line to Beam the usual way and does not pick its own runner in code. Both halves were tested on a real Beam
pipeline. Section 5.

**The biggest risks.**
1. A job that chooses its runner in its own code, or ignores its command line, slips past the check. Measured, not
   guessed: a probe job that did this ran unchecked.
2. A Beam job's own functions are opaque. The adapter will see that a job filters, joins or groups, but not *on
   which column*, so the "plan-shape" rules (required filter, required join keys, required group-by) cannot be
   checked and would be refused as unsupported. If those rules matter to the first users, the adapter's value drops.

**Decisions I need from you.** (Each is explained in section 4; a one-line answer is enough.)
1. Build it at all, scoped to the Java SDK, file formats (Parquet, Avro, text) and the in-process runner first?
   *Recommend yes.*
2. Accept "check before submit" (cannot block mid-run, cannot see inside functions)? *Recommend yes.*
3. Accept "partly" attachable, and also offer an explicit one-line call a job author can add? *Recommend both.*
4. A text file write cannot say whether it is CSV or JSON: let a text write satisfy a declared `csv`/`json`/`text`?
   *Recommend yes, disclosed as a limit.*
5. A collection with no visible schema (plain Java objects): refuse the write rather than pass it? *Recommend refuse.*
6. A pipeline step that writes by itself and is not a known writer: refuse as unverifiable, accepting some false
   alarms? *Recommend refuse.*
7. Scala for the adapter, with one small Java file only if Beam cannot load the Scala runner class? *Recommend yes.*

## How to read this document

- **What "conforming" means here.** Invaract has a *conformance suite*: 51 small, engine-neutral jobs ("scenarios")
  every adapter is run against. An adapter declares what it can and cannot do (its *declaration*), and each scenario
  must come out the way that declaration promises. **Refusing safely is conforming:** if an adapter says "I cannot
  check this", the suite expects it to refuse any contract that depends on it, rather than let the job through
  unchecked. So a ❌ below is a limit to understand, not a failing grade.
- **Forecast marks.** ✅ expected to do the check and produce what the suite expects. ⚠️ only partly, or with a
  workaround, or depends on something not yet verified. ❌ cannot do the check, so the suite expects a safe refusal or
  a skip. ➖ does not apply to this engine.
- **Confidence.** **High**: shown by a probe I ran. **Medium**: stated by the engine's own source or documentation,
  or each part was probed but the whole has not been assembled. **Low**: inferred. A forecast is a forecast: nothing
  here has been run as an adapter.
- **Words.** Each technical term is explained where it first appears and again in the glossary at the end.
- **"Probe".** A throwaway program I wrote and ran against real Beam to see what it reveals. The programs are not
  committed; what they printed is quoted below.

## 1. What Invaract asks of an engine

### The contract, and the check

A *data contract* is a small YAML file that says what a dataset must look like and what a job may do with it:
where it may read, where it must write, which columns come out and of what types, and a few rules. For example:

```yaml
id: orders_report
version: "1.0.0"
inputs:
  - name: orders
    location: in/orders
    format: parquet
    schema:
      fields:
        - { name: id, type: long, required: true }
        - { name: amount, type: long }
outputs:
  - name: report
    location: out/report
    format: parquet
    schema:
      fields:
        - { name: id, type: long, required: true }
        - { name: amount, type: long }
```

Invaract's job is to look at a data job *before it writes anything*, and compare what the job is about to do with the
contract. If they disagree (it writes to the wrong place, a column has the wrong type, a declared input is never
read) Invaract stops the write and prints why. Each kind of disagreement has a name, called a *violation type*
(`OUTPUT_LOCATION_MISMATCH`, `MISSING_INPUT`, and so on; the glossary lists those used below).

### The engine-neutral model (the "IR")

Every engine describes a job in its own vocabulary. To avoid writing every check once per engine, Invaract translates
each engine's description into one small engine-independent description of reads, filters, joins, groupings and
writes. This is the *IR* ("intermediate representation"). All the checking code works on the IR, and never sees the
engine. A three-step job as the IR holds it:

```
read orders  ->  filter amount > 0  ->  write report
```

For Spark, the translation takes Spark's own *query plan* (a tree Spark builds describing what a query will do,
available before it runs) and converts each node. An *adapter* is the piece of code that does this translation
for one engine and installs the check at the right moment. The Beam adapter's central job is therefore: get a
description of the Beam job, and turn it into this IR.

### Capabilities, declarations and scenarios

Invaract names 30 *capabilities*: things an adapter might be able to do ("recognise a batch read", "check column
types", "check required-filter rules"). Each adapter publishes a *declaration*: for every capability, `supported`,
`partial`, `unsupported` or `not-applicable`, with a note. The conformance suite then runs 51 *scenarios* and
checks that the adapter behaves as its own declaration says. Section 7 forecasts both.

## 2. The engine in plain words

Apache Beam is a toolkit for writing data jobs once and running them on different engines (Google Dataflow, Flink,
Spark, or Beam's own in-process runner for testing). A job is written in Java (also Python and Go; only Java is
surveyed here) as a *pipeline*:

- A **PCollection** is a named bundle of data flowing between steps (a "parallel collection": think of it as a
  table that may be spread over many machines). It is either *bounded* (finite, a batch) or *unbounded* (a stream).
  Each has a *coder* (how its items are turned into bytes), and sometimes a *schema* (column names and types).
- A **transform** is a step that takes collections and produces collections: read a file, filter, join, group,
  write. Transforms nest: a *composite* transform (for example "Parquet read") is made of smaller ones, down to
  *primitive* ones the engine actually executes.
- A **source** reads from outside (a file, a table) and a **sink** writes to outside.
- A **runner** is the piece that actually executes a pipeline on an engine. The job picks it with `--runner=...`.
  The `DirectRunner` runs the pipeline inside the same Java process, which is what makes it usable in tests.
- The team's own logic lives in **functions** (Beam calls them `DoFn`s, or lambdas passed to `MapElements` and
  `Filter`). To Beam these are opaque compiled Java code.

The job is *constructed* first (each `.apply(...)` call adds a step to an in-memory graph, nothing runs), then
`pipeline.run()` hands that finished graph to the runner, which executes it. The moment between "constructed" and
"running" is where Invaract can look and stop things.

```
 Parquet read  ->  Filter (team's function)  ->  Parquet write
   (source)             (opaque code)                (sink)
        \_______ all of this exists as a graph before any data moves _______/
```

## 3. How the adapter would get hold of a job

Everything below was observed with Beam 2.77.0 on the DirectRunner, with Java 21, on 2026-10-13. The
"real example" in each block is verbatim program output, shortened only where marked `...`.

### The pipeline graph

- **What it is.** The list of steps the job will run, and which collection each step reads and writes: like a recipe
  that exists before anyone starts cooking.
- **How we get hold of it.** `Pipeline.traverseTopologically(visitor)`, a public Beam call that walks every step in
  an order where inputs come before the steps that use them. It can be called as soon as the job is constructed,
  *before* anything runs, from the code of a wrapper runner (see "The hook" below).
- **What it contains.** For every step: its full name, its Java class (for example `AutoValue_ParquetIO_Read`), the
  collections it consumes and produces, and each collection's boundedness and schema when it has one. Composite
  steps contain their children, so each file read is one step with a handful of internals beneath it.
- **What it leaves out.** What a team's function does. A `Filter` appears as a filter, but its test is a compiled
  predicate (`display predicate = Filter.predicate` in the probe below is all it says). Join keys and group-by
  fields are held in non-public state (Beam's own source; not shown by any public call I found).
- **A real example.** A Parquet read followed by a Parquet write, captured by my graph probe (`IoProbe`), top
  level only:

```
[COMPOSITE] ReadParquet   <AutoValue_ParquetIO_Read>
    out: ReadParquet/ParquetIO.ReadFiles/ParDo(SplitRead)/ParMultiDo(SplitRead).output BOUNDED schema=none
    display inferBeamSchema = false  (BOOLEAN)
    display projectionSchema = null  (STRING)
    display schema = {"type":"record","name":"Orders","fields":[{"name":"id","type":"long"},{"name":"amount","type":["null","long"],"default":null}]}  (STRING)
    display filePattern = in/orders/*.parquet  (STRING)
    display avroDataModel = null  (STRING)
...
[COMPOSITE] WriteParquet   <AutoValue_FileIO_Write>
    in : ReadParquet/ParquetIO.ReadFiles/ParDo(SplitRead)/ParMultiDo(SplitRead).output BOUNDED schema=none
    out: WriteParquet/WriteFiles/FinalizeTempFileBundles/Reshuffle.ViaRandomKey/Values/Values/Map/ParMultiDo(Anonymous).output BOUNDED schema=none
```

Reading it: the first block is the step named `ReadParquet`. `filePattern` is where the data comes from
(`in/orders/*.parquet`, which the contract spells as the folder `in/orders`). `schema` is the Avro schema the job
told the reader to expect: `id` is a required `long`, `amount` a nullable `long` (`["null","long"]` is Avro's
way of saying "may be null"). The second block is the write. Notice it states **no location and no schema** in its
own display data; the next two blocks show where those come from.

### A step's display data

- **What it is.** A short label sheet each Beam step can fill in about itself, meant for monitoring screens.
- **How we get hold of it.** `DisplayData.from(node.getTransform())`, public, available at construction time.
- **What it contains.** Whatever the step's author chose to publish. `TextIO` publishes its file prefix and pattern,
  `ParquetIO.read` its file pattern and Avro schema, `BigQueryIO` its table, write disposition and schema.
- **What it leaves out.** Anything the author did not publish. `FileIO.write` (which the Parquet sink runs on)
  publishes nothing but the name of its sink class.
- **A real example.** Captured by the opaque-filter probe, the text write at the end of a job:

```
[COMPOSITE] Write   <Write>
    in : Format/Map/ParMultiDo(Anonymous).output BOUNDED schema=none
    display fileSuffix =   (STRING)
    display writableByteChannelFactory = UNCOMPRESSED  (STRING)
    display shardNameTemplate = -SSSSS-of-NNNNN  (STRING)
    display filenamePattern = /tmp/.../beam-probe/out/opaque-SSSSS-of-NNNNN  (STRING)
    display filePrefix = /tmp/.../beam-probe/out/opaque  (STRING)
```

Reading it: `filePrefix` is where the text file will be written. Beam already turned the relative path I gave
(`out/opaque`) into an absolute one, so the adapter must compare locations after making both sides absolute. A
*read* by contrast states its pattern as given. This asymmetry is why the location rule is in decision D3.

### The write's sink

- **What it is.** The object inside a file write that knows how to write the files (and where).
- **How we get hold of it.** `((WriteFiles) node.getTransform()).getSink().getTempDirectoryProvider().get()`, all
  public calls, at construction time. For a `FileIO` write the temporary folder defaults to the output folder.
- **What it contains.** The destination folder.
- **What it leaves out.** If the job sets a *separate* temporary folder (`withTempDirectory`), that folder is what
  this call returns, and it is not the destination. The adapter cannot tell the two apart from this call alone
  (UNVERIFIED how to detect it; cheapest check in section 10).
- **A real example.** Captured by `SinkProbe`:

```
WriteParquet/WriteFiles  sink=ViaFileBasedSink  tempDirectory=/tmp/.../beam-probe/out/report/
WriteText/WriteFiles  sink=TextSink  tempDirectory=/tmp/.../beam-probe/out/
```

Reading it: the Parquet write's destination is `out/report/`. The text write's temporary folder is `out/`, the
*parent* of its prefix `out/text`, so for text writes the adapter reads the prefix from display data instead.

### A collection's coder and schema

- **What it is.** The description of one collection's items: how to encode them, and for structured data, the
  columns. It is how the types of a Parquet write's output are known even though the write step states none.
- **How we get hold of it.** `PCollection.getCoder()` and `PCollection.getSchema()`, public, at construction time. A
  collection of Avro records carries its Avro schema in its coder; a collection of Beam rows carries a Beam schema.
- **What it contains.** Column names, types and nullability.
- **What it leaves out.** A collection of plain Java objects (`SerializableCoder`) has no schema at all. A collection
  produced by a lambda must be given an explicit coder or schema by the job, or Beam refuses to build the pipeline,
  so that case is at least a clear signal.
- **A real example.** Captured by `CoderProbe`, on the output of the Parquet read above:

```
coder class      : org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder
coder's schema   : {"type":"record","name":"Orders","fields":[{"name":"id","type":"long"},{"name":"amount","type":["null","long"],"default":null}]}
Beam schema      : none (inferBeamSchema was false)
Beam row schema from the Avro schema (library conversion): {
  id: <INT64 NOT NULL>
  amount: <INT64>
}
```

Reading it: the collection's coder carries the full Avro schema, and Beam's own library converts it to
`id: INT64 NOT NULL`, `amount: INT64` (nullable), which map directly to the contract's `long`, required and not.
Note this is the schema *the job states it expects*, not the schema of the files on disk. Beam never opens the files
before the job runs, so a mismatch between a job's declared read schema and the real files is not something this
check can see (it would surface at run time as a read error).

### Bounded and unbounded sources

- **What it is.** Whether a collection is finite (a batch) or never-ending (a stream).
- **How we get hold of it.** `PCollection.isBounded()`, public, at construction time.
- **What it contains.** `BOUNDED` or `UNBOUNDED`.
- **What it leaves out.** Nothing relevant, but see section 7.2: whether the in-process runner can run a stream
  *to completion* for a test is unverified.
- **A real example.** Captured by the probe that reads text files and a counting generator:

```
[COMPOSITE] BatchFiles   <AutoValue_TextIO_Read>
    out: BatchFiles/Read/ParDo(BoundedSourceAsSDFWrapper)/ParMultiDo(BoundedSourceAsSDFWrapper).output BOUNDED schema=none
    display filePattern = in/events/*.txt  (STRING)
...
[COMPOSITE] Counter   <AutoValue_GenerateSequence>
    out: Counter/Read(UnboundedCountingSource)/ParDo(StripIds)/ParMultiDo(StripIds).output UNBOUNDED schema=none
```

### Joins and groupings

- **What it is.** How Beam spells "combine two collections by a key" and "aggregate by a key".
- **How we get hold of it.** The same graph walk. A join appears as one composite step with two inputs; a grouping as
  one composite step with a key/value output.
- **What it contains.** That a join or grouping happened, and the *output* columns (`lhs`, `rhs`; `key`, `value`).
- **What it leaves out.** The key fields. They are held in non-public state, so a rule such as "must group by
  `customer_id`" cannot be checked from public calls. Supporting it would need reflection into Beam's internals,
  which can break on any Beam upgrade, so the draft declares these rules unsupported (section 7).
- **A real example.** Top-level steps of the join-and-group probe:

```
[COMPOSITE] JoinCustomers   <Impl>
    in : Orders/MapElements/Map/ParMultiDo(Anonymous).output BOUNDED schema={ id: <INT64 NOT NULL> amount: <INT64> label: <STRING> }
    in : Customers/MapElements/Map/ParMultiDo(Anonymous).output BOUNDED schema={ id: <INT64 NOT NULL> segment: <STRING NOT NULL> }
    out: ... schema={ lhs: <ROW NOT NULL> {...} rhs: <ROW NOT NULL> {...} }
[COMPOSITE] GroupById   <AutoValue_Group_CombineFieldsByFields>
```

(Shortened: the probe prints each schema over several lines.)

### A step Beam cannot see into

- **What it is.** A function that does something Beam does not model, such as calling a web service or inserting into a
  database.
- **How we get hold of it.** It shows up in the same graph walk, as a step with no declared destination.
- **What it contains.** The step's name and the Java class of the function.
- **What it leaves out.** Everything it does.
- **A real example.** A function reading a BigQuery table and pushing each row somewhere unknown:

```
[COMPOSITE] PushToSomeService   <SingleOutput>
    in : ReadBq2/PassThroughThenCleanup/ParMultiDo(Identity).out0 BOUNDED schema=none
    out: PushToSomeService/ParMultiDo(Anonymous).output BOUNDED schema=none
    display fn = probe.IoProbe$1  (JAVA_CLASS)
```

Reading it: the step consumes the table and produces a collection nobody uses. The only hint is the class name of
the function. The adapter would treat "a last step that is not a recognised writer" as an unverifiable write and
refuse it (decision D6). A function that writes *in the middle* of a pipeline and passes data on is invisible; that
is a limit of every pre-run check on Beam (section 9).

### BigQueryIO steps

- **What it is.** Beam's built-in BigQuery reader and writer.
- **How we get hold of it.** The same graph walk. The pipeline can be *constructed* without contacting Google (my
  probe built one with no network access to Google), so a check can run with no credentials.
- **What it contains.** The table, the schema given to the writer, the write disposition (append, truncate, fail if
  not empty) and create disposition.
- **What it leaves out.** Whether the table exists and what its real schema is.
- **A real example.**

```
[COMPOSITE] ReadBq   <AutoValue_BigQueryIO_TypedRead>
    display table = proj:ds.orders  (STRING)
...
[COMPOSITE] WriteBq   <AutoValue_BigQueryIO_Write>
    display schema = {"fields":[{"mode":"REQUIRED","name":"id","type":"INTEGER"}]}  (STRING)
    display schemaUpdateOptions = []  (STRING)
    display writeDisposition = WRITE_TRUNCATE  (STRING)
    display table = {"datasetId":"ds","projectId":"proj","tableId":"report"}  (STRING)
    display createDisposition = CREATE_NEVER  (STRING)
```

This is why `check.saveMode` and `check.catalogRegistration` are marked *could be supported later* for BigQueryIO
only (they have no meaning for file writes). The first version does not include BigQueryIO.

### The hook: a wrapper runner

- **What it is.** A runner that does not execute anything itself: it receives the finished pipeline, looks at it,
  and then either refuses or hands it to the real runner.
- **How we get hold of it.** The job's command line names it: `--runner=<our class>`. Beam finds the class and calls
  its static `fromOptions(options)` method; later `pipeline.run()` calls the class's `run(pipeline)` method, still
  before any data moves. Beam options such as `--invaractContract=...` are declared in an interface and registered
  through Java's `ServiceLoader` (a standard "find all plugins on the classpath" mechanism), so the job needs no code
  to know about them.
- **What it contains.** The full pipeline and the options (which include the job name).
- **What it leaves out.** A running pipeline: once `run` hands over, Invaract cannot intervene.
- **A real example.** Three runs of one unchanged job, captured by the wrapper probe. **Pass** (`contract=pass`):

```
>>> ProbeRunner.run called BEFORE anything executes. contract=pass delegate=DirectRunner jobName=job-root-1013065800-1d5e447d
[COMPOSITE] Create.Values   <Values>
...
>>> handing over to DirectRunner
>>> job finished
```

**Block** (`contract=block`), the wrapper threw and nothing was submitted:

```
>>> ProbeRunner.run called BEFORE anything executes. contract=block delegate=DirectRunner jobName=job-root-1013065811-53b488a0
[error] java.lang.IllegalStateException: BLOCKED by the wrapper: the pipeline was never submitted
```

**Bypass**, a job that sets its own runner in code after reading the command line, with the same flags:

```
>>> job finished (the wrapper was bypassed if no '>>> ProbeRunner' line appeared above)
```

Reading it: no `ProbeRunner` line at all, so the wrapper never saw that job. This is the central limit on
attachability (section 5).

## 4. Key design decisions

### D1. Where does the check sit?

- **Options.** (A) Wrapper runner: gate before submit. (B) Walk the pipeline from a hook in the job's own code. (C)
  Wrap each runner separately. (D) Watch execution and report only.
- **Recommendation.** A, and offer B as an explicit one-line call (`InvaractBeam.verify(pipeline)`).
- **Why.** A is the only place where nothing has started, so the check can truly block, and it can be attached from
  the command line. B covers jobs that choose their runner in code. C repeats work per runner; D cannot block.
- **What it costs.** Declared enforcement point is `pre-submit-gate`, not `in-engine-blocking`: Invaract can refuse
  to start a job but not stop one mid-run.
- **What would change my mind.** Evidence that real jobs mostly choose their runner in code (then B becomes the main
  route and A the convenience).

### D2. How does the adapter get hold of the job and turn it into the IR?

- **Options.** (A) Public graph walk plus display data, coder and sink calls (section 3). (B) Also read Beam's
  private fields by reflection. (C) Convert the pipeline to Beam's portable (language-neutral) protobuf form and read
  that.
- **Recommendation.** A for the first version; revisit C later.
- **Why.** A uses only public calls that survive upgrades. B would make join keys and group-by fields readable but
  breaks on any Beam upgrade. C (`PipelineTranslation.toProto`) is public and standard, but I have not probed what it
  exposes (UNVERIFIED), so it is a candidate for closing the plan-shape gap rather than a reason to delay.
- **What it costs.** No plan-shape rules, no column lineage, no value analyses in version 1 (section 7).
- **What would change my mind.** A probe showing the portable form carries join and group fields.

### D3. How do Beam names become the locations a contract uses?

- **Options.** (A) Compare absolute paths after normalising both sides. (B) Compare exactly as written.
- **Recommendation.** A, plus the rule that a read pattern such as `in/orders/*.parquet` matches the contract folder
  `in/orders` when the pattern's folder is the contract's location.
- **Why.** The graph probe showed reads state the pattern as given while writes state absolute paths. Invaract's
  location matching is pluggable per adapter (`LocationMatching`), so this is adapter-side code, not a core change.
- **What it costs.** A read of `in/orders/2024-*.parquet` would match a contract for `in/orders`; precise enough to
  catch a wrong folder, not a wrong file. A contract naming a single file location also matches only that file.
- **What would change my mind.** Real users writing contracts per file rather than per folder.

### D4. How do Beam and Avro types map to the contract's types?

- **Options.** (A) Map from the collection's coder (Avro) or Beam row schema. (B) Require the job to publish types some
  other way.
- **Recommendation.** A, using Beam's own `AvroUtils.toBeamSchema` and a table from `docs/ENGINE_RESEARCH_BIGQUERY_BEAM.md`.
- **Why.** Shown to work (section 3): `INT64 NOT NULL` and `INT64` come out correctly. Arrays and maps are in the same
  library conversion.
- **What it costs.** Beam has narrower integers (`INT16`, `INT32`) than BigQuery; the research doc covers the table.
  Types Avro cannot express (interval, json, geography, time) are reported as a type mismatch, which is what scenario
  5 wants.
- **What would change my mind.** A real job using Beam row schemas with logical types the library does not convert.

### D5. How are writes described, and what are the defaults?

- **Options.** (A) The format is the writing transform (`ParquetIO` is parquet, `AvroIO` is avro, `TextIO` is text).
  (B) Also infer from the file suffix.
- **Recommendation.** A. For text, a write satisfies a declared `csv`, `json` or `text` (see the decisions list).
- **Why.** The transform is the only reliable signal. File-based Beam writes have no append/overwrite choice at
  all, so `check.saveMode` is declared unsupported for them: a contract that declares a save mode is refused safely.
- **What it costs.** A text write against a `csv` contract is passed even if the lines are not CSV.
- **What would change my mind.** A text write transform that publishes its delimiter or format.

### D6. What cannot be seen, and what happens then?

- **Options.** (A) Refuse a last step that is not a recognised writer (`UNVERIFIABLE_WRITE`). (B) Allow it with a warning.
  (C) Let the contract list steps to allow.
- **Recommendation.** A for version 1.
- **Why.** Invaract's rule is to fail closed: if it cannot tell, the answer is no. My probe showed such a step is
  recognisable as a final step that no recognised writer owns.
- **What it costs.** A pipeline that ends in a metrics-only or logging step is refused until an allow-list exists.
- **What would change my mind.** Real jobs with legitimate terminal steps, in numbers.

### D7. How are results reported?

- **Options.** Same as Spark: a validation event per check and a write event per write, to the configured sinks.
- **Recommendation.** Reuse `verification-core`'s sinks and events; name the engine `beam` and the job from
  `options.getJobName()` (the probe printed `job-root-1013065800-1d5e447d`).
- **Why.** The events already carry engine and job. No new format.
- **What it costs.** On Beam, the "write" event is published at the gate, before the write happens, so a row count is
  not available (Spark's is). A field left empty, not a wrong value.
- **What would change my mind.** A consumer that needs row counts: they would have to come from Beam metrics after run.

### D8. Language and packaging

- **Options.** (A) Scala, in a new module `beam-adapter`, package `com.invaract.beamadapter`. (B) Java.
- **Recommendation.** A; add one small Java file only if Beam cannot load the Scala runner class.
- **Why.** Same language as the rest of the engine and the SPI. Beam requires a static `fromOptions` on the runner
  class; a Scala companion object normally generates one (UNVERIFIED for Beam's reflective lookup; cheapest check:
  a ten-line class and the wrapper probe).
- **What it costs.** A Hadoop and Parquet dependency set the probe needed (`hadoop-client` and
  `beam-sdks-java-io-parquet`), which is heavy; keep them optional so a text-only user is not forced to take them.
- **What would change my mind.** The static-forwarder check failing in an awkward way.

### D9. How does CI prove it without secrets?

- **Options.** (A) The `DirectRunner` with local files and the kit's scenarios. (B) A BigQuery emulator. (C) A real
  project on a schedule.
- **Recommendation.** A for the file formats; BigQueryIO graphs are constructible without credentials (probed), so
  the *check* is testable the same way, while "the write really worked" would need B or C and is out of scope.
- **Why.** Real engine in process, the same level of evidence Spark has with `local[*]`.
- **What it costs.** Nothing about real Dataflow or Flink is proven.
- **What would change my mind.** A user whose only runner is Dataflow.

## 5. Switching it on without touching the job: external attachability

**Verdict: Partly.** A platform team can turn it on, tested end to end on a probe job, if the job passes its command
line to Beam and does not set its own runner. Otherwise they cannot.

**The mechanism.** A runner class named on the command line (`--runner`) wraps the real runner. The Invaract settings
are Beam options, declared in an interface and registered through Java's `ServiceLoader`, so Beam accepts them as if
they were its own. The jar is added to the job's classpath. The job's source is untouched. Example (the shape the
probe used; class and flag names are PREDICTED for the real adapter):

```
java -cp job.jar:invaract-beam-adapter.jar com.example.Job \
  --runner=com.invaract.beamadapter.InvaractRunner \
  --invaractContract=contracts/orders_report.yaml \
  --invaractDelegate=DirectRunner
```

**The settings.** Beam option names are the setting name with `invaract` in front and the first letter capitalised
(`getInvaractContract` becomes `--invaractContract`; verified by the probe reading `contract=pass`).

| Setting | What it does | How it is given on this engine | Possible? |
|---|---|---|---|
| `contract` | which contract to enforce (a file, or `registry://id@version`) | `--invaractContract=...` | Yes |
| `dryRun` | observe and infer a contract instead of enforcing | `--invaractDryRun=true` | Yes |
| `notifyConfig` | where validation events are sent | `--invaractNotifyConfig=<properties file>` | Yes |
| `registryUrl` | where `registry://` contracts are fetched | `--invaractRegistryUrl=...` | Yes |
| `locationMap` | maps `ref://` names in a contract to real locations | `--invaractLocationMap=<file>` | Yes |
| `orgPolicy` | organisation-wide rules layered on the contract | `--invaractOrgPolicy=<file>` | Yes |
| `rejectUndeclaredInputs` / `rejectUndeclaredFields` | stricter checking | `--invaractRejectUndeclaredInputs=true` and `...Fields=true` | Yes |
| `computeFingerprint` / `staticDataQuality` / `roleConsistency` | optional analyses | an option exists, but turning one on gets a safe refusal | No: declared unsupported |

**How a job can get around it, and what is not possible.**
- A job that calls `options.setRunner(...)` in its own code overrides `--runner`. **Measured: the wrapper never ran.**
- A job that builds its options without reading its command line (`PipelineOptionsFactory.create()`) never sees the
  flags at all. (My graph probes did exactly this.)
- A job that launches another pipeline from inside a function is invisible to the outer one.
- Not possible: attaching to a job whose source we do not control *and* that does either of the first two. The only
  route there is the explicit call (D1, option B), which is a code change.
- UNVERIFIED: whether a wrapper can hand over to production runners (Dataflow, Flink) with their own required
  options. The mechanism is the same; the delegate's setup was only run for the `DirectRunner`.

## 6. What it would look like to a user

Everything in this section is PREDICTED except the mechanics shown in section 3.

**The contract** (what the data must look like): the YAML in section 1.

**The job** (what the team wrote; unchanged):

```java
PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
Pipeline p = Pipeline.create(options);
p.apply("ReadOrders", ParquetIO.read(SCHEMA).from("in/orders/*.parquet"))
 .apply("KeepPositive", Filter.by(r -> (Long) r.get("amount") > 0))
 .apply("WriteReport", FileIO.<GenericRecord>write().via(ParquetIO.sink(SCHEMA)).to("out/report").withSuffix(".parquet"));
p.run().waitUntilFinish();
```

**Turning it on** (what the platform team does): the command in section 5.

**A passing run.** The job runs as before. Invaract prints one summary line (PREDICTED) and publishes a
`CONTRACT_VALIDATION` event with status `PASSED` naming engine `beam` and the job name.

**A blocked run (PREDICTED).** If the job writes to `out/elsewhere`, the wrapper refuses to submit it and throws
Invaract's four-part message (what the contract expects, what the plan contains, why it violates, how to fix):

```
Contract violation: 'orders_report@1.0.0' rejected this transformation. Write aborted.

What the contract expects:
  input  'orders' at in/orders: id: long, amount: long (optional)
  output 'report' at out/report: id: long, amount: long (optional)

What the plan contains:
  Read in/orders (parquet)
  Filter
  Write out/elsewhere (parquet)

Why it violates the contract (1 violation):
  1. [OUTPUT_LOCATION_MISMATCH] Write to 'out/elsewhere' does not match any declared output location.

How to correct it:
  1. Write to 'out/report', or update the contract's output location.
```

(The shape is copied from `VerificationPipeline.explain`; the plan text and message wording depend on the
adapter's translation.)

**Dry-run mode (PREDICTED).** With `--invaractDryRun=true`, the job runs unchanged and Invaract reports a contract
it inferred from the graph: the input's folder and Avro-derived schema, the output's folder and schema. Text
outputs would have no schema to infer, so those fields would be empty. The inferred contract goes to the
configured sink, as on Spark.

**Where the events go.** To the same sinks as on Spark (`--invaractNotifyConfig`), with the engine named `beam`
and the job name from the Beam options.

## 7. How much of the suite it is expected to pass

### 7.1 The capability declaration, forecast

This is the declaration the adapter would publish, as a draft (a real one is written in step 2 of section 8). The
scenario forecast in 7.2 was generated from exactly this content. Confidence is for the status chosen.

| Capability | What it means | Would declare | Why | Confidence |
|---|---|---|---|---|
| `read.batch` | A batch read is recognised as an input | supported | Reads are steps with a stated file pattern (ParquetIO, AvroIO, TextIO) | High |
| `read.streaming` | A streaming read is recognised | partial | Unbounded collections are visible; running streaming scenarios in-process is unproven | Low |
| `write.batch` | A batch write is recognised and checked before it runs | supported | Writes are steps with a destination (section 3); unrecognised last steps are refused | High |
| `write.streaming` | A streaming write is recognised | partial | As `read.streaming` | Low |
| `write.rowLevelDml` | Row-level MERGE / UPDATE / DELETE is recognised | not-applicable | Beam has no such operation | High |
| `write.stateChange` | A schema change without rows is checked | not-applicable | Beam has no such operation | High |
| `check.inputExistence` | Declared inputs must be read; undeclared reads can be rejected | supported | Reads and their patterns are in the graph | High |
| `check.location` | A write lands where the contract says | supported | Read from display data or the sink, after making paths absolute | High |
| `check.schema` | Field presence, types and nullability | partial | Known when the collection carries an Avro or Beam schema; not for plain Java objects | High |
| `check.nestedTypes` | Arrays, maps, structs compared structurally | supported | Avro records, arrays and maps convert with Beam's own library | Medium |
| `check.format` | A declared output format is checked | partial | The format is the writing transform; text cannot tell csv from json | High |
| `check.saveMode` | A declared save mode is checked | unsupported | File writes have no append/overwrite choice in Beam; BigQueryIO does (later) | High |
| `check.catalogRegistration` | A declared catalog requirement is checked | unsupported | File writes have no catalog; BigQueryIO writes are to catalog tables (later) | High |
| `rules.dml` | MERGE/DELETE rules | not-applicable | No row-level DML | High |
| `rules.planShape` | required_group_by, forbid_cross_join, required_join_columns, required_filter_columns | unsupported | The fields filters, joins and groupings use are not public (section 3) | Medium |
| `rules.custom` | Custom rule types run | unsupported | Custom rules judge a row-level change; Beam has none | High |
| `analysis.staticDataQuality` | Static proof of column properties | unsupported | Needs to see how a column is computed; functions are opaque | High |
| `analysis.roleConsistency` | Declared input role vs observed use | unsupported | Needs column-level usage | Medium |
| `analysis.fingerprint` | Semantic fingerprint of the transformation | unsupported | A graph-only fingerprint would miss the logic in functions and mislead | Medium |
| `analysis.functionCatalog` | Engine function names map to a canonical catalog | unsupported | Beam jobs call Java methods, not named functions | High |
| `analysis.sensitivityPropagation` | Input tags follow to output columns | unsupported | Needs column-level lineage | High |
| `lineage.columnLevel` | What each output column derives from | unsupported | Opaque functions hide it | High |
| `lineage.boundaryResolution` | A checkpoint or cache is seen through | not-applicable | A Beam pipeline is an immutable graph; nothing erases history | Medium |
| `policy.organizational` | Organisation-wide rules on the contract | supported | Engine-neutral, applied from `verification-core` | High |
| `config.zeroCodeInstall` | Installed through the engine's own configuration | partial | `--runner` plus registered options work; a job choosing its runner in code bypasses | High |
| `config.locationRefs` | `ref://id` locations resolve through configuration | supported | Via `--invaractLocationMap`, like any option | Medium |
| `config.contractRegistry` | `registry://id@version` contracts fetched | supported | Via `--invaractContract` and `--invaractRegistryUrl` | Medium |
| `reporting.notifications` | Events are published to a sink | supported | Reuse of `verification-core` | High |
| `reporting.dryRunInference` | A draft contract is inferred from a real write | partial | Inputs and outputs and their schemas inferable; text outputs have no schema | Medium |
| `failClosed.unverifiableWrites` | An untranslatable writing operation is refused | partial | A final step that is not a recognised writer is refused; mid-pipeline side effects are invisible | Medium |

**Enforcement point:** `pre-submit-gate`: Invaract can refuse to start a job; it cannot stop a pipeline once it is
running, and it sees only what the pipeline description holds.

### 7.2 Scenario by scenario

Generated by `./dev/adapter-survey` from the draft declaration (the "kit expects" column is the kit's own rule), then
filled in by hand. Each scenario writes a small job in the engine's terms; for Beam that means empty Parquet files
for inputs and `ParquetIO`/`FileIO` for outputs, built with Beam's own join, filter and grouping transforms.

**Structural checks: strong.** (1–5, 12–23, 26–27)

| # | Scenario | What it checks | Kit expects of `beam` | Forecast | Why | Confidence |
|---|---|---|---|---|---|---|
| 1 | `conforming-write` | a matching write is allowed and a PASSED event is published | does the checks, allowed | ✅ | Read pattern, write destination and schema are all readable at the gate | Medium |
| 2 | `wrong-output-location` | a write elsewhere is blocked | blocked: `OUTPUT_LOCATION_MISMATCH` | ✅ | Destination read from the sink, compared as absolute paths | Medium |
| 3 | `missing-output-field` | a declared output field the job lacks is blocked | blocked: `MISSING_OUTPUT_FIELD` | ✅ | Output schema read from the last collection's coder | Medium |
| 4 | `output-type-mismatch` | a column of a different type is blocked | blocked: `OUTPUT_FIELD_TYPE_MISMATCH` | ✅ | Avro to Beam types shown correct in section 3 | Medium |
| 5 | `extended-type-not-produced` | interval/json/geography/time not satisfied by another type | blocked: `OUTPUT_FIELD_TYPE_MISMATCH` | ✅ | Avro cannot express those, so any other type is a mismatch | Medium |
| 12 | `output-nullability-mismatch` | a required column the job can leave null is blocked | blocked: `OUTPUT_FIELD_NULLABILITY_MISMATCH` | ✅ | Avro `["null","long"]` versus `long` seen in the probe | Medium |
| 13 | `undeclared-output-column-allowed-by-default` | an extra output column is allowed by default | allowed | ✅ | Same schema read | Medium |
| 14 | `undeclared-output-column-rejected` | with the stricter option, an extra column is blocked | blocked: `UNDECLARED_OUTPUT_COLUMN` | ✅ | Same schema read | Medium |
| 15 | `declared-input-not-read` | a declared input never read is blocked | blocked: `MISSING_INPUT` | ✅ | Reads and their patterns are in the graph | Medium |
| 16 | `undeclared-input-rejected` | reading an undeclared dataset is blocked (strict) | blocked: `UNDECLARED_INPUT` | ✅ | Same | Medium |
| 17 | `derived-from-excuses-an-input-the-output-is-not-built-from` | an input feeding only another output is not required | allowed | ✅ | Needs which reads reach which write: follow the collections' edges upstream | Medium |
| 18 | `derived-from-requires-every-source-of-the-output` | reading a non-source input does not stand in for the real one | blocked: `MISSING_INPUT` | ✅ | Same edge-following | Medium |
| 19 | `derived-from-rejects-a-read-of-another-outputs-source` | strict: reading another output's source is blocked | blocked: `UNDECLARED_INPUT` | ✅ | Same | Medium |
| 20 | `unrecognized-step-keeps-the-reads-beneath-it-visible` | an unrepresentable step does not hide the input beneath it | allowed | ✅ | Beam's graph is edges between collections, so reads beneath an opaque step stay in the walk (probe: the opaque filter's input edge reaches the source) | Medium |
| 21 | `unrecognized-step-does-not-hide-an-undeclared-read` | strict: an undeclared read beneath such a step is blocked | blocked: `UNDECLARED_INPUT` | ✅ | Same | Medium |
| 22 | `output-format-matches` | the declared format is allowed | allowed | ✅ | ParquetIO is parquet | Medium |
| 23 | `output-format-mismatch` | a csv write against a parquet contract is blocked | blocked: `OUTPUT_FORMAT_MISMATCH` | ✅ | TextIO is text, not parquet. (A contract that declares csv against TextIO cannot be checked: D5.) | Medium |
| 26 | `nested-type-matches` | a nested column of the declared structure is allowed | allowed | ✅ | Avro arrays/maps/records convert with Beam's library | Medium |
| 27 | `nested-type-mismatch` | a nested column with a different element type is blocked | blocked: `OUTPUT_FIELD_TYPE_MISMATCH` | ✅ | Same | Medium |

**Fail-closed and contract checks: strong.**

| # | Scenario | What it checks | Kit expects of `beam` | Forecast | Why | Confidence |
|---|---|---|---|---|---|---|
| 50 | `untranslatable-write-fails-closed` | a data-changing operation the adapter cannot translate is blocked | blocked: `UNVERIFIABLE_WRITE` | ✅ | A final step with no recognised writer, as in the `PushToSomeService` probe, is refused | Medium |
| 51 | `invalid-contract` | an unsound contract is rejected before the job is looked at | blocked: `INVALID_CONTRACT` | ✅ | Engine-neutral: `verification-core` does it | High |

**Streaming: unproven.**

| # | Scenario | What it checks | Kit expects of `beam` | Forecast | Why | Confidence |
|---|---|---|---|---|---|---|
| 48 | `streaming-write-is-checked-like-a-batch-write` | a streaming job's source and sink are checked as for batch | allowed | ⚠️ | Unbounded collections are visible (section 3), but a streaming test must also *finish*; a file stream that stops needs a termination condition. UNVERIFIED | Low |
| 49 | `streaming-write-to-the-wrong-location` | a streaming sink elsewhere is blocked | blocked: `OUTPUT_LOCATION_MISMATCH` | ⚠️ | As 48 | Low |

**Plan-shape rules: not available; refused safely.** (The checks are made by the kit, which expects the refusal `UNSUPPORTED_CONTRACT_FEATURE` for each.)

| # | Scenario | What it checks | Forecast | Why | Confidence |
|---|---|---|---|---|---|
| 28 | `required-filter-column-present` | a required-filter rule is satisfied | ❌ refuses | The filter's column is inside the function | Medium |
| 29 | `required-filter-column-missing` | the rule is violated when the job never filters on it | ❌ refuses | Same | Medium |
| 30 | `forbid-cross-join-allows-a-keyed-join` | a cross-join ban allows a keyed join | ❌ refuses | A join appears (section 3), but its keys are not public | Medium |
| 31 | `forbid-cross-join-blocks-a-cartesian-product` | the ban blocks a no-condition join | ❌ refuses | Same | Medium |
| 32 | `required-join-columns-present` | a join on the required column passes | ❌ refuses | Same | Medium |
| 33 | `required-join-columns-missing` | a join on another column is blocked | ❌ refuses | Same | Medium |
| 34 | `required-group-by-present` | a grouping by the required column passes | ❌ refuses | The grouping's key fields are not public | Medium |
| 35 | `required-group-by-wrong-keys` | a grouping by another column is blocked | ❌ refuses | Same | Medium |

What the user loses: a contract with any of these four rules (`required_filter_columns`, `forbid_cross_join`,
`required_join_columns`, `required_group_by`) cannot be used on Beam; the job is refused, with the reason named. What
would close it: reading the portable pipeline form or, riskily, Beam's private fields (D2).

**Catalog and save mode: not available for file writes; refused safely.**

| # | Scenario | What it checks | Forecast | Why | Confidence |
|---|---|---|---|---|---|
| 10 | `catalog-required-output-unregistered` | an output required to be registered in a catalog is blocked if written to a bare path | ❌ refuses | A file write has no catalog | High |
| 11 | `catalog-required-output-registered` | allowed when written as a catalog table | ❌ refuses | Same (BigQueryIO would add this later) | High |
| 24 | `output-save-mode-matches` | the declared save mode is allowed | ❌ refuses | A file write has no save mode | High |
| 25 | `output-save-mode-mismatch` | a different save mode is blocked | ❌ refuses | Same (BigQueryIO has write dispositions) | High |

**Value analysis and lineage: not available.**

| # | Scenario | What it checks | Forecast | Why | Confidence |
|---|---|---|---|---|---|
| 36 | `fingerprint-flags-nothing-for-a-deterministic-job` | a deterministic job reports no non-deterministic column | ❌ skipped | Fingerprint declared unsupported | Medium |
| 37 | `fingerprint-flags-a-generated-id` | a per-row generated id is reported non-deterministic | ❌ skipped | Same | Medium |
| 38 | `static-data-quality-proves-a-constant` | a constant is proven to meet its range | ❌ refuses | Needs to see how a column is computed | High |
| 39 | `static-data-quality-cannot-prove-a-passthrough` | a pass-through is reported not guaranteed | ❌ refuses | Same | High |
| 40 | `static-data-quality-violation-blocks` | a provably excluded constant blocks the write | ❌ refuses | Same | High |
| 41 | `role-source-contributing-conforms` | a SOURCE input whose data reaches the output is consistent | ❌ refuses | Needs column-level usage | Medium |
| 42 | `role-control-contributing-contradicts` | a CONTROL input reaching the output blocks | ❌ refuses | Same | Medium |
| 43 | `lineage-passthrough-cast-and-constant` | each output column traces to its input column | ❌ skipped | Opaque functions hide it | High |
| 44 | `lineage-through-join-and-filter` | a joined column traces to its input | ❌ skipped | Same | High |
| 45 | `sensitivity-follows-a-cast` | tags follow a cast to the output | ❌ skipped | Needs lineage | High |

**Not applicable to Beam.**

| # | Scenario | What it checks | Forecast | Why | Confidence |
|---|---|---|---|---|---|
| 6 | `row-level-delete-checked-as-write` | a row-level DELETE is checked as a write | ➖ skipped | Beam has no row-level DML | High |
| 7 | `row-level-delete-own-table-is-not-an-input` | a DELETE's own table is not an undeclared input | ➖ skipped | Same | High |
| 8 | `unconditional-delete-forbidden` | a DELETE with no predicate is blocked | ➖ skipped | Same | High |
| 9 | `filtered-delete-allowed` | a DELETE with a predicate passes | ➖ skipped | Same | High |
| 46 | `boundary-filter-is-seen-through` | a filter before a checkpoint still counts | ➖ skipped | A Beam pipeline has no checkpoint that erases history | Medium |
| 47 | `boundary-does-not-excuse-a-missing-input` | a genuinely missing input is still reported after a checkpoint | ➖ skipped | Same | Medium |

### 7.3 Claims no scenario can check

Seven capabilities cannot be checked by running a neutral job (their claim must be demonstrated by the adapter's own
tests). The kit checks that the named test exists.

| Capability | What a test must show | How this adapter's test would show it | Forecast |
|---|---|---|---|
| `policy.organizational` | a contract that breaks an organisational policy is rejected when enforcement is set up, before any job runs | build the wrapper with a policy file via `--invaractOrgPolicy`; a violating contract is refused at `fromOptions`/`run` | ✅ (declared supported) |
| `config.zeroCodeInstall` | enforcement is installed through the engine's own configuration alone, then blocks a violating write | the section 3 probe turned into a test: an unmodified job, `--runner=...`, a violating write is refused; plus a test recording the bypass | ⚠️ (declared partial) |
| `config.locationRefs` | a `ref://id` location resolves through configuration and a real write is checked against it | a contract with `ref://orders`, `--invaractLocationMap`, a real DirectRunner write | ✅ |
| `config.contractRegistry` | a `registry://id@version` reference is fetched from the configured registry and enforced | a stub registry like the Spark adapter's tests use | ✅ |
| `reporting.dryRunInference` | dry-run lets a real write through and reports an inferred contract | a DirectRunner job with `--invaractDryRun=true`; assert the output exists and the inferred contract | ⚠️ (declared partial) |
| `write.stateChange` | a schema-changing operation is checked | not applicable: Beam has none | ➖ |
| `rules.custom` | a custom rule type resolves and blocks | unsupported: custom rules judge row-level changes | ❌ |

### 7.4 Totals

**21** of 51 by doing the check · **17** by refusing safely · **2** partly (streaming) · **11** skipped (6 not
applicable to Beam, 5 declared unsupported without blocking). In practice: for the contracts most data teams
write first (locations, columns, types, required inputs, formats), Beam is covered well; contracts with
transformation-shape rules, catalog, save-mode requirements or value analyses are refused with a clear reason
rather than silently passed.

## 8. What it would take

Sizes are small, medium or large; no dates.

1. **Settle the UNVERIFIED items** (small). The list in section 10, each with its cheapest check. Includes the Scala
   runner class, the temporary-folder question, empty Parquet inputs and a streaming test that terminates.
2. **Scaffold the module** (small). `dev/new-adapter beam`; fill the declaration from section 7.1; empty baseline.
3. **Translate the graph to the IR** (large). Recognisers for `ParquetIO`, `AvroIO`, `TextIO` reads and writes; follow
   collection edges for reads-beneath-steps; join, filter and group steps as IR nodes without keys; unknown steps as
   opaque nodes.
4. **Types and schemas** (medium). Coder and Beam-row schema to the contract's types; the table from the research doc.
5. **The wrapper runner and options** (medium). `fromOptions`, `run`, the options interface and `ServiceLoader`
   registration, the delegate hand-over, and `AttachSetup`/`VerificationSetup` from `verification-core` for contract
   resolution, sinks and policy.
6. **Scenario jobs on Beam** (medium). Each of the 51 scenarios built with Beam's join, filter, group and cast
   transforms and run on the `DirectRunner`.
7. **Attestations and baseline** (medium). The five attested tests of section 7.3; the checked-in conformance baseline.
8. **Docs and gates** (medium). A docs-site page and capability matrix entry; `modules.json` entry with mutation,
   coverage and API-compatibility CI.

A **first useful version** is steps 1 to 7 with Parquet, Avro and text. BigQueryIO support is a separate follow-up
(it adds `check.saveMode` and `check.catalogRegistration`), and the portable-form probe (D2) is the route to
plan-shape rules.

## 9. Risks and open questions

| Risk or question | Why it matters | How to settle it |
|---|---|---|
| Jobs that pick their own runner or ignore the command line slip past the gate | the main route is `--runner`; those jobs are unprotected | measure what real jobs do; ship the explicit call too |
| Plan-shape rules unsupported | contracts with those rules are refused on Beam | probe the portable pipeline form for join and group fields |
| A separate temporary folder looks like the destination | could pass a write to the wrong place or refuse a right one | detect `withTempDirectory`; probe (section 10, unverified item 2) |
| Input schema is what the job says, not what is on disk | a stale schema in the job passes the check and fails at run time | accept and document; Beam does not open files before running |
| A function that writes mid-pipeline is invisible | a side effect no check can see | document; encourage declaring sinks as Beam IO |
| A terminal step that is not a known writer is refused | false alarms on metrics-only endings | an allow-list later (D6) |
| Heavy dependencies (Hadoop and Parquet) | a bigger jar for users | keep optional modules separate |
| Only the `DirectRunner` and Beam 2.77.0 were probed | other runners or versions may differ | a Beam version matrix in CI, like Spark's; try Flink's runner later |
| The same pipeline can be built differently (SQL, schema transforms) | `SqlTransform` hides steps behind SQL text | treat as an unrecognised step until supported |

## 10. Evidence log

### Probes I ran

All run on 2026-10-13, Beam 2.77.0, Java 21, `DirectRunner`, from a throwaway sbt project (not committed).
Maven Central answered with HTTP 429 from this environment, so the project resolved dependencies from Google's public
mirror of it (`maven-central.storage-download.googleapis.com`), the only external resolver configured.

- **Graph probe (`GraphProbe`/`Walk`)**: `Pipeline.traverseTopologically`, printing each node, inputs, outputs,
  boundedness, schema and `DisplayData`. Used for the examples in section 3 (filter, join and group, opaque filter,
  bounded versus unbounded).
- **IO probe (`IoProbe`)**: Parquet read and `FileIO.write`, `BigQueryIO` read and write constructed offline, and a
  side-effecting `DoFn`.
- **Coder probe (`CoderProbe`)**: the Parquet read's output coder and Avro-to-Beam schema conversion.
- **Sink probe (`SinkProbe`)**: `WriteFiles.getSink().getTempDirectoryProvider()` on a Parquet and a text write.
- **Wrapper probe (`ProbeRunner`, `ProbeOptions`, `ProbeOptionsRegistrar`, `Job`, `JobThatPicksItsOwnRunner`)**: a
  runner named by `--runner` receives the pipeline in `run`, reads Invaract-style options registered through
  `ServiceLoader`, blocks or hands over, and a job picking its own runner bypasses it. Outputs quoted in section 3.
- **Draft declaration and forecast**: written for this survey; the scenario table came from
  `./dev/adapter-survey <out.md> <draft.yaml>`, which applies the kit's own rule.

### Sources

- Beam 2.77.0 source: `Pipeline.java`, `TransformHierarchy.java`, `FileIO.java` (`getOutputDirectory` is package
  private; `ViaFileBasedSink` passes the temporary directory), `FileBasedSink.java` (`getTempDirectoryProvider`
  public), `WriteFiles.java` (`getSink` public; display data names the sink class).
- `docs/ENGINE_RESEARCH_BIGQUERY_BEAM.md` for Beam and BigQuery type tables, write dispositions, and why BigQueryIO
  graphs can be built offline.
- Invaract: `adapter-testkit` (`Scenarios.scala`, `ScenarioCatalogue.scala`, `AttestedClaims.scala`),
  `verification-core` (`Capability.scala`, `VerificationPipeline.scala` for the four-part message), and
  `docs-site/src/content/docs/reference/violation-types.md`.

### Unverified

| # | Claim | Cheapest check |
|---|---|---|
| 1 | A Scala class with a companion `fromOptions` is accepted by Beam's `--runner` lookup | A ten-line Scala runner run through the wrapper probe |
| 2 | A `FileIO` write with a separate `withTempDirectory` can be told apart from the default | Build one and print `getTempDirectoryProvider()` and the other available state |
| 3 | `ParquetIO.read` accepts an empty Parquet file as the scenarios' empty input | Write one with parquet-avro and run a read on the `DirectRunner` |
| 4 | A streaming scenario can run to completion in-process (file stream with a stop condition) | A `watchForNewFiles` read with a termination condition on the `DirectRunner` |
| 5 | The portable pipeline form (`PipelineTranslation.toProto`) carries join and group fields | Convert the join-and-group probe's pipeline and print the transform specs |
| 6 | Handing over to Dataflow or Flink from the wrapper works with their options | A wrapper run delegating to `FlinkRunner` on a local cluster |
| 7 | A short runner name could be registered through `PipelineRunnerRegistrar` | A registrar class and a `--runner=Invaract` run |
| 8 | Reads beneath an opaque step stay reachable by following collection edges in an assembled translation | The first translation test (also covered by scenarios 20 and 21) |

## Glossary

- **Adapter.** Code that connects Invaract to one engine: gets the engine's description of a job, translates it to
  the IR, and installs the check.
- **Pipeline.** A Beam job: a graph of steps that read, transform and write data. Built first, run later.
- **Graph.** The steps and the data flowing between them, as boxes and arrows.
- **PCollection.** The data flowing between two steps; bounded (finite) or unbounded (a stream).
- **Transform.** One step in a pipeline. Composite transforms contain smaller ones; primitive ones are executed.
- **Source / sink.** A step that reads from, or writes to, the outside world.
- **Runner.** The piece that executes a pipeline on an engine. `--runner=` names it on the command line.
- **DirectRunner.** Beam's in-process runner, used for tests; no cluster needed.
- **Wrapper runner.** A runner that checks a pipeline and hands it to the real runner, or refuses.
- **Coder.** How a collection's items are turned into bytes; for Avro records it also carries their schema.
- **Schema.** The columns of a collection with their types and nullability.
- **Display data.** Labels a step can publish about itself for monitoring; includes file paths.
- **Function / DoFn.** The team's own code inside a step. Opaque to Beam.
- **ServiceLoader.** Java's standard way of finding plugins listed in a jar, so no code in the job is needed.
- **Fail closed.** When Invaract cannot tell whether something is safe, it refuses, rather than allowing it.
- **IR.** Invaract's engine-neutral description of a transformation.
- **Capability / declaration / scenario.** A thing an adapter might do; the adapter's statement of what it can do;
  one small job in the conformance suite.
- **Enforcement point.** Where the adapter can stop a job: inside the engine, before submission, or not at all.
- **`OUTPUT_LOCATION_MISMATCH`.** The job writes somewhere other than the contract's declared output.
- **`MISSING_INPUT` / `UNDECLARED_INPUT`.** A declared input is never read / a dataset the contract does not declare is
  read (strict option).
- **`MISSING_OUTPUT_FIELD`.** A declared output column is not produced.
- **`OUTPUT_FIELD_TYPE_MISMATCH` / `OUTPUT_FIELD_NULLABILITY_MISMATCH`.** An output column has a different type /
  can be null where the contract says required.
- **`UNDECLARED_OUTPUT_COLUMN`.** An output column the contract does not declare (strict option).
- **`OUTPUT_FORMAT_MISMATCH` / `OUTPUT_SAVE_MODE_MISMATCH`.** Written in another format / with another save mode.
- **`MISSING_OUTPUT_CATALOG_REGISTRATION`.** The contract requires a catalog table; the write is to a bare path.
- **`RULE_*_VIOLATION`.** A transformation-shape rule (required filter, join columns, group-by, no cross join) is broken.
- **`DATA_QUALITY_VIOLATION` / `ROLE_CONSISTENCY_VIOLATION`.** A provable value constraint is broken / a dataset is
  used contrary to its declared role.
- **`UNVERIFIABLE_WRITE`.** A step changes data but cannot be translated, so it is blocked.
- **`UNSUPPORTED_CONTRACT_FEATURE`.** The contract needs something the adapter declared unsupported, so it is refused.
- **`INVALID_CONTRACT`.** The contract itself is unsound.

## Retrospective

_Not yet: no adapter exists._
