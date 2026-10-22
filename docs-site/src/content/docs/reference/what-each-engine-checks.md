---
title: What Each Engine Checks
description: What Invaract checks on each engine, in plain words, and what you lose where an engine cannot make a check.
sidebar:
  order: 0.5
---

<!-- GENERATED from each adapter's invaract-capabilities-<adapter>.yaml and adapter-testkit's CapabilityGuide. Do not edit by hand: change those and run ./dev/capabilities. -->

Invaract works with more than one engine, and an engine can only be checked as far as it describes its own jobs.
This page says what Invaract checks on each one, and what it means for a job where it cannot.

One rule holds everywhere: if your contract relies on a check an engine cannot make, the job is **refused before it
writes anything** (`UNSUPPORTED_CONTRACT_FEATURE`). It is never run with part of the contract quietly unchecked.
A check that only reports, such as a fingerprint, is simply not produced.

For the same facts in more detail, with the notes each engine's adapter keeps, see
[Engine Capabilities](/reference/engine-capabilities/).

## How each engine stops a bad write

| Engine | How it stops a bad write | Set it up |
|---|---|---|
| Apache Beam (Java SDK) | Checks the whole pipeline before it is handed to a runner. If anything breaks the contract, nothing runs at all. | [Guide](/guides/using-invaract-with-apache-beam/) |
| Apache Beam (Python SDK) | Checks the whole pipeline before it is handed to a runner. If anything breaks the contract, nothing runs at all. | [Guide](/guides/using-invaract-with-beam-python/) |
| Apache Spark 3.5 | Checks each write inside the running job, just before it happens. A write that breaks the contract is stopped and the job fails; writes the job made before it have already happened. | [Guide](/guides/installing-the-enforcement-rule/) |

## At a glance

✅ checked · ◐ checked in part (see below) · ✖ not checked · — does not apply to that engine

### Which reads and writes it sees

| Check | Apache Beam (Java SDK) | Apache Beam (Python SDK) | Apache Spark 3.5 |
|---|---|---|---|
| [Batch reads](#batch-reads) | ✅ Yes | ✅ Yes | ✅ Yes |
| [Streaming reads](#streaming-reads) | ✅ Yes | ◐ Partly | ✅ Yes |
| [Batch writes](#batch-writes) | ✅ Yes | ✅ Yes | ✅ Yes |
| [Streaming writes](#streaming-writes) | ✅ Yes | ✅ Yes | ◐ Partly |
| [MERGE, UPDATE and DELETE](#merge-update-and-delete) | — Doesn't apply | — Doesn't apply | ✅ Yes |
| [Table rollbacks and similar changes](#table-rollbacks-and-similar-changes) | — Doesn't apply | — Doesn't apply | ✅ Yes |

### What it checks about each write

| Check | Apache Beam (Java SDK) | Apache Beam (Python SDK) | Apache Spark 3.5 |
|---|---|---|---|
| [Required inputs are read](#required-inputs-are-read) | ✅ Yes | ✅ Yes | ✅ Yes |
| [Writes land where the contract says](#writes-land-where-the-contract-says) | ✅ Yes | ✅ Yes | ◐ Partly |
| [Every promised output is written](#every-promised-output-is-written) | ✅ Yes | ✅ Yes | ◐ Partly |
| [Columns match the contract](#columns-match-the-contract) | ◐ Partly | ◐ Partly | ✅ Yes |
| [Nested columns are compared in full](#nested-columns-are-compared-in-full) | ✅ Yes | ✅ Yes | ✅ Yes |
| [The file format matches](#the-file-format-matches) | ◐ Partly | ◐ Partly | ✅ Yes |
| [The save mode matches](#the-save-mode-matches) | ◐ Partly | ◐ Partly | ✅ Yes |
| [Datasets are registered in a catalog](#datasets-are-registered-in-a-catalog) | ◐ Partly | ◐ Partly | ✅ Yes |

### Rules a contract can declare

| Check | Apache Beam (Java SDK) | Apache Beam (Python SDK) | Apache Spark 3.5 |
|---|---|---|---|
| [Rules for MERGE, UPDATE and DELETE](#rules-for-merge-update-and-delete) | — Doesn't apply | — Doesn't apply | ◐ Partly |
| [Rules about the transformation's shape](#rules-about-the-transformations-shape) | ◐ Partly | ✖ No | ✅ Yes |
| [Your own rule types](#your-own-rule-types) | ✖ No | ✖ No | ✅ Yes |

### Deeper analysis

| Check | Apache Beam (Java SDK) | Apache Beam (Python SDK) | Apache Spark 3.5 |
|---|---|---|---|
| [Proves value constraints](#proves-value-constraints) | ✖ No | ✖ No | ✅ Yes |
| [Datasets are used as their declared type says](#datasets-are-used-as-their-declared-type-says) | ✖ No | ✖ No | ✅ Yes |
| [Fingerprints the transformation's logic](#fingerprints-the-transformations-logic) | ✖ No | ✖ No | ✅ Yes |
| [Recognizes built-in functions](#recognizes-built-in-functions) | ✖ No | ✖ No | ✅ Yes |
| [Tracks sensitive columns](#tracks-sensitive-columns) | ✖ No | ✖ No | ✅ Yes |
| [Traces each output column to its inputs](#traces-each-output-column-to-its-inputs) | ✖ No | ✖ No | ◐ Partly |
| [Sees through checkpoints and caches](#sees-through-checkpoints-and-caches) | — Doesn't apply | — Doesn't apply | ◐ Partly |

### Organization-wide policy

| Check | Apache Beam (Java SDK) | Apache Beam (Python SDK) | Apache Spark 3.5 |
|---|---|---|---|
| [Organization-wide policy](#organization-wide-policy) | ✅ Yes | ✅ Yes | ✅ Yes |

### Setting it up

| Check | Apache Beam (Java SDK) | Apache Beam (Python SDK) | Apache Spark 3.5 |
|---|---|---|---|
| [Turn it on without changing the job](#turn-it-on-without-changing-the-job) | ◐ Partly | ◐ Partly | ✅ Yes |
| [Locations resolved when the job starts](#locations-resolved-when-the-job-starts) | ✅ Yes | ✅ Yes | ✅ Yes |
| [Contracts from a registry](#contracts-from-a-registry) | ✅ Yes | ✅ Yes | ✅ Yes |

### Reporting

| Check | Apache Beam (Java SDK) | Apache Beam (Python SDK) | Apache Spark 3.5 |
|---|---|---|---|
| [Sends events to your systems](#sends-events-to-your-systems) | ✅ Yes | ✅ Yes | ✅ Yes |
| [Writes a starting contract for you](#writes-a-starting-contract-for-you) | ◐ Partly | ◐ Partly | ✅ Yes |

### Safety net

| Check | Apache Beam (Java SDK) | Apache Beam (Python SDK) | Apache Spark 3.5 |
|---|---|---|---|
| [Refuses writes it cannot understand](#refuses-writes-it-cannot-understand) | ◐ Partly | ◐ Partly | ✅ Yes |

## Where each engine falls short

### Apache Beam (Java SDK)

Everything not listed here is checked in full.

**Checked in part**

- [Columns match the contract](#columns-match-the-contract): Columns are checked where the pipeline states them: Parquet and Avro schemas, BigQuery table schemas and Beam rows. A write of plain Java objects or of text lines has no columns to compare, so it is refused unless its output declares none.
- [The file format matches](#the-file-format-matches): The format comes from the IO the pipeline uses. A text write cannot tell CSV from JSON or plain text.
- [The save mode matches](#the-save-mode-matches): Only BigQuery writes state a save mode. A contract that declares one for a file, Kafka, JDBC or Pub/Sub write has that write refused.
- [Datasets are registered in a catalog](#datasets-are-registered-in-a-catalog): BigQuery tables are checked against the `bigquery` catalog. Files, Kafka, JDBC and Pub/Sub have no catalog in Beam, so a requirement on them is never met.
- [Rules about the transformation's shape](#rules-about-the-transformations-shape): Checked where the pipeline uses Beam's schema transforms (`Filter`, `Group`, `Join`, `CoGroup`) or Beam SQL's GROUP BY and JOIN. A filter, grouping or join written as code is a black box: it does not satisfy a rule, and under `forbid_cross_join` a step that combines collections in code is refused.
- [Turn it on without changing the job](#turn-it-on-without-changing-the-job): Turned on with `--runner` and the `--invaract...` options on the job's command line. A job that sets its runner in its own code bypasses it.
- [Writes a starting contract for you](#writes-a-starting-contract-for-you): Drafts list every read and write the pipeline locates, with columns where it states them; one without columns gets a draft that declares none, and says so.
- [Refuses writes it cannot understand](#refuses-writes-it-cannot-understand): A step that ends the pipeline without being a write Invaract recognizes is refused. A step that writes to an outside system in the middle of the pipeline cannot be seen before it runs.

**Not checked**

- [Your own rule types](#your-own-rule-types): Custom rule types judge row-level changes, and Beam has none. Refused if your contract relies on it.
- [Proves value constraints](#proves-value-constraints): Beam steps are Java code Invaract cannot look inside, so it cannot prove what a column will contain. Refused if you turn on `staticDataQuality`.
- [Datasets are used as their declared type says](#datasets-are-used-as-their-declared-type-says): Needs to know how each column is used, which Beam's description of a pipeline does not say. Refused if you turn on `roleConsistency`.
- [Fingerprints the transformation's logic](#fingerprints-the-transformations-logic): A fingerprint of the pipeline's shape alone would miss the logic inside its steps, so none is computed. Turning `computeFingerprint` on does not stop the job; no fingerprint is published, and a warning in the log says why.
- [Recognizes built-in functions](#recognizes-built-in-functions): Beam pipelines call Java methods, not the engine's own named functions. Only fingerprints use it.
- [Tracks sensitive columns](#tracks-sensitive-columns): Needs to trace each column through the pipeline, which Beam's description of it does not allow. No sensitivity report is produced; tags on the contract's fields are kept but not followed through the job.
- [Traces each output column to its inputs](#traces-each-output-column-to-its-inputs): Beam steps are Java code Invaract cannot look inside, so it cannot tell which input columns each output column comes from. Checks that rely on it, such as proving value constraints, are not available.

**Does not apply**

- [MERGE, UPDATE and DELETE](#merge-update-and-delete): Beam has no MERGE, UPDATE or DELETE: a pipeline only writes new output.
- [Table rollbacks and similar changes](#table-rollbacks-and-similar-changes): Beam has no operation that changes a table without writing rows.
- [Rules for MERGE, UPDATE and DELETE](#rules-for-merge-update-and-delete): Beam has no MERGE, UPDATE or DELETE to make rules about.
- [Sees through checkpoints and caches](#sees-through-checkpoints-and-caches): A Beam pipeline has no checkpoint or cache that hides what produced its data.

### Apache Beam (Python SDK)

Everything not listed here is checked in full.

**Checked in part**

- [Streaming reads](#streaming-reads): Pub/Sub streams are located by topic. A file stream (`fileio.MatchContinuously`) does not say which files it reads, so an input it may be is reported as unverifiable.
- [Columns match the contract](#columns-match-the-contract): Columns are checked where the pipeline states them: a Parquet write's pyarrow schema, an Avro schema, a BigQuery table schema, or typed rows (`ReadFromParquet(..., as_rows=True)`). A write of Python dictionaries has no columns to compare, so it is refused unless its output declares none.
- [The file format matches](#the-file-format-matches): The format comes from the sink the write uses. A text write cannot tell CSV from JSON or plain text.
- [The save mode matches](#the-save-mode-matches): Only BigQuery writes state a save mode. A contract that declares one for a file or Pub/Sub write has that write refused.
- [Datasets are registered in a catalog](#datasets-are-registered-in-a-catalog): BigQuery tables are checked against the `bigquery` catalog. Files and Pub/Sub have no catalog in Beam, so a requirement on them is never met.
- [Turn it on without changing the job](#turn-it-on-without-changing-the-job): Turned on with `--runner=invaract_beam.InvaractRunner` and the `--invaract_...` flags, with the `invaract_beam` package and Java 11 or later where the job is submitted. A job that sets its runner in its own code, or does not pass its command line to Beam, bypasses it.
- [Writes a starting contract for you](#writes-a-starting-contract-for-you): Drafts list every read and write the pipeline locates, with columns where it states them; one without columns gets a draft that declares none, and says so.
- [Refuses writes it cannot understand](#refuses-writes-it-cannot-understand): A step that ends the pipeline without being a write Invaract recognizes is refused, unless the platform trusts it (`--invaract_trusted_steps`). A step that writes to an outside system in the middle of the pipeline cannot be seen before it runs.

**Not checked**

- [Rules about the transformation's shape](#rules-about-the-transformations-shape): Python filters, joins and groupings are Python code, so Beam's description of the pipeline names no columns for them. Refused if your contract relies on it.
- [Your own rule types](#your-own-rule-types): Custom rule types judge row-level changes, and Beam has none. Refused if your contract relies on it.
- [Proves value constraints](#proves-value-constraints): Steps in a Python pipeline are Python code Invaract cannot look inside, so it cannot prove what a column will contain. Refused if you turn on `staticDataQuality`.
- [Datasets are used as their declared type says](#datasets-are-used-as-their-declared-type-says): Needs to know how each column is used, which Beam's description of a pipeline does not say. Refused if you turn on `roleConsistency`.
- [Fingerprints the transformation's logic](#fingerprints-the-transformations-logic): A fingerprint of the pipeline's shape alone would miss the logic inside its steps, so none is computed. Turning `computeFingerprint` on does not stop the job; no fingerprint is published, and a warning in the log says why.
- [Recognizes built-in functions](#recognizes-built-in-functions): Python pipelines call Python functions, not the engine's own named functions. Only fingerprints use it.
- [Tracks sensitive columns](#tracks-sensitive-columns): Needs to trace each column through the pipeline, which Beam's description of it does not allow. No sensitivity report is produced; tags on the contract's fields are kept but not followed through the job.
- [Traces each output column to its inputs](#traces-each-output-column-to-its-inputs): Steps in a Python pipeline are Python code Invaract cannot look inside, so it cannot tell which input columns each output column comes from. Checks that rely on it, such as proving value constraints, are not available.

**Does not apply**

- [MERGE, UPDATE and DELETE](#merge-update-and-delete): Beam has no MERGE, UPDATE or DELETE: a pipeline only writes new output.
- [Table rollbacks and similar changes](#table-rollbacks-and-similar-changes): Beam has no operation that changes a table without writing rows.
- [Rules for MERGE, UPDATE and DELETE](#rules-for-merge-update-and-delete): Beam has no MERGE, UPDATE or DELETE to make rules about.
- [Sees through checkpoints and caches](#sees-through-checkpoints-and-caches): A Beam pipeline has no checkpoint or cache that hides what produced its data.

### Apache Spark 3.5

Everything not listed here is checked in full.

**Checked in part**

- [Streaming writes](#streaming-writes): Streaming writes are checked for file and Delta Lake streams; a connector without a streaming sink of its own cannot be written to as a stream at all.
- [Writes land where the contract says](#writes-land-where-the-contract-says): Locations are matched by path. A CREATE TABLE AS SELECT into a new Hive table, and ClickHouse tables, have known gaps in how their location is worked out.
- [Every promised output is written](#every-promised-output-is-written): A declared output the job never writes is found only when the job ends, after its other writes have happened. It is reported then, and with `spark.invaract.failOnMissingOutput=true` the job also exits with a failure.
- [Rules for MERGE, UPDATE and DELETE](#rules-for-merge-update-and-delete): Checked in full for Delta Lake and Iceberg copy-on-write tables. On an Iceberg merge-on-read table, an `allowed_update_columns` rule cannot be checked, so the UPDATE is refused rather than let through.
- [Traces each output column to its inputs](#traces-each-output-column-to-its-inputs): A user-defined function is a black box: an output column it computes is traced only to the columns passed into it.
- [Sees through checkpoints and caches](#sees-through-checkpoints-and-caches): A checkpoint or cache made after Invaract was installed is seen through. One made before, or one Spark has dropped from its cache, hides what is behind it.

## What each check means

### Batch reads

Sees which datasets a batch job reads, so they can be checked against the contract's inputs.

Where an engine does not do it: A read it cannot see cannot be matched to the contract's inputs: a declared input it may be is reported as unverifiable. [Read more](/reference/connector-support/)

### Streaming reads

Sees which datasets a streaming job reads, as it does for a batch job.

Where an engine does not do it: A streaming read it cannot place is reported as unverifiable rather than checked. [Read more](/reference/connector-support/)

### Batch writes

Checks every batch write against the contract before the data is written.

Where an engine does not do it: A batch write it does not recognize is refused, not passed unchecked. [Read more](/reference/connector-support/)

### Streaming writes

Checks a streaming write against the contract when the stream starts.

Where an engine does not do it: A streaming write it does not recognize is refused, not passed unchecked. [Read more](/reference/connector-support/)

### MERGE, UPDATE and DELETE

Treats a row-level change to a table as a write, and checks it like one.

Where an engine does not do it: Row-level changes are not checked as writes. [Read more](/guides/enforcing-dml-rules/)

### Table rollbacks and similar changes

Checks an operation that changes what a table holds without writing new rows, such as rolling it back to an earlier snapshot.

Where an engine does not do it: Such an operation is not checked against the contract.

### Required inputs are read

Stops a write that does not read an input the contract requires (`MISSING_INPUT`). It can also stop a write that reads a dataset the contract does not list (`UNDECLARED_INPUT`).

Where an engine does not do it: If your contract relies on it, the job is refused before it writes anything (`UNSUPPORTED_CONTRACT_FEATURE`), rather than run with that part unchecked. [Read more](/guides/writing-a-contract/)

### Writes land where the contract says

Stops a write to any location other than the one the contract declares (`OUTPUT_LOCATION_MISMATCH`).

Where an engine does not do it: If your contract relies on it, the job is refused before it writes anything (`UNSUPPORTED_CONTRACT_FEATURE`), rather than run with that part unchecked. [Read more](/guides/writing-a-contract/)

### Every promised output is written

Catches a job that ends without writing an output its contract declares (`MISSING_OUTPUT`).

Where an engine does not do it: A job that skips one of its outputs is not caught. [Read more](/guides/installing-the-enforcement-rule/#every-declared-output-must-be-written)

### Columns match the contract

Stops a write whose columns are missing, of the wrong type, or nullable where the contract says they must not be.

Where an engine does not do it: If your contract relies on it, the job is refused before it writes anything (`UNSUPPORTED_CONTRACT_FEATURE`), rather than run with that part unchecked. [Read more](/guides/writing-a-contract/)

### Nested columns are compared in full

Compares arrays, maps and structs element by element, not only by their outer type.

Where an engine does not do it: If your contract relies on it, the job is refused before it writes anything (`UNSUPPORTED_CONTRACT_FEATURE`), rather than run with that part unchecked. [Read more](/reference/contract-format/)

### The file format matches

Stops a write in a different format from the one the contract declares, such as CSV where it says Parquet (`OUTPUT_FORMAT_MISMATCH`).

Where an engine does not do it: If your contract relies on it, the job is refused before it writes anything (`UNSUPPORTED_CONTRACT_FEATURE`), rather than run with that part unchecked. [Read more](/reference/contract-format/)

### The save mode matches

Stops a write that appends where the contract says overwrite, or the other way round (`OUTPUT_SAVE_MODE_MISMATCH`).

Where an engine does not do it: If your contract relies on it, the job is refused before it writes anything (`UNSUPPORTED_CONTRACT_FEATURE`), rather than run with that part unchecked. [Read more](/reference/contract-format/)

### Datasets are registered in a catalog

Checks that a dataset the contract says must be catalog-registered is, in the catalog it names.

Where an engine does not do it: If your contract relies on it, the job is refused before it writes anything (`UNSUPPORTED_CONTRACT_FEATURE`), rather than run with that part unchecked. [Read more](/guides/requiring-catalog-registration/)

### Rules for MERGE, UPDATE and DELETE

Enforces rules about row-level changes: the columns a MERGE must match on, no DELETE without a condition, the columns an UPDATE may change.

Where an engine does not do it: If your contract relies on it, the job is refused before it writes anything (`UNSUPPORTED_CONTRACT_FEATURE`), rather than run with that part unchecked. [Read more](/guides/enforcing-dml-rules/)

### Rules about the transformation's shape

Enforces rules about how the job computes its output: the columns it must group by, join on or filter on, and no join without a condition.

Where an engine does not do it: If your contract relies on it, the job is refused before it writes anything (`UNSUPPORTED_CONTRACT_FEATURE`), rather than run with that part unchecked. [Read more](/guides/enforcing-transformation-rules/)

### Your own rule types

Runs rule types your organization writes itself, named in the contract's `customRuleTypes`.

Where an engine does not do it: If your contract relies on it, the job is refused before it writes anything (`UNSUPPORTED_CONTRACT_FEATURE`), rather than run with that part unchecked. [Read more](/guides/enforcing-dml-rules/#write-your-own-rule-type)

### Proves value constraints

Proves from the job's own logic that a column always meets its declared constraint (never null, one of a set of values, in a range), before any row is written. Turned on with the `staticDataQuality` setting.

Where an engine does not do it: If you turn on `staticDataQuality`, the job is refused before it writes anything (`UNSUPPORTED_CONTRACT_FEATURE`), rather than run with that part unchecked. [Read more](/guides/verifying-static-data-quality/)

### Datasets are used as their declared type says

Checks that each dataset is used the way its declared type (`SOURCE`, `CONTROL`, `DATA_ASSET`) says. Turned on with the `roleConsistency` setting.

Where an engine does not do it: If you turn on `roleConsistency`, the job is refused before it writes anything (`UNSUPPORTED_CONTRACT_FEATURE`), rather than run with that part unchecked. [Read more](/guides/declaring-input-and-output-types/)

### Fingerprints the transformation's logic

Adds a fingerprint of what the job computes to each check, so a change to its logic can be spotted even when the columns stay the same. Turned on with the `computeFingerprint` setting.

Where an engine does not do it: Turning `computeFingerprint` on does not stop the job; no fingerprint is published, and a warning in the log says why. [Read more](/guides/fingerprinting-transformations/)

### Recognizes built-in functions

Knows which of the engine's built-in functions give a different answer on each run (random numbers, the current time), so fingerprints say so and compare across engines.

Where an engine does not do it: Only fingerprints use it. [Read more](/guides/fingerprinting-transformations/)

### Tracks sensitive columns

Reports output columns built from inputs tagged as sensitive (personal data, say) as sensitive too.

Where an engine does not do it: No sensitivity report is produced; tags on the contract's fields are kept but not followed through the job. [Read more](/reference/contract-format/)

### Traces each output column to its inputs

Works out which input columns each output column is built from. The deeper checks above rely on it.

Where an engine does not do it: Checks that rely on it, such as proving value constraints, are not available.

### Sees through checkpoints and caches

Still traces a job back to what it read when it checkpoints or caches data between reading and writing.

Where an engine does not do it: A checkpoint or cache hides what is behind it: an input read only behind one is reported as unverifiable.

### Organization-wide policy

Checks every contract a job uses against a policy your platform team sets, such as "every output must be catalog-registered".

Where an engine does not do it: A policy is not applied. [Read more](/guides/enforcing-organizational-policy/)

### Turn it on without changing the job

A platform team can attach Invaract to a job it does not own, through the engine's own launch settings, with no change to the job's code.

Where an engine does not do it: The job's own code has to install Invaract. [Read more](/reference/configuration/)

### Locations resolved when the job starts

A contract can name a location as `ref://<id>`, resolved from a mapping file when the job starts, so one contract works in every environment.

Where an engine does not do it: Contracts must name real locations. [Read more](/guides/resolving-dataset-locations/)

### Contracts from a registry

A job can be pointed at a contract kept in a contract registry (`registry://<id>@<version>`) instead of a file.

Where an engine does not do it: Contracts must be files the job can read. [Read more](/guides/using-a-contract-registry/)

### Sends events to your systems

Publishes an event for every check, pass or fail, to a file, an HTTP endpoint, Kafka or a sink you write.

Where an engine does not do it: No events are published. [Read more](/guides/notification-sinks/)

### Writes a starting contract for you

In dry-run mode, blocks nothing and writes out the contract each write would need, as a draft to review.

Where an engine does not do it: There is no dry-run mode: contracts are written by hand. [Read more](/guides/dry-run-mode/)

### Refuses writes it cannot understand

Refuses a step that writes data in a way Invaract cannot check (`UNVERIFIABLE_WRITE`), rather than letting it through unchecked.

Where an engine does not do it: A write it cannot understand may go through unchecked. [Read more](/concepts/fail-closed/)
