---
title: FAQ
description: Frequently asked questions about Invaract.
sidebar:
  order: 2
---

### Is there a published library I can add as a dependency?

Not on Maven Central yet. You can build the modules from source
([Installation](/getting-started/installation/)), or download prebuilt Spark jars from the
project's GitHub releases and use them from Maven, Gradle or plain `spark-submit`
([Use Prebuilt Jars Without sbt](/guides/using-prebuilt-jars-without-sbt/)). The Beam Python
package installs with `pip` straight from the repository
([Beam Python pipelines](/guides/using-invaract-with-beam-python/#what-a-job-needs)).

### Which engines does Invaract work with?

Apache Spark 3.5 and Apache Beam, for pipelines written in Java or Python. Each engine
checks a slightly different set of things; [What Each Engine Checks](/reference/what-each-engine-checks/)
lists them side by side, and a contract that relies on something an engine can't check is
refused rather than half-checked. Another engine needs its own adapter: the
transformation IR the checks run on is engine-independent (see
[The Transformation IR](/concepts/transformation-ir/)).

### Can I turn Invaract on without changing the job's code?

Yes, and that is the intended way. On Spark, add `spark.sql.extensions` and
`spark.invaract.contract` to `spark-submit`; on Beam, set `--runner` and the contract option.
Every other setting works the same way: see [Configuration Settings](/reference/configuration/).

### Can I try it on a job without blocking anything?

Yes: [dry-run mode](/guides/dry-run-mode/) blocks nothing and writes out the contract each
write would need. It is the usual way to start on an existing job.

### Can I check a Beam pipeline in CI, without running it?

Yes. `--invaractCheckOnly=true` (Java) or `--invaract_check_only=true` (Python) checks the
pipeline and stops before handing it to a runner, so the build needs no data, cluster or
credentials. See the [Java](/guides/using-invaract-with-apache-beam/#check-a-pipeline-in-ci-without-running-it)
and [Python](/guides/using-invaract-with-beam-python/#check-a-pipeline-in-ci-without-running-it) guides.

### Does Invaract verify that my transformation's logic is *correct*, not just its shape?

Not in general. The checks are structural: does each output land at the right location,
with the right schema, format and save mode; does the job read what the contract says;
do declared rules about the transformation hold (grouping and join keys, filters, and on
Spark row-level `MERGE`/`UPDATE`/`DELETE`). Narrow value properties, such as "this column is
never null", can be proved from the job's logic on Spark
([Verify Static Data Quality](/guides/verifying-static-data-quality/)). See
[What is Invaract?](/introduction/what-is-this/#what-it-verifies-today).

### What happens if my job writes to a connector or in a shape Invaract doesn't recognize?

The write is rejected, not silently allowed through — see
[Fail-Closed by Default](/concepts/fail-closed/). Check
[Connector Support](/reference/connector-support/) for what's recognized today.

### Can I use my existing ODCS contracts as-is?

Invaract's format mirrors ODCS and keeps any fields it doesn't recognize rather than
rejecting them (see [Data Contracts](/concepts/data-contracts/)). It interprets the
locations, schemas, formats and save modes of inputs and outputs, field constraints,
catalog requirements, dataset types, and its own rule types
([transformation shape](/guides/enforcing-transformation-rules/) and
[row-level DML](/guides/enforcing-dml-rules/) rules). Other fields (SLAs, governance
policies) parse and are kept, but aren't enforced.

### Does a passing contract mean my job's output is definitely correct?

It means the job's actual write matches what the contract declares. It doesn't verify
business logic (that an aggregation computed the right value, for instance). Treat it as
"the shape is what was promised," not "the values are right."

### Where do I report a bug or request a connector?

Open an issue on [GitHub](https://github.com/mlltx/Invaract/issues).
