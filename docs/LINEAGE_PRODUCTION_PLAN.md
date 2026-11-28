# Spark Lineage: Plan for Production-Grade Support

Status: plan. Written after the third review of Invaract's Spark column lineage (docs/OPENLINEAGE.md, section 11). That
review closed every gap a fix could close. What is left needs more than a fix: a dependency, a test fixture, a decision
about a convention, or a design choice that affects enforcement as well as lineage. For each item this document gives:

- what Invaract does today, measured where it could be;
- what production-grade support means for a team running it on every job;
- the design;
- how it is proven (a corpus job, a fixture, a parity test);
- what it costs.

"Measured" means a real run, with the probe or test named. "From the code" means read but not yet run, because the
library or service it needs is not available to the tests yet. Closing that gap is part of each item.

## What production grade means here

An item is done when all of these hold. They are the same gates every Spark lineage change already passes:

1. **Held by a real job.** The construct is a job in `lineage-crosscheck`'s corpus (or in the PySpark corpus, item 2),
   with its lineage in the baseline and every difference from openlineage-spark and Spline explained. A construct no
   job can run is documented as a limit on the generated page.
2. **Named as the other integrations name it.** Its datasets are named as openlineage-spark names them, so the same
   dataset is one node in a lineage graph. `NamingParitySpec` holds this for every location form a job cannot reach.
3. **Enforcement decided.** Where the item changes how a dataset is located, the contract side is specified and
   fails closed: a contract can declare the dataset, and a write Invaract cannot place is refused, not passed.
4. **Usual gates.** At least 70% mutation score on changed files, MiMa, coverage, the docs-site page, and
   `./dev/test`/`./dev/regression`.

## Order

| # | Item | Why this order | Size |
|---|---|---|---|
| 1 | Kafka (and other non-file streaming sources and sinks) | Wrong for enforcement as well as lineage: every topic is one dataset | M |
| 2 | PySpark in CI | Most Spark jobs are written in Python. Its operators are translated, but no PySpark job runs in CI | M |
| 3 | Schema-evolving MERGE | A column the MERGE adds has no lineage at all | S |
| 4 | Generated columns | A generated column has no lineage, though its expression is in the table's metadata | S |
| 5 | Connectors and catalogs beyond the tested set | Each one is a dataset naming question, and some are enforcement questions too | M per connector, S for naming |
| 6 | Tables a catalog places | Measured correct for a staging catalog; non-staging catalogs still to confirm | S |
| 7 | Streaming: lineage per micro-batch | A stream's run carries one write's lineage, not one per batch | M |
| 8 | Nested-field lineage | Needs a convention OpenLineage does not define yet | M, after a decision |

Already closed by the third review, and no longer in this plan:

- PySpark's pandas and Arrow operators, and pandas-on-Spark's default index (`AttachDistributedSequence`, which hid
  every pandas-on-Spark job);
- streaming watermarks, `dropDuplicatesWithinWatermark` and `mapGroupsWithState`;
- typed Dataset operations and RDDs;
- the MERGE convention for untouched rows;
- object-store naming (Azure, trailing `/`, bucket roots);
- the naming of Beam's Kafka and Pub/Sub topics.

## 1. Kafka and other non-file streaming sources and sinks

**Today (from the code).** A source read through `StreamingRelationV2` is located by its table's location when the
table has one, else by the source's short name (`SparkPlanAdapter.streamingRelationV2LocationOf`). Kafka's table has no
location, so every Kafka source in every job is the one dataset `kafka`:

- a contract cannot say which topic a job may read;
- every topic is one node in the lineage graph.

How a Kafka *sink* is located has not been measured. A streaming write goes to `KafkaTable` through `WriteToStream`. A
batch `df.write.format("kafka")` goes through `SaveIntoDataSourceCommand`, because `kafka` is in Spark's
`spark.sql.sources.useV1SourceList`. Neither has a path. `spark-sql-kafka-0-10` is not on any test classpath, so none
of this has run.

openlineage-spark names a topic as the conventions say: namespace `kafka://<bootstrap server>`, name the topic
(`KafkaRelationVisitor`, `KafkaBootstrapServerResolver`). Beam's adapter already locates a topic as `<servers>/<topic>`,
format `kafka`. Since the third review, OpenLineage naming turns that into `kafka://<first server>`, `<topic>`
(`OpenLineageNaming.dataset(..., format)`).

**Production grade.**

- A Kafka read is located per topic, the same way Beam locates it: `broker:9092/orders`, format `kafka`. A contract
  that declares `orders` matches the topic on any cluster, and one that declares `broker:9092/orders` matches only that
  cluster's topic. Both use the existing `LocationMatching.fromParts` rule.
- A Kafka write is located the same way, from its `topic` option.
- A topic decided per row (a `topic` column, no `topic` option) cannot be placed before the write. Under a contract it
  is refused as `UNVERIFIABLE_WRITE`, unless the contract's output declares the topic column's values are allowed. In
  lineage, the topic column's sources are reported, as for any other column.
- A read by `subscribePattern` or `assign` cannot name its topics before the query runs:
  - `assign` names them: one read per topic in its JSON;
  - a pattern is reported as a read of the pattern's location (`broker:9092/orders-.*`). A contract can declare it, but
    the pattern is not matched against topics: a contract either names the pattern or the read is undeclared;
  - this is documented, and the conformance kit's capability declaration says so.
- Column lineage reaches Kafka's fixed columns (`key`, `value`, `topic`, `partition`, `offset`, `timestamp`,
  `headers`). A column parsed out of `value` (`from_json`, `from_avro`) has `value` as its source. Which field of the
  payload it came from needs nested lineage (item 8).
- The same shape for the other non-file sources that have a stable identity:
  - Kinesis (`aws-kinesis` stream name);
  - Pub/Sub Lite;
  - Event Hubs (namespace and hub);
  - `rate` and `socket`, which have no identity, stay as their name and are documented as not declarable.

**Design.**

- A `StreamingSourceLocation` table in spark-adapter, keyed by the source's short name, that reads the source's
  options (`kafka.bootstrap.servers`, `subscribe`, `assign`, `subscribePattern`). It returns one or more locations and
  a format.
- `streamingRelationV2LocationOf` and the batch Kafka relation (`KafkaRelation`, matched by class name as JDBC's is)
  use it. A source with several topics becomes a `Union` of reads, the shape a several-path file read already has.
- The sink side reads `WriteToStream`'s and `SaveIntoDataSourceCommand`'s options through the same table.
- No compile-time dependency on `spark-sql-kafka`. Options are read as strings, and classes are matched by name, the
  way JDBC's are.

**Proof.**

- `spark-sql-kafka-0-10` and its test jar (`KafkaTestUtils`: an in-process broker, no Docker, so it runs on the macOS
  and Windows CI legs too) as test dependencies of spark-adapter and lineage-crosscheck.
- Corpus jobs:
  - a batch read and write;
  - a stream from one topic to another;
  - `subscribe` to two topics;
  - a `topic` column;
  - a `from_json` over `value`;
  - a `foreachBatch` MERGE into Delta from Kafka, the most common production shape.
- Each job is compared with openlineage-spark, which supports Kafka.
- A `ContractEnforcementRuleSpec` case for each enforcement rule above.

**Cost.** About two weeks. The enforcement change is visible to users: a contract that declares the input `kafka`
today must name its topic. The migration guide and the connector table say so, and the old form is refused with a
message naming the topic it should declare.

## 2. PySpark in CI

**Today (measured).** `pyspark_probe.py` ran real PySpark 3.5.9 jobs with Invaract and openlineage-spark both capturing:

| Construct | Invaract | openlineage-spark |
|---|---|---|
| a Python UDF (`@udf`) | the UDF's input columns, opaque `TRANSFORMATION` | none for the UDF's column |
| a pandas scalar UDF | the same | none |
| a pandas grouped-aggregate UDF | `AGGREGATION` | the same |
| `mapInPandas`, `applyInPandas`, a cogroup's `applyInPandas`, `mapInArrow` | none (measured before the third review's fix) | none |
| pandas-on-Spark (`ps.read_parquet`, a computed column, `to_parquet`) | none (measured before the fix) | every column |

The third review translated the plan nodes behind the last two rows: `MapInPandas`, `PythonMapInArrow`,
`FlatMapGroupsInPandas`, `FlatMapCoGroupsInPandas`, `FlatMapGroupsInPandasWithState` and pandas-on-Spark's
`AttachDistributedSequence`. `LineageEdgeCasesSpec` holds them, with each node built in Scala as PySpark builds it.

What no test runs is a PySpark job itself:

- pandas-on-Spark's other default index types (`sequence` is a `Window`, `distributed` a function call: both already
  translated) and its `__natural_order__` columns;
- Spark Connect, where the plan is built on the server from a client's protobuf;
- the Python-side shapes that change between PySpark releases.

Two environment facts were measured:

- PySpark 3.5 needs `pandas<2.2`, `pyarrow==14.0.2` and `numpy<2`;
- on Java 17 and later, Arrow fails at execution unless the JVM has `--add-opens=java.base/java.nio=ALL-UNNAMED` and
  `-Dio.netty.tryReflectionSetAccessible=true` (the flags Spark's own documentation gives). Lineage is captured at
  check time, before Arrow runs, so the check itself was unaffected.

**Production grade.**

- A PySpark corpus in lineage-crosscheck, run in CI beside the Scala corpus, with the same baseline and page:
  - every row above;
  - pandas-on-Spark with each index type;
  - `toPandas`/`createDataFrame(pandas)` round trips;
  - a Python UDTF;
  - a Spark Connect session.
- Each job's lineage is compared with openlineage-spark's. Spline has no Python integration beyond the JVM agent's,
  which sees the same plan.

**Design.**

- A `python/` folder in lineage-crosscheck holding the jobs, one function each, as `Corpus.scala` holds the Scala ones.
- A Scala test that starts each job in a Python process (`INVARACT_TEST_PYTHON`, as beam-adapter's Python tests do),
  with Invaract writing OpenLineage events to a file and openlineage-spark writing to another.
- The two files are read by the existing `Captured`/`Comparison` code, so differences, notes and the baseline work as
  they do for Scala jobs.
- The generated page gets a "PySpark" section with each job's Python code.
- A Spark Connect job starts `SparkConnectServer` in-process, with `spark.plugins` installing Invaract.

**Proof.** The corpus itself, in CI on Linux with Java 17 (Arrow's flags set), as a job of its own (`crosscheck-python`)
gated by `changes` like the Scala cross-check.

**Cost.** About a week and a half. The Python environment is pinned in a `requirements.txt` beside the jobs.

## 3. Schema-evolving MERGE

**Today (measured).** A Delta MERGE with `spark.databricks.delta.schema.autoMerge.enabled` and `UPDATE SET *` /
`INSERT *` from a source with a column the table lacks (`status`):

- the column is in the write's schema (`outputSchema`), and the schema facet shows it;
- it has no column lineage.

The lineage-only plan (`WriteCommandSupport.mergeClauses`) builds each clause's rows over the table's existing columns
(`target.output`), so a column the MERGE adds has no row to come from. An append with `mergeSchema` is already correct
(measured: its query has the column).

**Production grade.**

- Every column the MERGE adds gets lineage from each clause that assigns it, as an existing column does. A clause that
  does not assign it contributes no source; its rows have null there.
- The same for Iceberg's schema-evolving MERGE where the Iceberg version supports it, and for Delta's `WITH SCHEMA
  EVOLUTION` clause on Delta versions that have it.

**Design.** `mergeClauses` takes its output columns from the write's schema, not the table's:

- an existing column as today;
- a new column from each clause's assignment to that name. With schema evolution Delta resolves `*` against the source,
  so the assignment is present in the resolved clause; that will be confirmed against Delta 3.3.3's
  `DeltaMergeIntoClause` when this is built, not assumed.
- `ColumnDependencies` already types each source as it reaches the column.

**Proof.**

- A corpus job (`delta-merge-schema-evolution`) beside the existing MERGE jobs;
- a `ContractEnforcementRuleSpec` lineage-plan case;
- the Iceberg form gated on the verified Iceberg versions.

**Cost.** Two to three days.

## 4. Generated columns

**Today (measured).** A Delta table with `doubled BIGINT GENERATED ALWAYS AS (amount * 2)`, written by `INSERT INTO gen
(id, amount) SELECT ...`:

- `doubled` is in the write's schema;
- it has no lineage: the query does not compute it, Delta does, from metadata Invaract does not read.

An identity column (`GENERATED ALWAYS AS IDENTITY`) has no source column, which is correct. A column `DEFAULT` is a
constant, which is also correct.

**Production grade.** A generated column's lineage is its generation expression's columns: `doubled <- amount`,
`TRANSFORMATION`, traced through the query to the read that supplied `amount`. An identity column is documented as
having no source.

**Design.**

- Delta keeps the expression in the field's metadata (`delta.generationExpression`), which the table's schema carries.
- Where the write's schema has a column the query does not produce and that key is present:
  - parse the expression with the session's parser (`sessionState.sqlParser.parseExpression`);
  - resolve its names against the query's output columns by name, under `spark.sql.caseSensitive`;
  - append it to the lineage-only plan's projection.
- An expression that does not parse or resolve leaves the column without lineage and logs why. Enforcement is
  untouched: Delta checks generated values itself.

**Proof.**

- A corpus job (`delta-generated-column`). openlineage-spark does not read generation expressions: the job's note
  says so.
- A unit case for an expression that fails to resolve.

**Cost.** Two days.

## 5. Connectors and catalogs beyond the tested set

**Today.**

- **Tested** (docs-site "Connector Support"): Delta, Iceberg, Parquet, CSV, Avro, Hive, JDBC (H2), ClickHouse.
- **openlineage-spark** also handles:
  - BigQuery, Snowflake, Cosmos DB, Kusto, Cassandra and Kafka;
  - JDBC dialect naming for PostgreSQL, MySQL, Oracle, SQL Server, Db2, Teradata, Trino, Crate, Derby and OceanBase;
  - catalog symlinks for Glue, Hive Metastore, Hadoop, REST, Nessie, Unity Catalog, BigQuery Metastore, Snowflake,
    S3 Tables and Databricks Delta.
- **A connector Invaract does not recognise** fails closed under a contract (`UNVERIFIABLE_WRITE`). In dry-run or
  lineage-only use, it is published by whatever location the plan gives, which may be the connector's name.

`NamingParitySpec` (lineage-crosscheck) now holds Invaract's names to openlineage-spark's own naming code for every
file system form and for the JDBC dialects above. That needs no database, so the naming half of a JDBC dialect is
already held. Its first run found, and the third review fixed, eight ways Invaract named a JDBC table otherwise:

- default ports;
- SQL Server's `databaseName` property;
- Teradata's `DATABASE` and `DBS_PORT`;
- Oracle's thin URLs;
- Db2, MariaDB and Redshift paths;
- Trino's `catalog/schema`;
- a URL query that hid the table;
- a `dbfs:` trailing `/`.

One thing found there is an enforcement question, not a naming one. A JDBC table's *enforcement* location is
`jdbc:<the connection URL>/<table>`, so a URL that carries `user=`/`password=` in its query carries them into the
location a violation message shows. OpenLineage names strip them. Enforcement should strip them as well, under the
same rule, with a contract still matching the table without them.

**Production grade, per connector.** The `add-spark-connector` skill's ledger, both surfaces:

- every read and write shape is covered or fails closed;
- the dataset is named as openlineage-spark names it;
- a catalog table carries the `symlinks` facet as openlineage-spark gives it. For Hive that is `hive://<metastore
  host>:<port>`, `db.table`; for Glue it is `arn:aws:glue:<region>:<account>`, `table/db/table`.

**Order, by how often a production Spark job uses each.**

1. JDBC dialects: PostgreSQL, MySQL and SQL Server against real servers. The naming is already held; reads, writes and
   pushed-down queries are not.
2. Hive Metastore and Glue symlinks. These change only the symlink, not the dataset's name.
3. BigQuery (`spark-bigquery-connector`) and Snowflake (`spark-snowflake`), each a relation class matched by name, with
   its table identity read from its options.
4. Unity Catalog and Nessie, through Iceberg's and Delta's catalogs, which are already translated: naming and symlinks
   only.
5. Cassandra, MongoDB and Elasticsearch, on demand.

**Proof.** Per connector, the skill's investigation, a fixture (a real server where one runs in CI, as ClickHouse's does;
the connector's own emulator otherwise), and a corpus job compared with openlineage-spark.

**Cost.** Naming and symlinks: a day per catalog. A connector's read and write surface: one to two weeks each, as
ClickHouse's was.

## 6. Tables a catalog places

**Today (measured).** A CTAS into an Iceberg catalog that chooses where the table is stored (`InMemoryCatalog`, probed
with `CREATE TABLE mem.db.t USING iceberg AS SELECT ...`): the location is known when the write is checked, through the
staged table (`createdAt`), and equals the location `DESCRIBE TABLE EXTENDED` reports after the commit. So for a
catalog that stages its creates, which Iceberg's `SparkCatalog` always does, the documented limit ("named by its
catalog identifier") does not apply.

Not measured:

- a REST catalog, whose server picks the location when the table is staged;
- a Hive or Glue catalog;
- a DSv2 catalog that does not stage at all.

**Production grade.**

- A table a write creates is named by where it is stored whenever the catalog says before the write.
- When the catalog says only after the write, the run's completion event names it by its location, with the catalog
  identifier as a symlink. That way a backend sees one dataset, not two.
- The limit on the generated page is narrowed to what stays true.

**Design.**

- Confirm the staged path for REST (Iceberg's `RESTCatalogServer` from `iceberg-open-api` test fixtures, in-process)
  and Hive (the embedded metastore the Hive tests already use).
- For a non-staging catalog, look the table up in the existing `QueryExecutionListener` after a successful write and
  name the output by its location in the completion event. This needs a rule in `OpenLineageMapper`: an output named
  by identifier at check time and by location at completion is one dataset, and its earlier name becomes a symlink.

**Proof.** A corpus job per catalog kind. openlineage-spark resolves locations from the catalog too, so the jobs are
compared.

**Cost.** Two days to confirm. A week if the non-staging path is needed.

## 7. Streaming: lineage per micro-batch

**Today.** A streaming query is checked once, when it starts, and its run carries that lineage (docs/OPENLINEAGE.md,
section 12, item 16). That is correct: the plan does not change between batches. But a backend sees no activity while
the query runs, and no per-batch statistics. openlineage-spark emits an event per micro-batch.

**Production grade.**

- A long-running query's run shows it is alive:
  - one `RUNNING` event per micro-batch (or per N batches, or per interval, so a query with a one-second trigger does
    not flood the backend), carrying the batch's `outputStatistics` (rows, from the `StreamingQueryProgress`);
  - the run ends when the query stops.

**Design.**

- A `StreamingQueryListener` in spark-adapter, registered with the check rule. On each progress it publishes a
  `WriteEvent` with the batch's row count to the query's run (the run id is the query's id).
- `onQueryTerminated` calls `NotificationSink.endRun`.
- The interval is a neutral setting (`openLineageStreamingInterval`), so a platform can set it through `--conf`
  (External Attachability).

**Proof.** The corpus's streaming jobs, with the events per batch counted. A unit case for the interval.

**Cost.** About a week.

## 8. Nested-field lineage

**Today.**

- The schema facet is nested (a struct's fields under it).
- Column lineage is by top-level column. A field of a struct built in the plan is traced to its own source (the third
  review). A column read out of a struct (`payload.a`) has the struct column (`payload`) as its source.
- openlineage-spark also reports top-level columns.
- The OpenLineage column-lineage facet's `field` is a free string; the specification defines no convention for a
  nested field.

**Production grade.**

- Nested lineage is opt-in and off by default, so the default matches openlineage-spark and every backend reads it as
  it does today.
- When it is on, an edge to a struct's field names the field by path (`payload.a`, the form the schema facet's nesting
  implies).

**Design.**

- `ir.Lineage` already follows `GetStructField` chains when it builds struct fields.
- The change is to keep the path on the source side: `LineageSource(location, field, path)`.
- Emit it only under the setting (`openLineageNestedFields=true`, a neutral setting).

**Decision first.**

- Which form a backend joins on: DataHub has its own field-path form, and Marquez treats names as opaque strings.
- Whether OpenLineage is settling a convention. If it is, Invaract follows it rather than inventing one.

**Proof.** Corpus jobs over `events.payload`, with the setting on and off.

**Cost.** About a week once the convention is decided.
