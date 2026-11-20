# JDBC support

[← Back to Spark Adapter](../SPARK_ADAPTER.md#connector-support)

Spark's generic JDBC data source (`spark.read.jdbc(...)`, `df.write.jdbc(...)`,
`format("jdbc")`) is part of `spark-sql` itself, so it needs no extra dependency. Its tests
(`JdbcConnectorSpec`) run against an embedded, in-memory Apache Derby database, which is on
the test classpath through Spark's Hive dependencies; no database server is started.

**One location string for a table, read or written.** A JDBC table is located as
`jdbc:` + the connection URL + `/` + the table name:
`jdbc:jdbc:derby:memory:orders_db;create=true/ORDERS`. The URL's own `jdbc:` makes the prefix
appear twice; that is expected. A read has always been located this way
(`SparkPlanAdapter.jdbcLocationOf`, reading the relation's `JDBCOptions`); a write used to be
located by its whole options map rendered as text, which no contract could match and which
could put a password into an error message. Both now go through
`SparkPlanAdapter.jdbcLocation`, so they cannot drift apart. A save with neither a `path` nor
a JDBC `url` and `dbtable` is located as `<source class>(no path)`, with a diagnostic, and the
options map is never shown.

The format is `jdbc` on both sides (the provider's `DataSourceRegister.shortName()` for a
write; `SparkPlanAdapter.readFormatOf` for a read).

This pass covered what a contract needs to name a JDBC table and to check a job that reads or
overwrites one. It did not survey vendor-specific behaviour (type mapping per database,
`truncate`/`cascadeTruncate`, isolation levels, partitioned reads); those rows are marked ❓
below rather than assumed.

## JDBC operation-surface coverage ledger

| Operation | Status | Evidence / next step |
|---|---|---|
| `spark.read.jdbc(url, table, props)` / `format("jdbc").option("dbtable", ...)` | ✅ Covered | `LogicalRelation` over `JDBCRelation`, located by `jdbcLocationOf`. `JdbcConnectorSpec`'s "a JDBC table read and written is the same location to the contract", under `rejectUndeclaredInputs`. |
| `format("jdbc").option("query", ...)` | ❓ Not investigated | Located as `jdbc:<url>/<the query text>` by `JDBCOptions.tableOrQuery`; whether a contract should name a query at all is undecided. |
| `df.write.jdbc(url, table, props)`, `overwrite` | ✅ Covered | `SaveIntoDataSourceCommand` with `url` and `dbtable` options. `JdbcConnectorSpec`'s PASS/FAIL pair. |
| `df.write.jdbc(...)`, `append`/`ignore`/`error` | ❓ Not investigated | Same command and location; the save mode comes from `SaveMode` as for every other `save`. No test yet. |
| `.saveAsTable(...)` / `INSERT INTO` a JDBC table through a catalog | ❓ Not investigated | Needs a JDBC catalog (`JDBCTableCatalog`) configured; not set up in this environment. |
| `MERGE`/`UPDATE`/`DELETE` | 🚫 N/A | Spark has no row-level DML for the generic JDBC source. |
| Streaming read or write | 🚫 N/A | The generic JDBC source implements neither. |
| A save with no `path` and no `url`/`dbtable` | ✅ Covered | Located as `<source class>(no path)` with a diagnostic, never the options map. `JdbcConnectorSpec`'s location-precedence test. |

## JDBC feature-surface coverage ledger

| Feature | Status | Evidence / next step |
|---|---|---|
| Option names in any case (`URL`, `DBTable`) | ✅ Confirmed | The write's options are read case-insensitively, as Spark reads them. `JdbcConnectorSpec`'s location-precedence test. |
| Credentials in the options (`user`, `password`) | ✅ Confirmed not exposed | Neither the location nor any message is built from the options map. `JdbcConnectorSpec`'s FAIL test asserts the refusal text has no options in it. |
| Column names as the database reports them | ✅ Confirmed transparent | Derby upper-cases unquoted names (`ID`, `VALUE`); a contract declares them as the read reports them, and `spark.sql.caseSensitive=false` (the default) also lets lower-case declarations match. |
| Vendor type mapping (`NUMBER`, `VARCHAR2`, `TIMESTAMP WITH TIME ZONE`, ...) | ❓ Not investigated | Spark's per-dialect mapping decides the column types a read reports; only Derby was used. |
| `truncate=true` / `cascadeTruncate` on overwrite | ❓ Not investigated | Changes how Spark overwrites the table, not what the plan says; worth one test per dialect. |
| Partitioned reads (`partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`) | ❓ Not investigated | Expected to be transparent (same relation, same location); unconfirmed. |
