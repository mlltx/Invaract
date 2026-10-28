# Sink records survey: what each destination's own records can prove about a job's writes

> **Status: survey. No code exists.** It follows the native Python adapter survey (docs/PYTHON_ADAPTER_SURVEY.md). That
> survey ended by moving away from watching a Python job from inside, and from relying on permissions, towards a different
> idea: verify what a job wrote by reading the *destination's own record* of the changes made to it. This document asks, for
> each destination a Python job is likely to write to, how far that record can be trusted.
> Written: 2026-10-28. Probes run on this machine: Postgres 16 with pgaudit, MySQL 8.0, Delta Lake (delta-rs 1.6.6), Iceberg
> (pyiceberg 0.12.0) and local files (Linux fanotify). The cloud services were not probed (no credentials); their rows rest on
> search results and are marked UNVERIFIED where it matters. docs.aws.amazon.com, docs.cloud.google.com and
> docs.snowflake.com are outside this session's allowed network, so their pages could not be read directly.

## The short version

**The requirement.** Two things, for a job Invaract does not change:
- confirm that the job wrote to the places its contract declares, and nowhere else;
- confirm that what it wrote at each place matches the place's declared kind (a data asset, or a readiness signal).

**The approach.** Many destinations keep their own record of every change: a database's audit or change log, a cloud
provider's access log, a table format's commit log. After a run, Invaract reads that record for the run's time window and
attributes each change to the run or to someone else. That proof does not depend on which library the job used, which
credentials it held, or what permissions anyone granted.

**How confident each destination's record can make us** (details in section 3):

| Destination | Confidence | Why, in one line |
|---|---|---|
| **Postgres** with pgaudit | **High** | Every write logged as it runs, with the run's label and transaction; commit status from the database itself (probe 1) |
| **MySQL** | **Medium to high** | Binary log of committed changes with the connection, joined to the user; no per-run label (probe 2) |
| **BigQuery** | **Medium to high** (unverified) | Jobs history plus audit logs of row changes, with the caller; streaming writes only in the audit logs |
| **Delta Lake / Iceberg tables** | **High for what, low for who** | A complete commit history with operation, mode, rows and schema; no writer identity by default (probe 3) |
| **S3** | **Medium** (unverified) | CloudTrail object-level events, opt-in, about 5 minutes late on average; server access logs are best effort and not enough |
| **GCS** | **Medium** (unverified) | Data Access audit logs, off by default |
| **Snowflake** | **Medium** (unverified) | Access history names modified objects, but can lag by hours |
| **Local or shared files** | **Medium** with fanotify, **low** without | No log of their own. A machine-level watch sees every write with its process (probe 4), but attributing short-lived child processes needs process tracking |
| **SQLite, Kafka** | **Low** | No record of who wrote what; only before-and-after state |

**The pattern behind the table.** The strongest destinations have three things:
- a record of committed changes;
- a label that reaches the record without changing the job;
- immediate availability.

Postgres has all three. Table formats have the first only. Cloud stores have the first two, but late. Plain files have none,
so for them the machine has to keep the record.

**Decisions for you.**
1. Which destinations do your Python jobs actually write to? This survey ranked likely ones; your real list decides the
   order of work.
2. Accept that verification is per destination: a reader for each destination's record, not one mechanism for all.
   *Recommend yes; it is what makes the proof independent of the job.*
3. For destinations without attribution (table formats, SQLite, files without a machine-level watch), accept attribution by
   time window and one writer per place, and say so in each run's report? *Recommend yes.*
4. Require the records to be switched on (pgaudit, CloudTrail object-level events, GCS Data Access logs) as a precondition
   for a destination to be "verified", and report the destination as unverifiable otherwise? *Recommend yes.*

## 1. The questions asked of each destination

For a record to prove "this run wrote here, and only here, and this is what it wrote", it has to answer five questions:

| Question | Why it matters |
|---|---|
| **Complete**: does every kind of write appear (inserts, bulk loads, truncates, writes inside database functions, streaming)? | A write the record misses is a write nothing checks |
| **Committed-only**: does it tell a change that stayed from one that was rolled back? | Probe 3 of the Python survey's enforce toy counted rolled-back rows as written |
| **Attribution**: can each change be tied to this run, rather than another job writing at the same time? | Without it, another team's write looks like this job's |
| **Delay**: how soon after the run is the record complete? | Verification, and any readiness step waiting on it, waits that long |
| **Content**: does it give the place, the operation, the amount, and ideally the schema? | Classification (a data asset or a readiness signal) needs more than "something changed" |

And one practical question: **what must be switched on**, by whom, at what cost.

## 2. Which destinations are likely

Ranked by judgement, not by measured usage: no usage data was available here. These are the places a Python data job
commonly writes, through pandas, pyarrow, Polars, SQLAlchemy and the cloud clients:
1. object storage: S3, GCS (Azure Blob was not surveyed);
2. warehouses: BigQuery, Snowflake;
3. operational databases: Postgres, MySQL;
4. table formats on object storage: Delta Lake, Iceberg;
5. local or shared file systems;
6. less common for batch jobs: SQLite, Kafka.

## 3. Destination by destination

### Postgres

- **The record.** pgaudit (Postgres's audit extension) logs each write statement as it runs, with the session's application
  name and transaction number in the log line prefix. `pg_xact_status(xid)` says whether that transaction committed.
- **How the run is labelled.** `PGAPPNAME=<run id>` in the job's environment. libpq-based drivers read it: psycopg2,
  psycopg 3, and SQLAlchemy and pandas `to_sql` through them (probe 1). pg8000 does not read it (it is pure Python);
  asyncpg is UNVERIFIED. A job that sets its own application name overrides the label.
- **Probe 1 found.** One labelled run wrote in seven ways while another team wrote to the same table:
  - **Explicit transaction:** logged with its transaction number; committed.
  - **Autocommit:** logged with its transaction number (`xid=744`), unlike plain statement logging, which logs it with
    `xid=0`.
  - **Rolled back:** logged (`xid=740`); `pg_xact_status` says `aborted`, so it is excluded.
  - **Bulk `COPY`:** logged, but without the table in the object columns (`WRITE,COPY,,,COPY gold.report FROM STDIN`). The
    table is in the statement text, or in Postgres's change stream.
  - **A function that writes elsewhere** (`SELECT gold.refresh()`): its inner insert into `gold.other` was logged as a write
    of its own, with the same label.
  - **`TRUNCATE`:** logged as `TRUNCATE TABLE`, with the table only in the statement text.
  - **pandas `to_sql`:** logged with the label.
  - **The other team's insert:** logged with its own label (`other-team`), so it is told apart.
- **Delay.** Immediate: the line is written as the statement runs.
- **Content.**
  - The table: from pgaudit's object column, or from the statement text for `COPY` and `TRUNCATE`.
  - The operation, and rows per statement (`pgaudit.log_rows`, 1 for each insert here and 0 for `COPY`).
  - The schema: from `information_schema.columns`.
- **What must be switched on.** All of it is set by a database administrator, with a restart:
  - pgaudit in `shared_preload_libraries`, with `pgaudit.log = 'write'`;
  - a log line prefix with `%a` and `%x`;
  - read access to the log.

  Logging writes only is far lighter than logging every statement.
- **Alternative without pgaudit.** Postgres's change stream (logical decoding) lists committed changes per transaction,
  `COPY` and `TRUNCATE` included. It needs `wal_level = logical` and a replication slot, which keeps write-ahead log until
  read and can fill a disk if left unread. Plain statement logging misses the transaction number for autocommit writes
  (the native Python survey, "Writing to databases").
- **Confidence: High.** The run label is the weak point: it depends on the driver honouring `PGAPPNAME`, and on the job not
  overriding it.

### MySQL

- **The record.**
  - The binary log (row format) holds committed changes only, each transaction with the connection's thread id.
  - The general log maps each thread id to the user who connected.
- **Probe 2 found.**
  - A rolled-back insert was absent from the binary log.
  - An autocommit insert was present.
  - Another user's insert to the same table carried a different thread id, and so a different user.
- **How the run is labelled.** Only by database user. No environment variable sets a per-run label for the common drivers
  (PyMySQL, mysql-connector), as far as checked (UNVERIFIED). Two concurrent runs of the same job under the same user cannot
  be told apart.
- **Delay.** Immediate.
- **Content.**
  - The table and operation, and the row images with `-v`.
  - The original statement with `binlog_rows_query_log_events`.
  - The schema: from `information_schema`.
- **What must be switched on.**
  - The binary log is usually on already, since replication uses it.
  - The general log records every statement, reads included, so it is heavy. An audit plugin (MySQL Enterprise, Percona
    or MariaDB) is the lighter alternative (UNVERIFIED).
- **Confidence: Medium to high.** High when each job has its own database user and runs one at a time.

### BigQuery (not probed: no credentials)

- **The record.**
  - The jobs history (`INFORMATION_SCHEMA.JOBS`) holds near real-time metadata about every job in a project, with 180 days
    of history, clustered by `user_email` (search result).
  - The audit logs record table data changes: inserted and deleted row counts, a truncation flag, the job or the Write API
    stream that made the change, and a reason that distinguishes Write API writes (search result, from the
    `BigQueryAuditMetadata.TableDataChange` reference).
- **Streaming writes** do not create jobs, so they appear only in the audit logs (the native Python survey's probe 17
  showed the client's `insertAll` request).
- **How the run is labelled.** The service account the run uses (one per job, or one per run if the platform can mint
  them). Job labels would need the job's code, or a client default, to set them (UNVERIFIED).
- **Delay.** "Near real-time" for the jobs history; UNVERIFIED for the audit logs.
- **Content.** The column names `destination_table`, `labels` and the DML row counts could not be read from the
  documentation here (UNVERIFIED). The schema comes from the table itself (`tables.get`).
- **Confidence: Medium to high, unverified.** One check settles it: a load, a DML statement and a streaming insert from a
  test project, then the jobs history and the audit logs read back.

### Delta Lake and Iceberg

- **The record.** The table's own commit log: the table *is* its log, so it is complete and committed-only by construction.
- **Probe 3 found.**
  - **Delta**, written with delta-rs: each commit records
    `operation: WRITE`, `operationParameters: {'mode': 'Overwrite' | 'Append'}`, `num_added_rows`, `num_added_files`, a
    timestamp, `engineInfo: delta-rs:py-1.6.6`, and the schema.
  - **Iceberg**, written with pyiceberg: each snapshot records `operation: append | delete`, `added-records`,
    `added-data-files`, totals, a timestamp and the schema. An overwrite showed as a delete followed by an append.
  - Neither recorded who made the commit. Custom commit metadata needs the job's code.
- **How the run is labelled.** By time window only, unless the storage underneath has an audit log of who wrote the commit
  file (S3 CloudTrail, GCS Data Access logs).
- **Delay.** Immediate.
- **Content.** The best of any destination: operation, mode, rows, files and schema, per commit.
- **Confidence: High for what changed and how; low for who**, unless the storage's audit log is joined in.

### S3 (not probed: no credentials, and AWS documentation outside the allowed network)

- **The record.** CloudTrail object-level ("data") events: PutObject, DeleteObject and the rest. They are not logged by
  default; a trail must be configured for the buckets (search result).
- **Delay.** "CloudTrail typically delivers logs within an average of about 5 minutes of an API call. This time is not
  guaranteed" (search result quoting the CloudTrail user guide).
- **Not enough on its own.** S3 server access logs: "Server access log records are delivered on a best-effort basis", and
  there is "no way to know whether all log records for a certain time interval have been delivered" (search result quoting
  the S3 user guide).
- **How the run is labelled.** The caller identity on each event. If the platform starts each run with its own assumed-role
  session named after the run, the session name should appear in the event's identity (UNVERIFIED).
- **Content.** The bucket and key, and the operation. No schema: that comes from reading the written files (the native
  Python survey's probe 11).
- **Confidence: Medium, unverified.** Late, and busy buckets produce large log volumes.

### GCS (not probed)

- **The record.** Data Access audit logs, type `DATA_WRITE`, covering object creation and deletion. These are "disabled by
  default and aren't written unless explicitly enabled" (search result quoting the Cloud Storage audit-logging page).
- **How the run is labelled.** The caller's identity (the service account).
- **Delay.** UNVERIFIED.
- **Confidence: Medium, unverified.**

### Snowflake (not probed)

- **The record.** `ACCOUNT_USAGE.ACCESS_HISTORY`: the objects each query modified, with the user and query id, kept for
  365 days (search result).
- **Delay.** Account Usage views have a latency of up to 2 to 3 hours (search result). The edition required is UNVERIFIED.
- **How the run is labelled.** The user, or a query tag the platform can set as a user default (UNVERIFIED).
- **Confidence: Medium, unverified.** The delay is the main cost: a readiness step waiting on it would wait hours.

### Local or shared files

- **The record.** None of their own. A machine-level watch can make one: fanotify (Linux's file-access notification, which
  needs administrator capability) reports every file written on a file system, with the writing process.
- **Probe 4 found.**
  - The watch saw every write: pandas (Python), Polars (Rust), DuckDB (C++), a shell command started by the job, and
    another team's write.
  - Writes by the job's own process were attributed to the run.
  - The shell command's write, and the other team's, came out as "unknown". Both processes had exited before their parent
    could be looked up, so a short-lived child of the job is indistinguishable from an outsider.
  - Tracking the process tree as it forks would close this: the kernel's process events, or Linux's audit system, which
    records each process's parent and login identity at the moment of the call (UNVERIFIED here).
- **Without a watch.** Before-and-after snapshots (the native Python survey's verifier) see that a declared place changed,
  but not who changed it.
- **Limits.**
  - The watch sees writes made on this machine only. A shared file system written by other machines needs its own server's
    audit.
  - Containers may not grant the capability.
- **Confidence: Medium with a watch and process tracking; low with snapshots only.**

### SQLite and Kafka (not probed)

- **SQLite.** No log of who wrote what: only the file's state before and after, and table counts. Confidence: low.
- **Kafka.** Brokers do not keep a per-producer record of writes by default. Attribution would need the authorizer's logs
  of each producer principal (UNVERIFIED). Confidence: low, unverified.

## 4. What this means for the design

- **A reader per destination.** Each destination gets a small reader that turns its record into the same neutral list:
  place, operation, amount, committed or not, and who. Invaract's existing checks then judge that list against the
  contract.
- **Shape of the product.** This shifts Invaract's unit of work from the *engine* that runs a job to the *destination* a job
  writes to. Any job, in any language, writing to a supported destination is covered. That is a strategic choice beyond this
  survey.
- **The run label is the linchpin.** Where a label reaches the record without changing the job (`PGAPPNAME`, a service
  account per job, an assumed-role session per run), attribution is strong. Where none does, verification falls back to a
  time window and one writer per place, and the report must say so.
- **Delay sets when readiness can be published.** It is immediate for databases and table formats, minutes for CloudTrail,
  and up to hours for Snowflake's access history.
- **Classification needs content.** "A data asset received rows with the declared columns" and "a readiness signal
  received a status row or a marker" come from the record's operation and row counts, plus the destination's schema
  (`information_schema`, a table's own schema, a file's footer).

## 5. Evidence log

### Probes I ran (2026-10-28, not committed)

1. **Postgres 16.15 with pgaudit** (installed from the Ubuntu package `postgresql-16-pgaudit`).
   - Settings: `pgaudit.log = 'write, ddl'`, `pgaudit.log_relation = on`, `pgaudit.log_rows = on`, log line prefix
     `app=%a user=%u xid=%x pid=%p`, `wal_level = logical`.
   - One run labelled `PGAPPNAME=invaract-run-1` wrote in seven ways: psycopg2 2.9.13 for the explicit transaction, the
     rollback, `COPY`, the function call, `TRUNCATE` and autocommit; pandas through SQLAlchemy 2.1.6 for `to_sql`.
   - Before it, another connection labelled `other-team` inserted into the same table.
   - The pgaudit lines and the logical-decoding stream are quoted in section 3.
   - `pg_xact_status` returned `committed` for 739, 741 and 744, and `aborted` for 740.
2. **MySQL 8.0.46.** Settings: `log-bin`, `binlog_format = ROW`, `binlog_rows_query_log_events = ON`, `general_log = ON`.
   - Writers:
     - user `other` inserted into `gold.report`;
     - user `job`, through PyMySQL, inserted 2 rows and committed, then inserted into `gold.other` and rolled back;
     - user `job`, through mysql-connector with autocommit, inserted into `gold.other`.
   - `mysqlbinlog -v` showed thread ids 11 (other), 12 (job, the 2 rows) and 13 (job, autocommit), and no rolled-back row.
     The general log mapped 11 to `other@localhost` and 12 and 13 to `job@localhost`.
3. **Delta Lake and Iceberg.**
   - delta-rs 1.6.6: an overwrite and an append of a 2-row pyarrow table. `DeltaTable.history()` is quoted in section 3.
   - pyiceberg 0.12.0 with a SQLite catalog and a local warehouse: an append and an overwrite. Each snapshot's summary is
     quoted in section 3.
4. **fanotify.**
   - Set-up: `fanotify_init(FAN_CLASS_NOTIF)`, then `fanotify_mark(FAN_MARK_FILESYSTEM, FAN_CLOSE_WRITE)` on the root file
     system.
   - The job wrote with Polars, DuckDB, pandas, a `sh -c` child and a plain `open`. Meanwhile another process wrote a file.
   - Each event's process was traced up to the job's process.
   - The job's own writes were attributed. The shell child and the other process came out "unknown: the process had already
     exited".

### Sources (search results; the documentation pages themselves were not reachable from this session)

- [AWS CloudTrail: working with log files](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-working-with-log-files.html):
  delivery "within an average of about 5 minutes ... not guaranteed".
- [Amazon S3: server access logging](https://docs.aws.amazon.com/us_en/AmazonS3/latest/userguide/ServerLogs.html):
  "delivered on a best-effort basis".
- [CloudTrail data events for S3](https://docs.amazonaws.cn/en_us/awscloudtrail/latest/userguide/logging-data-events-with-cloudtrail.md):
  object-level events, opt-in.
- [BigQuery JOBS view](https://docs.cloud.google.com/bigquery/docs/information-schema-jobs): near real-time, 180 days,
  clustered by `user_email`.
- [BigQueryAuditMetadata.TableDataChange](https://docs.cloud.google.com/dotnet/docs/reference/Google.Cloud.Audit/latest/Google.Cloud.Audit.BigQueryAuditMetadata.Types.TableDataChange):
  inserted and deleted row counts, truncation, the job or stream.
- [Cloud Storage audit logs](https://docs.cloud.google.com/storage/docs/audit-logs) and
  [Enable Data Access audit logs](https://docs.cloud.google.com/logging/docs/audit/configure-data-access): `DATA_WRITE`, off
  by default.
- [Snowflake ACCESS_HISTORY](https://docs.snowflake.com/sql-reference/account-usage/access_history) and
  [Account Usage](https://docs.snowflake.cn/sql-reference/account-usage): 365 days; up to 2 to 3 hours of latency.

### Unverified

| Claim | The cheapest check |
|---|---|
| BigQuery's jobs history has `destination_table`, `labels` and DML row counts, and its audit logs record streaming and Write API rows with the caller | a load, a DML statement and a streaming insert in a test project, then read both back |
| An assumed-role session named per run appears in CloudTrail's S3 events | one PutObject with a named session, then the event |
| GCS Data Access log delay, and Snowflake's edition requirement | a test bucket's write, timed; Snowflake's documentation |
| asyncpg honours `PGAPPNAME` | probe 1 with asyncpg |
| A MySQL driver can carry a per-run label without code, or an audit plugin records connection attributes | the drivers' documentation; probe 2 with Percona's or MariaDB's audit plugin |
| Process-tree tracking attributes short-lived children's file writes | probe 4 with the kernel's process events, or Linux audit with `auid` |
| Kafka can attribute writes to a producer principal | a broker with authorizer logging at the right level |
