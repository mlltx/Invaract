# Avro support

[← Back to Spark Adapter](../SPARK_ADAPTER.md#connector-support)

Sixth connector onboarded via the `add-spark-connector` skill's process
(docs/ADDING_A_SPARK_CONNECTOR.md). Unlike Parquet/CSV (Spark's own
bundled `FileFormat`s, already on the `provided` `spark-sql` dependency),
Avro is a separate first-party artifact Spark splits out of `spark-sql`
(`org.apache.spark %% "spark-avro" % "3.5.1"`) — the first real `% "test"`
dependency addition since Delta/Iceberg/Hive, pinned to the exact same
`sparkVersion` since `spark-avro` ships per-Spark-release rather than on
its own version line.

A real reflective jar scan (docs/ADDING_A_SPARK_CONNECTOR.md's Phase 3 —
`JarFile` + `Class.forName` + `Command.isAssignableFrom` over all 55
classes in `spark-avro_2.12-3.5.1.jar`) found **zero `Command`-shaped
classes** — no SQL-extension commands at all, unlike Delta/Iceberg/Hive.
`AvroFileFormat` (`org.apache.spark.sql.avro`) is a plain `FileFormat`
implementation, so this connector's entire operation surface routes
through the same generic mechanisms Parquet/CSV's passes already proved
out (`InsertIntoHadoopFsRelationCommand`, `CreateDataSourceTableAsSelectCommand`,
`WriteToStream`/`FileStreamSink`) — confirmed empirically via `injectCheckRule`
probes against a real Avro-enabled session, not assumed by analogy. A
`spark-avro`-only `org.apache.spark.sql.v2.avro` DataSourceV2
implementation also exists in the jar (`AvroTable`/`AvroWrite`/`AvroScan`),
but — the same as Parquet's own bundled V2 classes — it's never reached
under Spark's default `useV1SourceList`, confirmed by `.writeTo()`
against a plain Avro table being rejected the same way Parquet/CSV's is.

**A real, previously-documented-but-unfixed bug found and fixed this
pass, not Avro-specific.** Parquet's own coverage ledger
(docs/connectors/parquet.md) flagged, but explicitly left unattempted, a
risk with a path-less new-table
`.saveAsTable()`/`.writeTo(...).create()` (no explicit `.option("path", ...)`):
`CreateDataSourceTableAsSelectCommand.table.storage.locationUri` is unset
at analysis time for a `MANAGED` table (Spark only populates it when the
command actually runs), so `WriteCommandSupport.createDataSourceTableAsSelect`
fell back to the bare qualified identifier (`spark_catalog.default.t`) as
the outer command's location — which can never equal the nested
`InsertIntoHadoopFsRelationCommand`'s real physical warehouse path
(`file:/.../t`), so no single contract `location` value could ever
satisfy both checks for what is, logically, one write. Confirmed
empirically for real (not re-derived from Parquet's comment) via a direct
`WriteCommandSupport.combined` comparison on both captured plans before
writing any fix: `outer.location != nested.location`, unconditionally.
Fixed by computing the identical `SessionCatalog.defaultTablePath` Spark
itself uses internally — the same "ask the active session's own
resolution logic via `SparkSession.active`" technique
`StateChangingCallSupport.resolveIdentifier` already uses for Iceberg's
`CALL` procedures — so the two now agree by construction. This is a
general fix benefiting every V1-format connector's path-less new-table
create (Parquet/CSV/Hive/Avro alike), not something specific to Avro's
own translation; verified with a dedicated `AvroConnectorSpec` test
comparing outer/nested locations directly, plus a PASS enforcement test
against a contract declared at the resolved default warehouse path.

**What's genuinely new for Avro: the feature surface.** Avro is a
schema-carrying binary format with logical types and explicit
external-schema support — real behaviors with no Parquet/CSV analog:

- **`avroSchema` option (explicit external reader schema).** Confirmed
  transparent: Invaract sees exactly the schema Spark reports for the
  read, including an extra field the underlying data doesn't carry
  (reading back `null`) — no special handling needed, a contract
  declaring the wider schema is satisfied normally.
- **Logical types (`decimal`, `date`, `timestamp`).** Confirmed to
  round-trip exactly — Avro represents these via its own logical-type
  annotations over primitive Avro types (`bytes`/`int`/`long`), not
  native container types the way Parquet does, but the DataFrame-facing
  schema and values are unaffected. `date`/`timestamp` satisfy a
  declaring contract normally.
- **A real, pre-existing `contract`/`StructuralVerifier` type-vocabulary
  gap, found via Avro's decimal type but not Avro-specific.**
  `ContractValidator.KnownTypes` accepts the bare literal `"decimal"` as
  a valid `type:` value, but `StructuralVerifier.checkSchema` compares
  against Spark's own `DataType.typeName`, which for `DecimalType`
  *always* includes precision/scale (e.g. `"decimal(10,2)"`) — so a
  contract declaring the bare `decimal` keyword can never match *any*
  decimal field, from any connector, and `KnownTypes` has no
  parametrized `decimal(p,s)` form to declare instead. Confirmed
  empirically this pass; no existing Parquet/CSV/Delta/Iceberg/Hive test
  had ever declared a decimal-typed contract field either, so this was a
  real, previously-unexercised gap, not an Avro regression. **Out of
  scope to fix in this connector pass** — it's a `contract`/`ir` module
  design change (teaching `ContractValidator`/`StructuralVerifier` to
  accept and compare a parametrized decimal type), not a spark-adapter
  translation bug. Documented and given a permanent regression test
  (`AvroConnectorSpec`'s dedicated test) rather than silently worked
  around, so it's discoverable rather than rediscovered from scratch by
  the next connector or contract author who hits it.
- **Nullability on read-back.** Every field reports `nullable = true`
  after an Avro write+read round-trip, regardless of the original
  schema's nullability — Avro represents an optional field as a
  `["null", T]` union type, confirmed independently for Avro (not
  inherited from any other connector's reader). Same practical
  consequence for contract authors as Parquet/CSV/Delta/Iceberg's own
  version of this finding.
- **`recordName`/`recordNamespace` write options.** Confirmed
  transparent: purely a writer-side detail of the emitted Avro schema's
  own `name`/`namespace` metadata, zero effect on the DataFrame-facing
  schema Invaract sees on read-back.
- **Compression codec options.** Confirmed transparent: a storage-
  representation detail only, zero effect on the read-back schema or row
  content.
- **`ignoreExtension`.** Avro-specific: by default (`ignoreExtension=true`)
  every file in a directory is read regardless of its extension; set to
  `false`, only `.avro`-suffixed files are. Confirmed for real: a
  non-`.avro`-named copy of a real Avro file is included by default and
  excluded when `ignoreExtension=false` — a read-time filtering detail,
  no effect on schema/verification either way once a file is included.

## Avro operation-surface coverage ledger

| Operation | Status | Evidence / next step |
|---|---|---|
| `.read.format("avro").load(path)` | ✅ Covered | Same generic `LogicalRelation`+`HadoopFsRelation` handling as Parquet/CSV, confirmed for Avro specifically via `injectCheckRule` probe. `AvroConnectorSpec`'s "PASS: a contract's declared input schema is satisfied by a plain avro .load(path) read." |
| Catalog table reference (`spark.table(...)`/`SELECT * FROM t`) | ✅ Covered | Same `LogicalRelation` shape via the default `spark_catalog`, confirmed via probe. `AvroConnectorSpec`'s "PASS: a contract's declared input schema is satisfied by a catalog table reference read." |
| Time travel / snapshot reads | 🚫 **N/A, not a real gap** | Plain Avro (`USING avro`, no table-format layer) has no versioning concept at all — the same architectural constraint as Parquet/CSV. Nothing to translate. |
| Streaming read (`readStream`) | ✅ Covered | Generic `StreamingRelation`/`FileSource` handling, confirmed empirically for Avro. `AvroConnectorSpec`'s "PASS: a streaming avro source satisfies a contract's declared input schema." |
| Change-data-feed / incremental read | 🚫 **N/A, not a real gap** | No CDC mechanism exists for plain Avro — a table-format-only feature, the same constraint as Parquet/CSV. Nothing to translate. |
| `.save(path)`, all four save modes | ✅ Covered | `InsertIntoHadoopFsRelationCommand`, confirmed for Avro via real probe (`append`/`overwrite`/`ignore`/`error`) and `AvroConnectorSpec`'s translation test plus PASS/FAIL enforcement pair. |
| `.saveAsTable(...)`, new table | ✅ Covered | `CreateDataSourceTableAsSelectCommand`, `table.provider = "avro"`. The path-less case specifically closed this pass — see the location fix above and its dedicated tests. |
| `.saveAsTable(...)`, existing table (append) | ✅ Covered | Same nested-double-write pattern Parquet/CSV's passes documented, confirmed independently for Avro via a real PASS/FAIL pair (the FAIL half asserts the nested insert never ran). |
| `.insertInto(...)` | ✅ Covered | Same `InsertIntoHadoopFsRelationCommand` shape. `AvroConnectorSpec`'s translation test via `SparkAdapterListener`. |
| `.writeTo(...)` (DataFrameWriterV2) — `.append()`/`.overwrite(cond)` against an *existing* table | 🚫 **N/A — rejected by Spark itself, not an Invaract gap** | Confirmed empirically for Avro specifically: `AnalysisException: Cannot write into v1 table` — the same V1/V2 constraint as Parquet/CSV (Avro's own `org.apache.spark.sql.v2.avro` classes exist but aren't reached under the default `useV1SourceList`). `AvroConnectorSpec`'s test proving rejection and zero committed rows. |
| `.writeTo(...).create()`, new table | ✅ Covered | With the format made explicit (`.using("avro")`), reuses the exact same `CreateDataSourceTableAsSelectCommand` path `.saveAsTable()` already uses — confirmed via `injectCheckRule`. `AvroConnectorSpec`'s translation test. |
| `.writeTo(...).createOrReplace()`/`.replace()` | 🚫 **N/A — rejected by Spark itself, not an Invaract gap** | Same `StagingTableCatalog` constraint as Parquet/CSV — not re-probed with a dedicated test this pass (CSV's pass already confirmed this is connector-agnostic for any V1-provider-backed format); the generic `.writeTo()`-against-existing-table test above exercises the same rejection path. **Next step**: a dedicated Avro-specific `createOrReplace()`/`.replace()` test, if ever doubted — cheap to add, mirroring CSV's. |
| Format-specific DML (`MERGE`/`UPDATE`/`DELETE`) | 🚫 **N/A — not an Avro capability at all** | Confirmed empirically: Spark itself rejects all three against a plain Avro table, the same messages as Parquet/CSV's. `AvroConnectorSpec`'s regression test proving rejection and an unchanged row count. |
| Streaming write | ✅ Covered | `WriteToStream`/`FileStreamSink`, confirmed to generalize to Avro via `AvroConnectorSpec`'s streaming PASS/FAIL pair. A per-microbatch `WriteToMicroBatchDataSourceV1` node was also observed reaching `injectCheckRule` during investigation — confirmed **not** `Command`-shaped (`isInstanceOf[Command] == false`), so it's inert with respect to `ContractEnforcementRule`'s fail-closed net; the one real enforcement point remains the outer `WriteToStream` node, already covered. |
| Maintenance operations that touch data | 🚫 **N/A — no such mechanism exists** | Plain Avro has no connector-level maintenance command, the same constraint as Parquet/CSV. Nothing to classify. |

## Avro feature-surface coverage ledger

| Feature | Status | Evidence / next step |
|---|---|---|
| `avroSchema` option (explicit external reader schema) | ✅ Confirmed transparent | Invaract sees exactly the schema Spark reports, including an extra undeclared-in-the-data field (reads back `null`). `AvroConnectorSpec`'s test. |
| Logical types (`decimal`/`date`/`timestamp`) round-trip | ✅ Confirmed — `date`/`timestamp`; see the dedicated gap row below for `decimal` | Exact round-trip confirmed for all three at the schema/value level; `date`/`timestamp` satisfy a declaring contract normally. `AvroConnectorSpec`'s test. |
| Contract type declaration for decimal fields | 🔧 **Found, not fixed this pass — pre-existing, cross-connector gap** | `ContractValidator`'s bare `"decimal"` keyword can never match `StructuralVerifier`'s `DataType.typeName` comparison (always precision/scale-qualified, e.g. `"decimal(10,2)"`), and there's no parametrized form to declare instead — true for every connector, not Avro-specific, and never previously exercised by any existing connector's tests. **Next step**: teach `ContractValidator`/`StructuralVerifier` to accept and compare a parametrized `decimal(p,s)` contract type — a `contract`/`ir` module design change, out of scope for a spark-adapter connector pass. `AvroConnectorSpec`'s dedicated test documents and pins the current (mismatching) behavior. |
| Nullability on read-back | ✅ Confirmed — independently, not inherited | Every field reports `nullable = true` after an Avro write+read round-trip regardless of declared nullability (Avro's `["null", T]` union representation). `AvroConnectorSpec`'s test. |
| `recordName`/`recordNamespace` write options | ✅ Confirmed transparent | Writer-side Avro schema metadata only; zero effect on the read-back DataFrame schema. `AvroConnectorSpec`'s test. |
| Compression codec options | ✅ Confirmed transparent | Storage-representation detail only; zero effect on read-back schema or data. `AvroConnectorSpec`'s test. |
| `ignoreExtension` | ✅ Confirmed — genuinely Avro-specific | Default (`true`) includes every file in a directory regardless of extension; `false` restricts to `.avro`-suffixed files. `AvroConnectorSpec`'s test. |

Net assessment: Avro's operation surface is fully enumerated with no ❓
rows — every row is either ✅ Covered (via mechanisms Parquet/CSV's
passes already proved out, confirmed to generalize) or a genuinely
reasoned 🚫 N/A. Beyond the connector's own coverage, this pass also
closed a real, general `WriteCommandSupport` bug (the path-less
new-table location mismatch) that Parquet's own documentation had
explicitly flagged and left open — a fix that benefits every V1-format
connector, not just Avro. The feature surface has one genuine
found-but-deliberately-unfixed item (the decimal contract-type-vocabulary
gap), documented with a clear next step and a permanent test pinning the
current behavior, rather than silently worked around or silently passed
over. `AvroConnectorSpec`: 23 tests, all passing.

