// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.apache.spark.sql.catalyst.analysis.{NamedRelation, ResolvedIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable => SparkCatalogTable}
import org.apache.spark.sql.catalyst.plans.logical.{
  AppendData,
  CreateTableAsSelect,
  DeleteFromTable,
  LogicalPlan,
  OverwriteByExpression,
  OverwritePartitionsDynamic,
  ReplaceTableAsSelect,
  RowLevelWrite
}
import org.apache.spark.sql.catalyst.streaming.WriteToStream
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, SaveIntoDataSourceCommand}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{StructField, StructType}

/** Everything needed to represent one Spark write command as `ir.Write`:
  * where it writes, the (untranslated) plan being written, its format and
  * save mode, the schema to verify a contract's output against, and an
  * optional diagnostic if something had to be guessed. `query` is left
  * untranslated — the actual recursive translation (and diagnostic
  * accumulation for the *rest* of the plan tree) stays with
  * `SparkPlanAdapter.Translator`; this is pure, stateless extraction from
  * one command node, reusable by callers that don't want a full
  * translation (see `ContractEnforcementRule`/`SparkAdapterListener`
  * below).
  */
private[sparkadapter] case class WriteCommandInfo(
  location: String,
  query: LogicalPlan,
  format: Option[String],
  saveMode: Option[String],
  outputSchema: StructType,
  diagnostic: Option[Diagnostic] = None
)

/** One entry per Spark write-command *shape* this module recognizes — see
  * docs/SPARK_ADAPTER.md's "Translation coverage" for what each of the
  * three shapes below actually is, and docs/ADDING_A_SPARK_CONNECTOR.md
  * for the process a new connector follows to potentially add a fourth.
  *
  * ## Why this exists
  *
  * Before this, "is this plan a write, and what does it mean" was
  * implemented three separate times: once in
  * `SparkPlanAdapter.Translator.translatePlan` (to actually translate
  * it), once in `ContractEnforcementRule.verifyOrThrow` (just to pull the
  * right output schema — a `Command` node's own `.schema` is empty, so
  * this needs the underlying query's), and once in
  * `SparkAdapterListener.onSuccess` (just to decide whether to capture a
  * write for `demo/output/report.json`). Three independent
  * `case cmd: X =>` matches, hand-kept in lockstep. Both of this module's
  * real Delta-support bugs were exactly a write shape added to
  * `translatePlan` and missed in one of the other two matches — not a
  * one-off mistake, a structural hazard built into having the same fact
  * ("is this a write, and what schema does it write") encoded three
  * times.
  *
  * `combined` is now the single source of truth all three sites consult.
  * Adding a write shape here reaches every site that needs it
  * automatically — there is no second match to remember, and the
  * `WriteCommandInfo` shape makes it structurally impossible to translate
  * a write without also supplying the schema `ContractEnforcementRule`
  * needs, the way the original Delta bug did.
  */
private[sparkadapter] object WriteCommandSupport {

  private val insertIntoHadoopFsRelation: PartialFunction[LogicalPlan, WriteCommandInfo] = {
    case cmd: InsertIntoHadoopFsRelationCommand =>
      WriteCommandInfo(
        location = cmd.outputPath.toString,
        query = unwrapWriteWrapper(cmd.query),
        format = SparkPlanAdapter.formatOf(cmd.fileFormat),
        saveMode = SparkPlanAdapter.saveModeOf(cmd.mode),
        outputSchema = cmd.query.schema
      )
  }

  // Delta Lake (and any other CreatableRelationProvider-based source
  // written via `.save(...)` rather than Spark's FileFormat-based write
  // path above) goes through this command instead of
  // InsertIntoHadoopFsRelationCommand - confirmed empirically against a
  // real Delta-enabled session, not assumed (see docs/SPARK_ADAPTER.md's
  // "Delta Lake support" section): `df.write.format("delta").save(path)`
  // analyzes to exactly this node, with Delta's own DeltaDataSource as
  // `dataSource`. No connector-specific code or dependency is needed to
  // translate it: SaveIntoDataSourceCommand and DataSourceRegister are
  // both plain, public spark-sql classes already on this module's
  // existing Spark dependency, and any CreatableRelationProvider that
  // also mixes in DataSourceRegister (as Delta's does, and as JDBC's
  // does) gets a precise format for free via `formatOf`.
  private val saveIntoDataSource: PartialFunction[LogicalPlan, WriteCommandInfo] = {
    case cmd: SaveIntoDataSourceCommand =>
      val (location, diagnostic) = cmd.options.get("path") match {
        case Some(path) => (path, None)
        case None =>
          val msg = s"No 'path' option on a ${cmd.dataSource.getClass.getSimpleName} write; " +
            "using its options map as a best-effort location"
          (cmd.options.toString, Some(Diagnostic("SaveIntoDataSourceCommand", msg)))
      }
      WriteCommandInfo(
        location = location,
        query = cmd.query,
        format = SparkPlanAdapter.formatOf(cmd.dataSource),
        saveMode = SparkPlanAdapter.saveModeOf(cmd.mode),
        outputSchema = cmd.query.schema,
        diagnostic = diagnostic
      )
  }

  // `.saveAsTable(...)`/`CREATE TABLE ... USING <format> AS SELECT ...`
  // against a *new* V1 data source table - confirmed empirically (see
  // docs/SPARK_ADAPTER.md's "Fail-closed on unverifiable writes" section):
  // analyzes to this command wrapping the actual data write, distinct
  // from both shapes above. `table.provider` is already the clean format
  // string `formatOf` derives from `DataSourceRegister` elsewhere - no
  // lookup needed here.
  //
  // A path-less new-table create (no explicit `.option("path", ...)`) is a
  // real, previously-documented-but-unfixed gap (see Parquet's own "shared
  // pitfall" write-up in docs/SPARK_ADAPTER.md): `cmd.table.storage.locationUri`
  // is unset at analysis time for a MANAGED table - Spark only populates it
  // when `CreateDataSourceTableAsSelectCommand.run()` actually executes,
  // via `SessionCatalog.defaultTablePath`. Falling back to the bare
  // qualified identifier (as this used to) produces a location that can
  // never agree with the nested `InsertIntoHadoopFsRelationCommand`'s real
  // physical path - confirmed empirically (not assumed) via the Avro
  // connector pass: `spark_catalog.default.t` vs.
  // `file:/warehouse/t`, unconditionally unequal, so no single contract
  // `location` value could ever satisfy both checks for the exact same
  // logical write. Fixed by computing the identical
  // `SessionCatalog.defaultTablePath` Spark itself will use - the same
  // "ask the active session's own resolution logic" technique
  // `StateChangingCallSupport.resolveIdentifier` already uses for Iceberg's
  // `CALL` procedures - so the two now agree by construction, the same
  // guarantee the DSv2 `StagedTable` fix below already gives the V2 side.
  private val createDataSourceTableAsSelect: PartialFunction[LogicalPlan, WriteCommandInfo] = {
    case cmd: CreateDataSourceTableAsSelectCommand =>
      val (location, diagnostic) = cmd.table.storage.locationUri match {
        case Some(uri) => (uri.toString, None)
        case None =>
          scala.util.Try(
            org.apache.spark.sql.SparkSession.active.sessionState.catalog.defaultTablePath(cmd.table.identifier)
          ) match {
            case scala.util.Success(uri) => (uri.toString, None)
            case scala.util.Failure(_) =>
              val msg = s"No storage location on new table '${cmd.table.identifier}', and its default " +
                "warehouse path could not be resolved; using its table identifier as a best-effort location"
              (cmd.table.identifier.unquotedString, Some(Diagnostic("CreateDataSourceTableAsSelectCommand", msg)))
          }
      }
      WriteCommandInfo(
        location = location,
        query = cmd.query,
        format = cmd.table.provider,
        saveMode = SparkPlanAdapter.saveModeOf(cmd.mode),
        outputSchema = cmd.query.schema,
        diagnostic = diagnostic
      )
  }

  // Streaming writes (`.writeStream.start(...)`/`.toTable(...)`) analyze to
  // a single WriteToStream node, emitted once per query - not once per
  // micro-batch (confirmed empirically: a real streaming Delta write under
  // Trigger.AvailableNow() produced exactly one WriteToStream instance
  // through injectCheckRule, alongside several per-micro-batch plans) -
  // carrying the query being written (`inputQuery`) and the resolved
  // sink/table. Before this case existed, WriteToStream wasn't
  // Command-shaped, so ContractEnforcementRule's fail-closed policy (which
  // only gates Command-shaped plans) never even saw it: not "fails closed,
  // unverified" like every other gap this module tracks, but genuinely
  // unenforced - a streaming write committed with no contract check at
  // all, confirmed by a probe showing zero of the plans injectCheckRule
  // saw during a real streaming Delta write were Command-shaped, and by
  // `javap` confirming WriteToStream doesn't implement Command. Adding it
  // to this registry - the same one every other write shape goes through -
  // means ContractEnforcementRule.verifyOrThrow verifies it the normal
  // way, no special-casing needed there: see docs/SPARK_ADAPTER.md's
  // "Streaming writes" section.
  private val writeToStream: PartialFunction[LogicalPlan, WriteCommandInfo] = {
    case ws: WriteToStream =>
      val (location, format, diagnostic) = streamSinkLocationAndFormat(ws)
      WriteCommandInfo(
        location = location,
        query = ws.inputQuery,
        format = format,
        // Streaming's OutputMode (Append/Update/Complete) is a different
        // concept from batch SaveMode (append/overwrite/ignore/error) - not
        // modeled here. StructuralVerifier already skips the save-mode
        // check when the actual side is unknown (None), same as every
        // other format-detection miss elsewhere in this module.
        saveMode = None,
        outputSchema = ws.inputQuery.schema,
        diagnostic = diagnostic
      )
  }

  /** `WriteToStream.sink` is typed as a generic V2 `Table`, but Delta's
    * `DeltaSink` (and several of Spark's own built-in streaming sinks) is
    * actually a *legacy* V1 `execution.streaming.Sink` wrapped to look like
    * one - and that wrapper's `name()`/`schema()` unconditionally throw
    * `IllegalStateException("should not be called")` (confirmed
    * empirically - `Sink`'s own default implementation). So this can't just
    * call `sink.name()` the way a real V2 `Table` would allow.
    *
    * Tries, in order, each confirmed empirically against a real
    * Delta-enabled session: (1) a populated `catalogTable` - `.toTable(...)`
    * resolves one with `storage.locationUri` and `provider` already filled
    * in, the same fields `createDataSourceTableAsSelect` above uses; (2)
    * `sink.name()`, guarded, for a genuine V2 sink where it doesn't throw;
    * (3) a reflective call to a public `path()` accessor - confirmed
    * present on `DeltaSink` via `javap` and returning the exact physical
    * sink path for a path-based `.start(path)` write - the same
    * reflection-over-a-class-this-module-has-no-compile-time-dependency-on
    * technique `jdbcLocationOf` uses for `JDBCRelation`. If none of those
    * resolve, falls back to the sink's own `toString`, exactly like every
    * other unresolvable-location case elsewhere in this module, with a
    * diagnostic explaining why.
    *
    * Found while adding Parquet connector support
    * (docs/ADDING_A_SPARK_CONNECTOR.md): tier 3 (the reflective `path()`
    * lookup) previously only tried a *public method* named `path` -
    * exactly what `DeltaSink` exposes, but not what Spark's own built-in
    * `org.apache.spark.sql.execution.streaming.FileStreamSink` does (the
    * sink for every plain `FileFormat`-based streaming write -
    * `.writeStream.format("parquet"/"csv"/"json"/"orc"/"text")...`, not
    * just Parquet). `FileStreamSink.name()` throws the same `Sink`-default
    * `IllegalStateException` `DeltaSink.name()` does (confirmed via
    * `javap` and a direct-construction probe - it does *not* override
    * `name()` with a non-throwing implementation, despite first appearing
    * to when only its `toString()` had been observed), so tier 2 already
    * correctly falls through for it. But `FileStreamSink.path` (confirmed
    * via `javap`) is a `private final` field with no public accessor at
    * all - unlike `DeltaSink.path()` - so the old method-only
    * `reflectiveSinkPath` found nothing there either, and every plain
    * `FileFormat`-based streaming write fell all the way to the last-resort
    * `ws.sink.toString` (`"FileSink[<path>]"`, not a bare path) -
    * `OUTPUT_LOCATION_MISMATCH` against any contract's real declared
    * physical location, unconditionally. Real bug, confirmed via
    * `ParquetConnectorSpec` PASS tests that previously failed this way.
    * Fixed by extending `reflectiveSinkPath` to also try the *declared
    * field* directly (`reflectivePrivateField` below) when the public-method
    * lookup finds nothing - the same no-compile-time-dependency-tolerant
    * convention as everywhere else in this file, just extended from
    * "public method" to "declared field" since `FileStreamSink`, while a
    * plain public class already on this module's `provided` Spark
    * dependency, exposes neither `path` nor `fileFormat` as a public
    * method.
    */
  private def streamSinkLocationAndFormat(ws: WriteToStream): (String, Option[String], Option[Diagnostic]) =
    ws.catalogTable match {
      case Some(table) =>
        val location = table.storage.locationUri.map(_.toString).getOrElse(table.identifier.unquotedString)
        (location, table.provider, None)
      case None =>
        scala.util.Try(Option(ws.sink.name())).toOption.flatten match {
          case Some(name) => (name, streamSinkFormatOf(ws.sink), None)
          case _ =>
            reflectiveSinkPath(ws.sink) match {
              case Some(path) => (path, streamSinkFormatOf(ws.sink), None)
              case None =>
                val msg = s"Could not determine a precise location for streaming sink " +
                  s"${ws.sink.getClass.getSimpleName}; using its toString as a best-effort location"
                (ws.sink.toString, streamSinkFormatOf(ws.sink), Some(Diagnostic("WriteToStream", msg)))
            }
        }
    }

  /** `sink` is a `Table` instance, not a `TableProvider`/`RelationProvider`
    * - it doesn't implement `DataSourceRegister` the way the format-source
    * objects `formatOf` matches on do (confirmed for Delta's `DeltaSink` via
    * `javap`), so that mechanism doesn't apply here. Matched by simple class
    * name instead, the same reflection-friendly convention as
    * `jdbcLocationOf`/`unwrapWriteWrapper`.
    *
    * `FileStreamSink` (Spark's own built-in sink for any `FileFormat`-based
    * streaming write, not Parquet-specific) holds its `FileFormat` in a
    * private `fileFormat` field, confirmed via `javap` to have no public
    * accessor - reflected into directly (`reflectiveFileSinkFormat`) and
    * passed through the same `formatOf` every other write shape in this
    * file already uses, since `FileFormat`'s `DataSourceRegister.shortName()`
    * mechanism doesn't care how the instance was obtained.
    */
  private def streamSinkFormatOf(sink: AnyRef): Option[String] =
    if (sink.getClass.getSimpleName == "DeltaSink") Some("delta")
    else if (sink.getClass.getSimpleName == "FileStreamSink") reflectiveFileSinkFormat(sink)
    else None

  private def reflectiveFileSinkFormat(sink: AnyRef): Option[String] =
    reflectivePrivateField(sink, "fileFormat").flatMap(SparkPlanAdapter.formatOf)

  private def reflectiveSinkPath(sink: AnyRef): Option[String] =
    scala.util.Try(sink.getClass.getMethod("path").invoke(sink).toString).toOption
      .orElse(reflectivePrivateField(sink, "path").map(_.toString))

  /** `FileStreamSink`'s `path`/`fileFormat` fields are `private final`, with
    * no public accessor (confirmed via `javap` - unlike `DeltaSink`, which
    * exposes a public `path()` method `reflectiveSinkPath`'s method-based
    * lookup already covers). `setAccessible` is safe here: reading a value
    * already fully constructed on Spark's own public, `provided`-dependency
    * class, not modifying anything.
    */
  private def reflectivePrivateField(obj: AnyRef, fieldName: String): Option[AnyRef] =
    scala.util.Try {
      val f = obj.getClass.getDeclaredField(fieldName)
      f.setAccessible(true)
      f.get(obj)
    }.toOption

  // DataSourceV2 catalog writes against an *existing* table -
  // `.saveAsTable(...)` append, `.insertInto(...)`, and
  // `.writeTo(...).append()` all confirmed empirically (via injectCheckRule
  // against a real Delta-enabled session, not assumed) to analyze to this
  // single command, none of them InsertIntoHadoopFsRelationCommand/
  // SaveIntoDataSourceCommand - those are V1 shapes; a DSv2 catalog
  // (Delta's included) always resolves an existing-table write through
  // this V2 command instead. Confirmed previously (see the "Delta Lake
  // operation-surface coverage ledger" in docs/SPARK_ADAPTER.md) to
  // correctly fail closed; this closes it for real.
  private val appendData: PartialFunction[LogicalPlan, WriteCommandInfo] = {
    case cmd: AppendData =>
      val (location, format, diagnostic) = namedRelationLocationAndFormat(cmd.table)
      val (outputSchema, generatedColumnsDiagnostic) =
        outputSchemaWithTargetOnlyFields(cmd.table, cmd.query, "AppendData")
      WriteCommandInfo(
        location = location,
        query = cmd.query,
        format = format,
        saveMode = Some("append"),
        outputSchema = outputSchema,
        diagnostic = diagnostic.orElse(generatedColumnsDiagnostic)
      )
  }

  // `.writeTo(...).overwrite(condition)` - confirmed empirically to
  // analyze to this command, carrying the same NamedRelation target shape
  // as AppendData above plus a `deleteExpr` (the overwrite predicate -
  // `true` for a full overwrite, an arbitrary expression for a
  // conditional/dynamic-partition one). `ir.Write` has no field for that
  // predicate - not needed for what StructuralVerifier actually checks
  // (schema/format/location/save mode), so this maps to the contract's
  // coarse-grained "overwrite" saveMode uniformly rather than needing an
  // IR extension, unlike what was first assumed (see ROADMAP.md).
  private val overwriteByExpression: PartialFunction[LogicalPlan, WriteCommandInfo] = {
    case cmd: OverwriteByExpression =>
      val (location, format, diagnostic) = namedRelationLocationAndFormat(cmd.table)
      val (outputSchema, generatedColumnsDiagnostic) =
        outputSchemaWithTargetOnlyFields(cmd.table, cmd.query, "OverwriteByExpression")
      WriteCommandInfo(
        location = location,
        query = cmd.query,
        format = format,
        saveMode = Some("overwrite"),
        outputSchema = outputSchema,
        diagnostic = diagnostic.orElse(generatedColumnsDiagnostic)
      )
  }

  // `.writeTo(...).overwritePartitions()` - Spark's "dynamic partition
  // overwrite" (replace only the partitions the query's rows actually
  // touch, leaving the rest untouched). Confirmed empirically (a real
  // Iceberg-enabled session, not assumed - see docs/SPARK_ADAPTER.md's
  // Iceberg section) to be a real, previously-unrecognized write shape:
  // Command-shaped, not on FailClosedCommands' safe list, so it was
  // already failing closed rather than silently passing - this closes it
  // for real instead. Exact same NamedRelation-`table`-plus-`query` shape
  // as AppendData/OverwriteByExpression above (confirmed via `V2WriteCommand`,
  // the shared supertype all three implement), so it reuses the same two
  // helpers rather than re-deriving anything connector- or shape-specific.
  // Maps to the contract's coarse-grained "overwrite" saveMode, the same
  // approximation OverwriteByExpression's own conditional/dynamic case
  // already makes - StructuralVerifier's save-mode check doesn't
  // distinguish "overwrite everything" from "overwrite only the touched
  // partitions" any more than it does "overwrite matching a predicate".
  private val overwritePartitionsDynamic: PartialFunction[LogicalPlan, WriteCommandInfo] = {
    case cmd: OverwritePartitionsDynamic =>
      val (location, format, diagnostic) = namedRelationLocationAndFormat(cmd.table)
      val (outputSchema, generatedColumnsDiagnostic) =
        outputSchemaWithTargetOnlyFields(cmd.table, cmd.query, "OverwritePartitionsDynamic")
      WriteCommandInfo(
        location = location,
        query = cmd.query,
        format = format,
        saveMode = Some("overwrite"),
        outputSchema = outputSchema,
        diagnostic = diagnostic.orElse(generatedColumnsDiagnostic)
      )
  }

  // `.format("delta").saveAsTable(...)` on a *new* table, and
  // `.writeTo(...).createOrReplace()` - confirmed empirically to both
  // analyze to this V2 command (not CreateDataSourceTableAsSelectCommand,
  // which only V1 sources use for a new-table saveAsTable). `name`
  // resolves to a ResolvedIdentifier, not yet a full CatalogTable with a
  // storage location, since the table doesn't exist yet at analysis time
  // - so unlike createDataSourceTableAsSelect above, there is no physical
  // path to prefer; the qualified catalog identifier is the best
  // available location. `tableSpec.provider` gives format directly, the
  // V2 counterpart of `cmd.table.provider` above.
  private val replaceTableAsSelect: PartialFunction[LogicalPlan, WriteCommandInfo] = {
    case cmd: ReplaceTableAsSelect =>
      val (location, diagnostic) = v2CreateOrReplaceLocation(cmd.name, "ReplaceTableAsSelect")
      WriteCommandInfo(
        location = location,
        query = cmd.query,
        format = cmd.tableSpec.provider,
        // A REPLACE (with or without OR CREATE) always replaces the
        // table's entire prior content wholesale - unlike
        // OverwriteByExpression above, there's no partial/conditional
        // case to blur, so "overwrite" is exact here, not an
        // approximation.
        saveMode = Some("overwrite"),
        outputSchema = cmd.query.schema,
        diagnostic = diagnostic
      )
  }

  // `.writeTo(...).create()` - explicit-create V2 CTAS (fails if the
  // table already exists, unless `ignoreIfExists` - `CREATE TABLE IF NOT
  // EXISTS ... AS SELECT`). Confirmed empirically (a real Iceberg-enabled
  // session, not assumed) to be a real, previously-unrecognized write
  // shape distinct from `ReplaceTableAsSelect` above - `.saveAsTable()`
  // and `.writeTo(...).createOrReplace()` both map to REPLACE semantics
  // under a V2 catalog (confirmed via the same probe), but a bare,
  // explicit `.writeTo(...).create()` produces this separate command -
  // was Command-shaped, not on FailClosedCommands' safe list, so it was
  // already failing closed rather than silently passing; not
  // Iceberg-specific - any DSv2-catalog connector's explicit-create CTAS
  // hits the identical gap, closed here once for all of them. Same
  // `name`/`tableSpec`/`query` shape as ReplaceTableAsSelect, so it
  // reuses the same location-resolution helper.
  private val createTableAsSelect: PartialFunction[LogicalPlan, WriteCommandInfo] = {
    case cmd: CreateTableAsSelect =>
      val (location, diagnostic) = v2CreateOrReplaceLocation(cmd.name, "CreateTableAsSelect")
      WriteCommandInfo(
        location = location,
        query = cmd.query,
        format = cmd.tableSpec.provider,
        // ignoreIfExists distinguishes CREATE TABLE IF NOT EXISTS (silently
        // skip if the table's already there) from a bare CREATE TABLE
        // (error if it is) - the same distinction SaveMode.Ignore/
        // SaveMode.ErrorIfExists make on the V1 side.
        saveMode = Some(if (cmd.ignoreIfExists) "ignore" else "error"),
        outputSchema = cmd.query.schema,
        diagnostic = diagnostic
      )
  }

  /** Shared by `replaceTableAsSelect`/`createTableAsSelect` above: both
    * commands name their (not-yet-existing, or about-to-be-replaced)
    * target the same way - a `LogicalPlan` that's a `ResolvedIdentifier`
    * once analyzed, with no physical storage location yet since the table
    * doesn't exist until this write actually runs. Kept in one place so
    * the two can't drift into two subtly different fallback messages for
    * what's really the same situation.
    */
  private def v2CreateOrReplaceLocation(name: LogicalPlan, tag: String): (String, Option[Diagnostic]) =
    name match {
      case ri: ResolvedIdentifier =>
        val qualified = qualifiedIdentifier(ri.catalog, ri.identifier)
        val msg = s"No physical location resolved yet for new/replaced table '$qualified' " +
          s"($tag names a table that doesn't exist until this write runs); " +
          "using its qualified catalog identifier as the location"
        (qualified, Some(Diagnostic(tag, msg)))
      case other =>
        val msg = s"Could not resolve a table identifier from $tag's unresolved " +
          s"name (${other.getClass.getSimpleName}); using its toString as a best-effort location"
        (other.toString, Some(Diagnostic(tag, msg)))
    }

  /** Shared by `appendData`/`overwriteByExpression` above and
    * `replaceTableAsSelect`'s `ResolvedIdentifier` case: the exact same
    * `catalogName.namespace.tableName` format both need, kept in one
    * place so the two can never drift into two subtly different qualified
    * forms for what's actually the same underlying table (see this
    * method's use in `namedRelationLocationAndFormat` below for why that
    * matters here specifically, not just as a style preference).
    */
  private def qualifiedIdentifier(catalog: org.apache.spark.sql.connector.catalog.CatalogPlugin, identifier: org.apache.spark.sql.connector.catalog.Identifier): String =
    s"${catalog.name}.${identifier.namespace.mkString(".")}.${identifier.name}"

  /** Shared by `appendData`/`overwriteByExpression` above: both commands'
    * `table: NamedRelation` resolves, for any DataSourceV2 catalog
    * (Delta's included), to a `DataSourceV2Relation` wrapping a `Table`
    * handle — the same handle `SparkPlanAdapter.tableLocationAndFormat`
    * already reads `properties()` from for the analogous
    * `StreamingRelationV2` read case.
    *
    * Three tiers, not two — confirmed necessary empirically, not assumed:
    * `.saveAsTable(...)`/`.writeTo(...).createOrReplace()`/`.create()` on
    * a *new* table produces both a top-level `ReplaceTableAsSelect`/
    * `CreateTableAsSelect` (handled by `replaceTableAsSelect`/
    * `createTableAsSelect` above) *and* an internal, nested `AppendData`
    * against a `StagedTable` (Spark's own public 2-phase-commit protocol
    * for atomic CTAS/RTAS — Delta's `StagedDeltaTableV2` implements it) —
    * both visible to `injectCheckRule` for the same one call. Without this
    * middle tier the two writes would resolve to two different, mismatched
    * locations for what's really one destination, and whichever one a
    * contract's declared location matched, the other would fail with
    * `OUTPUT_LOCATION_MISMATCH`, aborting a genuinely contract-satisfying
    * write.
    *
    * The tier is keyed on `StagedTable` itself (Spark's own public marker
    * for "not committed yet"), not on whether `properties()` happens to
    * omit `"location"` — confirmed empirically, the hard way, that the
    * latter doesn't generalize: Delta's `StagedDeltaTableV2` reports no
    * `"location"` pre-commit, but Iceberg's staged table *does* report one
    * (a real path that turned out not to match `CreateTableAsSelect`'s own
    * qualified-identifier resolution — caught by a real
    * `OUTPUT_LOCATION_MISMATCH` test failure, not assumed to generalize
    * from the Delta case). A staged table's reported location, even when
    * present, isn't trustworthy for this purpose - the table isn't
    * committed yet - so `StagedTable` forces the qualified-identifier tier
    * unconditionally, before `tableLocationAndFormat` is even consulted.
    * `DataSourceV2Relation`'s own `catalog`/`identifier` fields (confirmed
    * populated even for a staged table) give the same qualified form
    * `qualifiedIdentifier` above computes from `ReplaceTableAsSelect`'s/
    * `CreateTableAsSelect`'s own `ResolvedIdentifier` — the two now always
    * agree for the same table by construction, not by coincidence.
    *
    * Falls back to the relation's own `name()` only when neither a
    * physical location nor `catalog`+`identifier` are available.
    */
  private def namedRelationLocationAndFormat(table: NamedRelation): (String, Option[String], Option[Diagnostic]) =
    table match {
      case v2: DataSourceV2Relation =>
        val (rawLocation, format) = SparkPlanAdapter.tableLocationAndFormat(v2.table)
        val location =
          if (v2.table.isInstanceOf[org.apache.spark.sql.connector.catalog.StagedTable]) None else rawLocation
        location match {
          case Some(loc) => (loc, format, None)
          case None =>
            (v2.catalog, v2.identifier) match {
              case (Some(catalog), Some(identifier)) =>
                val qualified = qualifiedIdentifier(catalog, identifier)
                val msg = s"No physical location trusted for write target '$qualified' (a StagedTable pending " +
                  "an atomic CREATE/REPLACE commit isn't committed yet, so any location it reports isn't " +
                  "trustworthy - confirmed empirically that this varies by connector); using its qualified " +
                  "catalog identifier as the location"
                (qualified, format, Some(Diagnostic("V2Write", msg)))
              case _ =>
                val msg = s"No 'location' property and no catalog/identifier on write target '${table.name}'; " +
                  "using its name() as a best-effort location"
                (table.name, format, Some(Diagnostic("V2Write", msg)))
            }
        }
      case _ =>
        val msg = s"Write target ${table.getClass.getSimpleName} is not a DataSourceV2Relation; " +
          "using its name() as a best-effort location"
        (table.name, None, Some(Diagnostic("V2Write", msg)))
    }

  /** A resolved write target can legitimately have fields the write's own
    * `query` doesn't supply, that will still exist in the committed row -
    * Delta's generated columns (`GENERATED ALWAYS AS (...)`, computed by
    * Delta at commit time) and Iceberg's `write.spark.accept-any-schema`
    * partial/narrower writes (the omitted, already-existing column is
    * NULL-filled) are two real, connector-specific *mechanisms* for the
    * same underlying situation. Both were originally two separate special
    * cases here (a Delta-specific reflective metadata lookup for
    * generated columns) until a real Iceberg investigation found the
    * second mechanism and, in confirming it, found this simpler,
    * connector-agnostic fix subsumes the first one too: `cmd.table.schema()`
    * (the resolved `NamedRelation`'s current, already-committed schema -
    * a plain, public API, no reflection) already carries a generated
    * column's *name*, confirmed empirically even though its `.metadata`
    * (the part that would have identified it as specifically "generated")
    * is stripped - so detecting *which* target-only fields are generated
    * was never actually necessary; only whether the target has fields the
    * query doesn't.
    *
    * Why this is safe, not just convenient: by the time a `DataSourceV2Relation`-based
    * write reaches this check rule at all, Spark's own analyzer has
    * *already* validated the write's schema is acceptable against the
    * target - confirmed empirically (a real probe, since deleted) that an
    * Iceberg table *without* `accept-any-schema` rejects a narrower write
    * with `AnalysisException` before it ever produces an `AppendData`
    * node for this rule to see at all. So a genuinely-missing required
    * field (the case `MISSING_OUTPUT_FIELD` exists to catch) is never
    * silenced by this: either Spark's analyzer already rejected the write
    * for real (this code never runs), or the target's own connector has
    * already endorsed the field being absent from `query` as valid,
    * meaning unioning it into `outputSchema` reports what will actually
    * be committed, not what the writer merely provided.
    */
  private def outputSchemaWithTargetOnlyFields(
    table: NamedRelation,
    query: LogicalPlan,
    tag: String
  ): (StructType, Option[Diagnostic]) = {
    val targetFields = table match {
      // Table.columns() (not the deprecated Table.schema()) - a Column
      // only guarantees name/dataType/nullable, exactly what's needed
      // here; no other Column field (default value, generation
      // expression, comment) is used.
      case v2: DataSourceV2Relation =>
        v2.table.columns().toSeq.map(c => StructField(c.name(), c.dataType(), c.nullable()))
      case _ => Seq.empty[StructField]
    }
    val (unioned, targetOnlyFields) = unionNewFields(query.schema, targetFields)
    if (targetOnlyFields.isEmpty) (query.schema, None)
    else {
      val msg = "Target has field(s) not present in the write's own schema " +
        s"(${targetOnlyFields.map(_.name).mkString(", ")}) - a resolved write reaching this point " +
        "means the target's own connector has already endorsed their absence as valid (e.g. a " +
        "Delta generated column, or an Iceberg accept-any-schema partial write), so they're unioned " +
        "into outputSchema as a best-effort approximation of the committed schema."
      (unioned, Some(Diagnostic(tag, msg)))
    }
  }

  /** Shared by `outputSchemaWithTargetOnlyFields` above and
    * `deltaRowLevelDml`'s MERGE schema-evolution branch below: both need
    * "which fields does `candidateFields` have that `base` doesn't (by
    * name), and what does `base` look like with those unioned in" - the
    * exact same computation over two different schema pairs (a target
    * schema vs. the resolved target's own current schema; a target
    * schema vs. an evolving MERGE's source). Returns the *new* fields
    * alongside the unioned `StructType` so each call site can still
    * decide independently whether "no new fields" is worth a diagnostic -
    * they currently differ on that, deliberately (see each caller).
    */
  private def unionNewFields(base: StructType, candidateFields: Seq[StructField]): (StructType, Seq[StructField]) = {
    val baseFieldNames = base.fieldNames.toSet
    val newFields = candidateFields.filterNot(f => baseFieldNames.contains(f.name))
    (StructType(base.fields ++ newFields), newFields)
  }

  // Delta's row-level DML - MERGE INTO / UPDATE / DELETE - all analyze to
  // Delta-internal command classes (org.apache.spark.sql.delta.commands.*),
  // confirmed empirically via injectCheckRule, not assumed. Matched by
  // fully-qualified class name (a Set[String], not `case cmd: X`) and
  // read via plain public-method reflection (`target()`/`catalogTable()`/
  // `source()` are all public, confirmed via javap - no `setAccessible`
  // needed) - the same convention `jdbcLocationOf`/`unwrapWriteWrapper`
  // use, for the same reason: this module has no compile-time dependency
  // on Delta, and these three classes are Delta-internal (undocumented,
  // no cross-version API guarantee), unlike AppendData/
  // OverwriteByExpression/ReplaceTableAsSelect above, which are stable
  // public Spark classes. Wrapped in Try, unlike those: if a future Delta
  // version renames one of these methods, this must degrade to the
  // existing fail-closed default (safe, if unverified) rather than let a
  // raw ReflectiveOperationException escape into a real Spark job -
  // `Function.unlift` turns "guard matched but extraction failed" into
  // "this case isn't defined after all," not a thrown exception.
  //
  // What this verifies, and - just as importantly - what it deliberately
  // doesn't: row-level DML has no "new output" the way every other write
  // shape does (a MERGE/UPDATE/DELETE's own `output` is a row-count
  // summary, not data - confirmed empirically), so there's no committed
  // schema to check a contract's declared fields against the usual way.
  // What's genuinely checkable, and what this checks: that the operation
  // actually targets the contract's declared output location (catching a
  // real mistake - operating on the wrong table) and that the target's
  // *current* schema still satisfies the contract (catching schema
  // drift). The operation's actual row-level logic - the merge condition,
  // which columns an UPDATE touches, whether a DELETE is unconditional -
  // is NOT verified: there is no contract vocabulary for that yet (see
  // docs/CONTRACT_MODEL.md's `rules` field - recorded, not interpreted -
  // and ROADMAP.md's "Full semantic DML verification" item, which this
  // deliberately does not attempt). MERGE's `source` (this case's `query`
  // for MergeIntoCommand) is recognized as a contract input -
  // `ContractEnforcementRule.verifyOrThrow`'s input-schema collection
  // explicitly walks this case's `query` in addition to the raw analyzed
  // plan, specifically *because* Delta's DML commands are leaf nodes in
  // the tree-traversal sense (`source`/`target` are ordinary case-class
  // fields, not exposed as `children`), so a plain `plan.collect` never
  // reaches them on its own the way it does for every other write shape
  // here - confirmed the hard way by a real FAIL test never throwing
  // before that collection was fixed, not assumed to "just work."
  private val deltaRowLevelDml: PartialFunction[LogicalPlan, WriteCommandInfo] =
    Function.unlift { (plan: LogicalPlan) =>
      if (!deltaDmlClassNames.contains(plan.getClass.getName)) None
      else
        scala.util.Try {
          val target = plan.getClass.getMethod("target").invoke(plan).asInstanceOf[LogicalPlan]
          val catalogTable =
            plan.getClass.getMethod("catalogTable").invoke(plan).asInstanceOf[Option[SparkCatalogTable]]
          val (location, diagnostic) = catalogTable.flatMap(_.storage.locationUri).map(_.toString) match {
            case Some(loc) => (loc, None)
            case None =>
              val fallback = catalogTable.map(_.identifier.unquotedString).getOrElse(target.toString)
              val msg = s"No catalog storage location for ${plan.getClass.getSimpleName}'s target; " +
                s"using ${if (catalogTable.isDefined) "its table identifier" else "the target plan's toString"} as a best-effort location"
              (fallback, Some(Diagnostic(plan.getClass.getSimpleName, msg)))
          }
          // Only MergeIntoCommand has a separate `source` - UPDATE/DELETE
          // mutate `target` in place based on `condition` alone, so
          // `target` doubles as the only sensible "query" to render, and
          // (since UPDATE/DELETE can never introduce a column that didn't
          // already exist - there's no SQL syntax for it) target.schema is
          // always the right outputSchema for them, no evolution handling
          // needed.
          val isMerge = plan.getClass.getSimpleName == "MergeIntoCommand"
          val query =
            if (isMerge) plan.getClass.getMethod("source").invoke(plan).asInstanceOf[LogicalPlan]
            else target

          // A MERGE with schema evolution active is a real, common Delta
          // pattern - confirmed empirically (not assumed) that target.schema
          // at analysis time is the *pre-merge* schema, not the schema the
          // commit is about to produce: a contract requiring a field the
          // merge is legitimately about to add would otherwise be
          // MISSING_OUTPUT_FIELD-rejected for a write that would have
          // satisfied it. `schemaEvolutionEnabled()` (public, confirmed via
          // javap) makes this detectable. Fix is deliberately a best-effort
          // approximation, not a full simulation of Delta's evolution rules
          // (type widening, nested-struct merging, column reordering are
          // all real Delta behaviors this doesn't attempt) - consistent
          // with this whole case's structural-only scope: the source's
          // *new* fields (ones target doesn't already have) are unioned in,
          // covering the specific false-rejection this was found through,
          // with a diagnostic making the approximation visible rather than
          // silently precise-looking.
          val (outputSchema, evolutionDiagnostic) =
            if (isMerge && plan.getClass.getMethod("schemaEvolutionEnabled").invoke(plan).asInstanceOf[Boolean]) {
              val (evolved, _) = unionNewFields(target.schema, query.schema.fields.toSeq)
              val msg = "MERGE has schema evolution enabled (schemaEvolutionEnabled=true); outputSchema is target.schema " +
                "plus the source's new fields, a best-effort approximation of the post-commit schema - not a full " +
                "simulation of Delta's evolution rules (type widening, nested-struct merging are not modeled)"
              (evolved, Some(Diagnostic("MergeIntoCommand", msg)))
            } else (target.schema, None)

          WriteCommandInfo(
            location = location,
            query = query,
            format = Some("delta"),
            saveMode = None, // in-place mutation isn't append/overwrite/ignore/error
            outputSchema = outputSchema,
            diagnostic = diagnostic.orElse(evolutionDiagnostic)
          )
        }.toOption
    }

  private val deltaDmlClassNames: Set[String] = Set(
    "org.apache.spark.sql.delta.commands.MergeIntoCommand",
    "org.apache.spark.sql.delta.commands.UpdateCommand",
    "org.apache.spark.sql.delta.commands.DeleteCommand"
  )

  // Row-level DML (MERGE/UPDATE/DELETE) for connectors implementing
  // Spark's standard DSv2 `SupportsRowLevelOperations` - Iceberg's
  // mechanism, confirmed empirically (a real Iceberg-enabled session, not
  // assumed) to be genuinely different from Delta's: Spark's own
  // `RewriteRowLevelOperation` optimizer-rule family rewrites MERGE/
  // UPDATE/DELETE into one of two *stable, public* Spark classes -
  // `ReplaceData` (copy-on-write) or `WriteDelta` (merge-on-read) - both
  // implementing the shared `RowLevelWrite` trait (which extends
  // `V2WriteCommand`, the same `table`/`query` shape `AppendData`/
  // `OverwriteByExpression`/`OverwritePartitionsDynamic` already share).
  // Unlike `deltaRowLevelDml` above, this needs no reflection at all -
  // `RowLevelWrite` is a real, importable Spark type, not a
  // connector-internal one - so this is a genuinely connector-agnostic
  // case: any future DSv2 connector using Spark's standard row-level-
  // operation API is covered by this same case, not a per-connector copy.
  //
  // Scope mirrors `deltaRowLevelDml` deliberately, for the same reason:
  // structural verification only. `cmd.table`'s *current* schema is what's
  // checked against the contract (not `cmd.query`, which is the rewritten
  // rows to write for the matched/unmatched branches, not the whole
  // committed table) - the actual row-level logic (the merge condition,
  // which columns an UPDATE touches, whether a DELETE is unconditional)
  // has no IR representation and isn't checked; see ROADMAP.md's "Full
  // semantic DML verification" item and docs/ADDING_A_SPARK_CONNECTOR.md's
  // "Known limitations" for why that's deliberately out of scope here,
  // not an oversight. `saveMode = None`, same reasoning as
  // `deltaRowLevelDml`: in-place mutation isn't append/overwrite/ignore/
  // error.
  private val dsv2RowLevelWrite: PartialFunction[LogicalPlan, WriteCommandInfo] = {
    case cmd: RowLevelWrite =>
      val (location, format, diagnostic) = namedRelationLocationAndFormat(cmd.table)
      WriteCommandInfo(
        location = location,
        query = cmd.query,
        format = format,
        saveMode = None,
        outputSchema = cmd.table.schema,
        diagnostic = diagnostic
      )
  }

  /** `DELETE FROM <v2-table> WHERE <predicate>` against a connector that
    * implements plain `SupportsDelete` (predicate pushdown truncate/delete)
    * rather than `SupportsRowLevelOperations` - confirmed empirically for
    * ClickHouse (`add-spark-connector`'s onboarding pass): a real
    * `DELETE FROM ch.db.tbl WHERE id = 1` against a live ClickHouse-backed
    * catalog table executes successfully, staying as a plain
    * `DeleteFromTable` node - Spark's `RewriteRowLevelOperation` optimizer
    * rule never touches it, since that rewrite only fires for connectors
    * implementing `SupportsRowLevelOperations` (`RowLevelWrite`'s own
    * case above). A genuinely different, simpler write shape from
    * `RowLevelWrite`, not a special case of it - `DeleteFromTable.table`
    * is a plain `LogicalPlan` (possibly `SubqueryAlias`-wrapped), not a
    * `NamedRelation` directly, so the underlying relation is located by
    * `collectFirst` rather than passed straight to
    * `namedRelationLocationAndFormat`.
    *
    * Same scope as `deltaRowLevelDml`/`dsv2RowLevelWrite`: structural only
    * - the delete predicate itself has no IR representation and isn't
    * checked, only the target's location/schema (catching the
    * wrong-table mistake). `query`/`outputSchema` both use the target's
    * own schema since a DELETE has no separate "query being written" the
    * way INSERT/MERGE do; `saveMode = None` for the same reason
    * `dsv2RowLevelWrite` uses it - in-place mutation isn't append/
    * overwrite/ignore/error.
    */
  private val deleteFromTable: PartialFunction[LogicalPlan, WriteCommandInfo] = {
    case cmd: DeleteFromTable =>
      cmd.table.collectFirst { case r: NamedRelation => r } match {
        case Some(relation) =>
          val (location, format, diagnostic) = namedRelationLocationAndFormat(relation)
          WriteCommandInfo(
            location = location,
            query = relation,
            format = format,
            saveMode = None,
            outputSchema = relation.schema,
            diagnostic = diagnostic
          )
        case None =>
          val msg = s"No NamedRelation found under DeleteFromTable's target; " +
            "using its own toString as a best-effort location"
          WriteCommandInfo(
            location = cmd.table.toString,
            query = cmd.table,
            format = None,
            saveMode = None,
            outputSchema = cmd.table.schema,
            diagnostic = Some(Diagnostic("DeleteFromTable", msg))
          )
      }
  }

  // Hive support (org.apache.spark.sql.hive.execution package, part of the
  // separate `spark-hive` artifact - see build.sbt's comment for why this
  // module has no compile-time dependency on it, unlike HiveTableRelation
  // on the read side, which SparkPlanAdapter imports directly since it
  // turned out to live in plain, already-`provided` spark-catalyst). All
  // three classes below were found by the same reflective jar-scan
  // technique used for Delta/Iceberg (see docs/ADDING_A_SPARK_CONNECTOR.md)
  // - a real Hive-enabled session (embedded Derby metastore, no external
  // Hive install needed) confirmed spark-hive's own Command hierarchy has
  // exactly these three concrete classes, nothing more. Matched by fully-
  // qualified class name and read via public-method reflection, the same
  // convention deltaRowLevelDml uses - the difference here is these ARE
  // stable, documented Spark classes (not undocumented internals), it's
  // just that spark-hive is a separate, optional, and much heavier
  // artifact this module deliberately keeps off its provided/compile
  // classpath.
  private val createHiveTableAsSelectClassName = "org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand"
  private val insertIntoHiveTableClassName = "org.apache.spark.sql.hive.execution.InsertIntoHiveTable"
  private val insertIntoHiveDirClassName = "org.apache.spark.sql.hive.execution.InsertIntoHiveDirCommand"

  // `.format("hive").saveAsTable(...)` on a NEW table, and
  // `CREATE TABLE ... STORED AS ... AS SELECT ...` - confirmed empirically
  // (a real embedded-Derby Hive session, not assumed) to always analyze to
  // this command, for both a genuinely new table AND an append onto an
  // EXISTING one (`.mode("append").format("hive").saveAsTable(existing)`
  // produces the identical top-level CreateHiveTableAsSelectCommand,
  // confirmed via injectCheckRule) - unlike the V1 CreateDataSourceTableAsSelectCommand
  // case above, Hive's own command doesn't distinguish new-vs-existing at
  // the plan-shape level at all. Its own run() method internally creates
  // the table (or verifies compatibility with the existing one) and then
  // issues a SECOND, nested InsertIntoHiveTable to perform the actual
  // write - both visible to injectCheckRule for one logical call, the same
  // "shared pitfall" class already documented for Delta/Iceberg/Parquet's
  // atomic CTAS (see docs/SPARK_ADAPTER.md) - confirmed via a real PASS/
  // FAIL pair that the two agree on location (both resolve the same
  // catalog table's storage.locationUri once it exists), so no fix was
  // needed here the way the DSv2 StagedTable case needed one.
  private val createHiveTableAsSelect: PartialFunction[LogicalPlan, WriteCommandInfo] =
    Function.unlift { (plan: LogicalPlan) =>
      if (plan.getClass.getName != createHiveTableAsSelectClassName) None
      else
        scala.util.Try {
          val tableDesc = plan.getClass.getMethod("tableDesc").invoke(plan).asInstanceOf[SparkCatalogTable]
          val query = plan.getClass.getMethod("query").invoke(plan).asInstanceOf[LogicalPlan]
          val mode = plan.getClass.getMethod("mode").invoke(plan).asInstanceOf[SaveMode]
          val (location, diagnostic) = tableDesc.storage.locationUri match {
            case Some(uri) => (uri.toString, None)
            case None =>
              val msg = s"No storage location on new Hive table '${tableDesc.identifier}'; " +
                "using its table identifier as a best-effort location"
              (tableDesc.identifier.unquotedString, Some(Diagnostic("CreateHiveTableAsSelectCommand", msg)))
          }
          WriteCommandInfo(
            location = location,
            query = query,
            format = Some(tableDesc.provider.getOrElse("hive")),
            saveMode = SparkPlanAdapter.saveModeOf(mode),
            outputSchema = query.schema,
            diagnostic = diagnostic
          )
        }.toOption
    }

  // `.insertInto(...)`, `INSERT [OVERWRITE] [INTO] TABLE ...` against an
  // EXISTING Hive-format table, and the nested write CreateHiveTableAsSelectCommand
  // above issues internally - all confirmed empirically to be this single
  // command, carrying its own `overwrite: Boolean` (append vs. overwrite,
  // read directly rather than inferred).
  private val insertIntoHiveTable: PartialFunction[LogicalPlan, WriteCommandInfo] =
    Function.unlift { (plan: LogicalPlan) =>
      if (plan.getClass.getName != insertIntoHiveTableClassName) None
      else
        scala.util.Try {
          val table = plan.getClass.getMethod("table").invoke(plan).asInstanceOf[SparkCatalogTable]
          val query = plan.getClass.getMethod("query").invoke(plan).asInstanceOf[LogicalPlan]
          val overwrite = plan.getClass.getMethod("overwrite").invoke(plan).asInstanceOf[Boolean]
          val (location, locationDiagnostic) = table.storage.locationUri match {
            case Some(uri) => (uri.toString, None)
            case None =>
              val msg = s"No storage location for Hive table '${table.identifier}'; " +
                "using its table identifier as a best-effort location"
              (table.identifier.unquotedString, Some(Diagnostic("InsertIntoHiveTable", msg)))
          }
          // A real, found-and-fixed false-rejection bug (the same class as
          // Delta's generated columns/DSv2's target-only fields, see
          // outputSchemaWithTargetOnlyFields above): confirmed empirically
          // that a STATIC-partition insert (`INSERT INTO t PARTITION(dt=
          // '2024-01-01') SELECT ...`) supplies the partition value as a
          // literal in the PARTITION clause, so it never appears in
          // query.schema at all - a contract requiring that column would
          // otherwise be falsely rejected with MISSING_OUTPUT_FIELD for a
          // write that genuinely produces it. A DYNAMIC-partition insert
          // (`INSERT INTO t SELECT ..., dt`) does NOT have this problem -
          // confirmed the value comes from the SELECT itself, already part
          // of query.schema - so this union is a no-op there (unionNewFields
          // finds nothing new to add). table.schema (CatalogTable's data +
          // partition columns) is the field of fields the query might be
          // missing; safe for the same reason outputSchemaWithTargetOnlyFields
          // is safe - by the time this command reaches the check rule at
          // all, Spark's own analyzer has already validated the write's
          // column count/types against the target (a genuinely missing
          // DATA column fails analysis before any plan is produced here).
          val (outputSchema, evolutionDiagnostic) = unionNewFields(query.schema, table.schema.fields.toSeq) match {
            case (_, Nil) => (query.schema, None)
            case (unioned, newFields) =>
              val msg = s"Hive table has field(s) not present in the write's own query " +
                s"(${newFields.map(_.name).mkString(", ")}) - most likely static partition value(s) supplied via " +
                "the PARTITION clause rather than selected by the query; unioned into outputSchema as a " +
                "best-effort approximation of the committed schema"
              (unioned, Some(Diagnostic("InsertIntoHiveTable", msg)))
          }
          WriteCommandInfo(
            location = location,
            query = query,
            format = Some(table.provider.getOrElse("hive")),
            saveMode = Some(if (overwrite) "overwrite" else "append"),
            outputSchema = outputSchema,
            diagnostic = locationDiagnostic.orElse(evolutionDiagnostic)
          )
        }.toOption
    }

  // `INSERT [OVERWRITE] [LOCAL] DIRECTORY '<path>' [ROW FORMAT ...] SELECT
  // ...` - Hive's directory-export write, confirmed via the same
  // reflective jar-scan to be a real, previously-unknown Command class
  // (not found by Phase 2's operation-surface probing alone - it isn't a
  // catalog-table write at all, so none of the standard .save/.saveAsTable/
  // .insertInto probes would ever trigger it). Fits ir.Write's "write a
  // dataset to a location" shape directly: writes to an arbitrary
  // filesystem path, not a registered table, so there's no CatalogTable to
  // read a provider string from - format is reported as "hive" uniformly,
  // consistent with every other Hive-routed write in this file.
  private val insertIntoHiveDir: PartialFunction[LogicalPlan, WriteCommandInfo] =
    Function.unlift { (plan: LogicalPlan) =>
      if (plan.getClass.getName != insertIntoHiveDirClassName) None
      else
        scala.util.Try {
          val storage = plan.getClass.getMethod("storage").invoke(plan)
            .asInstanceOf[org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat]
          val query = plan.getClass.getMethod("query").invoke(plan).asInstanceOf[LogicalPlan]
          val overwrite = plan.getClass.getMethod("overwrite").invoke(plan).asInstanceOf[Boolean]
          val (location, diagnostic) = storage.locationUri match {
            case Some(uri) => (uri.toString, None)
            case None =>
              val msg = "INSERT ... DIRECTORY has no resolved storage location; using its toString as a best-effort location"
              (plan.toString, Some(Diagnostic("InsertIntoHiveDirCommand", msg)))
          }
          WriteCommandInfo(
            location = location,
            query = query,
            format = Some("hive"),
            saveMode = Some(if (overwrite) "overwrite" else "append"),
            outputSchema = query.schema,
            diagnostic = diagnostic
          )
        }.toOption
    }

  /** Every recognized write shape, combined into one lookup —
    * `SparkPlanAdapter.Translator.translatePlan`,
    * `ContractEnforcementRule.verifyOrThrow`, and
    * `SparkAdapterListener.onSuccess` all consult exactly this, never a
    * match of their own. Add a new connector's write shape (when it
    * actually needs one — most don't; see docs/ADDING_A_SPARK_CONNECTOR.md)
    * by adding one more `PartialFunction` above, following the three as
    * templates, and chaining it in here. Nothing else in this module
    * needs to change.
    */
  val combined: PartialFunction[LogicalPlan, WriteCommandInfo] =
    insertIntoHadoopFsRelation orElse saveIntoDataSource orElse createDataSourceTableAsSelect orElse writeToStream orElse
      appendData orElse overwriteByExpression orElse overwritePartitionsDynamic orElse replaceTableAsSelect orElse
      createTableAsSelect orElse deltaRowLevelDml orElse dsv2RowLevelWrite orElse deleteFromTable orElse
      createHiveTableAsSelect orElse insertIntoHiveTable orElse insertIntoHiveDir

  /** Spark 3.4+ inserts an internal `WriteFiles` wrapper between a write
    * command and its query in the optimized/analyzed plan (confirmed
    * empirically for 3.5.1 — see docs/SPARK_ADAPTER.md). It carries no
    * information relevant to this IR, so it's unwrapped by class name
    * rather than importing it directly: an internal class an adapter
    * targeting a different Spark version might not have. Only
    * `InsertIntoHadoopFsRelationCommand`'s query needs this — confirmed
    * empirically that the other two shapes' queries don't get this
    * wrapper inserted.
    */
  private def unwrapWriteWrapper(plan: LogicalPlan): LogicalPlan =
    if (plan.getClass.getSimpleName == "WriteFiles" && plan.children.size == 1) plan.children.head
    else plan
}
