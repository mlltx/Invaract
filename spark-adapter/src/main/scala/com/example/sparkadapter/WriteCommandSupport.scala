// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.sparkadapter

import org.apache.spark.sql.catalyst.analysis.{NamedRelation, ResolvedIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable => SparkCatalogTable}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression, ReplaceTableAsSelect}
import org.apache.spark.sql.catalyst.streaming.WriteToStream
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, SaveIntoDataSourceCommand}
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
  private val createDataSourceTableAsSelect: PartialFunction[LogicalPlan, WriteCommandInfo] = {
    case cmd: CreateDataSourceTableAsSelectCommand =>
      val (location, diagnostic) = cmd.table.storage.locationUri match {
        case Some(uri) => (uri.toString, None)
        case None =>
          val msg = s"No storage location on new table '${cmd.table.identifier}'; " +
            "using its table identifier as a best-effort location"
          (cmd.table.identifier.unquotedString, Some(Diagnostic("CreateDataSourceTableAsSelectCommand", msg)))
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
    */
  private def streamSinkFormatOf(sink: AnyRef): Option[String] =
    if (sink.getClass.getSimpleName == "DeltaSink") Some("delta") else None

  private def reflectiveSinkPath(sink: AnyRef): Option[String] =
    scala.util.Try(sink.getClass.getMethod("path").invoke(sink).toString).toOption

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
        outputSchemaWithGeneratedColumns(cmd.table, cmd.query, "AppendData")
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
        outputSchemaWithGeneratedColumns(cmd.table, cmd.query, "OverwriteByExpression")
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
      val (location, diagnostic) = cmd.name match {
        case ri: ResolvedIdentifier =>
          val qualified = qualifiedIdentifier(ri.catalog, ri.identifier)
          val msg = s"No physical location resolved yet for new/replaced table '$qualified' " +
            "(ReplaceTableAsSelect names a table that doesn't exist until this write runs); " +
            "using its qualified catalog identifier as the location"
          (qualified, Some(Diagnostic("ReplaceTableAsSelect", msg)))
        case other =>
          val msg = s"Could not resolve a table identifier from ReplaceTableAsSelect's unresolved " +
            s"name (${other.getClass.getSimpleName}); using its toString as a best-effort location"
          (other.toString, Some(Diagnostic("ReplaceTableAsSelect", msg)))
      }
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
    * `.saveAsTable(...)`/`.writeTo(...).createOrReplace()` on a *new*
    * table produces both a top-level `ReplaceTableAsSelect` (handled by
    * `replaceTableAsSelect` above) *and* an internal, nested `AppendData`
    * against a `StagedTable` (Spark's own public 2-phase-commit protocol
    * for atomic CTAS/RTAS — Delta's `StagedDeltaTableV2` implements it) —
    * both visible to `injectCheckRule` for the same one `.saveAsTable()`
    * call. A `StagedTable`'s `properties()` has no `"location"` yet (the
    * table doesn't physically exist until commit), so without this middle
    * tier the two writes would resolve to two different, mismatched
    * locations for what's really one destination — the outer command's
    * qualified identifier, and the inner one's bare, unqualified
    * `Table.name()` — and whichever one a contract's declared location
    * matched, the other would fail with `OUTPUT_LOCATION_MISMATCH`,
    * aborting a genuinely contract-satisfying write. `DataSourceV2Relation`'s
    * own `catalog`/`identifier` fields (confirmed populated even for a
    * staged table, unlike its `Table.properties()`) give the same
    * qualified form `qualifiedIdentifier` above computes from
    * `ReplaceTableAsSelect`'s `ResolvedIdentifier` — the two now always
    * agree for the same table by construction, not by coincidence.
    *
    * Falls back to the relation's own `name()` only when neither a
    * physical location nor `catalog`+`identifier` are available.
    */
  private def namedRelationLocationAndFormat(table: NamedRelation): (String, Option[String], Option[Diagnostic]) =
    table match {
      case v2: DataSourceV2Relation =>
        val (location, format) = SparkPlanAdapter.tableLocationAndFormat(v2.table)
        location match {
          case Some(loc) => (loc, format, None)
          case None =>
            (v2.catalog, v2.identifier) match {
              case (Some(catalog), Some(identifier)) =>
                val qualified = qualifiedIdentifier(catalog, identifier)
                val msg = s"No 'location' property on write target '$qualified' (likely a staged table pending " +
                  "an atomic CREATE/REPLACE commit); using its qualified catalog identifier as the location"
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

  /** Delta's generated columns (`GENERATED ALWAYS AS (...)`) are computed by
    * Delta itself at commit time, never supplied by the writer - so
    * `cmd.query.schema` for `AppendData`/`OverwriteByExpression` never
    * includes them, the same class of false-rejection schema evolution has
    * for MERGE (see `deltaRowLevelDml` below): a contract requiring a
    * generated column would be wrongly `MISSING_OUTPUT_FIELD`-rejected for
    * a write that would actually satisfy it once Delta computes the
    * column.
    *
    * Confirmed empirically (probes, since deleted, not assumed) that this
    * can't be read from any DataFrame-facing schema at all: neither
    * `spark.read.format("delta").load(path).schema`, nor a catalog-table
    * read (`spark.table(name).schema`), nor a DSv2 `Table.schema()`
    * (confirmed specifically for `DeltaTableV2.schema()` - the exact
    * handle `AppendData`/`OverwriteByExpression` resolve their target to)
    * carries the `delta.generationExpression` metadata key Delta itself
    * sets on a generated column's `StructField` - only Delta's own
    * internal `Snapshot.schema()` does (reached via
    * `DeltaTableV2.initialSnapshot()`). So this reads that, reflectively -
    * `DeltaTableV2`/`Snapshot` are Delta-internal, no compile-time
    * dependency on this module - the same convention `deltaRowLevelDml`
    * uses below, for the same reason.
    */
  private def outputSchemaWithGeneratedColumns(
    table: NamedRelation,
    query: LogicalPlan,
    tag: String
  ): (StructType, Option[Diagnostic]) = {
    val generatedFields = table match {
      case v2: DataSourceV2Relation => deltaGeneratedFields(v2.table)
      case _ => Seq.empty[StructField]
    }
    val (unioned, newGeneratedFields) = unionNewFields(query.schema, generatedFields)
    if (newGeneratedFields.isEmpty) (query.schema, None)
    else {
      val msg = "Target has Delta generated column(s) not present in the write's own schema " +
        s"(${newGeneratedFields.map(_.name).mkString(", ")}) - these are computed by Delta at " +
        "commit time, never supplied by the writer, so they're unioned into outputSchema as a " +
        "best-effort approximation of the committed schema, not a full evaluation of each " +
        "generation expression."
      (unioned, Some(Diagnostic(tag, msg)))
    }
  }

  /** Shared by `outputSchemaWithGeneratedColumns` above and
    * `deltaRowLevelDml`'s MERGE schema-evolution branch below: both need
    * "which fields does `candidateFields` have that `base` doesn't (by
    * name), and what does `base` look like with those unioned in" - the
    * exact same computation over two different schema pairs (a target
    * schema vs. its generated columns; a target schema vs. an evolving
    * MERGE's source). Returns the *new* fields alongside the unioned
    * `StructType` so each call site can still decide independently
    * whether "no new fields" is worth a diagnostic - they currently
    * differ on that, deliberately (see each caller).
    */
  private def unionNewFields(base: StructType, candidateFields: Seq[StructField]): (StructType, Seq[StructField]) = {
    val baseFieldNames = base.fieldNames.toSet
    val newFields = candidateFields.filterNot(f => baseFieldNames.contains(f.name))
    (StructType(base.fields ++ newFields), newFields)
  }

  /** Wrapped in `Try`, like `deltaRowLevelDml`: a future Delta version
    * renaming `initialSnapshot`/`schema`, or dropping the
    * `delta.generationExpression` metadata key, must degrade to "no
    * generated columns found" (safe - outputSchema just stays
    * `query.schema`, exactly this fix's pre-existing behavior) rather
    * than let a `ReflectiveOperationException` escape into a real Spark
    * job. Checking the metadata key directly - rather than reflecting
    * into Delta's own `GeneratedColumn.isGeneratedColumn(protocol, field)`
    * helper - needs no `Protocol` lookup and no overload resolution;
    * `StructField.metadata` is already a plain public Spark type this
    * file has on its main classpath, confirmed via a real probe to carry
    * this exact key on `Snapshot.schema()`'s fields.
    */
  private def deltaGeneratedFields(table: AnyRef): Seq[StructField] =
    scala.util.Try {
      if (table.getClass.getName != "org.apache.spark.sql.delta.catalog.DeltaTableV2") Seq.empty[StructField]
      else {
        val snapshot = table.getClass.getMethod("initialSnapshot").invoke(table)
        val schema = snapshot.getClass.getMethod("schema").invoke(snapshot).asInstanceOf[StructType]
        schema.fields.filter(_.metadata.contains("delta.generationExpression")).toSeq
      }
    }.getOrElse(Seq.empty)

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
      appendData orElse overwriteByExpression orElse replaceTableAsSelect orElse deltaRowLevelDml

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
