// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.ir

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Ascending,
  AttributeReference,
  BinaryOperator,
  Cast,
  Expression,
  NamedExpression,
  NullsFirst,
  WindowExpression,
  Literal => CatalystLiteral,
  SortOrder => CatalystSortOrder
}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.plans.{
  Cross,
  FullOuter,
  Inner,
  LeftAnti,
  LeftOuter,
  LeftSemi,
  RightOuter,
  JoinType => CatalystJoinType
}
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate,
  Deduplicate,
  Filter,
  GlobalLimit,
  Join,
  LocalLimit,
  LogicalPlan,
  Project,
  Repartition,
  RepartitionByExpression,
  Sort,
  SubqueryAlias,
  Union,
  Window
}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.catalog.{Table => V2Table}
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** A single point where the adapter could not translate a Spark construct
  * precisely and fell back to a best-effort or opaque representation.
  * Never blocks translation — see `TranslationResult`.
  */
case class Diagnostic(nodeType: String, message: String)

/** The result of translating a Spark logical plan: a best-effort IR plan,
  * always produced (translation never throws on an unrecognized
  * construct — see `SparkPlanAdapter`'s class doc), plus every point where
  * that translation had to guess, degrade, or give up.
  */
case class TranslationResult(plan: ir.Plan, diagnostics: List[Diagnostic])

/** Translates a Spark Catalyst logical plan into the Invaract
  * transformation IR (`com.invaract.ir`).
  *
  * ## Integration point
  *
  * Spark exposes a query's logical plan through several extension
  * mechanisms; this adapter's core function (`translate`) is agnostic to
  * which one supplied the `LogicalPlan` it's given. In order of increasing
  * weight:
  *
  *  - `Dataset.queryExecution.analyzed` — simplest option, for a caller
  *    that already holds a `DataFrame`. No session configuration needed.
  *    Used directly by this module's tests.
  *  - `org.apache.spark.sql.util.QueryExecutionListener` — a supported,
  *    documented extension point that observes every query a
  *    `SparkSession` executes, without requiring any change to how a
  *    query is written. `SparkAdapterListener` in this module wraps this
  *    for the `runner` integration (see docs/SPARK_ADAPTER.md).
  *  - `SparkSessionExtensions` (injecting a custom analyzer/optimizer
  *    rule) is Spark's heavier, session-construction-time mechanism,
  *    intended for *rewriting* plans. Not needed here since this adapter
  *    only needs to *observe* a plan, never modify it — the least
  *    invasive option that satisfies the actual requirement was chosen.
  *
  * `analyzed` is the plan stage used, not `optimizedPlan`: the optimizer's
  * `EliminateSubqueryAliases` rule strips relation aliases entirely, and
  * this IR needs them to disambiguate self-joins (see `ColumnRef.qualifier`
  * in `com.invaract.ir`). Confirmed empirically — see docs/SPARK_ADAPTER.md.
  *
  * ## Never throws
  *
  * `translate` always returns an `ir.Plan`. An unrecognized plan node
  * becomes `ir.Unsupported`; an unrecognized expression falls through to a
  * generic `FunctionCall` built from Catalyst's own `prettyName`/`children`
  * (see below) rather than needing to be enumerated. Both paths are
  * recorded as `Diagnostic`s. A partially understood pipeline is more
  * useful to a verification engine than an exception that discards
  * everything the adapter *did* understand.
  *
  * ## Visibility
  *
  * `private[sparkadapter]`: nothing outside this module calls `translate`/
  * `translateAsWrite`/`locationOf` directly (confirmed by grep before
  * narrowing it — `ContractEnforcementRule` and `SparkAdapterListener` are
  * the only real callers, both in this same package). A real Invaract
  * user gets translation and verification automatically via the installed
  * extension (`ContractEnforcementRule.forContract`) and never needs the
  * raw Catalyst-to-IR translator directly. Note this is a Scala-compiler-
  * enforced restriction, not a JVM one: the compiled class stays `public`
  * in raw bytecode (`javap` confirms it), so MiMa — which compares
  * bytecode, not Scala visibility qualifiers — doesn't and can't protect
  * this boundary the way it protects genuinely public members. The value
  * here is purely in stopping real Scala code from depending on this by
  * accident, not in getting a MiMa-enforced guarantee.
  */
private[sparkadapter] object SparkPlanAdapter {

  def translate(plan: LogicalPlan): TranslationResult = {
    val translator = new Translator
    val irPlan = translator.translatePlan(plan)
    TranslationResult(irPlan, translator.diagnostics)
  }

  /** Convenience for translating a bare relational plan (one with no Spark
    * write command at its root — e.g. `df.queryExecution.analyzed` for a
    * `DataFrame` that was never written) as if it were written to
    * `dataset`. If `plan` already translates to an `ir.Write` (a real Spark
    * write command), that translation is returned unchanged and `dataset`
    * is ignored.
    */
  def translateAsWrite(plan: LogicalPlan, dataset: ir.DatasetRef): TranslationResult = {
    val result = translate(plan)
    result.plan match {
      case _: ir.Write => result
      case other        => result.copy(plan = ir.Write(dataset, other))
    }
  }

  /** The physical location a resolved relation should be identified by —
    * the same logic `translate` uses internally for `ir.Read`/`ir.Write`
    * locations, factored out separately since `ContractEnforcementRule`
    * needs this to collect input schemas before deciding whether
    * verification even applies to a given analyzed plan.
    */
  def locationOf(lr: LogicalRelation): String = lr.relation match {
    case h: HadoopFsRelation => h.location.rootPaths.headOption.map(_.toString).getOrElse(lr.relation.toString)
    case other                =>
      jdbcLocationOf(other).getOrElse(lr.catalogTable.map(_.identifier.toString).getOrElse(lr.relation.toString))
  }

  /** The physical location a `HiveTableRelation` (a real Hive-format
    * catalog table read - a text/sequence-file/custom-SerDe table, or any
    * Hive table with `spark.sql.hive.convertMetastore{Parquet,Orc}` turned
    * off; a Parquet/ORC Hive table with conversion on, the default,
    * resolves to `LogicalRelation`/`HadoopFsRelation` instead, already
    * covered by `locationOf` above) should be identified by.
    *
    * Unlike every other Hive-specific case in this module
    * (`WriteCommandSupport`'s `createHiveTableAsSelect`/`insertIntoHiveTable`/
    * `insertIntoHiveDir`), this needs no reflection and no `spark-hive`
    * dependency at all: confirmed empirically (not assumed from the class
    * name alone) that `HiveTableRelation` itself lives in
    * `org.apache.spark.sql.catalyst.catalog`, part of plain `spark-catalyst`
    * - already on this module's `provided` Spark dependency, not the
    * separate `spark-hive` artifact `WriteCommandSupport`'s write-side
    * cases need reflection to avoid depending on.
    */
  private[sparkadapter] def hiveTableRelationLocationOf(htr: HiveTableRelation): String =
    htr.tableMeta.storage.locationUri.map(_.toString).getOrElse(htr.tableMeta.identifier.unquotedString)

  /** `JDBCRelation` is `private[sql]` in Spark, so it can't be named as a
    * pattern-match type outside `org.apache.spark.sql` — but the
    * `JDBCOptions` it carries (and returns from its public `jdbcOptions()`
    * accessor) is a fully public class. Identifying the relation by its
    * simple class name and fetching that accessor reflectively sidesteps
    * the visibility restriction without needing Spark's own package, and
    * gives a precise `url`/`table` location instead of the generic
    * `catalogTable`/`toString` fallback every other non-file relation gets.
    */
  /** The physical location and format a resolved DataSourceV2 `Table`
    * reports about itself, if any — shared by `WriteCommandSupport`'s
    * `AppendData`/`OverwriteByExpression` cases (whose `NamedRelation`
    * target resolves to a `DataSourceV2Relation` wrapping one of these)
    * and this class's own `StreamingRelationV2` read case, since both are
    * "a `Table` handle for an already-registered catalog table" — the
    * exact same fact, needed on both the read and write side. Confirmed
    * empirically (not assumed) that Delta's `DataSourceV2Relation.table`
    * (a `DeltaTableV2`) reports `properties()` including both
    * `"location"` (the physical warehouse path) and `"provider"`
    * (`"delta"`) — a plain, public `java.util.Map<String, String>` on the
    * public `connector.catalog.Table` interface, needing no reflection or
    * connector-specific dependency, unlike `WriteToStream`'s legacy V1
    * `Sink` wrapper (see that case's own doc for why that one *does* need
    * reflection).
    */
  private[sparkadapter] def tableLocationAndFormat(table: V2Table): (Option[String], Option[String]) = {
    val props = table.properties()
    (Option(props.get("location")), Option(props.get("provider")))
  }

  /** The physical location a legacy V1 `StreamingRelation` (Delta's
    * streaming read included — confirmed empirically the same way its
    * legacy V1 `DeltaSink` was on the write side, see `WriteCommandSupport`)
    * should be identified by. `dataSource.options` is Spark's own
    * normalized options map for the source, carrying the same `"path"`
    * key `SaveIntoDataSourceCommand`'s options map does on the write
    * side; `StreamingRelation`/`DataSource` are both plain public
    * spark-sql classes, no reflection needed here (unlike
    * `WriteToStream`'s sink). Falls back to the source's own name (e.g.
    * `"kafka"`) for a non-path-based source.
    */
  private[sparkadapter] def streamingRelationLocationOf(sr: StreamingRelation): String =
    sr.dataSource.options.getOrElse("path", sr.sourceName)

  /** The physical location and format a `StreamingRelationV2` node
    * reports, reusing `tableLocationAndFormat` on its resolved `table` —
    * the modern DataSourceV2 counterpart to `streamingRelationLocationOf`
    * above, for a source whose provider implements `TableProvider`
    * directly (Kafka, the built-in `rate`/`socket` sources, ...) rather
    * than the legacy V1 `Source` trait. Falls back to `sourceName` when
    * the table reports no `"location"` property (e.g. `rate`, which has
    * no physical location at all).
    */
  private[sparkadapter] def streamingRelationV2LocationOf(sr: StreamingRelationV2): String =
    tableLocationAndFormat(sr.table)._1.getOrElse(sr.sourceName)

  private def jdbcLocationOf(relation: BaseRelation): Option[String] =
    if (relation.getClass.getSimpleName == "JDBCRelation") {
      scala.util.Try {
        val opts = relation.getClass.getMethod("jdbcOptions").invoke(relation).asInstanceOf[JDBCOptions]
        s"jdbc:${opts.url}/${opts.tableOrQuery}"
      }.toOption
    } else None

  /** Both Spark's built-in file formats (Parquet/CSV/JSON/ORC/text/...,
    * `FileFormat`) and non-file data sources written via `.save(...)`
    * (Delta, JDBC, ..., `CreatableRelationProvider`) mix in
    * `DataSourceRegister`, whose `shortName()` is the same clean,
    * stable identifier ("parquet", "delta", ...) used everywhere else in
    * Spark (e.g. `df.write.format("delta")`) — including, notably, in a
    * contract's own declared `format` string, which is exactly what this
    * needs to line up with for `StructuralVerifier`'s format check. Takes
    * `AnyRef` rather than either specific provider trait since the check
    * is purely on the runtime type either way; a provider that doesn't
    * implement it has no comparably reliable name to fall back to, so
    * it's left as `None` rather than guessing from `getClass.getSimpleName`.
    * Shared with `WriteCommandSupport`, not private to the translator —
    * both need the same "what format did this actually write" logic.
    */
  private[sparkadapter] def formatOf(provider: AnyRef): Option[String] = provider match {
    case registered: DataSourceRegister => Some(registered.shortName())
    case _                                => None
  }

  /** Normalizes Spark's `SaveMode` enum to the same lowercase string
    * vocabulary a contract's `saveMode` field uses ("append", "overwrite",
    * "ignore", "error") — mirroring `formatOf`'s convention of matching
    * whatever a contract author would naturally write. Shared with
    * `WriteCommandSupport` for the same reason as `formatOf` above.
    */
  private[sparkadapter] def saveModeOf(mode: SaveMode): Option[String] = mode match {
    case SaveMode.Append        => Some("append")
    case SaveMode.Overwrite     => Some("overwrite")
    case SaveMode.ErrorIfExists => Some("error")
    case SaveMode.Ignore        => Some("ignore")
  }

  /** Translates one standalone Catalyst expression via the same
    * `Translator.translateExpr` every plan translation already uses,
    * without needing a full plan to translate around it. Used by
    * `RowMutationSupport` to translate a MERGE's `ON` condition — a bare
    * `Expression` reachable only via reflection off a Delta-internal
    * command, never part of any `LogicalPlan.children` this module's
    * ordinary `translatePlan` traversal would otherwise reach. `Translator`
    * itself is `private` to this object (not just `private[sparkadapter]`),
    * so this is the one sanctioned way another file in this package can
    * reach its expression translation; a throwaway instance is created
    * per call since `Translator`'s only per-translation state is a
    * diagnostics buffer this caller doesn't consume (see `RowMutation`'s
    * own doc for why it carries no diagnostic channel of its own).
    */
  private[sparkadapter] def translateExprStandalone(expr: Expression): ir.Expr = new Translator().translateExpr(expr)

  private class Translator {
    private val buffer = scala.collection.mutable.ListBuffer[Diagnostic]()
    def diagnostics: List[Diagnostic] = buffer.toList

    private def report(nodeType: String, message: String): Unit =
      buffer += Diagnostic(nodeType, message)

    // ---- Plan translation ---------------------------------------------

    def translatePlan(plan: LogicalPlan): ir.Plan = WriteCommandSupport.combined.lift(plan) match {
      // Every recognized Spark write-command shape (see
      // WriteCommandSupport's class doc for why this is a single shared
      // lookup rather than a match here) becomes an ir.Write over its
      // (recursively translated) query. Adding a new write shape never
      // touches this method — it touches WriteCommandSupport instead.
      case Some(info) =>
        info.diagnostic.foreach(d => report(d.nodeType, d.message))
        ir.Write(ir.DatasetRef(info.location), translatePlan(info.query), info.format, info.saveMode)

      case None => translateNonWritePlan(plan)
    }

    private def translateNonWritePlan(plan: LogicalPlan): ir.Plan = plan match {
      case sa: SubqueryAlias =>
        translatePlan(sa.child) match {
          case r: ir.Read => r.copy(alias = Some(sa.identifier.name))
          case other =>
            report(
              "SubqueryAlias",
              s"Dropped alias '${sa.identifier.name}' on a non-Read subplan " +
                s"(${other.getClass.getSimpleName}); the IR has no generic aliased-subplan node"
            )
            other
        }

      case lr: LogicalRelation =>
        val usedFallback = lr.relation match {
          case h: HadoopFsRelation => h.location.rootPaths.isEmpty
          case other if other.getClass.getSimpleName == "JDBCRelation" => false
          case _                    => lr.catalogTable.isEmpty
        }
        if (usedFallback)
          report(
            "LogicalRelation",
            s"Could not determine a precise location for relation ${lr.relation.getClass.getSimpleName}; using its toString as a best-effort location"
          )
        ir.Read(ir.DatasetRef(SparkPlanAdapter.locationOf(lr)))

      // A real Hive-format catalog table read - confirmed empirically (a
      // real embedded-Derby Hive session, not assumed) to be a genuine,
      // previously-unrecognized read shape, and a worse gap than it first
      // looked: HiveTableRelation is NOT a LogicalRelation-wrapped relation
      // the way Delta's read shape turned out to be (see "Delta Lake
      // reads" in docs/SPARK_ADAPTER.md) - it's its own top-level
      // LeafNode - so before this case existed it fell all the way through
      // to the generic Unsupported fallback below, meaning a Hive-native
      // table (any non-Parquet/ORC SerDe, or a Parquet/ORC table with
      // spark.sql.hive.convertMetastoreParquet/Orc explicitly disabled)
      // could never satisfy a contract's declared input at all - not
      // merely an imprecise location the way the generic LogicalRelation
      // fallback gives other unrecognized relations. A Parquet/ORC Hive
      // table with conversion ON (the default) resolves to
      // LogicalRelation/HadoopFsRelation instead, confirmed via the same
      // probe, already covered by the case above - this case is only
      // reached for the genuinely Hive-native path.
      case htr: HiveTableRelation =>
        if (htr.tableMeta.storage.locationUri.isEmpty)
          report(
            "HiveTableRelation",
            s"No storage location on Hive table '${htr.tableMeta.identifier}'; using its table identifier as a best-effort location"
          )
        ir.Read(ir.DatasetRef(SparkPlanAdapter.hiveTableRelationLocationOf(htr)))

      // A streaming source's top-level plan - confirmed empirically (not
      // assumed) to be one of two shapes depending on whether the
      // provider is a legacy V1 `Source` (Delta's included - `.readStream
      // .format("delta").load(path)` analyzes to this, not
      // StreamingRelationV2) or a modern DataSourceV2 `TableProvider`
      // (the built-in `rate` source used elsewhere in this module's own
      // streaming tests, Kafka, ...). Neither was previously recognized
      // here at all - not a translation gap that silently passed
      // (streaming writes cover that risk; see WriteCommandSupport's
      // WriteToStream case), but a real one all the same: a contract
      // declaring a streaming source as a required `input` would report
      // MISSING_INPUT even though data genuinely is being read, since
      // ContractEnforcementRule.verifyOrThrow's own input-schema
      // collection needs the same two cases (added there too - see
      // docs/SPARK_ADAPTER.md's "Streaming reads as a contract input").
      case sr: StreamingRelation =>
        if (!sr.dataSource.options.contains("path"))
          report(
            "StreamingRelation",
            s"No 'path' option on a '${sr.sourceName}' streaming source; using its source name as a best-effort location"
          )
        ir.Read(ir.DatasetRef(SparkPlanAdapter.streamingRelationLocationOf(sr)))

      case sr2: StreamingRelationV2 =>
        if (SparkPlanAdapter.tableLocationAndFormat(sr2.table)._1.isEmpty)
          report(
            "StreamingRelationV2",
            s"No 'location' property on a '${sr2.sourceName}' streaming source's table; using its source name as a best-effort location"
          )
        ir.Read(ir.DatasetRef(SparkPlanAdapter.streamingRelationV2LocationOf(sr2)))

      // A batch DataSourceV2 catalog read - confirmed empirically (a real
      // Iceberg-enabled session, not assumed) to be the read-side shape
      // for ANY "pure" DSv2 connector (one whose reads don't, unlike
      // Delta's, happen to be a LogicalRelation-wrapped subclass of
      // Spark's own V1 HadoopFsRelation - see the LogicalRelation case
      // above). Before this case existed, every such read fell through to
      // the generic Unsupported fallback below, meaning no DSv2-catalog-
      // only connector's reads could ever satisfy a contract's declared
      // input - not "fails closed on an unrecognized write" (the
      // FailClosedCommands policy doesn't gate reads at all), but a read
      // silently never counting as a read. Reuses tableLocationAndFormat,
      // the same "Table.properties()" lookup StreamingRelationV2 and
      // WriteCommandSupport's AppendData/OverwriteByExpression cases
      // already share - this is the same fact (a Table handle for an
      // already-registered catalog table), needed on a third site now.
      case dsv2: DataSourceV2Relation =>
        if (SparkPlanAdapter.tableLocationAndFormat(dsv2.table)._1.isEmpty)
          report(
            "DataSourceV2Relation",
            s"No 'location' property on read target '${dsv2.name}'; using its name() as a best-effort location"
          )
        ir.Read(ir.DatasetRef(SparkPlanAdapter.tableLocationAndFormat(dsv2.table)._1.getOrElse(dsv2.name)))

      case p: Project =>
        ir.Project(translatePlan(p.child), p.projectList.map(translateNamed).toList)

      case f: Filter =>
        ir.Filter(translatePlan(f.child), translateExpr(f.condition))

      case j: Join =>
        ir.Join(
          translatePlan(j.left),
          translatePlan(j.right),
          translateJoinType(j.joinType),
          j.condition.map(translateExpr)
        )

      case a: Aggregate =>
        ir.Aggregate(
          translatePlan(a.child),
          a.groupingExpressions.map(translateExpr).toList,
          a.aggregateExpressions.map(translateNamed).toList
        )

      case u: Union =>
        ir.Union(u.children.map(translatePlan).toList)

      case s: Sort =>
        ir.Sort(translatePlan(s.child), s.order.map(translateSortOrder).toList)

      case w: Window =>
        ir.Window(
          translatePlan(w.child),
          w.windowExpressions.map(translateNamed).toList,
          w.partitionSpec.map(translateExpr).toList,
          w.orderSpec.map(translateSortOrder).toList
        )

      // Row-count-only operators: they don't change which columns exist or
      // what they mean, so they're transparent for lineage purposes.
      // Deduplicate (.distinct()) removes duplicate rows but touches no
      // column's identity or type; Repartition/RepartitionByExpression
      // (.repartition()/.coalesce()) only change physical partitioning.
      // Previously these all fell through to the opaque Unsupported
      // placeholder — none of them actually needed one.
      case g: GlobalLimit =>
        translatePlan(g.child)
      case l: LocalLimit =>
        translatePlan(l.child)
      case d: Deduplicate =>
        translatePlan(d.child)
      case r: Repartition =>
        translatePlan(r.child)
      case r: RepartitionByExpression =>
        translatePlan(r.child)

      case other =>
        val description = s"${other.getClass.getSimpleName}: ${safeSimpleString(other)}"
        report(other.getClass.getSimpleName, "No translation for this plan node; using an opaque placeholder")
        ir.Unsupported(description, other.children.map(translatePlan).toList)
    }

    private def safeSimpleString(plan: LogicalPlan): String =
      scala.util.Try(plan.simpleString(80)).getOrElse(plan.getClass.getName)

    // ---- Expression translation ----------------------------------------

    private def translateNamed(ne: NamedExpression): ir.NamedExpr = ne match {
      case a: Alias => ir.NamedExpr(a.name, translateExpr(a.child))
      case other     => ir.NamedExpr(other.name, translateExpr(other))
    }

    def translateExpr(expr: Expression): ir.Expr = expr match {
      case a: AttributeReference =>
        ir.ColumnReference(ir.ColumnRef(a.name, a.qualifier.lastOption))

      // A nested Alias (not at the top of a Project/Aggregate/Window output
      // list, where translateNamed already unwraps it) has no home in this
      // IR's expression algebra — see Expr.scala's NamedExpr doc. Discard
      // the name, keep the computation.
      case a: Alias =>
        translateExpr(a.child)

      case l: CatalystLiteral =>
        ir.Literal(convertLiteralValue(l.value), typeNameOf(l.dataType))

      case c: Cast =>
        ir.FunctionCall("CAST", List(translateExpr(c.child), ir.Literal(typeNameOf(c.dataType), "type")))

      // The window spec (partition/order/frame) is captured once at the
      // plan level by ir.Window; re-representing it per expression here
      // would just duplicate that structure.
      case we: WindowExpression =>
        translateExpr(we.windowFunction)

      case ae: AggregateExpression =>
        ir.AggregateCall(ae.aggregateFunction.prettyName.toUpperCase, aggregateArg(ae.aggregateFunction), ae.isDistinct)

      case b: BinaryOperator =>
        ir.FunctionCall(b.symbol, List(translateExpr(b.left), translateExpr(b.right)))

      case udf: Expression if isOpaqueUdf(udf) =>
        report(
          udf.getClass.getSimpleName,
          "User-defined function body is opaque to lineage tracing; translated as a function call over its declared arguments only"
        )
        ir.FunctionCall(udf.prettyName.toUpperCase, udf.children.map(translateExpr).toList)

      // Every other built-in expression (arithmetic beyond BinaryOperator,
      // IS NULL, CASE WHEN, string/date functions, ...) exposes
      // `.prettyName` and `.children` generically on Expression — no need
      // to hardcode Spark's several dozen built-in function classes.
      case e: Expression =>
        ir.FunctionCall(e.prettyName.toUpperCase, e.children.map(translateExpr).toList)
    }

    private def isOpaqueUdf(e: Expression): Boolean = {
      val n = e.getClass.getSimpleName
      n == "ScalaUDF" || n == "PythonUDF" || n.endsWith("HiveSimpleUDF") || n.endsWith("HiveGenericUDF")
    }

    /** `AggregateCall` models exactly one argument; most aggregate
      * functions (SUM, COUNT, AVG, MIN, MAX) have exactly one, but e.g.
      * `COUNT(a, b)` does not. Multi-argument aggregates are combined into
      * a single synthetic wrapper rather than silently dropping arguments.
      */
    private def aggregateArg(fn: AggregateFunction): ir.Expr = fn.children.toList match {
      case Nil           => ir.Literal("*", "wildcard")
      case single :: Nil => translateExpr(single)
      case multiple =>
        report(
          fn.getClass.getSimpleName,
          s"Aggregate function takes ${multiple.size} arguments; combining them into a single ARGS(...) " +
            "wrapper since AggregateCall models one argument"
        )
        ir.FunctionCall("ARGS", multiple.map(translateExpr))
    }

    private def translateSortOrder(so: CatalystSortOrder): ir.SortOrder =
      ir.SortOrder(
        translateExpr(so.child),
        ascending = so.direction == Ascending,
        nullsFirst = so.nullOrdering == NullsFirst
      )

    private def translateJoinType(jt: CatalystJoinType): ir.JoinType = jt match {
      case Inner      => ir.JoinType.Inner
      case LeftOuter  => ir.JoinType.LeftOuter
      case RightOuter => ir.JoinType.RightOuter
      case FullOuter  => ir.JoinType.FullOuter
      case LeftSemi   => ir.JoinType.LeftSemi
      case LeftAnti   => ir.JoinType.LeftAnti
      case Cross      => ir.JoinType.Cross
      case other =>
        report("JoinType", s"Unrecognized join type '$other'; defaulting to Inner")
        ir.JoinType.Inner
    }

    private def convertLiteralValue(value: Any): Any = value match {
      case null          => null
      case u: UTF8String => u.toString
      case other          => other
    }

    private def typeNameOf(dt: DataType): String = dt match {
      case _: StringType    => "string"
      case _: IntegerType   => "integer"
      case _: LongType      => "long"
      case _: ShortType     => "short"
      case _: ByteType      => "byte"
      case _: DoubleType    => "double"
      case _: FloatType     => "float"
      case _: DecimalType   => "decimal"
      case _: BooleanType   => "boolean"
      case _: DateType      => "date"
      case _: TimestampType => "timestamp"
      case _: BinaryType    => "binary"
      case _: StructType    => "struct"
      case _: ArrayType     => "array"
      case _: MapType        => "map"
      case other              => other.simpleString
    }
  }
}
