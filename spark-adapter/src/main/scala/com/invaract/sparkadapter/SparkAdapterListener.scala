// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, Dataset, DatasetType}
import com.invaract.sparkadapter.notification.{CatalogInfo, NotificationSink, WriteEvent, WriteFieldInfo}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.catalog.{Table => V2Table}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

/** A `QueryExecutionListener` that captures the translated IR for every
  * Spark write command a session executes, without requiring any change to
  * how those writes are called elsewhere in the application.
  *
  * Registered once via `spark.listenerManager.register(listener)` — see
  * `SparkPlanAdapter`'s class doc for why this is the least invasive of
  * Spark's plan-inspection extension points for this purpose. `onSuccess`
  * fires for *every* query a session executes, including internal ones
  * triggered by schema inference, `.count()`, and so on, so this listener
  * only records executions whose analyzed plan root is a write command;
  * everything else is ignored.
  *
  * If `sink` is given, this is also where `WriteEvent`s are published —
  * deliberately here, not in `ContractEnforcementRule`: that check rule
  * fires at analysis time, before Spark has executed anything, so it can
  * only report "this write is allowed to proceed," not "this write
  * actually happened." `onSuccess` is Spark's own "this query executed
  * successfully" signal, so a `WriteEvent` published from here means the
  * output genuinely exists on disk — see `ContractValidationEvent`'s own
  * doc for the contrasting, earlier moment it represents.
  *
  * ## `lastWrite` and `.checkpoint()`
  *
  * A `WriteEvent` describes the write itself (location, format, schema,
  * metrics) and carries no lineage, so a `.checkpoint()` upstream doesn't
  * touch it. `lastWrite` is the one place this listener exposes a
  * translated plan, and translating `qe.analyzed` alone would show a
  * checkpoint as an opaque `LogicalRDD` - while the `ContractEnforcementRule`
  * that ran at analysis time (and knows what each checkpoint was made from,
  * see `CheckpointRegistry`) already translated the resolved plan. So the
  * rule attaches that translation to the very plan it verified (a Catalyst
  * `TreeNodeTag`, see `SparkAdapterListener.stash`) and `onSuccess` reads it
  * back off `qe.analyzed`, the same object. Nothing is shared between this
  * listener and the rule, no session lookup or global state is involved
  * (the value lives and dies with the plan), and it is deterministic: the
  * rule runs synchronously during analysis, strictly before the write
  * executes. With no rule installed (or the plan never seen by it) there is
  * no tag and `lastWrite` is `SparkPlanAdapter.translate(qe.analyzed)`, as
  * before.
  */
class SparkAdapterListener(
    sink: Option[NotificationSink],
    contractRef: Option[String],
    metadata: Map[String, Any],
    contractOutputs: List[Dataset] = Nil
) extends QueryExecutionListener {
  @volatile private var _lastWrite: Option[TranslationResult] = None

  /** No sink, no contract — a listener used purely for its original
    * purpose (capturing `lastWrite` for `demo/output/report.json`), the
    * same zero-argument constructor this class had before `WriteEvent`
    * publishing existed.
    */
  def this() = this(None, None, Map.empty, Nil)

  /** `contractRef`/`metadata`/`contractOutputs` derived from a real
    * `Contract` (its `"id@version"` ref, `extensions` bag, and declared
    * `outputs` list, respectively) — the convenience most callers that do
    * have a contract will actually want, rather than pulling those fields
    * out by hand.
    */
  def this(sink: Option[NotificationSink], contract: Option[Contract]) =
    this(
      sink,
      contract.map(c => s"${c.id}@${c.version}"),
      contract.map(_.extensions).getOrElse(Map.empty),
      contract.map(_.outputs).getOrElse(Nil)
    )

  /** `location` (as Spark itself reports it for a completed write) against
    * `contractOutputs`' own declared locations, via the same
    * `StructuralVerifier.locationsMatch` predicate the check rule uses to
    * match a contract-declared location against a real plan — never a
    * second, independent comparison. Takes the *first* matching output's
    * own declared `datasetType` (`.find`, not `.filter`), the identical
    * "whichever is declared first" resolution `StructuralVerifier`'s own
    * output-matching already uses for two outputs sharing one location
    * (`ContractValidator`'s own Warning for that case names this exact
    * behavior) — this listener intentionally doesn't invent a second,
    * different answer to the same ambiguity. `None` when nothing matches
    * at all, or when the matched output declares no `type:`.
    */
  private def datasetTypeFor(location: String): Option[DatasetType] =
    contractOutputs.find(output => StructuralVerifier.locationsMatch(output.location, location)).flatMap(_.datasetType)

  /** The most recently captured write's translation, if any query with a
    * write command at its root has executed since this listener was
    * registered. A single most-recent value is enough for this listener's
    * purpose — driving a demo/report of "what did the last write do" — not
    * a general-purpose multi-query capture store.
    */
  def lastWrite: Option[TranslationResult] = _lastWrite

  // Consults the same WriteCommandSupport.combined lookup
  // SparkPlanAdapter's translation and ContractEnforcementRule's
  // enforcement do, rather than a match of its own - this listener used
  // to have its own independent "is this a write" check, and that's
  // exactly what let a real write shape (Delta's) go uncaptured here even
  // after translation and enforcement were both already fixed for it. See
  // WriteCommandSupport's class doc. `.lift` (rather than the previous
  // `.isDefinedAt` + a second, independent `SparkPlanAdapter.translate`
  // call) gets the WriteCommandInfo this case also needs for the
  // WriteEvent below, at no extra cost - `SparkPlanAdapter.translate`
  // still runs its own full recursive translation for `_lastWrite`
  // exactly as before.
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    WriteCommandSupport.combined.lift(qe.analyzed).foreach { info =>
      _lastWrite = Some(SparkAdapterListener.translationFor(qe.analyzed))
      sink.foreach { s =>
        // rowCount/bytesWritten/fileCount come from Spark's own SQLMetrics
        // on the executed plan - confirmed empirically (not assumed)
        // present for a plain V1 write (InsertIntoHadoopFsRelationCommand,
        // wrapped as DataWritingCommandExec: "numOutputRows"/
        // "numOutputBytes"/"numFiles") and confirmed *absent* for every
        // Delta/Iceberg write shape probed (SaveIntoDataSourceCommand,
        // AppendDataExecV1, DSv2 AppendDataExec never populate these keys
        // at all) - .get on a Map that doesn't have the key correctly
        // yields None rather than a wrong guess. See WriteEvent's own doc
        // for why Delta/Iceberg need a separate, connector-specific
        // mechanism for the same counts, not attempted here.
        val metrics = qe.executedPlan.metrics
        s.publish(
          WriteEvent(
            contract = contractRef,
            location = info.location,
            format = info.format,
            saveMode = info.saveMode,
            schema = info.outputSchema.fields.map(f => WriteFieldInfo(f.name, f.dataType.typeName, f.nullable)).toList,
            timestamp = System.currentTimeMillis(),
            metadata = metadata,
            durationMs = durationNs / 1000000L,
            rowCount = metrics.get("numOutputRows").map(_.value),
            bytesWritten = metrics.get("numOutputBytes").map(_.value),
            fileCount = metrics.get("numFiles").map(_.value),
            applicationId = Some(qe.sparkSession.sparkContext.applicationId),
            deltaVersion = if (info.format.contains("delta")) SparkAdapterListener.deltaVersionOf(qe.sparkSession, info.location) else None,
            icebergSnapshotId = info.catalogTableRef.flatMap { case (catalog, identifier) =>
              SparkAdapterListener.icebergSnapshotIdOf(catalog, identifier)
            },
            operation = info.operation,
            catalog = info.catalogIdentity.map(CatalogInfo.from),
            partitionColumns = info.partitionColumns,
            datasetType = datasetTypeFor(info.location)
          )
        )
      }
    } // else not a write; ignore (schema inference, count(), etc.)

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ()
}

private[sparkadapter] object SparkAdapterListener {

  /** A translation, and the plan it was made for. Catalyst copies a node's tags
    * onto plans derived from it by a transformation, so a tag alone could be
    * found on a plan it does not describe; carrying the plan lets `translationFor`
    * accept it only on the exact instance it was attached to.
    */
  private final case class Stashed(plan: LogicalPlan, translation: TranslationResult)

  private val StashedTranslation = new TreeNodeTag[Stashed]("invaract.stashedTranslation")

  /** Called by `ContractEnforcementRule` with the analyzed plan it just verified
    * (or inferred from) and the translation it used - the one taken *after*
    * checkpoint resolution.
    */
  private[sparkadapter] def stash(analyzed: LogicalPlan, translation: TranslationResult): Unit =
    analyzed.setTagValue(StashedTranslation, Stashed(analyzed, translation))

  /** What the rule translated `analyzed` to, if it did and `analyzed` is the very
    * plan it saw; otherwise a fresh translation of `analyzed` itself.
    */
  private[sparkadapter] def translationFor(analyzed: LogicalPlan): TranslationResult =
    analyzed.getTagValue(StashedTranslation).filter(_.plan eq analyzed).map(_.translation)
      .getOrElse(SparkPlanAdapter.translate(analyzed))

  /** `DeltaLog.forTable(session, path).snapshot.version` via reflection -
    * this module has no compile-time dependency on Delta (`delta-spark` is
    * `test`-scope only in build.sbt, used to build/run this module's own
    * tests against a real Delta session, never a runtime dependency this
    * jar forces on every consumer). A real user's job that doesn't touch
    * Delta never resolves `delta-spark` because of this call: `Class.forName`
    * fails with `ClassNotFoundException`, caught by the `Try` below and
    * turned into `None`, the same "fail closed to no information, never
    * throw" contract every other reflective lookup in this file's sibling
    * `WriteCommandSupport` already follows (`deltaRowLevelDml`,
    * `createHiveTableAsSelect`). A real Delta write, on the other hand,
    * already has `delta-spark` on its classpath - it could not have
    * written the table otherwise - so this reflection is only ever
    * attempted when it's already known to succeed.
    *
    * No explicit refresh (Delta's own `DeltaLog.update()`, which - unlike
    * `snapshot` - takes defaulted parameters, meaning a bare `update()`
    * call site is really the Scala compiler inserting three separate
    * default-value method calls, not one that reflection can replicate as
    * simply) is needed here: confirmed empirically (a real throwaway probe
    * against a live Delta session, since deleted) that the plain,
    * argument-free `.snapshot.version` read already reflects the
    * just-committed version at the exact moment `onSuccess` fires for the
    * write command itself - the same per-path-cached `DeltaLog` instance
    * Delta's own commit protocol already mutated in place during that
    * commit.
    */
  private[sparkadapter] def deltaVersionOf(session: SparkSession, location: String): Option[Long] =
    scala.util.Try {
      val deltaLogClass = Class.forName("org.apache.spark.sql.delta.DeltaLog")
      val path = new org.apache.hadoop.fs.Path(location)
      val deltaLog = deltaLogClass
        .getMethod("forTable", classOf[SparkSession], classOf[org.apache.hadoop.fs.Path])
        .invoke(null, session, path)
      val snapshot = deltaLog.getClass.getMethod("snapshot").invoke(deltaLog)
      snapshot.getClass.getMethod("version").invoke(snapshot).asInstanceOf[java.lang.Long].longValue()
    }.toOption

  /** A fresh `TableCatalog.loadTable(identifier)` call, followed by
    * `SparkTable.table().refresh().currentSnapshot().snapshotId()` via
    * reflection for the Iceberg-specific part only (`iceberg-spark-runtime`
    * is `test`-scope-only in build.sbt, the same zero-compile-time-
    * dependency reason `deltaVersionOf` above documents) - `None`
    * immediately, no reflection attempted at all, unless the freshly-loaded
    * table's concrete runtime class is actually Iceberg's `SparkTable`
    * (matched by simple class name, the same convention
    * `WriteCommandSupport.streamSinkFormatOf` already uses for `DeltaSink`/
    * `FileStreamSink`).
    *
    * Deliberately a *fresh* `loadTable` call, not a reflective read on
    * `WriteCommandInfo`'s previously-resolved `Table` handle (an earlier
    * version of this method took that handle directly) - confirmed
    * empirically, the hard way, via a real cross-suite test failure: when a
    * prior suite in the same JVM had already exercised Iceberg's own
    * catalog/table caching, the *already-resolved* `Table` object captured
    * from the analyzed plan (at analysis time, before the write executed)
    * failed to see the just-committed snapshot even after an explicit
    * `.refresh()` call on it - while a plain SQL query against the same
    * table, and this fresh `loadTable` call, both saw it correctly. This
    * mirrors `deltaVersionOf` above, which was never affected by the same
    * class of problem specifically because `DeltaLog.forTable` is already a
    * fresh lookup by path, not a previously-captured object - the fix here
    * is to make the Iceberg path do the same. `TableCatalog`/`CatalogPlugin`/
    * `Identifier` are Spark's own stable, compile-time-available
    * `connector.catalog` types, so this reload itself needs no reflection;
    * only what's done with the `Table` `loadTable` returns does.
    *
    * `currentSnapshot()` can genuinely return `null` (a table with no
    * snapshot yet is not a case this call site should ever see, since it
    * only runs after a write just succeeded, but `Option(...)` guards it
    * regardless rather than risk a `NullPointerException`).
    */
  private[sparkadapter] def icebergSnapshotIdOf(
    catalog: org.apache.spark.sql.connector.catalog.CatalogPlugin,
    identifier: org.apache.spark.sql.connector.catalog.Identifier
  ): Option[Long] =
    catalog match {
      case tableCatalog: org.apache.spark.sql.connector.catalog.TableCatalog =>
        scala.util.Try(tableCatalog.loadTable(identifier)).toOption.flatMap(icebergSnapshotIdOfTable)
      case _ => None
    }

  private[sparkadapter] def icebergSnapshotIdOfTable(table: V2Table): Option[Long] =
    if (table.getClass.getSimpleName != "SparkTable") None
    else
      scala.util.Try {
        val icebergTable = table.getClass.getMethod("table").invoke(table)
        val tableClass = icebergTable.getClass
        tableClass.getMethod("refresh").invoke(icebergTable)
        val snapshot = tableClass.getMethod("currentSnapshot").invoke(icebergTable)
        // `snapshot`'s *runtime* class (org.apache.iceberg.BaseSnapshot) is
        // package-private, confirmed via a real IllegalAccessException, not
        // assumed - even though the interface it implements
        // (org.apache.iceberg.Snapshot) and snapshotId() itself are both
        // public. java.lang.reflect.Method.invoke enforces accessibility
        // against a Method object's own *declaring class* - obtained here
        // via getMethod - not the method's visibility modifier alone, so
        // looking the method up on `snapshot.getClass` (the inaccessible
        // impl class) throws regardless of `snapshotId()` being public.
        // Looking it up via the public `Snapshot` interface class instead
        // avoids this: `invoke` still dispatches virtually to whatever
        // `snapshot` really is, but the accessibility check now passes
        // against a public declaring class. `Class.forName`, not
        // `classOf[...]`, for the same zero-compile-time-Iceberg-dependency
        // reason every other reflective lookup in this file uses it.
        Option(snapshot).map { s =>
          val snapshotInterface = Class.forName("org.apache.iceberg.Snapshot")
          snapshotInterface.getMethod("snapshotId").invoke(s).asInstanceOf[java.lang.Long].longValue()
        }
      }.toOption.flatten
}
