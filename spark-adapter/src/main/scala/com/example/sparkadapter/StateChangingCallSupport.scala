// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, TableCatalog}
import org.apache.spark.sql.types.{StructField, StructType}

/** The resolved effect of a supported state-changing procedure call: the
  * table it targets and the schema the table will have once the operation
  * completes. Not derived from any Spark write/query (there isn't one).
  * `callName` (the CALL's own procedure name, e.g. `"rollback_to_snapshot"`)
  * is carried through only for error messages - the check itself never
  * branches on which procedure produced this value.
  */
private[sparkadapter] case class StateChangeInfo(callName: String, location: String, resultingSchema: StructType)

/** Verification support for Iceberg `CALL` procedures that change a table's
  * *current* committed state without a Spark write ever occurring. Nine
  * procedures are recognized: the six in `currentStateChangingProcedureClasses`
  * (see "The 'moves what's current' procedures" below), plus `add_files`
  * and `migrate` (same mechanism - see "add_files and migrate" below), plus
  * `snapshot` (a genuinely different mechanism - see "snapshot" below).
  * `FailClosedCommands.safeIcebergProcedureClasses`'s doc comment covers
  * `rewrite_table_path`, the one remaining procedure - confirmed via a real
  * probe (since deleted) to never touch any catalog table's own state at
  * all, so it needs no verification here, not a harder one. See
  * docs/SPARK_ADAPTER.md's "Verifying rollback_to_snapshot" and "Extending
  * to..." sections for the pilot and first extension this generalizes from.
  *
  * ## Why this isn't `WriteCommandSupport`
  *
  * `WriteCommandInfo` bundles a `query: LogicalPlan` - the untranslated
  * data being written - because every entry in that registry really is a
  * write: some `DataFrame`/query produces rows, and this module verifies
  * the *query's* schema against the contract. None of the procedures here
  * has a query in that sense - even `add_files`, which does bring in new
  * data, does so by importing already-written files whose schema the
  * *target table's own declared schema* governs, not a Spark query
  * Invaract could translate (see "add_files and migrate" below). Forcing
  * a `None`/dummy `query` into `WriteCommandInfo` would misrepresent what
  * actually happens and widen a stable, central type for a shape that
  * doesn't fit it. This is a separate, narrower registry instead -
  * `ContractEnforcementRule.verifyOrThrow` consults it before falling
  * through to `FailClosedCommands`' blanket rejection, the same priority
  * `WriteCommandSupport` gets.
  *
  * ## What's actually checked, and why (a real correction, not the original design)
  *
  * The first version of this file (the `rollback_to_snapshot` pilot)
  * extracted the *target snapshot's own* historical schema (via Iceberg's
  * `SparkTable.copyWithSnapshotId(id).schema()`) and checked that against
  * the contract. That turned out to check the wrong thing: confirmed
  * empirically (a real rollback, followed by an explicit `refreshTable`,
  * still reported the table's *current*, post-evolution schema - not the
  * target snapshot's) and independently corroborated by Apache Iceberg's
  * own issue tracker (apache/iceberg#15165, open, unresolved as of this
  * writing): `rollback_to_snapshot` moves which snapshot's *data* is
  * current, but does **not** revert `current-schema-id` - schema
  * evolution and snapshot state are independent in Iceberg's model.
  *
  * The corrected check verifies what any of these procedures can actually
  * affect, together:
  *
  *  - **Location**: does this CALL even target the contract's declared
  *    output? A state-changing CALL on a table the active contract
  *    doesn't govern is correctly allowed, not swept up by a blanket
  *    rejection.
  *  - **Schema** - the CURRENT schema of the table the operation leaves
  *    governed by the contract: for the six "moves what's current"
  *    procedures and for `add_files`/`migrate`, that's the *target*
  *    table's own current schema (which none of them can change - see
  *    below); for `snapshot`, which creates a brand-new table, it's the
  *    *source* table's current schema, since that becomes the new
  *    table's schema (see "snapshot" below).
  *
  * This holds even for `fast_forward` targeting a non-`"main"` branch,
  * where the table's *default* read is genuinely unaffected (confirmed
  * empirically): the check doesn't need to special-case that, since it
  * only asserts an invariant (current schema can't move) that's true
  * regardless of which branch a given call happens to target, not a claim
  * about what that specific call changes.
  *
  * ## The "moves what's current" procedures
  *
  * Each pairs a real `Procedure` class (`Call.procedure()`, confirmed via
  * `javap` on `iceberg-spark-runtime-3.5_2.12:1.11.0`, not guessed) with a
  * distinct way of moving what's current, but the same "can't touch
  * schema" property, confirmed for each via a real probe (since deleted)
  * before being relied on here:
  *
  *  - `rollback_to_snapshot` / `rollback_to_timestamp` - move current to
  *    an already-committed snapshot by id or by timestamp.
  *  - `cherrypick_snapshot` - merges a snapshot's changes onto current
  *    (confirmed via probe: the table's row count after cherry-picking a
  *    snapshot created on a branch reflects the merge on `main`).
  *  - `publish_changes` - merges a WAP-staged snapshot (looked up by its
  *    `spark.wap.id`, confirmed via probe: a staged write is genuinely
  *    invisible on `main` until published) onto current.
  *  - `set_current_snapshot` - three declared parameters (table,
  *    `snapshot_id`, `ref`), not two: `snapshot_id` and `ref` are
  *    mutually exclusive (confirmed via probe: exactly one of `args(1)`/
  *    `args(2)` is a non-null `Literal` depending on which was supplied),
  *    but neither's value is read here any more than `rollback_to_snapshot`'s
  *    snapshot id is - only whether the call resolves at all matters, and
  *    Iceberg's own analyzer (`ResolveProcedures`) already guarantees a
  *    well-formed, fully-bound `args` array matching `parameters()` by
  *    the time this check rule sees the plan, so no shape validation
  *    beyond extracting `args.head` is needed.
  *  - `fast_forward` - `FastForwardBranchProcedure`, table plus two
  *    branch-name arguments (`branch`, the one being moved; `to`, the
  *    source ref) - confirmed via probe that fast-forwarding `"main"`
  *    changes the table's default read, while fast-forwarding any other
  *    named branch leaves it unchanged; explained above why the check
  *    doesn't need to know which case it's in.
  *
  * ## `add_files` and `migrate`
  *
  * Both looked, before investigating, like they might need genuinely new
  * mechanism (reading a schema from a table/path named in a CALL
  * argument) - real investigation via a probe (since deleted) found they
  * don't:
  *
  *  - `add_files(table, source_table, ...)` never changes `table`'s own
  *    schema, confirmed two ways: importing a source with an *extra*
  *    column left the target's schema unchanged (the extra column is
  *    simply not imported), and importing a source *missing* a column the
  *    target has still succeeded (Iceberg NULL-fills it, the same
  *    narrower-append behavior `WriteCommandSupport.outputSchemaWithTargetOnlyFields`
  *    already handles for ordinary writes). So `source_table` (`args(1)`)
  *    is never read here at all - only `args.head` (`table`), exactly
  *    like the six procedures above.
  *  - `migrate(table, ...)` converts `table` *in place*: same identifier,
  *    before and after. Confirmed via probe, using the actual production
  *    code path (`TableCatalog.loadTable`, not a `spark.table(...)`
  *    DataFrame read) that this resolves the table's schema *before*
  *    migration runs (this check rule runs during analysis, before the
  *    procedure's own `call()` executes) - which Iceberg's migrate always
  *    preserves unchanged, confirmed by comparing schemas before/after a
  *    real migration. So the existing extraction (`args.head`,
  *    `tableCatalogOf(procedure).loadTable(...)`) already gives exactly
  *    the resulting schema, with zero new code beyond adding both
  *    procedures' classes to `currentStateChangingProcedureClasses`.
  *
  * ## `snapshot`
  *
  * The one procedure that's genuinely different, confirmed via probe:
  * `snapshot(source_table, table, ...)` creates a *new* Iceberg table
  * (`table`, `args(1)`) whose schema comes from an *existing*, different
  * table (`source_table`, `args.head`) - the contract-relevant location is
  * `args(1)`, but the schema to check comes from `args.head`, the
  * opposite pairing from every other procedure here. Both arguments can
  * also be qualified with a *different* catalog than the one the CALL
  * itself was invoked against - confirmed via probe: an unqualified
  * destination resolved under the *session's* current/default catalog,
  * not the CALL's own bound catalog (`BaseProcedure.tableCatalog()`,
  * which every other procedure's extraction above relies on) - so this
  * needs Spark's own multi-catalog resolution (`SparkSession.active`'s
  * `CatalogManager`, both fully public APIs, no reflection needed at all
  * - unlike every other procedure's `tableCatalogOf`, which reflects into
  * a *protected* Iceberg-internal method because there's no public
  * alternative for "the catalog this specific procedure was resolved
  * against"). `resolveIdentifier` below re-implements the same
  * first-segment-is-a-registered-catalog-name check Iceberg's own
  * `Spark3Util.catalogAndIdentifier` uses, confirmed against this exact
  * behavior via probe rather than assumed to match.
  *
  * Any reflection or resolution failure anywhere in this file returns
  * `None` - falls through to `FailClosedCommands`' existing blanket
  * rejection, never silently treated as "nothing to check." Same
  * fail-closed discipline as `FailClosedCommands.isKnownSafeIcebergProcedureCall`.
  */
private[sparkadapter] object StateChangingCallSupport {

  private val icebergCallClassName = "org.apache.spark.sql.catalyst.plans.logical.Call"

  private val currentStateChangingProcedureClasses: Map[String, String] = Map(
    "org.apache.iceberg.spark.procedures.RollbackToSnapshotProcedure" -> "rollback_to_snapshot",
    "org.apache.iceberg.spark.procedures.RollbackToTimestampProcedure" -> "rollback_to_timestamp",
    "org.apache.iceberg.spark.procedures.CherrypickSnapshotProcedure" -> "cherrypick_snapshot",
    "org.apache.iceberg.spark.procedures.PublishChangesProcedure" -> "publish_changes",
    "org.apache.iceberg.spark.procedures.SetCurrentSnapshotProcedure" -> "set_current_snapshot",
    "org.apache.iceberg.spark.procedures.FastForwardBranchProcedure" -> "fast_forward",
    "org.apache.iceberg.spark.procedures.AddFilesProcedure" -> "add_files",
    "org.apache.iceberg.spark.procedures.MigrateTableProcedure" -> "migrate"
  )

  private val snapshotTableProcedureClassName = "org.apache.iceberg.spark.procedures.SnapshotTableProcedure"

  // Mutation testing: this guard survives as "mutated to always-true" -
  // accepted as a genuine near-equivalent, same as the pilot's own
  // finding. Any real non-Call plan lacks a procedure() method, so the
  // outer try/catch below produces an identical None regardless of
  // whether this check ran.
  def extract(plan: LogicalPlan): Option[StateChangeInfo] =
    if (plan.getClass.getName != icebergCallClassName) None
    else
      try extractCurrentStateChange(plan).orElse(extractSnapshotTable(plan))
      catch { case _: Throwable => None }

  private def extractCurrentStateChange(plan: LogicalPlan): Option[StateChangeInfo] = {
    val procedure = plan.getClass.getMethod("procedure").invoke(plan)
    val callName = currentStateChangingProcedureClasses.getOrElse(procedure.getClass.getName, return None)

    val args = plan.getClass.getMethod("args").invoke(plan).asInstanceOf[Seq[Expression]]
    val tableArg = args.headOption match {
      case Some(Literal(t, _)) => t.toString
      case _ => return None
    }

    val tableCatalog = tableCatalogOf(procedure)
    val parts = tableArg.split("\\.")
    val identifier = Identifier.of(parts.dropRight(1), parts.last)
    val table = tableCatalog.loadTable(identifier)
    val currentSchema = schemaOf(table)

    Some(StateChangeInfo(callName, location = s"${tableCatalog.name()}.$tableArg", resultingSchema = currentSchema))
  }

  private def extractSnapshotTable(plan: LogicalPlan): Option[StateChangeInfo] = {
    val procedure = plan.getClass.getMethod("procedure").invoke(plan)
    // Mutation testing: this guard also survives as "mutated to
    // always-true," accepted after checking every real non-snapshot
    // procedure this could actually run against (every safe-listed
    // procedure plus every procedure in currentStateChangingProcedureClasses
    // above - this method only ever runs at all when the procedure isn't
    // one of those). None has two arguments that are BOTH, simultaneously,
    // real loadable table identifiers the way snapshot's source/table
    // pair are - so bypassing this check either fails resolveIdentifier's
    // own loadTable call (caught by the outer try/catch, same None either
    // way) or fails args extraction outright. Distinguishing the mutant
    // for real would need a procedure that doesn't exist in this connector
    // version, not a realistic gap.
    if (procedure.getClass.getName != snapshotTableProcedureClassName) return None

    val args = plan.getClass.getMethod("args").invoke(plan).asInstanceOf[Seq[Expression]]
    def stringArg(i: Int): Option[String] = args.lift(i) match {
      case Some(Literal(v, _)) => Some(v.toString)
      case _ => None
    }
    val sourceArg = stringArg(0).getOrElse(return None)
    val destArg = stringArg(1).getOrElse(return None)

    val spark = SparkSession.active
    val (sourceCatalog, sourceIdentifier) = resolveIdentifier(sourceArg, spark)
    val (destCatalog, destIdentifier) = resolveIdentifier(destArg, spark)

    val sourceSchema = schemaOf(sourceCatalog.loadTable(sourceIdentifier))
    val location = (destCatalog.name() +: destIdentifier.namespace() :+ destIdentifier.name()).mkString(".")

    Some(StateChangeInfo("snapshot", location, sourceSchema))
  }

  private def schemaOf(table: org.apache.spark.sql.connector.catalog.Table): StructType =
    StructType(table.columns().toSeq.map(c => StructField(c.name(), c.dataType(), c.nullable())))

  // Re-implements Iceberg's own Spark3Util.catalogAndIdentifier resolution
  // (confirmed to match via probe, not assumed): if the identifier's first
  // segment names a registered catalog, that catalog governs the rest;
  // otherwise the session's current/default catalog does, and the whole
  // string is the identifier - never `BaseProcedure.tableCatalog()` (the
  // CALL's own bound catalog), which a probe confirmed `snapshot` does NOT
  // use for either of its two table arguments.
  private def resolveIdentifier(raw: String, spark: SparkSession): (TableCatalog, Identifier) = {
    val catalogManager = spark.sessionState.catalogManager
    val parts = raw.split("\\.")
    // Mutation testing: `> 1` survives as `>= 1` - accepted. The two only
    // differ for a single-segment string that happens to exactly match a
    // registered catalog name (e.g. calling snapshot('local', ...) with
    // no namespace/table at all) - a nonsensical identifier no real
    // caller would pass, and not what `> 1` exists to guard against
    // anyway (a single part can never be "maybe a catalog qualifier",
    // only ever a bare identifier - this mirrors Iceberg's own
    // Spark3Util.catalogAndIdentifier).
    val (catalog, identifierParts): (CatalogPlugin, Array[String]) =
      if (parts.length > 1 && catalogManager.isCatalogRegistered(parts.head)) (catalogManager.catalog(parts.head), parts.tail)
      else (catalogManager.currentCatalog, parts)
    // A bare, single-segment name (no database given at all, e.g. plain
    // "my_table") falls back to the catalog's current namespace - confirmed
    // via probe to match Iceberg's own resolution; a zero-part namespace
    // isn't valid (Spark's V1-catalog compatibility layer rejects it
    // outright with REQUIRES_SINGLE_PART_NAMESPACE).
    val namespace = if (identifierParts.length > 1) identifierParts.dropRight(1) else catalogManager.currentNamespace
    (catalog.asInstanceOf[TableCatalog], Identifier.of(namespace, identifierParts.last))
  }

  // BaseProcedure.tableCatalog() is protected, declared on BaseProcedure
  // itself (not any concrete procedure subclass) - walk the hierarchy to
  // find the declaring class, same technique any reflective access to an
  // inherited non-public member needs.
  private def tableCatalogOf(procedure: Any): TableCatalog = {
    def declaringClass(cls: Class[_]): Class[_] =
      try {
        cls.getDeclaredMethod("tableCatalog")
        cls
      } catch {
        case _: NoSuchMethodException => declaringClass(cls.getSuperclass)
      }
    val method = declaringClass(procedure.getClass).getDeclaredMethod("tableCatalog")
    method.setAccessible(true)
    method.invoke(procedure).asInstanceOf[TableCatalog]
  }
}
