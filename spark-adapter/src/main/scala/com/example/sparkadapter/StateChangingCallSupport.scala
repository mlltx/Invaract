// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.sparkadapter

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.types.{StructField, StructType}

/** The resolved effect of a supported state-changing procedure call: the
  * table it targets and the schema the table will have once the operation
  * completes. Not derived from any Spark write/query (there isn't one).
  * `callName` (the CALL's own procedure name, e.g. `"rollback_to_snapshot"`)
  * is carried through only for error messages - the check itself never
  * branches on which of the six procedures below produced this value.
  */
private[sparkadapter] case class StateChangeInfo(callName: String, location: String, resultingSchema: StructType)

/** Verification support for Iceberg `CALL` procedures that change a table's
  * *current* committed state without a Spark write ever occurring. Six
  * procedures are recognized (see `currentStateChangingProcedureClasses`
  * below); `FailClosedCommands.safeIcebergProcedureClasses`'s doc comment
  * lists the remaining four (`add_files`/`migrate`/`snapshot`/
  * `rewrite_table_path`) still deliberately unmodeled - each needs a
  * materially different extraction mechanism (parsing/binding a schema
  * from a table/path named in the CALL's own arguments, not just its
  * target) that this file's catalog-schema-read approach doesn't cover -
  * not attempted here. See docs/SPARK_ADAPTER.md's "Verifying
  * rollback_to_snapshot" section for the pilot this generalizes from.
  *
  * ## Why this isn't `WriteCommandSupport`
  *
  * `WriteCommandInfo` bundles a `query: LogicalPlan` - the untranslated
  * data being written - because every entry in that registry really is a
  * write: some `DataFrame`/query produces rows, and this module verifies
  * the *query's* schema against the contract. None of the six procedures
  * here has a query: nothing is written: a table's current pointer simply
  * moves to an already-committed snapshot, by one means or another (see
  * "The six procedures" below). Forcing a `None`/dummy `query` into
  * `WriteCommandInfo` would misrepresent what actually happens and widen a
  * stable, central type for a shape that doesn't fit it. This is a
  * separate, narrower registry instead - `ContractEnforcementRule.
  * verifyOrThrow` consults it before falling through to `FailClosedCommands`'
  * blanket rejection, the same priority `WriteCommandSupport` gets.
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
  * evolution and snapshot state are independent in Iceberg's model. None
  * of the other five procedures below touches schema either (same
  * property, confirmed per-procedure - see "The six procedures"), so
  * comparing against a *target* snapshot's schema was never the right
  * question for any of them, not just the first one investigated.
  *
  * The corrected check verifies what any of these six can actually
  * affect, together:
  *
  *  - **Location**: does this CALL even target the contract's declared
  *    output? A state-changing CALL on a table the active contract
  *    doesn't govern is correctly allowed, not swept up by a blanket
  *    rejection.
  *  - **Current schema**: since none of the six can change it, checking
  *    the table's schema *right now* (not any snapshot's) is exactly
  *    checking what will still be true afterward - catching a table
  *    that's already out of compliance (e.g. an out-of-band schema
  *    change) at the point of a state-changing operation on it, the same
  *    way any other write-adjacent check in this module gates on current
  *    compliance.
  *
  * This holds even for `fast_forward` targeting a non-`"main"` branch,
  * where the table's *default* read is genuinely unaffected (confirmed
  * empirically - see below): the check doesn't need to special-case that,
  * since it only asserts an invariant (current schema can't move) that's
  * true regardless of which branch a given call happens to target, not a
  * claim about what that specific call changes.
  *
  * ## The six procedures
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
  * ## The extraction mechanism
  *
  * Two reflective hops, each confirmed against the real jar via `javap`
  * and real probe tests (since deleted) before being written here, not
  * assumed:
  *
  *  1. `BaseProcedure.tableCatalog()` (protected, so `setAccessible`) gives
  *     the real `TableCatalog` (a plain public Spark connector-catalog
  *     type) the procedure was resolved against.
  *  2. `TableCatalog.loadTable(Identifier)` (fully public - no reflection)
  *     resolves the target table, named by `Call.args.head` (a plain
  *     Catalyst `Literal` expression) - confirmed via probe that the
  *     table identifier is always the first positional argument across
  *     all six procedures, regardless of how many parameters each
  *     declares or whether the CALL used named or positional syntax.
  *
  * From there, `Table.columns()` (not the deprecated `Table.schema()`,
  * same convention `WriteCommandSupport.outputSchemaWithTargetOnlyFields`
  * already established) is a plain public method on the base, connector-
  * agnostic `org.apache.spark.sql.connector.catalog.Table` interface - no
  * further reflection, and no Iceberg-specific type needed at all.
  *
  * Any reflection failure at either hop returns `None` - falls through to
  * `FailClosedCommands`' existing blanket rejection, never silently
  * treated as "nothing to check." Same fail-closed discipline as
  * `FailClosedCommands.isKnownSafeIcebergProcedureCall`.
  */
private[sparkadapter] object StateChangingCallSupport {

  private val icebergCallClassName = "org.apache.spark.sql.catalyst.plans.logical.Call"

  private val currentStateChangingProcedureClasses: Map[String, String] = Map(
    "org.apache.iceberg.spark.procedures.RollbackToSnapshotProcedure" -> "rollback_to_snapshot",
    "org.apache.iceberg.spark.procedures.RollbackToTimestampProcedure" -> "rollback_to_timestamp",
    "org.apache.iceberg.spark.procedures.CherrypickSnapshotProcedure" -> "cherrypick_snapshot",
    "org.apache.iceberg.spark.procedures.PublishChangesProcedure" -> "publish_changes",
    "org.apache.iceberg.spark.procedures.SetCurrentSnapshotProcedure" -> "set_current_snapshot",
    "org.apache.iceberg.spark.procedures.FastForwardBranchProcedure" -> "fast_forward"
  )

  def extract(plan: LogicalPlan): Option[StateChangeInfo] =
    if (plan.getClass.getName != icebergCallClassName) None
    else
      try extractCurrentStateChange(plan)
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
    val currentSchema = StructType(table.columns().toSeq.map(c => StructField(c.name(), c.dataType(), c.nullable())))

    Some(StateChangeInfo(callName, location = s"${tableCatalog.name()}.$tableArg", resultingSchema = currentSchema))
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
