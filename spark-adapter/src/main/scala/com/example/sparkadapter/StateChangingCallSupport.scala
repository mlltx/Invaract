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
  */
private[sparkadapter] case class StateChangeInfo(location: String, resultingSchema: StructType)

/** Verification support for Iceberg `CALL` procedures that change a table's
  * *current* committed state without a Spark write ever occurring -
  * `rollback_to_snapshot` is the only one implemented (a deliberate pilot,
  * not the full set - see docs/SPARK_ADAPTER.md's "Verifying rollback_to_snapshot"
  * section). `FailClosedCommands.safeIcebergProcedureClasses`'s doc comment
  * lists the other nine state-changing procedures still deliberately
  * unmodeled; each would need its own extraction mechanism (a table/path
  * read from a CALL argument, for `add_files`/`migrate`/`snapshot`, rather
  * than this file's catalog-schema-read approach) - not attempted here.
  *
  * ## Why this isn't `WriteCommandSupport`
  *
  * `WriteCommandInfo` bundles a `query: LogicalPlan` - the untranslated
  * data being written - because every entry in that registry really is a
  * write: some `DataFrame`/query produces rows, and this module verifies
  * the *query's* schema against the contract. `rollback_to_snapshot` has no
  * query: nothing is written, the target table's current pointer simply
  * moves to an already-committed historical snapshot. Forcing a `None`/
  * dummy `query` into `WriteCommandInfo` would misrepresent what actually
  * happens and widen a stable, central type for one shape that doesn't fit
  * it. This is a separate, narrower registry instead - `ContractEnforcementRule.
  * verifyOrThrow` consults it before falling through to `FailClosedCommands`'
  * blanket rejection, the same priority `WriteCommandSupport` gets.
  *
  * ## What's actually checked, and why (a real correction, not the original design)
  *
  * The first version of this file extracted the *target snapshot's own*
  * historical schema (via Iceberg's `SparkTable.copyWithSnapshotId(id).schema()`)
  * and checked that against the contract. That turned out to check the wrong
  * thing: confirmed empirically (a real rollback, followed by an explicit
  * `refreshTable`, still reported the table's *current*, post-evolution
  * schema - not the target snapshot's) and independently corroborated by
  * Apache Iceberg's own issue tracker (apache/iceberg#15165, open, unresolved
  * as of this writing): `rollback_to_snapshot` moves which snapshot's *data*
  * is current, but does **not** revert `current-schema-id` - schema
  * evolution and snapshot rollback are independent in Iceberg's model. So a
  * rollback can never change a table's schema at all; comparing the
  * contract against the *target* snapshot's schema was answering a question
  * that doesn't correspond to anything the operation actually does.
  *
  * The corrected check verifies what a rollback can actually affect,
  * together:
  *
  *  - **Location**: does this CALL even target the contract's declared
  *    output? A rollback on a table the active contract doesn't govern is
  *    now correctly allowed, not wrongly swept up by a blanket rejection
  *    the way every rollback was before this file existed.
  *  - **Current schema**: since the operation can't change it, checking the
  *    table's schema *right now* (not the target snapshot's) is exactly
  *    checking what will still be true afterward - catching a table that's
  *    already out of compliance (e.g. an out-of-band schema change) at the
  *    point of a state-changing operation on it, the same way any other
  *    write-adjacent check in this module gates on current compliance.
  *
  * ## The extraction mechanism
  *
  * `Call.procedure()` returns a real, concrete `Procedure` instance per
  * procedure type (see `FailClosedCommands`'s own doc for how that's
  * confirmed, not guessed) - `RollbackToSnapshotProcedure` here. Two
  * reflective hops, each confirmed against the real jar via `javap` and a
  * real probe test (since deleted) before being written here, not assumed:
  *
  *  1. `BaseProcedure.tableCatalog()` (protected, so `setAccessible`) gives
  *     the real `TableCatalog` (a plain public Spark connector-catalog
  *     type) the procedure was resolved against.
  *  2. `TableCatalog.loadTable(Identifier)` (fully public - no reflection)
  *     resolves the target table, named by `Call.args` (plain Catalyst
  *     `Literal` expressions, matched positionally against
  *     `RollbackToSnapshotProcedure`'s declared parameter order - table
  *     identifier, then snapshot id - confirmed via `javap`, not assumed).
  *
  * From there, `Table.columns()` (not the deprecated `Table.schema()`,
  * same convention `WriteCommandSupport.outputSchemaWithTargetOnlyFields`
  * already established) is a plain public method on the base, connector-
  * agnostic `org.apache.spark.sql.connector.catalog.Table` interface - no
  * further reflection, and no Iceberg-specific type needed at all, unlike
  * the original design's Iceberg-specific `SparkTable` cast.
  *
  * Any reflection failure at either hop returns `None` - falls through to
  * `FailClosedCommands`' existing blanket rejection, never silently
  * treated as "nothing to check." Same fail-closed discipline as
  * `FailClosedCommands.isKnownSafeIcebergProcedureCall`.
  */
private[sparkadapter] object StateChangingCallSupport {

  private val icebergCallClassName = "org.apache.spark.sql.catalyst.plans.logical.Call"

  private val rollbackToSnapshotProcedureClassName =
    "org.apache.iceberg.spark.procedures.RollbackToSnapshotProcedure"

  def extract(plan: LogicalPlan): Option[StateChangeInfo] =
    if (plan.getClass.getName != icebergCallClassName) None
    else
      try extractRollbackToSnapshot(plan)
      catch { case _: Throwable => None }

  private def extractRollbackToSnapshot(plan: LogicalPlan): Option[StateChangeInfo] = {
    val procedure = plan.getClass.getMethod("procedure").invoke(plan)
    if (procedure.getClass.getName != rollbackToSnapshotProcedureClassName) return None

    // Both args are validated for shape (a well-formed 2-arg CALL) even
    // though only the table identifier's value is actually used below -
    // the snapshot id itself plays no role in what's checked, since the
    // resulting schema is the table's current one regardless of which
    // snapshot becomes current (see this file's own doc above).
    val args = plan.getClass.getMethod("args").invoke(plan).asInstanceOf[Seq[Expression]]
    val tableArg = (args.head, args(1)) match {
      case (Literal(t, _), Literal(_, _)) => t.toString
      case _ => return None
    }

    val tableCatalog = tableCatalogOf(procedure)
    val parts = tableArg.split("\\.")
    val identifier = Identifier.of(parts.dropRight(1), parts.last)
    val table = tableCatalog.loadTable(identifier)
    val currentSchema = StructType(table.columns().toSeq.map(c => StructField(c.name(), c.dataType(), c.nullable())))

    Some(StateChangeInfo(location = s"${tableCatalog.name()}.$tableArg", resultingSchema = currentSchema))
  }

  // BaseProcedure.tableCatalog() is protected, declared on BaseProcedure
  // itself (not the concrete RollbackToSnapshotProcedure subclass) - walk
  // the hierarchy to find the declaring class, same technique any
  // reflective access to an inherited non-public member needs.
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
