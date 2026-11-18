// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.Contract
import com.invaract.verification.{OutputChecker, UnverifiableOperation}
import org.apache.spark.sql.catalyst.analysis.{ResolvedIdentifier, ResolvedTable}
import org.apache.spark.sql.catalyst.plans.logical.{
  AddColumns, AlterColumn, AlterTableCommand, DropColumns, LogicalPlan, RenameColumn, RenameTable, ReplaceColumns, SetTableLocation
}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableChange}
import org.apache.spark.sql.execution.command.{
  AlterTableAddColumnsCommand, AlterTableAddPartitionCommand, AlterTableChangeColumnCommand, AlterTableRenameCommand,
  AlterTableSetLocationCommand
}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}

import scala.util.Try

/** `ALTER TABLE` statements that change what a contract describes about an output without writing to it
  * (docs/SPARK_ADAPTER_HARDENING_PLAN.md, H-05): its columns, or where it is. They change no rows, so they were on the
  * fail-closed safe list and passed unchecked - an output could lose a required column, or gain one the contract
  * rejects, or be repointed at other data, with no validation at all.
  *
  * Checked as an Iceberg procedure that changes a table's state is (`StateChangingCallSupport`): a statement that
  * changes the columns has the columns it leaves checked against the declared output it names, with the same rules and
  * messages as a write (`VerificationPipeline.verifyStateChange`, which also lets a table no declared output names
  * through); a statement that moves a declared output away from where the contract says it is is refused (`refusal`).
  * A statement this does not recognize keeps its place on the safe list.
  */
private[sparkadapter] object SchemaDdlSupport {

  sealed trait SchemaDdl { def statement: String }

  /** The table's columns change: `schema` is what they are after the statement. */
  final case class NewSchema(statement: String, target: TableTargets.Target, schema: StructType) extends SchemaDdl

  /** The table stops being where, or only what, the contract says (SET LOCATION, ADD PARTITION ... LOCATION), or its
    * new columns cannot be worked out. */
  final case class Displaced(statement: String, target: TableTargets.Target, problem: String) extends SchemaDdl

  /** RENAME TO: `source` stops being, and `target` (the new name) becomes, a table with `schema`'s columns. */
  final case class RenamedOnto(statement: String, source: TableTargets.Target, target: TableTargets.Target, schema: StructType)
      extends SchemaDdl

  val RepointsTable = "repoints the table at a different location"
  val RepointsPartition = "repoints a partition at data no write produced"
  val AttachesData = "attaches data no write produced"
  val RenamesAway = "renames the table away"
  val Unworkable = "changes the table's columns in a way Invaract could not work out"

  def extract(plan: LogicalPlan): Option[SchemaDdl] = plan match {
    // Delta, Iceberg: ADD/RENAME/DROP/ALTER/REPLACE COLUMNS. A command that changes only properties changes no column.
    // `changes` can throw (SetTableLocation with a partition, which a V2 table does not support): no column change then.
    case cmd: AlterTableCommand if Try(cmd.changes).toOption.exists(_.exists(_.isInstanceOf[TableChange.ColumnChange])) =>
      cmd.table match {
        case rt: ResolvedTable =>
          val statement = s"ALTER TABLE ... ${verbOf(cmd)}"
          val target = TableTargets.ofResolvedTable(rt)
          Some(
            applySchemaChanges(columnsOf(rt.table), cmd.changes, Option(rt.table.properties.get("provider")))
              .map(NewSchema(statement, target, _))
              .getOrElse(Displaced(statement, target, Unworkable))
          )
        case _ => None
      }
    // A V1 table (parquet, CSV, Hive): the columns are added before the partition columns, as Spark adds them.
    case a: AlterTableAddColumnsCommand =>
      TableTargets.v1Table(a.table).map { t =>
        NewSchema("ALTER TABLE ... ADD COLUMNS", TableTargets.ofCatalogTable(t), StructType(t.dataSchema ++ a.colsToAdd ++ t.partitionSchema))
      }
    case c: AlterTableChangeColumnCommand =>
      val resolver = SQLConf.get.resolver
      TableTargets.v1Table(c.tableName).map { t =>
        NewSchema(
          "ALTER TABLE ... CHANGE COLUMN",
          TableTargets.ofCatalogTable(t),
          StructType(t.schema.map(f => if (resolver(f.name, c.columnName)) c.newColumn else f))
        )
      }
    case s: AlterTableSetLocationCommand =>
      TableTargets.ofV1(s.tableName).map(Displaced("ALTER TABLE ... SET LOCATION", _, if (s.partitionSpec.isEmpty) RepointsTable else RepointsPartition))
    case a: AlterTableAddPartitionCommand if a.partitionSpecsAndLocs.exists(_._2.isDefined) =>
      TableTargets.ofV1(a.tableName).map(Displaced("ALTER TABLE ... ADD PARTITION ... LOCATION", _, AttachesData))
    case r: AlterTableRenameCommand if !r.isView =>
      for {
        table <- TableTargets.v1Table(r.oldName)
        // RENAME TO keeps a table in its database: a new name without one is in the old one's.
        target <- TableTargets.ofV1(r.newName.copy(database = r.newName.database.orElse(table.identifier.database)))
      } yield RenamedOnto("ALTER TABLE ... RENAME TO", TableTargets.ofCatalogTable(table), target, table.schema)
    // The catalyst forms, for a table of a V2 catalog.
    case SetTableLocation(rt: ResolvedTable, partitionSpec, _) =>
      Some(Displaced("ALTER TABLE ... SET LOCATION", TableTargets.ofResolvedTable(rt), if (partitionSpec.isEmpty) RepointsTable else RepointsPartition))
    case RenameTable(rt: ResolvedTable, newName, false) if newName.nonEmpty =>
      val namespace = if (newName.size > 1) newName.init.toArray else rt.identifier.namespace
      val target = TableTargets.ofResolvedIdentifier(ResolvedIdentifier(rt.catalog, Identifier.of(namespace, newName.last)))
      Some(RenamedOnto("ALTER TABLE ... RENAME TO", TableTargets.ofResolvedTable(rt), target, columnsOf(rt.table)))
    case _ => None
  }

  /** A V2 table's columns. `Table.columns()`, not the deprecated `Table.schema()`. */
  private def columnsOf(table: Table): StructType =
    StructType(table.columns().toSeq.map(c => StructField(c.name(), c.dataType(), c.nullable())))

  /** The columns `changes` leave, worked out by Spark's own `CatalogV2Util.applySchemaChanges` - the method a V2
    * catalog's ALTER TABLE uses - so nested fields, positions and every kind of change come out exactly as Spark makes
    * them. It is not public API, so it is called by reflection; `None` if this Spark has no such method, and the
    * statement is then refused on a declared output rather than let through unchecked.
    */
  private[sparkadapter] def applySchemaChanges(schema: StructType, changes: Seq[TableChange], provider: Option[String]): Option[StructType] =
    Try {
      Class
        .forName("org.apache.spark.sql.connector.catalog.CatalogV2Util")
        .getMethod("applySchemaChanges", classOf[StructType], classOf[scala.collection.Seq[_]], classOf[Option[_]], classOf[String])
        .invoke(null, schema, changes, provider, "ALTER TABLE")
        .asInstanceOf[StructType]
    }.toOption

  private def verbOf(cmd: AlterTableCommand): String = cmd match {
    case _: AddColumns     => "ADD COLUMNS"
    case _: ReplaceColumns => "REPLACE COLUMNS"
    case _: DropColumns    => "DROP COLUMN"
    case _: RenameColumn   => "RENAME COLUMN"
    case _: AlterColumn    => "ALTER COLUMN"
    case _                 => "ALTER TABLE"
  }

  /** The refusal for moving `target` away with `statement`, if `target` is a declared output; `None` (not this
    * contract's concern) otherwise. */
  def refusal(contract: Contract, statement: String, target: TableTargets.Target, problem: String): Option[UnverifiableOperation] =
    OutputChecker.matchOutput(contract.outputs, target.all).map { output =>
      UnverifiableOperation(
        statement,
        s"$statement $problem, and '${output.name}' is an output this contract declares",
        "Write the output with a query Invaract can check, or change the contract's declared location in the same change."
      )
    }
}
