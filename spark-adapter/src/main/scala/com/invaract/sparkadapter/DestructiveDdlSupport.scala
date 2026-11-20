// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, RuleType}
import com.invaract.verification.{MutationKind, OutputChecker, RuleVerifier, UnverifiableOperation, Violation, Violations}
import org.apache.spark.sql.catalyst.analysis.{ResolvedIdentifier, ResolvedTable}
import org.apache.spark.sql.catalyst.plans.logical.{DropPartitions, DropTable, LogicalPlan, TruncatePartition, TruncateTable}
import org.apache.spark.sql.execution.command.{AlterTableDropPartitionCommand, DropTableCommand, TruncateTableCommand}

/** `DROP TABLE`, `TRUNCATE TABLE` and `ALTER TABLE ... DROP PARTITION` (docs/SPARK_ADAPTER_HARDENING_PLAN.md, H-10):
  * statements that remove a table's data without a query to check. They used to be refused whatever the contract said,
  * so the everyday way to make a job re-runnable (drop or truncate its output, then rebuild it) could not be put under a
  * contract at all.
  *
  * A contract lists what a job may change. So: removing data from a declared output is allowed when the contract lets
  * that output be overwritten; removing data from anything else is refused (`judge`).
  */
private[sparkadapter] object DestructiveDdlSupport {

  /** @param statement the statement, as a message names it (`DROP TABLE`)
    * @param removesEverything whether every row goes (`DROP TABLE`, `TRUNCATE TABLE`), not one partition's
    */
  final case class DestructiveDdl(statement: String, target: TableTargets.Target, removesEverything: Boolean) {
    def location: String = target.location
    def tableNames: List[String] = target.names
    def table: String = target.display
  }

  sealed trait Verdict
  object Verdict {
    case object Allowed extends Verdict

    /** The table is no output of the contract: nothing in it says the job may remove its data. */
    final case class Undeclared(operation: UnverifiableOperation) extends Verdict

    /** A declared output, which the contract says may not lose its data this way. */
    final case class Refused(violation: Violation) extends Verdict
  }

  val DropTableStatement = "DROP TABLE"
  val TruncateTableStatement = "TRUNCATE TABLE"
  val TruncatePartitionStatement = "TRUNCATE TABLE ... PARTITION"
  val DropPartitionStatement = "ALTER TABLE ... DROP PARTITION"

  /** The statement `plan` is, if it removes a table's data. The analyzed plan is a catalyst command for a table of a
    * V2 catalog (Delta, Iceberg, and `DROP TABLE` of any table), and an `execution.command` for a V1 table.
    */
  def extract(plan: LogicalPlan): Option[DestructiveDdl] = plan match {
    case DropTable(ri: ResolvedIdentifier, _, _) => Some(DestructiveDdl(DropTableStatement, TableTargets.ofResolvedIdentifier(ri), removesEverything = true))
    // A view holds no data: `DROP VIEW` arrives as this command with `isView`, and is on the safe list instead.
    case d: DropTableCommand if !d.isView => TableTargets.ofV1(d.tableName).map(DestructiveDdl(DropTableStatement, _, removesEverything = true))
    case t: TruncateTableCommand =>
      val whole = t.partitionSpec.isEmpty
      TableTargets.ofV1(t.tableName).map(DestructiveDdl(if (whole) TruncateTableStatement else TruncatePartitionStatement, _, removesEverything = whole))
    case TruncateTable(rt: ResolvedTable) => Some(DestructiveDdl(TruncateTableStatement, TableTargets.ofResolvedTable(rt), removesEverything = true))
    case a: AlterTableDropPartitionCommand => TableTargets.ofV1(a.tableName).map(DestructiveDdl(DropPartitionStatement, _, removesEverything = false))
    case d: DropPartitions =>
      d.table match {
        case rt: ResolvedTable => Some(DestructiveDdl(DropPartitionStatement, TableTargets.ofResolvedTable(rt), removesEverything = false))
        case _                 => None
      }
    case t: TruncatePartition =>
      t.table match {
        case rt: ResolvedTable => Some(DestructiveDdl(TruncatePartitionStatement, TableTargets.ofResolvedTable(rt), removesEverything = false))
        case _                 => None
      }
    case _ => None
  }

  /** What `contract` says about `ddl`:
    *   - the table is no declared output: refused, as an operation the contract never allowed;
    *   - it is one, with a `saveMode` other than `overwrite`: refused (`OUTPUT_SAVE_MODE_MISMATCH`) - removing its data
    *     is overwriting it;
    *   - it is one, every row goes, and a rule about deletes applies: refused - `RULE_UNCONDITIONAL_DELETE` for
    *     `forbid_unconditional_delete` (a removal of every row is a delete with no condition), otherwise
    *     `RULE_UNVERIFIABLE_DML` (a custom rule about deletes, which this statement cannot be judged against);
    *   - otherwise allowed.
    */
  def judge(contract: Contract, ddl: DestructiveDdl): Verdict =
    OutputChecker.matchOutput(contract.outputs, ddl.target.all) match {
      case None =>
        Verdict.Undeclared(
          UnverifiableOperation(
            ddl.statement,
            s"${ddl.statement} removes data from '${ddl.table}', which this contract does not declare as an output",
            s"Declare '${ddl.table}' as an output of the contract if this job owns it (the job must then write it), or remove the statement."
          )
        )
      case Some(output) if output.saveMode.exists(!_.equalsIgnoreCase("overwrite")) =>
        Verdict.Refused(Violations.outputSaveModeMismatch(output.saveMode.get, "overwrite", ddl.location))
      case Some(_) if ddl.removesEverything && RuleVerifier.anyRuleAppliesTo(contract.rules, MutationKind.Delete, contract.customRuleTypes) =>
        Verdict.Refused(
          if (contract.rules.exists(_.ruleType == RuleType.ForbidUnconditionalDelete))
            Violations.unconditionalDeleteRule().copy(location = Some(ddl.location), rule = Some(RuleType.ForbidUnconditionalDelete))
          else Violations.unverifiableDml("DELETE", Some(ddl.location))
        )
      case Some(_) => Verdict.Allowed
    }
}
