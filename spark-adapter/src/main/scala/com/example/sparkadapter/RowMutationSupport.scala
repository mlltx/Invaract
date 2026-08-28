// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.sparkadapter

import com.example.ir

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, LogicalPlan}

/** Extracts `ir.RowMutation` — the structural facts a contract's declared
  * DML rules (`com.example.contract.RuleType`) need — from a *standalone*
  * row-level DML command. A sibling to `WriteCommandSupport`, not a
  * replacement: the same Delta command classes `WriteCommandSupport.deltaRowLevelDml`
  * already recognizes (to produce `ir.Write`, for `StructuralVerifier`)
  * are matched again here, independently, to produce the different facts
  * `RuleVerifier` needs. Kept separate rather than folded into
  * `WriteCommandInfo` because adding a field to that case class would be
  * a binary-incompatible change to an existing public constructor — a new
  * standalone extractor is the MiMa-safe way to add a fact these commands
  * carry, the same reasoning `StateChangingCallSupport` already
  * establishes as a pattern in this module.
  *
  * ## Scope
  *
  * Delta's `UpdateCommand`/`DeleteCommand`/`MergeIntoCommand` (matched by
  * fully-qualified class name and read via public-method reflection, the
  * same convention `WriteCommandSupport.deltaRowLevelDml` uses and for
  * the same reason — this module has no compile-time dependency on
  * Delta), plus DSv2's plain `DeleteFromTable` (a real, stable, public
  * Spark class already recognized by `WriteCommandSupport.deleteFromTable`
  * for connectors implementing `SupportsDelete` rather than
  * `SupportsRowLevelOperations` — see that case's own doc for the
  * ClickHouse finding that established it).
  *
  * Deliberately does not cover Iceberg's (or any other DSv2
  * `SupportsRowLevelOperations` connector's) MERGE/UPDATE/DELETE —
  * `WriteCommandSupport.dsv2RowLevelWrite`'s `ReplaceData`/`WriteDelta`
  * nodes are Spark's own *rewritten* form of the operation (a copy-on-write
  * scan-and-replace, or a merge-on-read delta write), not a form that
  * still carries a clean "the match condition" / "the columns an UPDATE
  * assigns" fact the way Delta's own command classes do — recovering
  * those from the rewritten plan is real, unstarted work, not something
  * this pass attempts. See ROADMAP.md's "Full semantic DML verification"
  * item.
  */
private[sparkadapter] object RowMutationSupport {

  private val deltaUpdateClassName = "org.apache.spark.sql.delta.commands.UpdateCommand"
  private val deltaDeleteClassName = "org.apache.spark.sql.delta.commands.DeleteCommand"
  private val deltaMergeClassName = "org.apache.spark.sql.delta.commands.MergeIntoCommand"

  // Confirmed empirically against Delta 3.2.0's own source
  // (PreprocessTableUpdate.toCommand / UpdateExpressionsSupport.generateUpdateExpressions,
  // not assumed): `updateExpressions` is always aligned 1:1 with
  // `target.output` (one entry per target column, in target column
  // order). A column the SQL `SET` clause doesn't mention gets its
  // *original* `target.output` attribute back as that column's entry
  // (`defaultExpr` in Delta's own generator) — so comparing each pair for
  // semantic equality is exactly "did this column's value expression
  // change," with no risk of a false "updated" from Delta's own
  // resolution machinery reusing the identical attribute for an untouched
  // column.
  private val deltaUpdate: PartialFunction[LogicalPlan, ir.RowMutation] =
    Function.unlift { (plan: LogicalPlan) =>
      if (plan.getClass.getName != deltaUpdateClassName) None
      else
        scala.util.Try {
          val target = plan.getClass.getMethod("target").invoke(plan).asInstanceOf[LogicalPlan]
          val updateExpressions =
            plan.getClass.getMethod("updateExpressions").invoke(plan).asInstanceOf[Seq[Expression]]
          val updatedColumns = target.output.zip(updateExpressions).collect {
            case (targetAttr, expr) if !expr.semanticEquals(targetAttr) => targetAttr.name
          }.toList
          ir.RowMutation(updatedColumns = updatedColumns)
        }.toOption
    }

  private val deltaDelete: PartialFunction[LogicalPlan, ir.RowMutation] =
    Function.unlift { (plan: LogicalPlan) =>
      if (plan.getClass.getName != deltaDeleteClassName) None
      else
        scala.util.Try {
          val condition =
            plan.getClass.getMethod("condition").invoke(plan).asInstanceOf[Option[Expression]]
          ir.RowMutation(delete = deleteScopeOf(condition))
        }.toOption
    }

  private val deltaMerge: PartialFunction[LogicalPlan, ir.RowMutation] =
    Function.unlift { (plan: LogicalPlan) =>
      if (plan.getClass.getName != deltaMergeClassName) None
      else
        scala.util.Try {
          val condition = plan.getClass.getMethod("condition").invoke(plan).asInstanceOf[Expression]
          ir.RowMutation(matchCondition = Some(SparkPlanAdapter.translateExprStandalone(condition)))
        }.toOption
    }

  // `DELETE FROM <v2-table> WHERE <predicate>` against a connector
  // implementing plain `SupportsDelete` — same shape
  // `WriteCommandSupport.deleteFromTable` already recognizes for `ir.Write`
  // purposes. Confirmed empirically (Spark 3.5.1's own parser,
  // AstBuilder.visitDeleteFromTable, not assumed): a bare `DELETE FROM t`
  // with no `WHERE` sets `condition` to `Literal.TrueLiteral`, never
  // `None` — DeleteFromTable has no `Option` here the way Delta's
  // DeleteCommand does, so "unconditional" is detected by comparing
  // against that literal rather than by absence.
  private val dsv2Delete: PartialFunction[LogicalPlan, ir.RowMutation] = { case cmd: DeleteFromTable =>
    ir.RowMutation(delete = deleteScopeOf(Some(cmd.condition)))
  }

  private def deleteScopeOf(condition: Option[Expression]): ir.DeleteScope = condition match {
    case None                                                   => ir.DeleteScope.Unconditional
    case Some(c) if c.semanticEquals(Literal.TrueLiteral)       => ir.DeleteScope.Unconditional
    case Some(c)                                                => ir.DeleteScope.Conditional(SparkPlanAdapter.translateExprStandalone(c))
  }

  /** Every recognized standalone-DML shape, combined into one lookup — the
    * same "one shared PartialFunction, not an independent match per
    * caller" convention `WriteCommandSupport.combined` and
    * `StateChangingCallSupport.extract` already establish.
    */
  val combined: PartialFunction[LogicalPlan, ir.RowMutation] =
    deltaUpdate orElse deltaDelete orElse deltaMerge orElse dsv2Delete
}
