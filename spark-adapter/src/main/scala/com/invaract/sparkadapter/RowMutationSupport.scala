// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.ir

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, If, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, LogicalPlan, Project, ReplaceData, RowLevelWrite}
import org.apache.spark.sql.connector.write.{RowLevelOperation => SparkRowLevelOperation}

/** Extracts `ir.RowMutation` — the structural facts a contract's declared
  * DML rules (`com.invaract.contract.RuleType`) need — from a row-level
  * DML command. A sibling to `WriteCommandSupport`, not a replacement:
  * the same command classes `WriteCommandSupport.deltaRowLevelDml`/
  * `dsv2RowLevelWrite`/`deleteFromTable` already recognize (to produce
  * `ir.Write`, for `StructuralVerifier`) are matched again here,
  * independently, to produce the different facts `RuleVerifier` needs.
  * Kept separate rather than folded into `WriteCommandInfo` because
  * adding a field to that case class would be a binary-incompatible
  * change to an existing public constructor — a new standalone extractor
  * is the MiMa-safe way to add a fact these commands carry, the same
  * reasoning `StateChangingCallSupport` already establishes as a pattern
  * in this module.
  *
  * ## Recognized shapes
  *
  *   - Delta's `MergeIntoCommand`/`UpdateCommand`/`DeleteCommand`
  *     (matched by fully-qualified class name and read via public-method
  *     reflection, the same convention `WriteCommandSupport.deltaRowLevelDml`
  *     uses and for the same reason — this module has no compile-time
  *     dependency on Delta).
  *   - Any DSv2 connector implementing Spark's standard
  *     `SupportsRowLevelOperations` (Iceberg's mechanism, and any future
  *     connector using the same public API) — `RowLevelWrite`
  *     (`ReplaceData`/`WriteDelta`), no reflection needed since these are
  *     real, importable Spark types.
  *   - DSv2's plain `DeleteFromTable`, for connectors implementing only
  *     `SupportsDelete` (ClickHouse's mechanism — see
  *     `WriteCommandSupport.deleteFromTable`'s own doc for the finding
  *     that established it).
  *
  * ## `classify`: recognized but unextractable is not the same as
  * unrecognized
  *
  * A plan being row-level-DML-*shaped* (this module knows its operation
  * kind — MERGE/UPDATE/DELETE) and this module successfully *extracting*
  * the fact a rule needs from it are two different things, and
  * `ContractEnforcementRule` needs to tell them apart: a plan that isn't
  * DML at all makes a DML rule simply inapplicable (the normal, common
  * case — most writes aren't row-level DML), but a plan that genuinely
  * *is* a MERGE/UPDATE/DELETE, under a contract that declares a rule for
  * exactly that kind, whose fact this module couldn't extract, must fail
  * closed rather than silently let the rule go unchecked — the same
  * "unverifiable, not passed" principle `ContractEnforcementRule`'s
  * general `UnverifiableWrite` fail-closed policy already applies to
  * writes this module can't translate at all. `classify` returns `None`
  * for the first case and `Some(Classification)` for the second,
  * distinguishing `Extracted` from `Unverifiable` — see each case's own
  * doc.
  *
  * Two concrete cases actually reach `Unverifiable` today, both
  * confirmed empirically, not hypothetical: a future Delta version
  * renaming one of the reflected methods (this module already tolerated
  * this by falling through to `UnverifiableWrite` for the *write* as a
  * whole; now the same failure mode is distinguished from "not a rule
  * this module checks" for rule-checking specifically), and Iceberg's
  * merge-on-read UPDATE (`WriteDelta`) — its rewritten plan has no
  * per-column before/after pairing the way copy-on-write's `ReplaceData`
  * does, so `updatedColumnsOfReplaceData` deliberately doesn't guess.
  */
private[sparkadapter] object RowMutationSupport {

  /** The three DML operation kinds `RuleVerifier`'s rules care about —
    * connector-agnostic, independent of whether extraction for that kind
    * actually succeeds.
    */
  sealed trait Kind
  object Kind {
    case object Merge extends Kind
    case object Update extends Kind
    case object Delete extends Kind
  }

  sealed trait Classification { def kind: Kind }
  object Classification {
    /** This plan is `kind`-shaped DML, and its rule-relevant facts were
      * successfully extracted into `mutation`.
      */
    case class Extracted(kind: Kind, mutation: ir.RowMutation) extends Classification

    /** This plan is genuinely `kind`-shaped DML, but this module could
      * not extract the fact a rule of that kind needs — see this file's
      * class doc for the two cases this covers.
      */
    case class Unverifiable(kind: Kind) extends Classification
  }

  private val deltaDmlClassNames: Map[String, Kind] = Map(
    "org.apache.spark.sql.delta.commands.MergeIntoCommand" -> Kind.Merge,
    "org.apache.spark.sql.delta.commands.UpdateCommand" -> Kind.Update,
    "org.apache.spark.sql.delta.commands.DeleteCommand" -> Kind.Delete
  )

  /** The single entry point every caller uses — `None` if `plan` isn't
    * row-level DML at all (a DML rule is simply inapplicable to it),
    * `Some(...)` otherwise. See this file's class doc for the
    * `Extracted`/`Unverifiable` distinction.
    */
  def classify(plan: LogicalPlan): Option[Classification] =
    deltaDmlClassNames.get(plan.getClass.getName) match {
      case Some(kind) => Some(classifyDelta(plan, kind))
      case None =>
        plan match {
          case rlw: RowLevelWrite  => Some(classifyIcebergStyle(rlw))
          case df: DeleteFromTable => Some(Classification.Extracted(Kind.Delete, ir.RowMutation(delete = deleteScopeOf(Some(df.condition)))))
          case _                   => None
        }
    }

  /** Convenience for callers that only want a successfully extracted
    * mutation (`RuleVerifier.verify`'s input) — the `Unverifiable` case
    * is handled separately by `ContractEnforcementRule`, which needs the
    * `kind` `combined` alone would discard.
    */
  val combined: PartialFunction[LogicalPlan, ir.RowMutation] = Function.unlift { plan =>
    classify(plan).collect { case Classification.Extracted(_, mutation) => mutation }
  }

  // Wrapped in Try, the same convention WriteCommandSupport.deltaRowLevelDml
  // already uses and for the same reason: this module has no compile-time
  // dependency on Delta, and a future Delta version renaming/removing one
  // of these methods must degrade to `Unverifiable`, not let a raw
  // ReflectiveOperationException escape into a real Spark job or (worse)
  // silently skip the rule.
  private def classifyDelta(plan: LogicalPlan, kind: Kind): Classification = {
    val extracted = scala.util.Try {
      kind match {
        case Kind.Merge =>
          val condition = plan.getClass.getMethod("condition").invoke(plan).asInstanceOf[Expression]
          ir.RowMutation(matchCondition = Some(SparkPlanAdapter.translateExprStandalone(condition)))
        case Kind.Delete =>
          val condition = plan.getClass.getMethod("condition").invoke(plan).asInstanceOf[Option[Expression]]
          ir.RowMutation(delete = deleteScopeOf(condition))
        case Kind.Update =>
          // Confirmed empirically against Delta 3.2.0's own source
          // (PreprocessTableUpdate.toCommand /
          // UpdateExpressionsSupport.generateUpdateExpressions, not
          // assumed): `updateExpressions` is always aligned 1:1 with
          // `target.output`. A column the SQL `SET` clause doesn't
          // mention gets its *original* `target.output` attribute back
          // as that column's entry (Delta's own `defaultExpr` fallback),
          // so comparing each pair for semantic equality is exactly "did
          // this column's value expression change."
          val target = plan.getClass.getMethod("target").invoke(plan).asInstanceOf[LogicalPlan]
          val updateExpressions =
            plan.getClass.getMethod("updateExpressions").invoke(plan).asInstanceOf[Seq[Expression]]
          val updatedColumns = target.output.zip(updateExpressions).collect {
            case (targetAttr, expr) if !expr.semanticEquals(targetAttr) => targetAttr.name
          }.toList
          ir.RowMutation(updatedColumns = updatedColumns)
      }
    }.toOption
    extracted match {
      case Some(mutation) => Classification.Extracted(kind, mutation)
      case None            => Classification.Unverifiable(kind)
    }
  }

  // Any DSv2 connector implementing SupportsRowLevelOperations (Iceberg's
  // mechanism, confirmed empirically against a real Iceberg 1.11.0
  // session, not assumed - see this method's helpers for what was
  // confirmed). No reflection: RowLevelWrite/ReplaceData/WriteDelta and
  // RowLevelOperation are real, stable, public Spark connector-API types,
  // unlike Delta's proprietary internal classes.
  private def classifyIcebergStyle(rlw: RowLevelWrite): Classification = {
    // `rlw.operation.command()` (org.apache.spark.sql.connector.write.
    // RowLevelOperation.Command) is a direct, reliable way to know which
    // of MERGE/UPDATE/DELETE this plan represents - confirmed empirically
    // via a real probe (since deleted) capturing real ReplaceData/
    // WriteDelta plans for all three, both copy-on-write and
    // merge-on-read: `command()` matched the actual SQL statement in
    // every case. This is what lets classification be reliable even when
    // extraction (below) isn't.
    val kind = rlw.operation.command() match {
      case SparkRowLevelOperation.Command.MERGE  => Kind.Merge
      case SparkRowLevelOperation.Command.UPDATE => Kind.Update
      case SparkRowLevelOperation.Command.DELETE => Kind.Delete
    }
    // `RowLevelWrite.condition` (present on both ReplaceData and
    // WriteDelta, copy-on-write and merge-on-read alike - confirmed
    // empirically for all four combinations) is, per Spark's own
    // RewriteDeleteFromTable/RewriteUpdateTable/RewriteMergeIntoTable
    // source, exactly the original predicate: a MERGE's `ON` clause for
    // Kind.Merge, and the WHERE clause (or Literal.TrueLiteral if absent)
    // for Kind.Delete/Kind.Update - the same "not assumed" standard
    // Delta's own extraction above holds to, just confirmed by reading
    // Spark's source and a real probe instead of Delta's.
    val extracted = kind match {
      case Kind.Merge  => Some(ir.RowMutation(matchCondition = Some(SparkPlanAdapter.translateExprStandalone(rlw.condition))))
      case Kind.Delete => Some(ir.RowMutation(delete = deleteScopeOf(Some(rlw.condition))))
      case Kind.Update => updatedColumnsOfReplaceData(rlw).map(cols => ir.RowMutation(updatedColumns = cols))
    }
    extracted match {
      case Some(mutation) => Classification.Extracted(kind, mutation)
      case None            => Classification.Unverifiable(kind)
    }
  }

  /** Only reliably extractable for copy-on-write's `ReplaceData` — its
    * rewritten `query` is a `Project` where EVERY target column is
    * wrapped `Alias(If(matchCondition, assignedExpr, originalAttr), name)`,
    * confirmed empirically against a real Iceberg 1.11.0 session, not
    * assumed: `UPDATE t SET doubled = doubled + 1 WHERE id > 2` produced
    * exactly `if ((id > 2)) (doubled + 1) else doubled AS doubled`, and
    * an untouched column produced `if ((id > 2)) id else id AS id` — the
    * identical attribute on both branches. So a column was genuinely
    * reassigned iff its `If`'s true-branch isn't semantically identical
    * to its false-branch (which is always the original, pre-update
    * attribute — Spark's own rewrite, `RewriteUpdateTable.
    * buildReplaceDataUpdateProjection`, wraps *every* target column this
    * way, touched or not).
    *
    * Merge-on-read's `WriteDelta` rewrites to a structurally different
    * `Expand`-based plan (confirmed via the same probe) with no
    * equivalent per-column before/after pairing to compare — deliberately
    * not attempted. Returning `None` here (a documented gap, not a
    * guess) is what makes that combination correctly `Unverifiable`
    * rather than silently reporting zero changed columns — exactly the
    * silent-pass this module exists to avoid.
    */
  private def updatedColumnsOfReplaceData(rlw: RowLevelWrite): Option[List[String]] = rlw match {
    case rd: ReplaceData =>
      scala.util.Try {
        rd.query match {
          case Project(projectList, _) =>
            Some(
              projectList.zip(rd.originalTable.output).collect {
                case (Alias(If(_, assignedExpr, originalExpr), _), targetAttr)
                    if !assignedExpr.semanticEquals(originalExpr) =>
                  targetAttr.name
              }.toList
            )
          case _ => None
        }
      }.toOption.flatten
    case _ => None
  }

  // `DELETE FROM <v2-table> WHERE <predicate>` against a connector
  // implementing plain `SupportsDelete` — same shape
  // `WriteCommandSupport.deleteFromTable` already recognizes for `ir.Write`
  // purposes. Confirmed empirically (Spark 3.5.1's own parser,
  // AstBuilder.visitDeleteFromTable, not assumed): a bare `DELETE FROM t`
  // with no `WHERE` sets `condition` to `Literal.TrueLiteral`, never
  // `None` — DeleteFromTable has no `Option` here the way Delta's
  // DeleteCommand does, so "unconditional" is detected by comparing
  // against that literal rather than by absence. (See `classify` above
  // for where this case is matched.)
  private def deleteScopeOf(condition: Option[Expression]): ir.DeleteScope = condition match {
    case None                                             => ir.DeleteScope.Unconditional
    case Some(c) if c.semanticEquals(Literal.TrueLiteral) => ir.DeleteScope.Unconditional
    case Some(c)                                          => ir.DeleteScope.Conditional(SparkPlanAdapter.translateExprStandalone(c))
  }
}
