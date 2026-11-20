// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.Locations
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.AnalysisContext
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisOnlyCommand, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SQLExecution

import java.util.concurrent.atomic.AtomicLong
import scala.util.Try

/** Which write a check is of (docs/SPARK_ADAPTER_HARDENING_PLAN.md, H-16), published as
  * `ContractValidationEvent.writeKey`. Spark analyzes a statement, then, while running it, the commands it runs inside
  * it: a `saveAsTable` is checked as the table's creation and again as the insert that fills it, a Delta
  * `CREATE TABLE ... AS SELECT` three times. Every check must still run (a nested write that fails is blocked), so the
  * events are not dropped; they carry one key, and a consumer counts writes by key.
  *
  * A check made while a checked write is executing, of a write to the same place, is of that write: Spark runs a
  * command inside `SQLExecution.withNewExecutionId`, which records the command's plan against the execution id it puts
  * on the thread (an inheritable local property), so the plan of the write a nested check runs inside is found from the
  * thread (and the outermost execution's, which Spark records too), and the key is kept on that plan (a tree node tag,
  * which lives and dies with the plan). Any other check - a
  * second write to the same place, a write a `foreachBatch` function makes in each batch - gets a new key. One more
  * repeat happens before anything runs: a V2 `CREATE TABLE ... AS SELECT` is checked as analyzed and again as the copy
  * the analyzer marks analyzed, both on the same thread (`markedTwin`).
  */
private[sparkadapter] object WriteKeys {

  /** The key of the write a checked plan is, with the names it answers to (`LocationMatching.namesOf`). */
  private val Tag = TreeNodeTag[(String, List[String])]("invaract.writeKey")

  private val counter = new AtomicLong()

  /** The key for the write `plan` (the plan the check rule was given) answering to `names`: the key of the checked write
    * whose execution this check runs inside, when that write answers to one of the same names; otherwise a new one,
    * `<canonical first name>#<n>`. Recorded on `plan`, for the checks of the writes it runs in turn. */
  def keyFor(plan: LogicalPlan, names: List[String]): String = {
    val canonical = names.map(Locations.canonical)
    val key = markedTwin(plan)
      .orElse(
        enclosing.iterator
          .flatMap(_.getTagValue(Tag))
          .collectFirst { case (k, enclosingNames) if enclosingNames.exists(canonical.contains) => k }
      )
      .getOrElse(s"${canonical.headOption.getOrElse("")}#${counter.incrementAndGet()}")
    plan.setTagValue(Tag, (key, canonical))
    plan match {
      case c: AnalysisOnlyCommand if !c.isAnalyzed => unmarked.set(Some((c, key)))
      case _                                       => ()
    }
    key
  }

  // The last command checked on this thread before the analyzer marked it analyzed, with its key.
  private val unmarked = new ThreadLocal[Option[(AnalysisOnlyCommand, String)]] {
    override def initialValue(): Option[(AnalysisOnlyCommand, String)] = None
  }

  /** A `CREATE TABLE ... AS SELECT` (any `AnalysisOnlyCommand`) is checked twice before it runs: as analyzed, then as
    * the copy the analyzer marks analyzed (`markAsAnalyzed`). The second is the same statement: the key of the first,
    * when `plan` is exactly the first marked. */
  private def markedTwin(plan: LogicalPlan): Option[String] = plan match {
    case c: AnalysisOnlyCommand if c.isAnalyzed =>
      val twin = unmarked.get.collect {
        case (first, key) if Try(first.markAsAnalyzed(AnalysisContext.get) == c).toOption.contains(true) => key
      }
      unmarked.set(None)
      twin
    case _ => None
  }

  /** The plans of the SQL executions the current thread is running inside: the innermost, then the outermost (a
    * connector can run a write inside an execution of its own, as Delta's `CREATE TABLE ... AS SELECT` does). */
  private def enclosing: List[LogicalPlan] =
    Try(SparkSession.active).toOption.toList.flatMap { session =>
      List(SQLExecution.EXECUTION_ID_KEY, SQLExecution.EXECUTION_ROOT_ID_KEY).flatMap { property =>
        Option(session.sparkContext.getLocalProperty(property))
          .flatMap(id => Try(Option(SQLExecution.getQueryExecution(id.toLong))).toOption.flatten)
          .map(_.logical)
      }
    }
}
