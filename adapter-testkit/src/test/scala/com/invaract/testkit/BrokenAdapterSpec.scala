// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.contract.{Contract, LogicalType}
import com.invaract.ir.{CatalogIdentity, ColumnRef, ColumnReference, Comparison, DatasetRef, Filter, Function, Join, Literal, NamedExpr, Plan, Project, Read, UnknownPlan, Write}
import com.invaract.verification.{CheckedWrite, VerificationOptions, ViolationType}

import org.scalatest.funsuite.AnyFunSuite

/** Proves the kit can fail: a scenario that no broken adapter can make diverge says nothing about a real one.
  *
  * Each variant below is the reference adapter with exactly one thing wrong - a filter lost in translation, a
  * format spelled the engine's own way, a boundary it cannot see through - which is the kind of mistake a new
  * adapter makes. Each lists the scenarios that must catch it. The suite fails if a variant slips past one of
  * them, and fails if a scenario is not the target of any variant (or explicitly exempt, with the reason), so a
  * scenario added later has to come with the mistake it is there to catch.
  */
class BrokenAdapterSpec extends AnyFunSuite {

  /** The reference adapter with one thing wrong: a plan rewrite, a change to what is handed to the pipeline, or a
    * change to what is reported back. */
  private class Variant(
      rewritePlan: (ScenarioJob, Plan) => Plan = (_, p) => p,
      rewriteChecked: CheckedWrite => CheckedWrite = identity,
      rewriteOutcome: (ScenarioJob, ScenarioOutcome) => ScenarioOutcome = (_, o) => o,
      boundaryTypes: Set[String] = Set.empty
  ) extends ReferenceAdapter() {
    override protected def checkedWrite(job: ScenarioJob): CheckedWrite = {
      val c = super.checkedWrite(job)
      rewriteChecked(c.copy(plan = rewritePlan(job, c.plan), lineageBoundaryTypes = boundaryTypes))
    }
    override def run(scenarioId: String, contract: Contract, job: ScenarioJob, options: VerificationOptions): ScenarioOutcome =
      rewriteOutcome(job, super.run(scenarioId, contract, job, options))
  }

  /** Rewrites every node bottom-up where `f` is defined (the node kinds the scenarios build). */
  private def rewrite(plan: Plan)(f: PartialFunction[Plan, Plan]): Plan = {
    val rebuilt: Plan = plan match {
      case w: Write   => w.copy(input = rewrite(w.input)(f))
      case p: Project => p.copy(input = rewrite(p.input)(f))
      case fl: Filter => fl.copy(input = rewrite(fl.input)(f))
      case j: Join    => j.copy(left = rewrite(j.left)(f), right = rewrite(j.right)(f))
      case other      => other
    }
    f.applyOrElse(rebuilt, identity[Plan])
  }

  private def onWrite(f: Write => Write): (ScenarioJob, Plan) => Plan = (_, plan) => rewrite(plan) { case w: Write => f(w) }

  private val allIds: Set[String] = Scenarios.all.map(_.id).toSet

  /** A variant, and the scenarios that must report it. */
  private case class Mistake(name: String, adapter: ConformanceAdapter, mustCatch: Set[String])

  private val rejecting: Set[String] = Scenarios.all.filter(_.expect.isInstanceOf[Expectation.Reject]).map(_.id).toSet
  private val passing: Set[String] = allIds -- rejecting

  // The four blunt mistakes: they prove the verdict plumbing, not that any one check is sensitive.
  private val blunt = List(
    Mistake(
      "lets every write through",
      new Variant(rewriteOutcome = (_, o) => o match {
        case r: ScenarioOutcome.Rejected => ScenarioOutcome.Passed(List("PASSED"), r.nonDeterministicColumns, r.dataQuality, r.roles, r.unverifiableInputs)
        case p => p
      }),
      rejecting
    ),
    Mistake(
      "blocks every write",
      new Variant(rewriteOutcome = (_, o) => o match {
        case p: ScenarioOutcome.Passed => ScenarioOutcome.Rejected(Set(ViolationType.UnverifiableWrite), List("FAILED"), p.nonDeterministicColumns, p.dataQuality, p.roles, p.unverifiableInputs)
        case r => r
      }),
      passing
    ),
    Mistake(
      "blocks for the wrong reason",
      new Variant(rewriteOutcome = (_, o) => o match {
        case r: ScenarioOutcome.Rejected => r.copy(violationTypes = Set("SOME_OTHER_VIOLATION"))
        case p => p
      }),
      rejecting - "invalid-contract" - "untranslatable-write-fails-closed" - "extended-type-not-produced"
    ),
    Mistake(
      "publishes no validation event",
      new Variant(rewriteOutcome = (_, o) => o match {
        case r: ScenarioOutcome.Rejected => r.copy(statuses = Nil)
        case p: ScenarioOutcome.Passed   => p.copy(statuses = Nil)
      }),
      allIds
    ),
    Mistake(
      "publishes events that do not say which engine produced them",
      new Variant(rewriteOutcome = (_, o) => o match {
        case r: ScenarioOutcome.Rejected => r.copy(eventEngines = r.eventEngines.map(_ => None))
        case p: ScenarioOutcome.Passed   => p.copy(eventEngines = p.eventEngines.map(_ => None))
      }),
      allIds
    ),
    Mistake(
      "publishes events naming another engine",
      new Variant(rewriteOutcome = (_, o) => o match {
        case r: ScenarioOutcome.Rejected => r.copy(eventEngines = r.eventEngines.map(_ => Some("some-other-engine")))
        case p: ScenarioOutcome.Passed   => p.copy(eventEngines = p.eventEngines.map(_ => Some("some-other-engine")))
      }),
      allIds
    )
  )

  // Each of these breaks one thing an adapter's engine-specific half does.
  private val specific = List(
    Mistake("loses the filters in translation", new Variant(rewritePlan = (_, p) => rewrite(p) { case Filter(in, _) => in }),
      Set("required-filter-column-present", "boundary-filter-is-seen-through")),
    Mistake("loses the joins in translation", new Variant(rewritePlan = (_, p) => rewrite(p) { case Join(l, _, _, _) => l }),
      Set("undeclared-input-rejected", "lineage-through-join-and-filter")),
    Mistake("writes somewhere other than it reports", new Variant(rewritePlan = onWrite(_.copy(dataset = DatasetRef("somewhere/else")))),
      Set("conforming-write", "output-format-matches", "output-save-mode-matches", "nested-type-matches")),
    Mistake("reports every write as append", new Variant(rewritePlan = onWrite(w => if (w.saveMode.isDefined) w.copy(saveMode = Some("append")) else w)),
      Set("output-save-mode-matches", "output-save-mode-mismatch")),
    Mistake("spells formats its own way", new Variant(rewritePlan = onWrite(w => w.copy(format = w.format.map(_.toUpperCase + "_FILE")))),
      Set("output-format-matches")),
    Mistake("reports every format as parquet", new Variant(rewritePlan = onWrite(w => w.copy(format = w.format.map(_ => "parquet")))),
      Set("output-format-mismatch")),
    Mistake("reports the usual output location whatever it wrote to", new Variant(rewritePlan = onWrite(_.copy(dataset = DatasetRef("out/report")))),
      Set("wrong-output-location")),
    Mistake("does not recognize a streaming source as a read",
      new Variant(rewritePlan = (job, p) => if (job.streaming) rewrite(p) { case _: Read => UnknownPlan("streaming source", "StreamingRelation") } else p),
      Set("streaming-write-is-checked-like-a-batch-write")),
    Mistake("reports a stream's sink as the usual output location",
      new Variant(rewritePlan = (job, p) => if (job.streaming) rewrite(p) { case w: Write => w.copy(dataset = DatasetRef("out/report")) } else p),
      Set("streaming-write-to-the-wrong-location")),
    Mistake("invents a filter nobody wrote",
      new Variant(rewritePlan = (_, p) => rewrite(p) {
        case Project(in, cols) => Project(Filter(in, Comparison(">", ColumnReference(ColumnRef("amount", None)), Literal(0L, "long"))), cols)
      }),
      Set("required-filter-column-missing")),
    Mistake("never reports a catalog identity", new Variant(rewritePlan = onWrite(_.copy(catalog = None))),
      Set("catalog-required-output-registered")),
    Mistake("reports a catalog identity for every write",
      new Variant(rewritePlan = onWrite(_.copy(catalog = Some(CatalogIdentity(table = Some("some_table")))))),
      Set("catalog-required-output-unregistered")),
    Mistake("spells the unique-id function its own way",
      new Variant(rewritePlan = (_, p) => rewrite(p) {
        case Project(in, cols) => Project(in, cols.map(c => c.expr match { case f: Function => c.copy(expr = f.copy(name = "native_uuid_v4")); case _ => c }))
      }),
      Set("fingerprint-flags-a-generated-id")),
    Mistake("loses what each column is derived from",
      new Variant(rewritePlan = (_, p) => rewrite(p) { case Project(in, cols) => Project(in, cols.map(c => NamedExpr(c.name, Literal(0L, "long")))) }),
      Set("lineage-passthrough-cast-and-constant", "lineage-through-join-and-filter", "sensitivity-follows-a-cast", "role-control-contributing-contradicts", "role-source-contributing-conforms")),
    Mistake("cannot classify a row-level change", new Variant(rewriteChecked = _.copy(rowMutation = None)),
      Set("unconditional-delete-forbidden")),
    Mistake("flattens nested types to strings",
      new Variant(rewriteChecked = c => c.copy(outputSchema = c.outputSchema.copy(fields = c.outputSchema.fields.map { f =>
        f.dataType match {
          case _: LogicalType.ArrayType => f.copy(dataType = LogicalType.StringType)
          case _                        => f
        }
      }))),
      Set("nested-type-matches")),
    Mistake("claims every output column cannot be null",
      new Variant(rewriteChecked = c => c.copy(outputSchema = c.outputSchema.copy(fields = c.outputSchema.fields.map(_.copy(nullable = false))))),
      Set("output-nullability-mismatch")),
    Mistake("does not report what its analyses found",
      new Variant(rewriteOutcome = (_, o) => o match {
        case p: ScenarioOutcome.Passed   => p.copy(dataQuality = Set.empty, roles = Map.empty)
        case r: ScenarioOutcome.Rejected => r.copy(dataQuality = Set.empty, roles = Map.empty)
      }),
      Set("static-data-quality-proves-a-constant", "static-data-quality-cannot-prove-a-passthrough", "static-data-quality-violation-blocks", "role-source-contributing-conforms", "role-control-contributing-contradicts")),
    Mistake("cannot see through a checkpoint or cache",
      new Variant(
        rewritePlan = (job, p) => if (job.boundary) rewrite(p) { case Project(_, cols) => Project(UnknownPlan("cached relation", "CachedRDD"), cols) } else p,
        rewriteOutcome = (job, o) => o,
        boundaryTypes = Set("CachedRDD")
      ),
      Set("boundary-filter-is-seen-through", "boundary-does-not-excuse-a-missing-input"))
  )

  // Scenarios that are a negative control (the happy path, or the default of an option) rather than the catch for a mistake.
  private val controls: Map[String, String] = Map(
    "undeclared-output-column-allowed-by-default" -> "the default of rejectUndeclaredFields: it is the control for undeclared-output-column-rejected",
    "fingerprint-flags-nothing-for-a-deterministic-job" -> "the control for the generated-id scenario",
    "output-type-mismatch" -> "an adapter that maps a cast wrongly is caught by the shared type check, which verification-core's own tests hold",
    "extended-type-not-produced" -> "holds the engine-neutral rule that a type an engine lacks is a mismatch; verification-core's own tests hold the rule",
    "missing-output-field" -> "holds the shared field-presence check; verification-core's own tests hold it",
    "declared-input-not-read" -> "holds the shared missing-input check; verification-core's own tests hold it",
    "undeclared-output-column-rejected" -> "holds the shared undeclared-field option; verification-core's own tests hold it",
    "nested-type-mismatch" -> "holds the shared structural comparison; verification-core's own tests hold it",
    "row-level-delete-checked-as-write" -> "an adapter that does not recognize a DELETE as a write fails the scenario's operation, not its verdict",
    "row-level-delete-own-table-is-not-an-input" -> "holds the shared in-place read rule; verification-core's own tests hold it",
    "filtered-delete-allowed" -> "the control for unconditional-delete-forbidden",
    "untranslatable-write-fails-closed" -> "an adapter that passes an operation it cannot translate skips the scenario's whole point; caught by the blunt mistake",
    "invalid-contract" -> "holds the shared contract validation before any job is looked at"
  )

  private def diverging(adapter: ConformanceAdapter): Set[String] =
    Conformance.evaluate(adapter).results.collect { case ScenarioResult(s, _: ScenarioVerdict.Diverges) => s.id }.toSet

  (blunt ++ specific).foreach { mistake =>
    test(s"the kit catches an adapter that ${mistake.name}") {
      val caught = diverging(mistake.adapter)
      val missed = mistake.mustCatch -- caught
      assert(missed.isEmpty, s"an adapter that ${mistake.name} got past: ${missed.toList.sorted.mkString(", ")}")
    }
  }

  test("a mistake does not make an unrelated scenario fail") {
    // The kit reports what is wrong, not everything: a lost filter does not fail the plain conforming write.
    val caught = diverging(specific.head.adapter)
    assert(!caught.contains("conforming-write"))
    assert(!caught.contains("wrong-output-location"))
  }

  test("every scenario is the catch for some mistake, or says why not") {
    val targeted = specific.flatMap(_.mustCatch).toSet
    val uncovered = allIds -- targeted -- controls.keySet
    assert(uncovered.isEmpty, s"scenarios with no mistake that targets them (add one to BrokenAdapterSpec, or list them as a control): ${uncovered.toList.sorted.mkString(", ")}")
    val stale = controls.keySet -- allIds
    assert(stale.isEmpty, s"controls naming no scenario: $stale")
    // An exempt scenario really is untargeted: otherwise the exemption is dead weight hiding a coverage fact.
    val needless = controls.keySet.intersect(targeted)
    assert(needless.isEmpty, s"scenarios exempted as controls that a mistake already targets: ${needless.toList.sorted.mkString(", ")}")
  }
}
