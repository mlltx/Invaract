// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.contract.{Contract, LogicalType}
import com.invaract.ir.{Aggregate, CatalogIdentity, ColumnRef, ColumnReference, Comparison, DatasetRef, Filter, Function, Join, JoinType, Literal, NamedExpr, Plan, Project, Read, UnknownPlan, Write}
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
      boundaryTypes: Set[String] = Set.empty,
      outputsWritten: Boolean = true
  ) extends ReferenceAdapter() {
    override protected def checksOutputsWritten: Boolean = outputsWritten
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
      case u: UnknownPlan => u.copy(children = u.children.map(rewrite(_)(f)))
      case a: Aggregate   => a.copy(input = rewrite(a.input)(f))
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
    Mistake("drops what is beneath a step it cannot translate",
      new Variant(rewritePlan = (_, p) => rewrite(p) { case u: UnknownPlan => u.copy(children = Nil) }),
      Set("unrecognized-step-keeps-the-reads-beneath-it-visible", "unrecognized-step-does-not-hide-an-undeclared-read")),
    Mistake("reports every join as having no condition",
      new Variant(rewritePlan = (_, p) => rewrite(p) { case j: Join => j.copy(condition = None) }),
      Set("forbid-cross-join-allows-a-keyed-join", "required-join-columns-present")),
    Mistake("reports a cartesian product as a keyed join",
      new Variant(rewritePlan = (_, p) => rewrite(p) {
        case j @ Join(_, _, JoinType.Cross, None) => j.copy(joinType = JoinType.Inner, condition = Some(Comparison("=", Literal(1L, "long"), Literal(1L, "long"))))
      }),
      Set("forbid-cross-join-blocks-a-cartesian-product")),
    Mistake("loses the aggregation in translation",
      new Variant(rewritePlan = (_, p) => rewrite(p) { case a: Aggregate => a.input }),
      Set("required-group-by-present")),
    Mistake("reports a grouping column under the name the output gives it",
      new Variant(rewritePlan = (job, p) => rewrite(p) {
        case a: Aggregate =>
          a.copy(groupBy = a.groupBy.map {
            case ColumnReference(ref) =>
              val alias = job.columns.collectFirst { case OutColumn(out, ColumnSource.FromInput(0, ref.name)) => out }.getOrElse(ref.name)
              ColumnReference(ref.copy(name = alias))
            case other => other
          })
      }),
      Set("required-group-by-wrong-keys")),
    Mistake("loses the joins in translation", new Variant(rewritePlan = (_, p) => rewrite(p) { case Join(l, _, _, _) => l }),
      Set("undeclared-input-rejected", "derived-from-rejects-a-read-of-another-outputs-source", "lineage-through-join-and-filter")),
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
    Mistake("does not refuse a job that never writes a declared output", new Variant(outputsWritten = false),
      Set("declared-output-never-written")),
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

  /** Why a scenario is not the catch for a mistake of its own. Every exemption points at something this suite can
    * check, so "verification-core's tests hold it" cannot outlive the test it names: a prose reason is a claim
    * nobody re-reads, and a deleted or ignored test would leave the scenario unguarded with the list still green.
    */
  private sealed trait Exemption { def reason: String }

  /** The happy path, or the default of an option, paired with a scenario that is itself guarded: targeted by a mistake,
    * or exempted for a reason that is not itself a pairing (a pairing with a pairing guards nothing). */
  private case class ControlFor(scenario: String, reason: String) extends Exemption

  /** The engine-neutral rule the scenario shows is checked by one of `verification-core`'s own tests: `file` (relative
    * to the repository root) declares a `test("<test>")`. */
  private case class HeldByCore(file: String, test: String, reason: String) extends Exemption

  /** The scenario is caught by one of the blunt mistakes above, which prove the verdict plumbing. */
  private case class CaughtBluntly(reason: String) extends Exemption

  /** The kit's own test (a ScalaTest suite on this classpath and a test in it) demonstrates the behaviour. */
  private case class HeldByKit(attestation: Attestation, reason: String) extends Exemption

  private val schemaChecker = "verification-core/src/test/scala/com/invaract/verification/SchemaCheckerSpec.scala"
  private val inputOutputChecker = "verification-core/src/test/scala/com/invaract/verification/InputOutputCheckerSpec.scala"
  private val pipeline = "verification-core/src/test/scala/com/invaract/verification/VerificationPipelineSpec.scala"

  // Scenarios that are a negative control (the happy path, or the default of an option) rather than the catch for a mistake.
  private val controls: Map[String, Exemption] = Map(
    "undeclared-output-column-allowed-by-default" -> ControlFor("undeclared-output-column-rejected", "the default of rejectUndeclaredFields"),
    "fingerprint-flags-nothing-for-a-deterministic-job" -> ControlFor("fingerprint-flags-a-generated-id", "the deterministic job a generated id is told apart from"),
    "filtered-delete-allowed" -> ControlFor("unconditional-delete-forbidden", "the delete the rule must not block"),
    "derived-from-excuses-an-input-the-output-is-not-built-from" -> ControlFor("derived-from-rejects-a-read-of-another-outputs-source", "the write the mapping must let through"),
    "derived-from-requires-every-source-of-the-output" -> HeldByCore(inputOutputChecker,
      "classifyUnread: no evidence => MISSING_INPUT; evidence => unverifiable instead; the derivedFrom nudge only for a derivedFrom-less output of a multi-output contract",
      "the shared rule that an output's own sources are what it must read"),
    "output-type-mismatch" -> HeldByCore(schemaChecker, "scalars: matching type and nullability is clean; a wrong scalar type reports the bare typeName on both sides",
      "an adapter that maps a cast wrongly is caught by the shared type check"),
    "extended-type-not-produced" -> HeldByCore(schemaChecker, "an engine type with no logical equivalent never satisfies a declared standard type",
      "a type an engine lacks is a mismatch, whatever the engine"),
    "missing-output-field" -> HeldByCore(schemaChecker, "a missing field is only flagged when required; nullability is one-directional", "the shared field-presence check"),
    "declared-input-not-read" -> HeldByCore(inputOutputChecker, "notRead: the declared inputs no read location matches, in declaration order", "the shared missing-input check"),
    "undeclared-output-column-rejected" -> HeldByCore(schemaChecker, "undeclared columns are reported only under rejectUndeclaredFields, in schema order", "the shared undeclared-field option"),
    "nested-type-mismatch" -> HeldByCore(schemaChecker, "struct properties: a nested type mismatch is reported with the dotted path", "the shared structural comparison"),
    "row-level-delete-own-table-is-not-an-input" -> HeldByCore(pipeline, "DML: the table an in-place change reads is not an undeclared input", "the shared in-place read rule"),
    "invalid-contract" -> HeldByCore(pipeline, "verifyWrite: an invalid contract is rejected BEFORE the adapter is asked to build the write", "contract validation before any job is looked at"),
    "required-join-columns-missing" -> CaughtBluntly("an adapter that lets a write through unchecked passes a join that is not on the required column"),
    "untranslatable-write-fails-closed" -> CaughtBluntly("an adapter that passes an operation it cannot translate skips the scenario's whole point"),
    "row-level-delete-checked-as-write" -> HeldByKit(
      Attestation("com.invaract.testkit.ConformanceKitSpec", "a scenario that needs something the engine does not have is skipped with the adapter's own reason, not failed"),
      "an adapter that does not recognize a DELETE as a write fails the scenario's operation, not its verdict"
    )
  )

  /** The scenarios something guards: those a mistake targets, and those exempted for a reason that points elsewhere. */
  private def guardedScenarios(targeted: Set[String]): Set[String] =
    targeted ++ controls.collect { case (id, e) if !e.isInstanceOf[ControlFor] => id }

  /** The repository root, found from where the tests run (`modules.json` marks it). */
  private lazy val repositoryRoot: java.io.File = {
    var dir: java.io.File = new java.io.File(sys.props("user.dir")).getAbsoluteFile
    while (dir != null && !new java.io.File(dir, "modules.json").exists) dir = dir.getParentFile
    require(dir != null, s"no modules.json above ${sys.props("user.dir")}: the repository root was not found")
    dir
  }

  /** What is wrong with an exemption, or empty when what it points at is there. */
  private def problemWith(scenario: String, exemption: Exemption, guarded: Set[String], bluntlyCaught: Set[String]): List[String] = exemption match {
    case ControlFor(other, _) =>
      if (!allIds.contains(other)) List(s"$scenario is a control for '$other', which is not a scenario")
      else if (!guarded.contains(other)) List(s"$scenario is a control for '$other', which nothing guards: no mistake targets it and it has no exemption of its own")
      else Nil
    case CaughtBluntly(_) =>
      if (bluntlyCaught.contains(scenario)) Nil else List(s"$scenario says a blunt mistake catches it, but none lists it")
    case HeldByKit(attestation, _) =>
      AttestedClaims.exists(attestation, getClass.getClassLoader).left.toOption.map(why => s"$scenario: $why").toList
    case HeldByCore(file, test, _) =>
      val path = new java.io.File(repositoryRoot, file)
      if (!path.exists) List(s"$scenario is held by $file, which does not exist")
      else {
        val source = scala.io.Source.fromFile(path, "UTF-8")
        // closed, or Windows cannot delete the file afterwards
        val declared = try source.getLines().exists(_.trim.startsWith(s"""test("$test""")) finally source.close()
        if (declared) Nil else List(s"""$scenario is held by $file, which declares no test("$test")""")
      }
  }

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
    val bluntlyCaught = blunt.flatMap(_.mustCatch).toSet
    val problems = controls.toList.sortBy(_._1).flatMap { case (id, e) => problemWith(id, e, guardedScenarios(targeted), bluntlyCaught) }
    assert(problems.isEmpty, s"exemptions that point at nothing:\n${problems.mkString("\n")}")
    assert(controls.values.forall(_.reason.trim.nonEmpty))
    // An exempt scenario really is untargeted: otherwise the exemption is dead weight hiding a coverage fact.
    val needless = controls.keySet.intersect(targeted)
    assert(needless.isEmpty, s"scenarios exempted as controls that a mistake already targets: ${needless.toList.sorted.mkString(", ")}")
  }

  // --- the exemptions are checked, so a dangling one is caught ------------------------------------------------

  private val targetedByMistakes = specific.flatMap(_.mustCatch).toSet
  private val bluntTargets = blunt.flatMap(_.mustCatch).toSet
  private def problems(e: Exemption, scenario: String = "some-scenario"): List[String] = problemWith(scenario, e, guardedScenarios(targetedByMistakes), bluntTargets)

  test("an exemption held by a verification-core test is accepted only while that file declares that test") {
    val real = controls.collectFirst { case (_, h: HeldByCore) => h }.get
    assert(problems(real) == Nil)
    assert(problems(real.copy(test = "a test that was renamed")).exists(_.contains("declares no test")))
    assert(problems(real.copy(file = "verification-core/src/test/scala/Nope.scala")).exists(_.contains("does not exist")))
  }

  test("a test that is ignored, or named only inside another test's body, does not hold an exemption") {
    val real = controls.collectFirst { case (_, h: HeldByCore) => h }.get
    // inside the repository (not the system temp directory, which can be on another drive), so the path is relative to the root
    val dir = repositoryRoot.toPath.resolve("adapter-testkit").resolve("target").resolve("exemption-fixture")
    java.nio.file.Files.createDirectories(dir)
    val file = dir.resolve("Spec.scala")
    java.nio.file.Files.write(file, s"""  ignore("the ignored one") { }\n  // test("a commented one")\n""".getBytes("UTF-8"))
    try {
      val relative = repositoryRoot.toPath.relativize(file).toString
      assert(problems(real.copy(file = relative, test = "the ignored one")).exists(_.contains("declares no test")))
      assert(problems(real.copy(file = relative, test = "a commented one")).exists(_.contains("declares no test")))
    } finally java.nio.file.Files.deleteIfExists(file)
  }

  test("a control must name a scenario that exists and that a mistake targets") {
    val targetedOne = targetedByMistakes.head
    assert(problems(ControlFor(targetedOne, "x")) == Nil)
    assert(problems(ControlFor("no-such-scenario", "x")).exists(_.contains("not a scenario")))
    val unguarded = (allIds -- guardedScenarios(targetedByMistakes)).head
    assert(problems(ControlFor(unguarded, "x")).exists(_.contains("nothing guards")))
    // a scenario exempted for a reason of its own guards its pair; one that is only a pairing does not
    assert(problems(ControlFor("undeclared-output-column-rejected", "x")) == Nil)
    assert(problems(ControlFor("undeclared-output-column-allowed-by-default", "x")).exists(_.contains("nothing guards")))
  }

  test("a 'caught bluntly' exemption holds only for a scenario a blunt mistake lists") {
    assert(problems(CaughtBluntly("x"), bluntTargets.head) == Nil)
    assert(problems(CaughtBluntly("x"), "not-listed-anywhere").exists(_.contains("none lists it")))
  }

  test("an exemption held by a kit test is accepted only while that suite has that live test") {
    val real = controls.collectFirst { case (_, h: HeldByKit) => h }.get
    assert(problems(real) == Nil)
    assert(problems(real.copy(attestation = real.attestation.copy(test = "no such test"))).nonEmpty)
    assert(problems(real.copy(attestation = real.attestation.copy(suite = "com.invaract.testkit.NoSuchSuite"))).nonEmpty)
  }
}
