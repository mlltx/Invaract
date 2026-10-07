// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, ContractParser, LogicalSchema}
import com.invaract.contract.LogicalType.{IntegerType, LongType}
import com.invaract.ir.{ColumnRef, ColumnReference, DatasetRef, DeleteScope, NamedExpr, Plan, Project, Read, RowMutation, UnknownPlan, Write}
import com.invaract.sparkadapter.notification.{ContractValidationEvent, TestNotificationSink}

import org.scalatest.funsuite.AnyFunSuite

/** `VerificationPipeline` is what every engine adapter calls once it has translated an operation, so
  * it is tested here with hand-built `ir` plans and `LogicalSchema`s - no engine at all. That is the
  * point of the SPI: if this passes, nothing Spark-shaped was needed to get a verdict, a published
  * event and a rejection. (`spark-adapter`'s own specs prove the same path end to end through a real
  * Spark job.)
  */
class VerificationPipelineSpec extends AnyFunSuite {

  private val contract: Contract = ContractParser.parse(
    """id: pipe
      |version: "1.0.0"
      |inputs:
      |  - name: src
      |    location: in/orders
      |    type: SOURCE
      |    schema:
      |      fields:
      |        - name: id
      |          type: long
      |          required: true
      |        - name: amount
      |          type: integer
      |outputs:
      |  - name: out
      |    location: out/report
      |    type: DATA_ASSET
      |    derivedFrom: [src]
      |    schema:
      |      fields:
      |        - name: id
      |          type: long
      |          required: true
      |        - name: total
      |          type: integer
      |          required: true
      |""".stripMargin
  )

  private val orders = "in/orders"

  private def col(name: String) = ColumnReference(ColumnRef(name, Some(orders)))

  private val goodPlan: Plan =
    Write(DatasetRef("out/report"), Project(Read(DatasetRef(orders)), List(NamedExpr("id", col("id")), NamedExpr("total", col("amount")))))

  private val inputSchema = Cols().add("id", LongType, nullable = false).add("amount", IntegerType)
  // A field declared `required: true` is non-nullable unless it says otherwise.
  private val goodOutput = Cols().add("id", LongType, nullable = false).add("total", IntegerType, nullable = false)

  private def checked(
      plan: Plan = goodPlan,
      output: LogicalSchema = goodOutput,
      rowMutation: Option[MutationClassification] = None,
      boundaries: Set[String] = Set.empty,
      notes: List[String] = Nil
  ) = CheckedWrite(plan, List(orders -> inputSchema.toLogical), output, caseSensitive = false, rowMutation, boundaries, notes)

  private def validationEvents(sink: TestNotificationSink): List[ContractValidationEvent] =
    sink.events.collect { case e: ContractValidationEvent => e }

  // --- verifyWrite -----------------------------------------------------------------------------

  test("verifyWrite: a conforming write passes and publishes a PASSED event naming the contract") {
    val sink = new TestNotificationSink
    VerificationPipeline.verifyWrite(contract, checked(), VerificationOptions(), Some(sink), Some("app-1"))
    val events = validationEvents(sink)
    assert(events.map(_.status) == List("PASSED"))
    assert(events.head.contract == "pipe@1.0.0")
    assert(events.head.violations.isEmpty)
    assert(events.head.applicationId.contains("app-1"))
  }

  test("verifyWrite: with no sink a passing write just returns, and a failing one still throws") {
    VerificationPipeline.verifyWrite(contract, checked(), VerificationOptions())
    intercept[ContractViolationException] {
      VerificationPipeline.verifyWrite(contract, checked(output = Cols().add("id", LongType, nullable = false)), VerificationOptions())
    }
  }

  test("verifyWrite: a violation throws ContractViolationException, publishes FAILED first, and explains itself") {
    val sink = new TestNotificationSink
    val ex = intercept[ContractViolationException] {
      VerificationPipeline.verifyWrite(contract, checked(output = Cols().add("id", LongType, nullable = false)), VerificationOptions(), Some(sink))
    }
    assert(ex.result.violations.map(_.violationType) == List(ViolationType.MissingOutputField))
    assert(validationEvents(sink).map(_.status) == List("FAILED"))
    // The four things a developer reading only this text needs.
    val text = ex.getMessage
    assert(text.contains("Contract violation: 'pipe@1.0.0' rejected this transformation. Write aborted."))
    assert(text.contains("What the contract expects:"))
    assert(text.contains("input  'src' at in/orders: id: long, amount: integer (optional)"))
    assert(text.contains("output 'out' at out/report (derived from src): id: long, total: integer"))
    assert(text.contains("What the plan contains:") && text.contains("Write("))
    assert(text.contains("Why it violates the contract (1 violation):"))
    assert(text.contains("1. [MISSING_OUTPUT_FIELD]"))
    assert(text.contains("How to correct it:"))
    assert(!text.contains("Fingerprints"))
  }

  test("verifyWrite: more than one violation is reported as 'violations', numbered in order") {
    val ex = intercept[ContractViolationException] {
      VerificationPipeline.verifyWrite(contract, checked(output = Cols()), VerificationOptions())
    }
    assert(ex.result.violations.size == 2)
    assert(ex.getMessage.contains("Why it violates the contract (2 violations):"))
    assert(ex.getMessage.contains("2. [MISSING_OUTPUT_FIELD]"))
  }

  test("verifyWrite: an invalid contract is rejected BEFORE the adapter is asked to build the write") {
    val invalid = ContractParser.parse("id: bad\nversion: \"1.0.0\"\n") // no outputs
    var built = false
    val sink = new TestNotificationSink
    val ex = intercept[ContractViolationException] {
      VerificationPipeline.verifyWrite(invalid, { built = true; checked() }, VerificationOptions(), Some(sink))
    }
    assert(!built, "the write must not be built (translated, schemas gathered) for a contract that is already invalid")
    assert(ex.result.violations.forall(_.violationType == ViolationType.InvalidContract))
    assert(ex.getMessage.contains("no transformation plan exists yet - contract validation failed before any plan was checked"))
    assert(validationEvents(sink).map(_.status) == List("FAILED"))
  }

  test("verifyWrite: a valid contract's write is built exactly once") {
    var built = 0
    VerificationPipeline.verifyWrite(contract, { built += 1; checked() }, VerificationOptions())
    assert(built == 1)
  }

  test("verifyWrite: a customRuleTypes entry naming a class that does not resolve is an INVALID_CONTRACT rejection") {
    val withBadRule = ContractParser.parse(
      """id: pipe
        |version: "1.0.0"
        |outputs:
        |  - name: out
        |    location: out/report
        |    schema:
        |      fields:
        |        - name: id
        |          type: long
        |rules:
        |  - type: my_rule
        |customRuleTypes:
        |  my_rule: com.example.DoesNotExist
        |""".stripMargin
    )
    val ex = intercept[ContractViolationException] {
      VerificationPipeline.verifyWrite(withBadRule, checked(), VerificationOptions())
    }
    assert(ex.result.violations.exists(v => v.violationType == ViolationType.InvalidContract && v.message.contains("com.example.DoesNotExist")))
  }

  // --- DML classification ----------------------------------------------------------------------

  private val forbidDelete = contract.copy(rules = List(com.invaract.contract.ContractRule("forbid_unconditional_delete", Map.empty)))

  test("DML: an Extracted unconditional DELETE under forbid_unconditional_delete is a violation about this write's location") {
    val unconditional = Some(MutationClassification.Extracted(MutationKind.Delete, RowMutation(delete = DeleteScope.Unconditional)))
    val ex = intercept[ContractViolationException] {
      VerificationPipeline.verifyWrite(forbidDelete, checked(rowMutation = unconditional), VerificationOptions())
    }
    val v = ex.result.violations.find(_.violationType == ViolationType.RuleUnconditionalDelete).get
    assert(v.location.contains("out/report"))
  }

  test("DML: an Unverifiable DELETE fails closed only when a declared rule is about DELETE") {
    val unverifiable = Some(MutationClassification.Unverifiable(MutationKind.Delete))
    val ex = intercept[ContractViolationException] {
      VerificationPipeline.verifyWrite(forbidDelete, checked(rowMutation = unverifiable), VerificationOptions())
    }
    assert(ex.result.violations.map(_.violationType).contains(ViolationType.RuleUnverifiableDml))
    assert(ex.result.violations.find(_.violationType == ViolationType.RuleUnverifiableDml).get.message.contains("DELETE"))
    // The same classification under a contract with no rule about DELETE is nothing to fail closed over.
    VerificationPipeline.verifyWrite(contract, checked(rowMutation = unverifiable), VerificationOptions())
    // And a rule about another kind does not make an Unverifiable DELETE a problem.
    val mergeOnly = contract.copy(rules = List(com.invaract.contract.ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("id")))))
    VerificationPipeline.verifyWrite(mergeOnly, checked(rowMutation = unverifiable), VerificationOptions())
  }

  test("DML: names each unverifiable kind (MERGE, UPDATE) in the violation") {
    val rules = List(
      MutationKind.Merge -> com.invaract.contract.ContractRule("merge_condition", Map("columns" -> java.util.Arrays.asList("id"))),
      MutationKind.Update -> com.invaract.contract.ContractRule("allowed_update_columns", Map("columns" -> java.util.Arrays.asList("total")))
    )
    rules.foreach { case (kind, rule) =>
      val ex = intercept[ContractViolationException] {
        VerificationPipeline.verifyWrite(contract.copy(rules = List(rule)), checked(rowMutation = Some(MutationClassification.Unverifiable(kind))), VerificationOptions())
      }
      assert(ex.result.violations.exists(v => v.violationType == ViolationType.RuleUnverifiableDml && v.message.contains(kind.toString.toUpperCase)), kind)
    }
  }

  // --- lineage boundaries ----------------------------------------------------------------------

  private val behindBoundary: Plan =
    Write(
      DatasetRef("out/report"),
      Project(UnknownPlan("a checkpoint", "SomeBoundary"), List(NamedExpr("id", col("id")), NamedExpr("total", col("amount"))))
    )

  test("lineage boundary: an unread declared input is only 'unverifiable' when the engine declares that node type a boundary") {
    val sink = new TestNotificationSink
    VerificationPipeline.verifyWrite(contract, checked(plan = behindBoundary, boundaries = Set("SomeBoundary")), VerificationOptions(), Some(sink))
    val event = validationEvents(sink).head
    assert(event.status == "PASSED")
    assert(event.unverifiableInputs.map(_.inputName) == List("src"))
    assert(event.unverifiableInputs.head.unknownNodeTypes == List("SomeBoundary"))

    val ex = intercept[ContractViolationException] {
      VerificationPipeline.verifyWrite(contract, checked(plan = behindBoundary), VerificationOptions())
    }
    assert(ex.result.violations.map(_.violationType).contains(ViolationType.MissingInput))
  }

  // --- fingerprint, data quality, role consistency ---------------------------------------------

  test("computeFingerprint: the published event and the explanation carry the fingerprint; off by default") {
    val sink = new TestNotificationSink
    VerificationPipeline.verifyWrite(contract, checked(), VerificationOptions(computeFingerprint = true), Some(sink))
    assert(validationEvents(sink).head.fingerprints.isDefined)

    val off = new TestNotificationSink
    VerificationPipeline.verifyWrite(contract, checked(), VerificationOptions(), Some(off))
    assert(validationEvents(off).head.fingerprints.isEmpty)

    val ex = intercept[ContractViolationException] {
      VerificationPipeline.verifyWrite(contract, checked(output = Cols()), VerificationOptions(computeFingerprint = true))
    }
    assert(ex.result.fingerprints.isDefined)
    assert(ex.getMessage.contains("Fingerprints (v"))
    val fp = ex.result.fingerprints.get
    assert(ex.getMessage.contains(s"  overall: ${fp.overall.value}\n"))
    assert(fp.outputs.nonEmpty)
    assert(ex.getMessage.contains("  outputs:\n"))
    fp.outputs.foreach { case (name, output) => assert(ex.getMessage.contains(s"    $name: ${output.combined.value}\n"), name) }
  }

  test("explain: a fingerprint with no per-output entries prints only the overall hash") {
    val noOutputs = com.invaract.fingerprint.TransformationFingerprinter.fingerprint(Write(DatasetRef("out/report"), Read(DatasetRef(orders))), None)
    val result = VerificationResult.of("pipe@1.0.0", List(Violations.missingInput(contract.inputs.head, nudgeTowardDerivedFrom = false)), fingerprints = Some(noOutputs))
    val text = VerificationPipeline.explain(contract, goodPlan, result)
    assert(text.contains(s"  overall: ${noOutputs.overall.value}\n"))
    if (noOutputs.outputs.isEmpty) assert(!text.contains("  outputs:")) else assert(text.contains("  outputs:\n"))
  }

  test("computeFingerprint: a boundary or a resolution note is disclosed (logged) but never changes the verdict") {
    VerificationPipeline.verifyWrite(contract, checked(plan = behindBoundary, boundaries = Set("SomeBoundary")), VerificationOptions(computeFingerprint = true))
    VerificationPipeline.verifyWrite(contract, checked(notes = List("picked the most recent plan")), VerificationOptions(computeFingerprint = true))
  }

  test("staticDataQuality and roleConsistency each run only when their own option is on, and attach their own results") {
    def run(options: VerificationOptions): ContractValidationEvent = {
      val sink = new TestNotificationSink
      VerificationPipeline.verifyWrite(contract, checked(), options, Some(sink))
      validationEvents(sink).head
    }
    val neither = run(VerificationOptions())
    assert(neither.dataQuality.isEmpty && neither.roleConformance.isEmpty)

    val dqOnly = run(VerificationOptions(staticDataQuality = true))
    assert(dqOnly.dataQuality.nonEmpty && dqOnly.roleConformance.isEmpty)

    val roleOnly = run(VerificationOptions(roleConsistency = true))
    assert(roleOnly.roleConformance.nonEmpty && roleOnly.dataQuality.isEmpty)

    val both = run(VerificationOptions(staticDataQuality = true, roleConsistency = true))
    assert(both.dataQuality.nonEmpty && both.roleConformance.nonEmpty)
    assert(both.status == "PASSED")
  }

  test("unresolvedBoundaryTypes: only declared boundary node types, each once, in first-seen order") {
    val plans = List(UnknownPlan("a", "B2"), UnknownPlan("b", "Other"), UnknownPlan("c", "B1"), UnknownPlan("d", "B2"))
    assert(VerificationPipeline.unresolvedBoundaryTypes(plans, Set("B1", "B2")) == List("B2", "B1"))
    assert(VerificationPipeline.unresolvedBoundaryTypes(plans, Set.empty).isEmpty)
    assert(VerificationPipeline.unresolvedBoundaryTypes(Nil, Set("B1")).isEmpty)
  }

  test("fingerprintCaveat: nothing to disclose is None; each reason, alone and together, is named in the text") {
    val lead = "computeFingerprint: this transformation's fingerprint is stable and deterministic for this exact plan " +
      "but may not reflect everything upstream of a lineage boundary (checkpoint/cache)"
    assert(VerificationPipeline.fingerprintCaveat(Nil, Nil).isEmpty)
    assert(VerificationPipeline.fingerprintCaveat(List("A", "B"), Nil).contains(lead + "; unresolved boundary: A, B"))
    assert(VerificationPipeline.fingerprintCaveat(Nil, List("n1", "n2", "n1")).contains(lead + "; n1 / n2"))
    assert(VerificationPipeline.fingerprintCaveat(List("A"), List("n1")).contains(lead + "; unresolved boundary: A; n1"))
  }

  // --- verifyStateChange -----------------------------------------------------------------------

  test("verifyStateChange: an operation on a location the contract doesn't declare is not this contract's concern") {
    val sink = new TestNotificationSink
    VerificationPipeline.verifyStateChange(contract, "CALL x(...)", "somewhere/else", Cols(), caseSensitive = false, VerificationOptions(), Some(sink))
    assert(validationEvents(sink).map(_.status) == List("PASSED"))
  }

  test("verifyStateChange: the resulting schema at a declared location is checked, and the rejection names the operation") {
    val sink = new TestNotificationSink
    val ex = intercept[ContractViolationException] {
      VerificationPipeline.verifyStateChange(
        contract, "CALL rollback_to_snapshot(...) targeting 'out/report'", "out/report", Cols().add("id", LongType, nullable = false), caseSensitive = false, VerificationOptions(), Some(sink)
      )
    }
    assert(ex.result.violations.map(_.violationType) == List(ViolationType.MissingOutputField))
    assert(ex.getMessage.contains("UnknownPlan(CALL rollback_to_snapshot(...) targeting 'out/report')"))
    assert(validationEvents(sink).map(_.status) == List("FAILED"))
    // a conforming resulting schema passes
    VerificationPipeline.verifyStateChange(contract, "CALL x(...)", "out/report", goodOutput, caseSensitive = false, VerificationOptions())
  }

  test("verifyStateChange: an invalid contract is rejected before anything else") {
    val invalid = ContractParser.parse("id: bad\nversion: \"1.0.0\"\n")
    val ex = intercept[ContractViolationException] {
      VerificationPipeline.verifyStateChange(invalid, "CALL x(...)", "out/report", goodOutput, caseSensitive = false, VerificationOptions())
    }
    assert(ex.result.violations.forall(_.violationType == ViolationType.InvalidContract))
  }

  test("verifyStateChange honours case sensitivity of the resulting schema's column names") {
    val upper = Cols().add("ID", LongType, nullable = false).add("TOTAL", IntegerType, nullable = false)
    VerificationPipeline.verifyStateChange(contract, "CALL x(...)", "out/report", upper, caseSensitive = false, VerificationOptions())
    intercept[ContractViolationException] {
      VerificationPipeline.verifyStateChange(contract, "CALL x(...)", "out/report", upper, caseSensitive = true, VerificationOptions())
    }
  }

  // --- rejectUnverifiableWrite -----------------------------------------------------------------

  test("rejectUnverifiableWrite: always throws UNVERIFIABLE_WRITE naming the operation, after publishing FAILED") {
    val sink = new TestNotificationSink
    val ex = intercept[ContractViolationException] {
      VerificationPipeline.rejectUnverifiableWrite(contract, "MergeIntoCommand", UnknownPlan("a merge"), Some(sink), Some("app-2"))
    }
    assert(ex.result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite))
    assert(ex.result.violations.head.message.contains("MergeIntoCommand"))
    assert(ex.getMessage.contains("UnknownPlan(a merge)"))
    assert(validationEvents(sink).map(_.status) == List("FAILED"))
    assert(validationEvents(sink).head.applicationId.contains("app-2"))
    // and without a sink it still throws
    intercept[ContractViolationException] {
      VerificationPipeline.rejectUnverifiableWrite(contract, "X", UnknownPlan("y"))
    }
  }

  // --- publishValidation -----------------------------------------------------------------------

  test("publishValidation carries the contract's extensions as event metadata and is a no-op without a sink") {
    val withMeta = contract.copy(extensions = Map("team" -> "payments"))
    val sink = new TestNotificationSink
    VerificationPipeline.publishValidation(withMeta, VerificationResult.of("pipe@1.0.0", Nil), Some(sink), None)
    assert(validationEvents(sink).head.metadata == Map("team" -> "payments"))
    VerificationPipeline.publishValidation(withMeta, VerificationResult.of("pipe@1.0.0", Nil), None, None)
  }
}
