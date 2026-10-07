// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, ContractParser}
import com.invaract.contract.LogicalType.LongType
import com.invaract.ir.{ColumnRef, ColumnReference, DatasetRef, NamedExpr, Project, Read, Write}
import com.invaract.sparkadapter.notification.{ContractValidationEvent, TestNotificationSink}

import org.scalatest.funsuite.AnyFunSuite

class CapabilityCheckSpec extends AnyFunSuite {
  import CapabilityFixtures.parsed

  private val minimal: Contract = ContractParser.parse(
    """id: min
      |version: "1.0.0"
      |outputs:
      |  - name: out
      |    location: out/path
      |    schema:
      |      fields:
      |        - name: id
      |          type: long
      |""".stripMargin
  )

  private val rich: Contract = ContractParser.parse(
    """id: rich
      |version: "1.0.0"
      |inputs:
      |  - name: src
      |    location: in/path
      |    format: parquet
      |    schema:
      |      fields:
      |        - name: id
      |          type: long
      |        - name: tags
      |          type: array<string>
      |          sensitivityTags: [pii]
      |outputs:
      |  - name: out
      |    location: out/path
      |    saveMode: append
      |    catalog:
      |      required: true
      |    schema:
      |      fields:
      |        - name: id
      |          type: long
      |rules:
      |  - type: forbid_unconditional_delete
      |  - type: forbid_cross_join
      |customRuleTypes:
      |  my_rule: com.example.MyRule
      |""".stripMargin
  )

  private def ids(required: List[(Capability, String)]): List[String] = required.map(_._1.id)

  private def whyById(required: List[(Capability, String)]): Map[String, String] = required.map { case (c, r) => c.id -> r }.toMap

  // --- what a contract relies on ---------------------------------------------------------------

  test("required: a minimal contract relies only on location and schema checks") {
    assert(ids(CapabilityCheck.required(minimal, VerificationOptions())) == List("check.location", "check.schema"))
  }

  test("required: each declared feature maps to its capability, with a reason, in vocabulary order") {
    val required = CapabilityCheck.required(rich, VerificationOptions())
    assert(
      ids(required) == List(
        "check.inputExistence", "check.location", "check.schema", "check.nestedTypes", "check.format", "check.saveMode",
        "check.catalogRegistration", "rules.dml", "rules.planShape", "rules.custom", "analysis.sensitivityPropagation"
      )
    )
    val why = whyById(required)
    assert(why("check.inputExistence") == "it declares 1 input(s)")
    assert(why("check.nestedTypes") == "field 'tags' has a nested type")
    assert(why("check.format") == "a dataset declares a format")
    assert(why("check.saveMode") == "an output declares a save mode")
    assert(why("check.catalogRegistration") == "a dataset declares a catalog requirement")
    assert(why("rules.dml") == "it declares DML rule(s): forbid_unconditional_delete")
    assert(why("rules.planShape") == "it declares transformation-shape rule(s): forbid_cross_join")
    assert(why("rules.custom") == "it declares custom rule type(s): my_rule")
    assert(why("analysis.sensitivityPropagation") == "field 'tags' carries sensitivity tags")
  }

  test("required: the opt-in analyses are relied on only when their option is on") {
    val none = ids(CapabilityCheck.required(minimal, VerificationOptions()))
    assert(!none.exists(_.startsWith("analysis.")))
    val all = CapabilityCheck.required(minimal, VerificationOptions(staticDataQuality = true, roleConsistency = true, computeFingerprint = true))
    assert(ids(all).filter(_.startsWith("analysis.")) == List("analysis.staticDataQuality", "analysis.roleConsistency", "analysis.fingerprint"))
    assert(whyById(all)("analysis.staticDataQuality") == "staticDataQuality is enabled")
  }

  test("required: a nested type is a struct with properties, or any type written with '<'; a bare 'array' keyword is not") {
    def contractWith(field: String): Contract = ContractParser.parse(
      s"""id: n
         |version: "1.0.0"
         |outputs:
         |  - name: out
         |    location: o
         |    schema:
         |      fields:
         |$field
         |""".stripMargin
    )
    val struct = contractWith("        - name: addr\n          type: struct\n          properties:\n            - name: zip\n              type: string")
    assert(ids(CapabilityCheck.required(struct, VerificationOptions())).contains("check.nestedTypes"))
    val bare = contractWith("        - name: tags\n          type: array")
    assert(!ids(CapabilityCheck.required(bare, VerificationOptions())).contains("check.nestedTypes"))
    val map = contractWith("        - name: m\n          type: map<string,int>")
    assert(ids(CapabilityCheck.required(map, VerificationOptions())).contains("check.nestedTypes"))
    // a field nested inside a struct's properties is found too
    val deep = contractWith("        - name: a\n          type: struct\n          properties:\n            - name: b\n              type: array<int>\n              nullable: true")
    assert(whyById(CapabilityCheck.required(deep, VerificationOptions()))("check.nestedTypes") == "field 'a' has a nested type")
  }

  // --- what blocks -------------------------------------------------------------------------------

  test("violations: an adapter that supports everything the contract needs raises nothing") {
    assert(CapabilityCheck.violations(rich, VerificationOptions(), parsed()).isEmpty)
  }

  test("violations: partial never blocks, and neither does unsupported on a capability that does not enforce the contract") {
    val caps = parsed(
      Map(
        "check.catalogRegistration" -> (("partial", Some("only Hive"))),
        "analysis.sensitivityPropagation" -> (("unsupported", Some("no tags"))),
        "analysis.fingerprint" -> (("unsupported", Some("no fingerprints")))
      )
    )
    assert(CapabilityCheck.violations(rich, VerificationOptions(computeFingerprint = true), caps).isEmpty)
  }

  test("violations: unsupported on a capability the contract relies on is UNSUPPORTED_CONTRACT_FEATURE, one per capability, in order") {
    val caps = parsed(
      Map(
        "rules.dml" -> (("unsupported", Some("no row-level DML in this engine"))),
        "check.catalogRegistration" -> (("unsupported", Some("no catalogs"))),
        "check.nestedTypes" -> (("not-applicable", Some("flat schemas only")))
      ),
      adapter = "toy"
    )
    val vs = CapabilityCheck.violations(rich, VerificationOptions(), caps)
    assert(vs.map(_.violationType) == List.fill(2)(ViolationType.UnsupportedContractFeature))
    assert(vs.map(_.expected) == List(Some("check.catalogRegistration"), Some("rules.dml")))
    assert(vs.forall(_.actual.contains("unsupported by toy")))
    assert(vs.head.message == "the contract relies on 'check.catalogRegistration' (a dataset declares a catalog requirement), " +
      "but the 'toy' adapter declares it unsupported: no catalogs, so it would not be verified.")
    assert(vs.last.message.contains("DML rule(s): forbid_unconditional_delete") && vs.last.message.contains("no row-level DML in this engine"))
    assert(vs.head.remediation.contains("reference/engine-capabilities") && vs.head.remediation.contains("never passed as if it had been"))
  }

  test("violations: an unsupported capability the contract does not use is not raised") {
    val caps = parsed(Map("rules.dml" -> (("unsupported", Some("no DML"))), "check.format" -> (("unsupported", Some("no formats")))))
    assert(CapabilityCheck.violations(minimal, VerificationOptions(), caps).isEmpty)
  }

  test("violations: an opted-in analysis the adapter cannot do blocks only while the option is on") {
    val caps = parsed(Map("analysis.staticDataQuality" -> (("unsupported", Some("not implemented")))))
    assert(CapabilityCheck.violations(minimal, VerificationOptions(), caps).isEmpty)
    val vs = CapabilityCheck.violations(minimal, VerificationOptions(staticDataQuality = true), caps)
    assert(vs.map(_.expected) == List(Some("analysis.staticDataQuality")))
  }

  test("the violation names no reason when the adapter gave none") {
    val v = Violations.unsupportedContractFeature("x", Capability.CheckFormat, "a dataset declares a format", None)
    assert(v.message == "the contract relies on 'check.format' (a dataset declares a format), but the 'x' adapter declares it unsupported, so it would not be verified.")
  }

  // --- through the pipeline ------------------------------------------------------------------------

  private val catalogContract: Contract = ContractParser.parse(
    """id: cat
      |version: "1.0.0"
      |outputs:
      |  - name: out
      |    location: out/path
      |    schema:
      |      fields:
      |        - name: id
      |          type: long
      |          required: true
      |""".stripMargin
  )

  private val plan = Write(DatasetRef("out/path"), Project(Read(DatasetRef("in")), List(NamedExpr("id", ColumnReference(ColumnRef("id", Some("in")))))))
  private def write = CheckedWrite(plan, Nil, Cols().add("id", LongType, nullable = false), caseSensitive = false, None, Set.empty)

  test("pipeline: with no capabilities declared the check is skipped, exactly as before") {
    VerificationPipeline.verifyWrite(catalogContract, write, VerificationOptions())
    VerificationPipeline.verifyWrite(catalogContract, write, VerificationOptions(), capabilities = None)
  }

  test("pipeline: a contract that relies on an unsupported capability is rejected, with the violation first, and the event published") {
    val caps = parsed(Map("check.schema" -> (("unsupported", Some("this engine cannot see column types")))))
    val sink = new TestNotificationSink
    val ex = intercept[ContractViolationException] {
      VerificationPipeline.verifyWrite(catalogContract, write, VerificationOptions(), Some(sink), None, Some(caps))
    }
    assert(ex.result.violations.map(_.violationType) == List(ViolationType.UnsupportedContractFeature))
    assert(ex.getMessage.contains("[UNSUPPORTED_CONTRACT_FEATURE]") && ex.getMessage.contains("this engine cannot see column types"))
    assert(sink.events.collect { case e: ContractValidationEvent => e.status } == List("FAILED"))
  }

  test("pipeline: a fully supporting or partially supporting adapter lets a conforming write through") {
    VerificationPipeline.verifyWrite(catalogContract, write, VerificationOptions(), None, None, Some(parsed()))
    val partial = parsed(Map("check.schema" -> (("partial", Some("flat schemas only")))))
    VerificationPipeline.verifyWrite(catalogContract, write, VerificationOptions(), None, None, Some(partial))
  }

  test("pipeline: the capability violation comes before the structural ones, and does not hide them") {
    val caps = parsed(Map("check.schema" -> (("unsupported", Some("no types")))))
    val ex = intercept[ContractViolationException] {
      VerificationPipeline.verifyWrite(
        catalogContract, CheckedWrite(plan, Nil, Cols(), caseSensitive = false, None, Set.empty), VerificationOptions(), None, None, Some(caps)
      )
    }
    assert(ex.result.violations.map(_.violationType) == List(ViolationType.UnsupportedContractFeature, ViolationType.MissingOutputField))
  }
}
