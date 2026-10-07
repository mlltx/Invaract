// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import org.scalatest.funsuite.AnyFunSuite

class CapabilitySpec extends AnyFunSuite {
  import CapabilityFixtures.yaml

  private def errors(text: String): List[String] = AdapterCapabilities.parse(text).left.getOrElse(fail("expected the declaration to be rejected"))

  // --- the vocabulary -------------------------------------------------------------------------

  test("every capability has a unique dotted id, a description, and a category the matrix knows") {
    val ids = Capability.all.map(_.id)
    assert(ids.distinct == ids)
    assert(ids.forall(id => id.matches("[a-zA-Z]+(\\.[a-zA-Z]+)+")), ids.filterNot(_.matches("[a-zA-Z]+(\\.[a-zA-Z]+)+")).toString)
    assert(Capability.all.forall(_.description.trim.nonEmpty))
    assert(Capability.all.forall(c => CapabilityCategory.all.contains(c.category)))
    assert(Capability.byId.size == Capability.all.size)
    assert(Capability.byId("rules.dml") == Capability.RulesDml)
  }

  test("capabilities are listed grouped by category, in the categories' order") {
    val order = CapabilityCategory.all.map(c => Capability.all.indexWhere(_.category == c))
    assert(order.forall(_ >= 0), "every category has at least one capability")
    assert(order == order.sorted)
    assert(Capability.all.map(_.category).distinct == CapabilityCategory.all)
  }

  test("exactly the capabilities that make a declared contract requirement checked are the blocking ones") {
    val blocking = Capability.all.filter(_.enforcesContract).map(_.id).toSet
    assert(
      blocking == Set(
        "check.inputExistence", "check.location", "check.schema", "check.nestedTypes", "check.format", "check.saveMode",
        "check.catalogRegistration", "rules.dml", "rules.planShape", "rules.custom", "analysis.staticDataQuality", "analysis.roleConsistency"
      )
    )
  }

  test("the vocabularies of statuses and enforcement points are fixed and look up by id") {
    assert(Support.all.map(_.id) == List("supported", "partial", "unsupported", "not-applicable"))
    assert(Support.byId("partial") == Support.Partial)
    assert(EnforcementPoint.all.map(_.id) == List("in-engine-blocking", "pre-submit-gate", "observe-only"))
    assert(EnforcementPoint.byId("observe-only") == EnforcementPoint.ObserveOnly)
    assert(EnforcementPoint.all.forall(_.description.nonEmpty))
  }

  // --- parse: accepting ------------------------------------------------------------------------

  test("a complete declaration parses, keeping the adapter, engine, enforcement point and every entry") {
    val text = yaml(
      adapter = "toy", engine = "Toy Engine 1.0", point = "observe-only", enforcementNote = Some("reports after the fact"),
      overrides = Map(
        "rules.dml" -> (("partial", Some("only DELETE"))),
        "check.catalogRegistration" -> (("unsupported", Some("no catalogs here"))),
        "read.streaming" -> (("not-applicable", Some("batch only")))
      )
    )
    val caps = AdapterCapabilities.parse(text).fold(e => fail(e.mkString("; ")), identity)
    assert(caps.adapter == "toy" && caps.engine == "Toy Engine 1.0")
    assert(caps.enforcement == EnforcementPoint.ObserveOnly && caps.enforcementNote.contains("reports after the fact"))
    assert(caps.entries.size == Capability.all.size)
    assert(caps.supportOf(Capability.RulesDml) == Support.Partial)
    assert(caps.entryOf(Capability.RulesDml).note.contains("only DELETE"))
    assert(caps.supportOf(Capability.CheckCatalogRegistration) == Support.Unsupported)
    assert(caps.supportOf(Capability.ReadStreaming) == Support.NotApplicable)
    assert(caps.supportOf(Capability.CheckSchema) == Support.Supported)
  }

  test("a supported capability may carry a note and a docs path") {
    val text = yaml().replace("  check.schema:\n    status: supported\n", "  check.schema:\n    status: supported\n    note: names follow the case rule\n    docs: SPARK_ADAPTER.md\n")
    val caps = AdapterCapabilities.parse(text).fold(e => fail(e.mkString("; ")), identity)
    assert(caps.entryOf(Capability.CheckSchema) == CapabilityEntry(Support.Supported, Some("names follow the case rule"), Some("SPARK_ADAPTER.md")))
  }

  // --- parse: rejecting - the point of the file --------------------------------------------------

  test("an undeclared capability is rejected by name - an undeclared gap is exactly what this prevents") {
    val errs = errors(yaml(omit = Set("rules.dml", "check.format")))
    assert(errs.size == 2)
    assert(errs.exists(e => e.contains("'rules.dml' is not declared") && e.contains("undeclared gap")))
    assert(errs.exists(_.contains("'check.format' is not declared")))
  }

  test("an unknown capability id is rejected, listing the known ones") {
    val errs = errors(yaml(extra = "  made.up:\n    status: supported\n"))
    assert(errs.exists(e => e.contains("unknown capability 'made.up'") && e.contains("read.batch")))
  }

  test("a status other than the four is rejected, and so is a missing one") {
    val errs = errors(yaml(overrides = Map("check.schema" -> (("mostly", None)))))
    assert(errs == List("capability 'check.schema' needs a 'status' of: supported, partial, unsupported, not-applicable"))
    val missing = errors(yaml().replace("  check.schema:\n    status: supported\n", "  check.schema:\n    note: forgot the status\n"))
    assert(missing.exists(_.contains("'check.schema' needs a 'status'")))
  }

  test("partial, unsupported and not-applicable each need a note saying why") {
    List("partial", "unsupported", "not-applicable").foreach { status =>
      val errs = errors(yaml(overrides = Map("rules.dml" -> ((status, None)))))
      assert(errs.size == 1 && errs.head.contains("'rules.dml' is " + status) && errs.head.contains("needs a 'note'"), status)
    }
  }

  test("a capability that is not a mapping is rejected") {
    val errs = errors(yaml().replace("  check.schema:\n    status: supported\n", "  check.schema: supported\n"))
    assert(errs.exists(_.contains("'check.schema' must be a mapping with a 'status'")))
  }

  test("adapter, engine, enforcement.point and capabilities are each required") {
    val errs = errors("adapter: ''\nengine: ' '\nenforcement:\n  point: somewhere\n")
    assert(errs.exists(_.contains("'adapter' is required")))
    assert(errs.exists(_.contains("'engine' is required")))
    assert(errs.exists(e => e.contains("'enforcement.point' is required") && e.contains("in-engine-blocking, pre-submit-gate, observe-only")))
    assert(errs.exists(_.contains("'capabilities' is required")))
    // no enforcement block at all is the same problem
    assert(errors("adapter: x\nengine: y\ncapabilities: {}\n").exists(_.contains("'enforcement.point' is required")))
  }

  test("every problem is reported at once, not just the first") {
    val errs = errors(yaml(adapter = "''", omit = Set("rules.dml"), overrides = Map("check.format" -> (("unsupported", None)))))
    assert(errs.size == 3)
  }

  test("text that is not a mapping, or not YAML at all, is rejected without throwing") {
    assert(errors("- a\n- list\n").head.contains("must be a YAML mapping"))
    assert(errors("just a string").head.contains("must be a YAML mapping"))
    assert(errors("a: [unclosed").head.startsWith("not valid YAML"))
  }

  // --- fromResource ------------------------------------------------------------------------------

  test("fromResource reads a bundled declaration, and reports an absent one as None") {
    val caps = AdapterCapabilities.fromResource("capabilities-toy-fixture.yaml", getClass.getClassLoader)
    assert(caps.exists(_.isRight))
    assert(caps.get.right.get.adapter == "toy")
    assert(AdapterCapabilities.fromResource("no-such-declaration.yaml", getClass.getClassLoader).isEmpty)
  }
}
