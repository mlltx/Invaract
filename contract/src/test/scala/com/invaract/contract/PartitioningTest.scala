// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import org.scalatest.funsuite.AnyFunSuite

/** A dataset's partition keys (ODCS's `partitioned` and `partitionKeyPosition`) and `partitionOverwriteMode`: parsed,
  * written back, validated, and classified when they change. */
class PartitioningTest extends AnyFunSuite {

  private def contract(fields: String, extras: String = ""): Contract =
    ContractParser.parse(
      s"""id: parts
         |version: "1.0.0"
         |outputs:
         |  - name: report
         |    location: out/report
         |$extras    schema:
         |      fields:
         |$fields
         |""".stripMargin
    )

  private def output(fields: String, extras: String = ""): Dataset = contract(fields, extras).outputs.head

  private def errors(c: Contract): List[String] = ContractValidator.validate(c).issues.filter(_.severity == ValidationSeverity.Error).map(_.message)
  private def warnings(c: Contract): List[String] = ContractValidator.validate(c).issues.filter(_.severity == ValidationSeverity.Warning).map(_.message)

  private val regionDay =
    """        - { name: day, type: date, partitioned: true, partitionKeyPosition: 2 }
      |        - { name: total, type: long }
      |        - { name: region, type: string, partitioned: true, partitionKeyPosition: 1 }""".stripMargin

  // --- parsing ---------------------------------------------------------------------------------

  test("partitioned fields are the keys, ordered by position") {
    val o = output(regionDay)
    assert(o.partitionKeys == List("region", "day"))
    assert(o.schema.field("region").map(f => (f.partitioned, f.partitionKeyPosition)).contains((true, Some(1))))
    assert(o.schema.field("total").map(f => (f.partitioned, f.partitionKeyPosition)).contains((false, None)))
  }

  test("keys with no position come after the positioned ones, in declaration order") {
    val o = output(
      """        - { name: a, type: string, partitioned: true }
        |        - { name: b, type: string, partitioned: true, partitionKeyPosition: 1 }
        |        - { name: c, type: string, partitioned: true }
        |        - { name: v, type: long }""".stripMargin)
    assert(o.partitionKeys == List("b", "a", "c"))
  }

  test("ODCS's -1 position means not a key, and a field that does not say partitioned is not one") {
    val o = output(
      """        - { name: a, type: string, partitioned: false, partitionKeyPosition: -1 }
        |        - { name: b, type: string, partitionKeyPosition: 1 }
        |        - { name: v, type: long }""".stripMargin)
    assert(o.partitionKeys.isEmpty)
    assert(o.schema.field("a").flatMap(_.partitionKeyPosition).isEmpty)
    assert(o.schema.field("b").flatMap(_.partitionKeyPosition).contains(1))
  }

  test("a position that is not a whole number, or partitioned that is not a boolean, does not parse") {
    intercept[ContractParseException](output("        - { name: a, type: string, partitioned: true, partitionKeyPosition: first }"))
    intercept[ContractParseException](output("        - { name: a, type: string, partitioned: maybe }"))
  }

  test("the overwrite mode is the declared one, lower-cased, or static") {
    assert(output(regionDay).partitionOverwriteMode.isEmpty)
    assert(output(regionDay).effectivePartitionOverwriteMode == "static")
    assert(output(regionDay, "    partitionOverwriteMode: Dynamic\n").effectivePartitionOverwriteMode == "dynamic")
    assert(output(regionDay, "    partitionOverwriteMode: static\n").partitionOverwriteMode.contains("static"))
  }

  test("written back, partitioning reads the same") {
    val c = contract(regionDay, "    partitionOverwriteMode: dynamic\n")
    val again = ContractParser.parse(ContractParser.write(c))
    assert(again.outputs.head.partitionKeys == List("region", "day"))
    assert(again.outputs.head.partitionOverwriteMode.contains("dynamic"))
    assert(again == c)
    val plain = ContractParser.write(contract("        - { name: v, type: long }"))
    assert(!plain.contains("partitioned") && !plain.contains("partitionKeyPosition") && !plain.contains("partitionOverwriteMode"))
  }

  test("the keys are Invaract's now, so neither is flagged as an unknown key") {
    val yaml = "id: x\nversion: \"1.0.0\"\noutputs:\n  - name: o\n    location: o\n    partitionOverwriteMode: dynamic\n    schema:\n      fields:\n" +
      "        - { name: a, type: string, partitioned: true, partitionKeyPosition: 1 }\n        - { name: v, type: long }\n"
    assert(ContractParser.unknownKeyWarnings(yaml).isEmpty)
  }

  // --- validation ------------------------------------------------------------------------------

  test("a well-formed partitioning raises nothing") {
    val c = contract(regionDay, "    partitionOverwriteMode: dynamic\n")
    assert(errors(c).isEmpty)
    assert(!warnings(c).exists(_.toLowerCase.contains("partition")))
  }

  test("a key must be a top-level scalar field") {
    val nested = contract(
      """        - name: s
        |          type: struct
        |          properties:
        |            - { name: inner, type: string, partitioned: true }
        |        - { name: v, type: long }""".stripMargin)
    assert(errors(nested) == List("Only a top-level field can be a partition key"))
    val array = contract("        - { name: tags, type: \"array<string>\", partitioned: true }\n        - { name: v, type: long }")
    assert(errors(array) == List("A partition key must be a scalar field, not 'array<string>'"))
    val map = contract("        - { name: m, type: \"map<string,long>\", partitioned: true }\n        - { name: v, type: long }")
    assert(errors(map).exists(_.startsWith("A partition key must be a scalar field")))
    val struct = contract(
      """        - name: s
        |          type: struct
        |          partitioned: true
        |          properties:
        |            - { name: inner, type: string }
        |        - { name: v, type: long }""".stripMargin)
    assert(errors(struct).exists(_.startsWith("A partition key must be a scalar field")))
  }

  test("positions count from 1 and are each used once; a position without partitioned is a warning") {
    assert(errors(contract("        - { name: a, type: string, partitioned: true, partitionKeyPosition: 0 }\n        - { name: v, type: long }")) ==
      List("A partition key's position counts from 1, not 0"))
    assert(errors(contract("        - { name: a, type: string, partitioned: true, partitionKeyPosition: 1 }\n        - { name: v, type: long }")).isEmpty)
    assert(errors(contract(
      """        - { name: a, type: string, partitioned: true, partitionKeyPosition: 1 }
        |        - { name: b, type: string, partitioned: true, partitionKeyPosition: 1 }
        |        - { name: v, type: long }""".stripMargin)) == List("More than one partition key has position 1"))
    assert(warnings(contract("        - { name: a, type: string, partitionKeyPosition: 1 }\n        - { name: v, type: long }"))
      .contains("Has a partitionKeyPosition but is not 'partitioned: true', so it is not a partition key"))
  }

  test("not every field can be a key") {
    assert(errors(contract("        - { name: a, type: string, partitioned: true }")) == List("Every field is a partition key, which leaves no data in the files"))
  }

  test("the overwrite mode is one of static and dynamic, and means nothing without keys") {
    assert(errors(contract(regionDay, "    partitionOverwriteMode: replace\n")) == List("Unknown partitionOverwriteMode 'replace' (expected one of dynamic, static)"))
    assert(errors(contract(regionDay, "    partitionOverwriteMode: DYNAMIC\n")).isEmpty)
    assert(warnings(contract("        - { name: v, type: long }", "    partitionOverwriteMode: dynamic\n"))
      .contains("Declares a partitionOverwriteMode but no partition keys, so it has no effect"))
    assert(!warnings(contract(regionDay, "    partitionOverwriteMode: dynamic\n")).exists(_.contains("no effect")))
  }

  // --- compatibility ---------------------------------------------------------------------------

  private def changes(previous: Contract, next: Contract): List[(CompatibilityLevel, String)] =
    ContractCompatibility.diff(previous, next).changes.filter(c => c.path.contains("schema") && c.description.contains("partition") ||
      c.path.endsWith("partitionOverwriteMode")).map(c => c.level -> c.description)

  test("declaring keys, or changing them or their order, is breaking; dropping them is not") {
    val none = contract("        - { name: region, type: string }\n        - { name: day, type: date }\n        - { name: total, type: long }")
    val byRegion = contract("        - { name: region, type: string, partitioned: true }\n        - { name: day, type: date }\n        - { name: total, type: long }")
    val both = contract(regionDay)
    val swapped = contract(regionDay.replace("partitionKeyPosition: 2", "partitionKeyPosition: 9").replace("partitionKeyPosition: 1", "partitionKeyPosition: 10"))
    assert(changes(none, byRegion) == List(CompatibilityLevel.Breaking -> "the dataset is now required to be partitioned by region"))
    assert(changes(byRegion, both) == List(CompatibilityLevel.Breaking -> "the partition keys changed from region to region, day"))
    assert(changes(both, swapped) == List(CompatibilityLevel.Breaking -> "the partition keys changed from region, day to day, region"))
    assert(changes(both, none).isEmpty)
    assert(changes(both, both).isEmpty)
  }

  test("declaring or changing the overwrite mode is breaking; dropping it is not") {
    val plain = contract(regionDay)
    val dynamic = contract(regionDay, "    partitionOverwriteMode: dynamic\n")
    val static = contract(regionDay, "    partitionOverwriteMode: static\n")
    assert(changes(plain, dynamic).map(_._1) == List(CompatibilityLevel.Breaking))
    assert(changes(static, dynamic).map(_._1) == List(CompatibilityLevel.Breaking))
    assert(changes(dynamic, plain).isEmpty)
  }
}
