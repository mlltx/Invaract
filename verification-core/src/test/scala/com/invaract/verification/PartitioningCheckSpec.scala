// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{Contract, ContractParser, LogicalField, LogicalSchema, LogicalType}
import com.invaract.ir.{DatasetRef, Read, UnknownPlan, Write}

import org.scalatest.funsuite.AnyFunSuite

class PartitioningCheckSpec extends AnyFunSuite {

  private def contract(extras: String = "", keys: Boolean = true): Contract = ContractParser.parse(
    s"""id: parts
       |version: "1.0.0"
       |outputs:
       |  - name: report
       |    location: out/report
       |$extras    schema:
       |      fields:
       |        - { name: region, type: string${if (keys) ", partitioned: true, partitionKeyPosition: 1" else ""} }
       |        - { name: day, type: date${if (keys) ", partitioned: true, partitionKeyPosition: 2" else ""} }
       |        - { name: total, type: long }
       |""".stripMargin
  )

  private def write(saveMode: Option[String] = Some("overwrite"), location: String = "out/report") =
    Write(DatasetRef(location), Read(DatasetRef("in/orders")), Some("parquet"), saveMode)

  private def check(c: Contract, p: Option[WritePartitioning], saveMode: Option[String] = Some("overwrite"), location: String = "out/report") =
    OutputChecker.partitioningCheck(c, write(saveMode, location), p)

  private def static(cols: String*) = Some(WritePartitioning(cols.toList, "static"))
  private def dynamic(cols: String*) = Some(WritePartitioning(cols.toList, "dynamic"))

  test("the declared keys, in order, pass") {
    assert(check(contract(), static("region", "day")).isEmpty)
  }

  test("other keys, another order, a missing key or no partitioning at all is a mismatch naming both sides") {
    def one(p: Option[WritePartitioning]): (Option[String], Option[String]) = {
      val List(v) = check(contract(), p)
      assert(v.violationType == ViolationType.OutputPartitioningMismatch && v.location.contains("out/report"))
      v.expected -> v.actual
    }
    assert(one(static("day", "region")) == (Some("region, day") -> Some("day, region")))
    assert(one(static("region")) == (Some("region, day") -> Some("region")))
    assert(one(static("region", "day", "total")) == (Some("region, day") -> Some("region, day, total")))
    assert(one(static()) == (Some("region, day") -> Some("none")))
  }

  test("nothing is checked when the output declares no keys or the adapter does not know the partitioning") {
    assert(check(contract(keys = false), static("region")).isEmpty)
    assert(check(contract(keys = false, extras = "    partitionOverwriteMode: dynamic\n"), static()).isEmpty)
    assert(check(contract(), None).isEmpty)
  }

  test("the overwrite mode is checked for an overwrite only, against the declared mode or static") {
    // nothing declared means static
    assert(check(contract(), dynamic("region", "day")).map(v => (v.expected, v.actual)) == List((Some("static overwrite"), Some("dynamic overwrite"))))
    assert(check(contract("    partitionOverwriteMode: dynamic\n"), dynamic("region", "day")).isEmpty)
    assert(check(contract("    partitionOverwriteMode: dynamic\n"), static("region", "day")).map(_.expected) == List(Some("dynamic overwrite")))
    // the adapter's spelling of the mode does not matter
    assert(check(contract("    partitionOverwriteMode: dynamic\n"), Some(WritePartitioning(List("region", "day"), "DYNAMIC"))).isEmpty)
    assert(check(contract(), dynamic("region", "day"), saveMode = Some("OVERWRITE")).size == 1)
    // an append or an unknown save mode replaces nothing, so its mode is not looked at
    assert(check(contract(), dynamic("region", "day"), saveMode = Some("append")).isEmpty)
    assert(check(contract(), dynamic("region", "day"), saveMode = None).isEmpty)
    // keys that differ are the one finding, whatever the mode
    assert(check(contract(), dynamic("day")).map(_.expected) == List(Some("region, day")))
  }

  test("a plan that is no write, or a write to no declared output of a multi-output contract, is not checked here") {
    assert(OutputChecker.partitioningCheck(contract(), Read(DatasetRef("in/orders")), static()).isEmpty)
    val two = ContractParser.parse(
      """id: two
        |version: "1.0.0"
        |outputs:
        |  - name: a
        |    location: out/a
        |    schema:
        |      fields:
        |        - { name: k, type: string, partitioned: true }
        |        - { name: v, type: long }
        |  - name: b
        |    location: out/b
        |    schema:
        |      fields:
        |        - { name: v, type: long }
        |""".stripMargin)
    assert(OutputChecker.partitioningCheck(two, write(location = "out/elsewhere"), static()).isEmpty)
    assert(OutputChecker.partitioningCheck(two, write(location = "out/a"), static()).size == 1)
    assert(OutputChecker.partitioningCheck(two, write(location = "out/b"), static()).isEmpty)
  }

  test("the pipeline reports it with the write's other findings") {
    val schema = LogicalSchema(List(LogicalField("region", LogicalType.StringType, nullable = true), LogicalField("day", LogicalType.DateType, nullable = true),
      LogicalField("total", LogicalType.LongType, nullable = true)))
    val checked = CheckedWrite(write(), List("in/orders" -> LogicalSchema(Nil)), schema, caseSensitive = true, None, Set.empty)
    assert(VerificationPipeline.evaluateWrite(contract(), checked.copy(partitioning = static("region", "day")), VerificationOptions(), None).passed)
    val refused = VerificationPipeline.evaluateWrite(contract(), checked.copy(partitioning = static("day")), VerificationOptions(), None)
    assert(refused.violations.map(_.violationType) == List(ViolationType.OutputPartitioningMismatch))
  }

  test("a contract with partition keys on an output relies on check.partitioning; on an input alone it does not") {
    val why = CapabilityCheck.required(contract(), VerificationOptions()).toMap
    assert(why.get(Capability.CheckPartitioning).contains("output 'report' declares partition keys"))
    assert(!CapabilityCheck.required(contract(keys = false), VerificationOptions()).map(_._1).contains(Capability.CheckPartitioning))
    val inputOnly = ContractParser.parse(
      """id: in
        |version: "1.0.0"
        |inputs:
        |  - name: orders
        |    location: in/orders
        |    schema:
        |      fields:
        |        - { name: k, type: string, partitioned: true }
        |        - { name: v, type: long }
        |outputs:
        |  - name: o
        |    location: out/o
        |    schema:
        |      fields:
        |        - { name: v, type: long }
        |""".stripMargin)
    assert(!CapabilityCheck.required(inputOnly, VerificationOptions()).map(_._1).contains(Capability.CheckPartitioning))
  }

  test("a dry run's draft records the write's keys in order, and a dynamic overwrite") {
    val schema = LogicalSchema(List(LogicalField("total", LogicalType.LongType, nullable = true), LogicalField("day", LogicalType.DateType, nullable = true),
      LogicalField("region", LogicalType.StringType, nullable = true)))
    val plan = Write(DatasetRef("out/report"), UnknownPlan("job"), Some("parquet"), Some("overwrite"))
    def draft(p: Option[WritePartitioning]) = ContractInference.infer(InferredWrite("out/report", Some("parquet"), Some("overwrite"), schema, p), Nil, plan).outputs.head
    val d = draft(dynamic("region", "day"))
    assert(d.partitionKeys == List("region", "day"))
    assert(d.schema.fields.map(f => f.name -> f.partitionKeyPosition) == List("total" -> None, "day" -> Some(2), "region" -> Some(1)))
    assert(d.partitionOverwriteMode.contains("dynamic"))
    assert(draft(static("region")).partitionOverwriteMode.isEmpty)
    assert(draft(dynamic()).partitionOverwriteMode.isEmpty, "with no keys there is nothing to overwrite partition by partition")
    assert(draft(None).partitionKeys.isEmpty && draft(None).partitionOverwriteMode.isEmpty)
  }
}
