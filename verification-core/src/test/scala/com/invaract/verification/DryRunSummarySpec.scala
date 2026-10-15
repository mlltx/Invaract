// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{Contract, ContractParser, ContractVersion, Dataset, Field, Schema}
import com.invaract.verification.notification.{InferenceStatus, JobInfo, MergeStatus}
import org.scalatest.funsuite.AnyFunSuite

class DryRunSummarySpec extends AnyFunSuite {

  private def schema(fields: (String, String)*) = Schema(fields.toList.map { case (n, t) => Field(n, t, required = true, nullable = true) })

  private def draft(inputLocation: String, outputLocation: String, outputSchema: Schema): Contract =
    Contract(
      "inferred_contract",
      ContractVersion(0, 1, 0),
      "draft",
      List(Dataset("input", inputLocation, None, schema("id" -> "long"))),
      List(Dataset("output", outputLocation, Some("parquet"), outputSchema, derivedFrom = Some(List("input")))),
      Nil,
      Map.empty
    )

  private val job = JobInfo(name = Some("nightly orders"), engine = Some("beam"))

  test("drafts that agree merge into one contract named after the job, with the status counts and the job") {
    val e = DryRunSummary.event(
      Map(InferenceStatus.Inferred -> 2L),
      List(draft("/in/orders", "/out/a", schema("id" -> "long")), draft("/in/orders", "/out/b", schema("id" -> "long"))),
      job,
      42L,
      Map("team" -> "sales")
    )
    assert(e.mergeStatus == MergeStatus.Merged && e.mergeConflicts.isEmpty)
    val merged = ContractParser.parse(e.mergedContractYaml.get)
    assert(merged.id == "nightly_orders" && merged.outputs.map(_.location) == List("/out/a", "/out/b"))
    assert(e.statusCounts == Map(InferenceStatus.Inferred -> 2L))
    assert(e.job == job && e.timestamp == 42L && e.metadata == Map("team" -> "sales"))
  }

  test("drafts that disagree about one location are a conflict, and no contract is guessed at") {
    val e = DryRunSummary.event(
      Map(InferenceStatus.Inferred -> 2L),
      List(draft("/in/orders", "/out/a", schema("id" -> "long")), draft("/in/orders", "/out/a", schema("id" -> "string"))),
      job,
      0L,
      Map.empty
    )
    assert(e.mergeStatus == MergeStatus.Conflict && e.mergedContractYaml.isEmpty && e.mergeConflicts.nonEmpty)
  }

  test("no drafts is NO_DRAFTS, and a job that wrote nothing recognised says so in its counts") {
    val none = DryRunSummary.event(Map.empty, Nil, job, 0L, Map.empty)
    assert(none.mergeStatus == MergeStatus.NoDrafts && none.mergedContractYaml.isEmpty && none.mergeConflicts.isEmpty)
    assert(none.statusCounts == Map(InferenceStatus.NoWritesObserved -> 1L))
    val skipped = DryRunSummary.event(Map(InferenceStatus.SkippedUnrecognized -> 1L), Nil, job, 0L, Map.empty)
    assert(skipped.statusCounts == Map(InferenceStatus.SkippedUnrecognized -> 1L))
  }

  test("the merged contract's id is the job's stable identity, made safe for a contract id") {
    assert(DryRunSummary.mergedContractId(JobInfo(jobId = Some("orders.daily-v2"), name = Some("ignored"))) == "orders.daily-v2")
    assert(DryRunSummary.mergedContractId(JobInfo(name = Some("my job/2024"))) == "my_job_2024")
    assert(DryRunSummary.mergedContractId(JobInfo(name = Some("2024 run"))) == "job_2024_run", "an id starts with a letter")
    assert(DryRunSummary.mergedContractId(JobInfo(name = Some("_x"))) == "job__x")
    assert(DryRunSummary.mergedContractId(JobInfo()) == ContractInference.InferredId)
    assert(DryRunSummary.mergedContractId(JobInfo(name = Some(""))) == ContractInference.InferredId)
  }
}

class DryRunSelfCheckSpec extends AnyFunSuite {
  import com.invaract.contract.{LogicalField, LogicalSchema, LogicalType}
  import com.invaract.ir.{DatasetRef, Read, Write}
  import com.invaract.verification.notification.SelfCheckStatus

  private val draft = ContractParser.parse(
    """id: inferred_contract
      |version: 0.1.0
      |status: draft
      |inputs:
      |  - { name: input, location: /in/orders, schema: { fields: [ { name: id, type: long } ] } }
      |outputs:
      |  - { name: output, location: /out/report, format: parquet, schema: { fields: [ { name: id, type: long } ] } }
      |""".stripMargin)
  private val columns = LogicalSchema(List(LogicalField("id", LogicalType.LongType, nullable = true)))
  private def plan(out: String) = Write(DatasetRef(out), Read(DatasetRef("/in/orders")), Some("parquet"))
  private def check(c: Contract, out: String = "/out/report") =
    DryRunSelfCheck.of(c, plan(out), List("/in/orders" -> columns), columns, VerificationOptions(), caseSensitive = true, Set.empty)

  test("a draft its own write passes is PASSED, with nothing to report") {
    assert(check(draft) == ((SelfCheckStatus.Passed, Nil, Nil)))
  }

  test("a draft its own write would not pass is FAILED, with the violations") {
    val (status, violations, notes) = check(draft, "/out/elsewhere")
    assert(status == SelfCheckStatus.Failed && violations.map(_.violationType) == List(ViolationType.OutputLocationMismatch) && notes.isEmpty)
  }

  test("a draft that is not a valid contract is INVALID, naming what is wrong, and is not verified") {
    val (status, violations, _) = check(draft.copy(id = ""))
    assert(status == SelfCheckStatus.Invalid && violations.nonEmpty && violations.forall(_.violationType == ViolationType.InvalidContract))
  }

  test("a self-check that throws is ERROR, with what it threw, never an exception into the job") {
    val (status, violations, notes) = DryRunSelfCheck.of(draft, null, Nil, columns, VerificationOptions(), caseSensitive = true, Set.empty)
    assert(status == SelfCheckStatus.Error && violations.isEmpty && notes.exists(_.startsWith("self-check threw NullPointerException")))
  }

  test("an output schema whose conversion throws is ERROR too") {
    val (status, _, notes) =
      DryRunSelfCheck.of(draft, plan("/out/report"), Nil, throw new IllegalStateException("no schema"), VerificationOptions(), caseSensitive = true, Set.empty)
    assert(status == SelfCheckStatus.Error && notes == List("self-check threw IllegalStateException: no schema"))
  }
}
