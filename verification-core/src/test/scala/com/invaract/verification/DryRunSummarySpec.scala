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
