// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.testkit.{AdapterConformanceSpec, ConformanceAdapter, Scenarios}
import com.invaract.verification.PlanFacts

import org.scalatest.BeforeAndAfterAll

/** Spark through the adapter conformance kit (docs/MULTI_ENGINE_ADAPTERS.md, Stage 4): every
  * engine-neutral scenario runs as a real Spark job under the real enforcement rule, and each must
  * come out the way Spark's own capability declaration (`invaract-capabilities-spark.yaml`) promises.
  * A scenario Spark declares not applicable is canceled with the reason, not silently skipped.
  */
class SparkConformanceSpec extends AdapterConformanceSpec with BeforeAndAfterAll {
  private val sparkAdapter = new SparkConformanceAdapter

  override protected def adapter: ConformanceAdapter = sparkAdapter

  test("the opaque-step scenarios really put a node Spark's translation has no case for into the plan, over the reads beneath it") {
    val scenario = Scenarios.all.find(_.id == "unrecognized-step-does-not-hide-an-undeclared-read").get
    val plan = sparkAdapter.translation(scenario.id, scenario.job).getOrElse(fail("Spark exposes its translation"))
    val unknown = PlanFacts.of(plan).unknownPlans
    assert(unknown.map(_.sourceType) == List("Sample"), unknown.toString)
    assert(PlanFacts.of(unknown.head).reads.size == 2, "both inputs are still reads beneath the unrecognized node")
  }

  override def afterAll(): Unit = {
    sparkAdapter.close()
    super.afterAll()
  }
}
