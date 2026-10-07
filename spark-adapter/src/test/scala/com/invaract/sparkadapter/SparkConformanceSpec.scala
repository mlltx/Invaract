// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.testkit.{AdapterConformanceSpec, ConformanceAdapter}

import org.scalatest.BeforeAndAfterAll

/** Spark through the adapter conformance kit (docs/MULTI_ENGINE_ADAPTERS.md, Stage 4): every
  * engine-neutral scenario runs as a real Spark job under the real enforcement rule, and each must
  * come out the way Spark's own capability declaration (`invaract-capabilities-spark.yaml`) promises.
  * A scenario Spark declares not applicable is canceled with the reason, not silently skipped.
  */
class SparkConformanceSpec extends AdapterConformanceSpec with BeforeAndAfterAll {
  private val sparkAdapter = new SparkConformanceAdapter

  override protected def adapter: ConformanceAdapter = sparkAdapter

  override def afterAll(): Unit = {
    sparkAdapter.close()
    super.afterAll()
  }
}
