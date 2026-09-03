// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, ContractParser}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Shared scaffolding for every `*ConnectorSpec` in this module — the
  * `injectCheckRule` wiring that lets `ContractEnforcementRule.verifyOrThrow`
  * observe a suite's real Spark session, plus the `activeContract`/
  * `withContract` convention every connector spec uses to scope a contract
  * to one test. Extracted after the sixth connector (ClickHouse) copied the
  * same ~15 lines of this a sixth time — see docs/ADDING_A_SPARK_CONNECTOR.md.
  *
  * Session construction itself (extensions, catalog config, embedded
  * metastores, external test servers) stays in each spec's own `beforeAll`
  * — that part is genuinely connector-specific, not boilerplate to share.
  * A spec builds its session with `.withExtensions(injectContractCheck)` in
  * place of hand-writing the closure, and calls `super.afterAll()` first if
  * it needs its own additional teardown (e.g. stopping a test server).
  */
trait ConnectorSpecBase extends AnyFunSuite with BeforeAndAfterAll {
  protected var spark: SparkSession = _

  @volatile protected var activeContract: Option[Contract] = None
  @volatile protected var activeOptions: VerificationOptions = VerificationOptions()
  protected val capturedPlans: scala.collection.mutable.ListBuffer[LogicalPlan] =
    scala.collection.mutable.ListBuffer.empty[LogicalPlan]

  protected def injectContractCheck(ext: SparkSessionExtensions): Unit =
    ext.injectCheckRule { _ => (plan: LogicalPlan) =>
      capturedPlans += plan
      activeContract.foreach(c => ContractEnforcementRule.verifyOrThrow(c, plan, activeOptions))
    }

  override def afterAll(): Unit = spark.stop()

  protected def parseContract(yaml: String) = ContractParser.parse(yaml)

  protected def withContract[T](yaml: String, options: VerificationOptions = VerificationOptions())(body: => T): T = {
    activeContract = Some(parseContract(yaml))
    activeOptions = options
    try body
    finally activeContract = None
  }

  protected def df() = spark.createDataFrame(Seq((1L, 10L), (2L, 20L))).toDF("id", "value")
}
