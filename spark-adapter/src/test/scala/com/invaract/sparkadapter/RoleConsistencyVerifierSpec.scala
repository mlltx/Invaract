// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract._
import com.invaract.ir._

import org.scalatest.funsuite.AnyFunSuite

/** Pure-Scala coverage of `RoleConsistencyVerifier` — no Spark session
  * needed, since `ir.Plan`/`ir.Expr` and `Contract` are both plain data,
  * mirroring `StaticDataQualityVerifierSpec`'s/`PlanRuleVerifierSpec`'s own
  * style. Real end-to-end coverage through a live Spark session (proving
  * the observed-usage signal survives a real `SparkPlanAdapter.translate`)
  * lives in `ContractInferenceSpec`'s equivalent test for dry-run mode,
  * which exercises the identical `Lineage.trace`/
  * `PlanRuleVerifier.collectConditionReferences` machinery this module
  * reuses.
  */
class RoleConsistencyVerifierSpec extends AnyFunSuite {

  private def col(name: String, qualifier: Option[String] = None) = ColumnReference(ColumnRef(name, qualifier))
  private def project(input: Plan, name: String, expr: Expr): Plan = Project(input, List(NamedExpr(name, expr)))

  private def dataset(name: String, location: String, datasetType: Option[DatasetType] = None): Dataset =
    Dataset(name, location, Some("parquet"), Schema(List(Field("id", "string"))), datasetType = datasetType)

  private def contractWith(inputs: List[Dataset], outputs: List[Dataset] = List(dataset("out", "gold.out"))): Contract =
    Contract(
      id = "test_contract",
      version = ContractVersion(1, 0, 0),
      status = "active",
      inputs = inputs,
      outputs = outputs,
      rules = Nil,
      extensions = Map.empty
    )

  // --- Non-Write plans / no declared type ---------------------------------

  test("verify returns Nil for a plan that isn't a Write at all") {
    val contract = contractWith(List(dataset("in", "raw.control", Some(DatasetType.Control))))
    assert(RoleConsistencyVerifier.verify(contract, Read(DatasetRef("raw.control"))).isEmpty)
  }

  test("an input with no declared type produces no result at all, whatever its usage") {
    val contract = contractWith(List(dataset("in", "raw.source", None)))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.source")), "id", col("id")))
    assert(RoleConsistencyVerifier.verify(contract, plan).isEmpty)
  }

  test("an input declaring a type but never referenced anywhere in the plan produces no result - MissingInput's job, not this one") {
    val contract = contractWith(List(dataset("in", "raw.unused", Some(DatasetType.Control))))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.other")), "id", col("id")))
    assert(RoleConsistencyVerifier.verify(contract, plan).isEmpty)
  }

  // --- CONTROL ------------------------------------------------------------

  test("a CONTROL input whose column reaches a produced output column is Contradicts, and becomes a blocking Violation") {
    val contract = contractWith(List(dataset("calendar", "raw.calendar", Some(DatasetType.Control))))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.calendar")), "id", col("id")))

    val results = RoleConsistencyVerifier.verify(contract, plan)
    assert(results.size == 1)
    val result = results.head
    assert(result.dataset == "calendar")
    assert(result.datasetType == DatasetType.Control)
    assert(result.verdict == RoleConformanceVerdict.Contradicts)

    val violations = RoleConsistencyVerifier.violations(results)
    assert(violations.size == 1)
    assert(violations.head.violationType == ViolationType.RoleConsistencyViolation)
    assert(violations.head.message.contains("calendar"))
    assert(violations.head.message.contains("CONTROL"))
  }

  test("a CONTROL input referenced only in a Filter condition, never in output lineage, is Conforms and never blocks") {
    val calendarRead = Read(DatasetRef("raw.calendar"))
    val dataRead = Read(DatasetRef("raw.data"))
    val filtered = Filter(Join(dataRead, calendarRead, JoinType.Cross, None), Function("ISNOTNULL", List(col("gate", Some("raw.calendar")))))
    val plan = Write(DatasetRef("gold.out"), project(filtered, "id", col("id", Some("raw.data"))))

    val contract = contractWith(
      List(dataset("data", "raw.data"), dataset("calendar", "raw.calendar", Some(DatasetType.Control)))
    )
    val results = RoleConsistencyVerifier.verify(contract, plan)
    val calendarResult = results.find(_.dataset == "calendar").get
    assert(calendarResult.verdict == RoleConformanceVerdict.Conforms)
    assert(RoleConsistencyVerifier.violations(results).isEmpty)
  }

  test("a CONTROL input referenced only in a Join condition, never in output lineage, is Conforms") {
    val calendarRead = Read(DatasetRef("raw.calendar"))
    val dataRead = Read(DatasetRef("raw.data"))
    val joined = Join(dataRead, calendarRead, JoinType.Inner, Some(col("gate", Some("raw.calendar"))))
    val plan = Write(DatasetRef("gold.out"), project(joined, "id", col("id", Some("raw.data"))))

    val contract = contractWith(
      List(dataset("data", "raw.data"), dataset("calendar", "raw.calendar", Some(DatasetType.Control)))
    )
    val results = RoleConsistencyVerifier.verify(contract, plan)
    val calendarResult = results.find(_.dataset == "calendar").get
    assert(calendarResult.verdict == RoleConformanceVerdict.Conforms)
  }

  // --- DATA_ASSET / SOURCE -------------------------------------------------

  test("a DATA_ASSET input contributing to output is Conforms") {
    val contract = contractWith(List(dataset("customer", "raw.customer", Some(DatasetType.DataAsset))))
    val plan = Write(DatasetRef("gold.out"), project(Read(DatasetRef("raw.customer")), "id", col("id")))

    val results = RoleConsistencyVerifier.verify(contract, plan)
    assert(results.size == 1)
    assert(results.head.verdict == RoleConformanceVerdict.Conforms)
    assert(RoleConsistencyVerifier.violations(results).isEmpty)
  }

  test("a SOURCE input observed only in a Filter/Join condition, never in output lineage, is CannotDetermine - not a violation") {
    val sourceRead = Read(DatasetRef("raw.vendor_feed"))
    val dataRead = Read(DatasetRef("raw.data"))
    val filtered = Filter(Join(dataRead, sourceRead, JoinType.Cross, None), Function("ISNOTNULL", List(col("flag", Some("raw.vendor_feed")))))
    val plan = Write(DatasetRef("gold.out"), project(filtered, "id", col("id", Some("raw.data"))))

    val contract = contractWith(
      List(dataset("data", "raw.data"), dataset("vendor_feed", "raw.vendor_feed", Some(DatasetType.Source)))
    )
    val results = RoleConsistencyVerifier.verify(contract, plan)
    val vendorResult = results.find(_.dataset == "vendor_feed").get
    assert(vendorResult.verdict == RoleConformanceVerdict.CannotDetermine)
    // Never blocks - the whole point of a three-state verdict is that an
    // unproven contradiction must never be silently escalated to one.
    assert(RoleConsistencyVerifier.violations(results).isEmpty)
  }

  // --- Multiple inputs, mixed verdicts -------------------------------------

  test("verify reports one result per declared-type input observed in the plan, independent of the others' verdicts") {
    val calendarRead = Read(DatasetRef("raw.calendar"))
    val customerRead = Read(DatasetRef("raw.customer"))
    val joined = Join(customerRead, calendarRead, JoinType.Cross, None)
    val filtered = Filter(joined, Function("ISNOTNULL", List(col("as_of_date", Some("raw.calendar")))))
    val plan = Write(DatasetRef("gold.out"), project(filtered, "id", col("id", Some("raw.customer"))))

    val contract = contractWith(
      List(
        dataset("customer", "raw.customer", Some(DatasetType.DataAsset)),
        dataset("calendar", "raw.calendar", Some(DatasetType.Control))
      )
    )
    val results = RoleConsistencyVerifier.verify(contract, plan)
    assert(results.map(_.dataset).toSet == Set("customer", "calendar"))
    assert(results.find(_.dataset == "customer").get.verdict == RoleConformanceVerdict.Conforms)
    assert(results.find(_.dataset == "calendar").get.verdict == RoleConformanceVerdict.Conforms)
    assert(RoleConsistencyVerifier.violations(results).isEmpty)
  }
}
