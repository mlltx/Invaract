// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import com.invaract.contract.{LogicalField, LogicalType}
import com.invaract.testkit.{AdapterConformanceSpec, ConformanceAdapter, ReferenceAdapter, Scenarios}
import com.invaract.verification.ArrowSchemas

/** PythonIo through the adapter conformance kit: every engine-neutral scenario runs through the checker as the `invaract`
  * package will drive it (`PythonIoConformanceAdapter`), and each must come out the way `invaract-capabilities-pythonio.yaml`
  * promises. A scenario the declaration says does not apply is canceled with its reason, not silently skipped.
  */
class PythonIoConformanceSpec extends AdapterConformanceSpec {
  private val pythonioAdapter = new PythonIoConformanceAdapter

  override protected def adapter: ConformanceAdapter = pythonioAdapter

  test("the settings the conformance adapter calls inapplicable are those the checker refuses as not applicable") {
    assert(PythonIoConformanceAdapter.Inapplicable.keySet == PythonIoConfigSource.NotApplicable)
  }

  test("the Arrow names the conformance adapter sends map back to the kit's types, so the scenarios test what a real job sends") {
    import LogicalType._
    val jobs = Scenarios.all.map(_.job)
    val kitTypes = jobs.flatMap(_.inputs.flatMap(_.schema.fields)) ++ jobs.filterNot(_.untranslatableWrite).flatMap(ReferenceAdapter.outputSchema(_).fields)
    // every type a frame can hold, nested ones with a field the contract requires
    val everyType = List(
      StringType, BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DateType, TimestampType, TimestampNtzType,
      BinaryType, TimeType, IntervalType, DecimalType(10, 2), ArrayType(StringType), MapType(StringType, LongType),
      StructType(List(LogicalField("a", LongType, nullable = false), LogicalField("b", ArrayType(DoubleType)))), OtherType("uint64")
    )
    (kitTypes.map(_.dataType) ++ everyType).distinct.foreach { t =>
      assert(ArrowSchemas.dataType(PythonIoConformanceAdapter.arrowName(t)).contains(t), s"$t as ${PythonIoConformanceAdapter.arrowName(t)}")
    }
  }
}
