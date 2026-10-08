// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.SaveModes
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite

/** Spark's write dispositions land inside the canonical write vocabulary
  * (`com.invaract.contract.SaveModes`) - the rule every adapter follows so one contract means the
  * same thing on every engine (docs/MULTI_ENGINE_ADAPTERS.md, "Conventions every adapter follows").
  */
class SaveModeVocabularySpec extends AnyFunSuite {

  test("every Spark SaveMode maps to a distinct canonical name") {
    val mapped = SaveMode.values().toList.map(m => m -> SparkPlanAdapter.saveModeOf(m))
    mapped.foreach { case (m, name) => assert(name.exists(SaveModes.isCanonical), s"$m -> $name is not canonical") }
    assert(mapped.flatMap(_._2).toSet == SaveModes.All, "the four Spark modes cover the whole canonical vocabulary")
  }

  test("the mapping is the documented one") {
    assert(SparkPlanAdapter.saveModeOf(SaveMode.Append).contains(SaveModes.Append))
    assert(SparkPlanAdapter.saveModeOf(SaveMode.Overwrite).contains(SaveModes.Overwrite))
    assert(SparkPlanAdapter.saveModeOf(SaveMode.ErrorIfExists).contains(SaveModes.Error))
    assert(SparkPlanAdapter.saveModeOf(SaveMode.Ignore).contains(SaveModes.Ignore))
  }
}
