// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.contract.Contract
import com.invaract.ir.{ColumnRef, Lineage, Plan}
import com.invaract.verification.{LocationMatching, PlanRuleVerifier, SensitivityLineage}

/** Reads what a scenario expects out of an adapter's own translated plan: which input columns each output
  * column derives from, and which sensitivity tags reach it. Both are computed by the engine-neutral code
  * (`ir.Lineage`, `SensitivityLineage`) from the plan, so what they show is exactly how well the adapter's
  * translation preserved the job.
  */
object TranslationProbe {

  /** For each output column, the `(input number, column)` pairs it derives from. A source the plan cannot be
    * tied to one of the job's inputs is reported as input `-1`, so a lost or mislabelled source shows as a
    * difference rather than disappearing. (`ir.Lineage` always qualifies a traced source with the scope of its
    * `Read`, which the plan's own reads resolve to a location.)
    */
  def lineage(plan: Plan, job: ScenarioJob): Map[String, Set[(Int, String)]] = {
    val locationOf = PlanRuleVerifier.locationResolver(plan)
    def inputOf(source: ColumnRef): Int =
      source.qualifier.fold(-1) { scope =>
        val location = locationOf(scope)
        job.inputs.indexWhere(i => LocationMatching.matches(i.location, location))
      }
    Lineage.trace(plan).map(cl => cl.output.name -> cl.sources.map(s => inputOf(s) -> s.name)).toMap
  }

  /** For each output column, the sensitivity tags `contract` declares on the input fields it derives from. */
  def sensitivity(plan: Plan, contract: Contract): Map[String, Set[String]] =
    SensitivityLineage.propagate(plan, contract).map(c => c.lineage.output.name -> c.sensitivityTags).toMap
}
