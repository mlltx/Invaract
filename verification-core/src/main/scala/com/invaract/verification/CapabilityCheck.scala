// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{Contract, Dataset, Field, RuleType}

/** Which `Capability`s a contract relies on, and what to do when the engine adapter has declared
  * one of them unsupported.
  *
  * This is the guard against the quietest failure a second engine could introduce: a contract that
  * declares a catalog requirement, a rule, a nested type or a format, run through an adapter that
  * simply does not check it - the write passes and the requirement was never looked at. With each
  * adapter's `AdapterCapabilities` declared completely and consulted here, that case is a loud
  * `UNSUPPORTED_CONTRACT_FEATURE` rejection instead.
  *
  * Only a capability that `enforcesContract` and is declared `Unsupported` blocks. `Partial` is
  * documented in the matrix but never blocks on its own (the adapter still checks what it can, and
  * fails closed where it cannot, e.g. `RULE_UNVERIFIABLE_DML`); the non-enforcing capabilities
  * (fingerprint, reporting, ...) never block.
  */
object CapabilityCheck {

  /** The capabilities `contract` (run with `options`) relies on, each with a short reason, in the
    * vocabulary's order. Includes the non-enforcing ones (fingerprint, sensitivity propagation) so a
    * report can show everything a contract asks of an adapter; `violations` filters to what blocks.
    */
  def required(contract: Contract, options: VerificationOptions): List[(Capability, String)] = {
    val datasets: List[Dataset] = contract.inputs ++ contract.outputs
    val fields: List[Field] = datasets.flatMap(d => allFields(d.schema.fields))
    val ruleTypes = contract.rules.map(_.ruleType).toSet

    def when(cond: Boolean, capability: Capability, why: => String): List[(Capability, String)] =
      if (cond) List(capability -> why) else Nil

    val firstNested = fields.find(f => f.isStruct || f.fieldType.contains("<"))
    val firstSensitive = fields.find(_.sensitivityTags.nonEmpty)

    List(
      when(contract.inputs.nonEmpty, Capability.CheckInputExistence, s"it declares ${contract.inputs.size} input(s)"),
      // Every dataset declares a location, and a write is always checked against one.
      when(datasets.nonEmpty, Capability.CheckLocation, "it declares dataset locations"),
      when(fields.nonEmpty, Capability.CheckSchema, "it declares field schemas"),
      when(firstNested.isDefined, Capability.CheckNestedTypes, firstNested.map(f => s"field '${f.name}' has a nested type").getOrElse("")),
      when(datasets.exists(_.format.isDefined), Capability.CheckFormat, "a dataset declares a format"),
      when(contract.outputs.exists(_.saveMode.isDefined), Capability.CheckSaveMode, "an output declares a save mode"),
      when(datasets.exists(_.catalog.isDefined), Capability.CheckCatalogRegistration, "a dataset declares a catalog requirement"),
      when((ruleTypes & RuleType.DmlTypes).nonEmpty, Capability.RulesDml, s"it declares DML rule(s): ${(ruleTypes & RuleType.DmlTypes).toList.sorted.mkString(", ")}"),
      when((ruleTypes & RuleType.PlanShapeTypes).nonEmpty, Capability.RulesPlanShape, s"it declares transformation-shape rule(s): ${(ruleTypes & RuleType.PlanShapeTypes).toList.sorted.mkString(", ")}"),
      when(contract.customRuleTypes.nonEmpty, Capability.RulesCustom, s"it declares custom rule type(s): ${contract.customRuleTypes.keys.toList.sorted.mkString(", ")}"),
      when(options.staticDataQuality, Capability.AnalysisStaticDataQuality, "staticDataQuality is enabled"),
      when(options.roleConsistency, Capability.AnalysisRoleConsistency, "roleConsistency is enabled"),
      when(options.computeFingerprint, Capability.AnalysisFingerprint, "computeFingerprint is enabled"),
      when(firstSensitive.isDefined, Capability.AnalysisSensitivityPropagation, firstSensitive.map(f => s"field '${f.name}' carries sensitivity tags").getOrElse(""))
    ).flatten
  }

  /** One `UNSUPPORTED_CONTRACT_FEATURE` per required, contract-enforcing capability `capabilities`
    * declares `Unsupported`, in vocabulary order. Empty for an adapter that supports (or partially
    * supports) everything the contract needs.
    */
  def violations(contract: Contract, options: VerificationOptions, capabilities: AdapterCapabilities): List[Violation] =
    required(contract, options).collect {
      case (capability, why) if capability.enforcesContract && capabilities.supportOf(capability) == Support.Unsupported =>
        Violations.unsupportedContractFeature(capabilities.adapter, capability, why, capabilities.entryOf(capability).note)
    }

  private def allFields(fields: List[Field]): List[Field] = fields.flatMap(f => f :: allFields(f.properties))
}
