// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, ContractValidator, LogicalSchema}
import com.invaract.fingerprint.{TransformationFingerprint, TransformationFingerprinter}
import com.invaract.ir
import com.invaract.ir.PlanPrinter
import com.invaract.sparkadapter.notification.{ContractValidationEvent, NotificationSink}

import org.slf4j.LoggerFactory

/** Everything an engine adapter hands the pipeline about one write it has
  * translated, so that what happens next - every check, the fingerprint, the
  * published event, the rejection - is the same for every engine.
  *
  * @param plan the translated plan; its root is the `ir.Write` being checked.
  * @param inputSchemas every recognized read the write depends on, as
  *   (location, schema), already mapped to `LogicalSchema` by the adapter.
  * @param outputSchema the schema of the data being written (not of the
  *   engine's command node).
  * @param caseSensitive whether the engine matches column names
  *   case-sensitively (Spark: `spark.sql.caseSensitive`).
  * @param rowMutation the adapter's row-level DML classification of this plan,
  *   `None` when it is not DML at all.
  * @param lineageBoundaryTypes the `ir.UnknownPlan.sourceType`s this engine
  *   treats as an unresolved lineage boundary (see `StructuralVerifier.verify`).
  * @param resolutionNotes the adapter's caveats about lineage it had to assume
  *   when resolving boundaries (Spark: the `CheckpointResolution` diagnostics);
  *   disclosed with the fingerprint, since the hash cannot reflect them.
  */
final case class CheckedWrite(
    plan: ir.Plan,
    inputSchemas: List[(String, LogicalSchema)],
    outputSchema: LogicalSchema,
    caseSensitive: Boolean,
    rowMutation: Option[MutationClassification],
    lineageBoundaryTypes: Set[String],
    resolutionNotes: List[String] = Nil
)

/** The engine-neutral half of enforcement: given a contract and what an adapter
  * recognized about one operation, verify it, publish the outcome, and throw
  * `ContractViolationException` to abort it if it fails.
  *
  * An engine adapter is responsible for the engine-specific part only -
  * recognizing an operation, translating it into `ir.Plan`, mapping its schemas
  * into `LogicalSchema`, classifying DML - and then calls one of the three
  * entry points below. It does not re-implement validation, the verifier
  * ordering, fingerprinting, event publishing or the explanation text, so two
  * engines cannot drift apart on them. This is the code that used to live
  * inline in `spark-adapter`'s `ContractEnforcementRule.verifyOrThrow`.
  *
  *   - `verifyWrite` - a recognized write (`ir.Write`).
  *   - `verifyStateChange` - a state-changing, non-write operation that commits
  *     a schema change at a location (Spark: Iceberg `CALL` procedures).
  *   - `rejectUnverifiableWrite` - the fail-closed response to an operation
  *     that looks like it writes but the adapter could not translate.
  */
object VerificationPipeline {
  private val logger = LoggerFactory.getLogger(VerificationPipeline.getClass)

  /** Verifies `write` against `contract`, publishes a `ContractValidationEvent`
    * to `sink` when one is configured, and throws `ContractViolationException`
    * if any violation was found.
    *
    * `write` is by-name on purpose: the contract itself is validated *first*
    * (`requireValidContract`), and only then is the adapter asked to build the
    * `CheckedWrite`. Every check below assumes a structurally sound contract,
    * and building the write (gathering schemas, translating) can fail on a
    * plan the contract has no bearing on - an adapter must not be able to get
    * that order wrong by calling the two steps itself.
    */
  def verifyWrite(
      contract: Contract,
      write: => CheckedWrite,
      options: VerificationOptions,
      sink: Option[NotificationSink] = None,
      applicationId: Option[String] = None
  ): Unit = {
    requireValidContract(contract, sink, applicationId)
    val checked = write

    // The plan's shape (reads, unknown nodes, aggregates, joins, filters) is
    // gathered once here and shared by every verifier below, instead of each
    // walking the plan itself - see `PlanFacts`.
    val planFacts = PlanFacts.of(checked.plan)
    // Every rule/data-quality finding below is about this write.
    val writeLocation = PlanRuleVerifier.writeLocation(planFacts)

    val structuralResult =
      StructuralVerifier.verify(contract, planFacts, checked.inputSchemas, checked.outputSchema, options, checked.caseSensitive, checked.lineageBoundaryTypes)

    // Checked alongside (never instead of) StructuralVerifier's own checks.
    // `Extracted` runs the normal rule check; `Unverifiable` (the adapter
    // recognized the plan as DML of a given kind but could not extract what a
    // rule of that kind needs) fails closed instead of silently skipping the
    // rule, but only when the contract actually declares a rule that kind is
    // relevant to - RuleVerifier.anyRuleAppliesTo decides that (built-in or
    // custom rule types alike), so a DML this adapter can't fully verify
    // doesn't spuriously fail a contract that only declares a rule about
    // another kind.
    val ruleViolations = checked.rowMutation match {
      case Some(MutationClassification.Extracted(_, mutation)) =>
        RuleVerifier.verify(contract.rules, mutation, contract.customRuleTypes, writeLocation)
      case Some(MutationClassification.Unverifiable(kind)) =>
        if (RuleVerifier.anyRuleAppliesTo(contract.rules, kind, contract.customRuleTypes)) List(unverifiableDmlViolation(kind, writeLocation)) else Nil
      case None => Nil
    }
    // Independent of the DML-shaped ruleViolations above: PlanRuleVerifier
    // checks the other rule family (RuleType.PlanShapeTypes - grouping, join,
    // filter shape) against the whole translated plan, so it runs
    // unconditionally rather than being gated on `rowMutation`.
    val planRuleViolations = PlanRuleVerifier.verify(contract.rules, planFacts)

    // See docs/SEMANTIC_LINEAGE_FINGERPRINTING.md section 14.2: this is the
    // one branch with a real, complete ir.Plan in hand. The RowMutation (if
    // any) feeds the fingerprint too - a MERGE's ON condition, or a
    // conditional DELETE's predicate, is real transformation-defining
    // behavior that ir.Plan alone never captures. Only the Extracted case has
    // an actual RowMutation to pass.
    val fingerprints =
      if (options.computeFingerprint) {
        val mutation = checked.rowMutation.collect { case MutationClassification.Extracted(_, m) => m }
        // Disclosed, not silently absorbed: a boundary that stayed unresolved
        // can only be fingerprinted as an opaque node, and a resolution that
        // had to pick the most recent of several same-source plans is an
        // assumption. Either way the hash is still fully stable and
        // deterministic for this exact plan - it just may not reflect what
        // happened upstream of the boundary - so it is logged once per check,
        // at WARN, rather than left for a reader of the hash alone to discover.
        fingerprintCaveat(unresolvedBoundaryTypes(planFacts.unknownPlans, checked.lineageBoundaryTypes), checked.resolutionNotes).foreach(logger.warn)
        Some(TransformationFingerprinter.fingerprint(checked.plan, mutation))
      } else None

    // See VerificationOptions.staticDataQuality's own doc and
    // docs/STATIC_DATA_QUALITY_VERIFICATION.md: dataQualityResults is
    // report-only (every verdict, kept for VerificationResult.dataQuality),
    // while only its Violated entries become real Violations that can fail
    // this check and abort the write.
    val dataQualityResults = if (options.staticDataQuality) StaticDataQualityVerifier.verify(contract, planFacts) else Nil
    val dataQualityViolations = StaticDataQualityVerifier.violations(dataQualityResults, writeLocation)
    // See VerificationOptions.roleConsistency's own doc and
    // docs/CONTRACT_MODEL.md's "Input and Output Types" section: only
    // Contradicts entries become Violations; the rest are report-only.
    val roleConformanceResults = if (options.roleConsistency) RoleConsistencyVerifier.verify(contract, planFacts) else Nil
    val roleConsistencyViolations = RoleConsistencyVerifier.violations(contract, roleConformanceResults)

    val result = VerificationResult.of(
      structuralResult.contract,
      structuralResult.violations ++ ruleViolations ++ planRuleViolations ++ dataQualityViolations ++ roleConsistencyViolations,
      fingerprints,
      dataQualityResults,
      roleConformanceResults,
      structuralResult.unverifiableInputs
    )
    publishValidation(contract, result, sink, applicationId)
    if (!result.passed) {
      throw new ContractViolationException(result, explain(contract, checked.plan, result))
    }
  }

  /** The distinct `sourceType`s of `unknownPlans` the engine declared a lineage boundary, in
    * first-seen order - a plain unrecognized node is not a boundary and is not reported.
    */
  private[sparkadapter] def unresolvedBoundaryTypes(unknownPlans: List[ir.UnknownPlan], boundaryTypes: Set[String]): List[String] =
    unknownPlans.map(_.sourceType).filter(boundaryTypes.contains).distinct

  /** The WARN text disclosed alongside a fingerprint that cannot reflect everything
    * upstream of a lineage boundary, or `None` when there is nothing to disclose.
    * `unresolvedBoundaries` are the boundary node types that stayed opaque;
    * `resolutionNotes` the adapter's caveats about lineage it had to assume.
    */
  private[sparkadapter] def fingerprintCaveat(unresolvedBoundaries: List[String], resolutionNotes: List[String]): Option[String] =
    if (unresolvedBoundaries.isEmpty && resolutionNotes.isEmpty) None
    else
      Some(
        "computeFingerprint: this transformation's fingerprint is stable and deterministic for this exact plan " +
          "but may not reflect everything upstream of a lineage boundary (checkpoint/cache)" +
          (if (unresolvedBoundaries.isEmpty) "" else s"; unresolved boundary: ${unresolvedBoundaries.mkString(", ")}") +
          (if (resolutionNotes.isEmpty) "" else s"; ${resolutionNotes.distinct.mkString(" / ")}")
      )

  /** The state-changing, non-write counterpart of `verifyWrite`: an operation
    * with no query to translate that still commits a schema change at
    * `location` (Spark: Iceberg's `rollback_to_snapshot`). Checked against the
    * contract's declared output only - location scopes it (an operation on a
    * table the contract doesn't declare is simply not this contract's concern,
    * and passes), then the resulting schema is checked.
    *
    * @param description what the explanation shows in place of a plan tree
    *   (e.g. `CALL rollback_to_snapshot(...) targeting 'db.t'`).
    */
  def verifyStateChange(
      contract: Contract,
      description: String,
      location: String,
      resultingSchema: LogicalSchema,
      caseSensitive: Boolean,
      options: VerificationOptions,
      sink: Option[NotificationSink] = None,
      applicationId: Option[String] = None
  ): Unit = {
    // Same reasoning as verifyWrite: verifyStateChange assumes a structurally sound contract too.
    requireValidContract(contract, sink, applicationId)
    val result = StructuralVerifier.verifyStateChange(contract, location, resultingSchema, options, caseSensitive)
    publishValidation(contract, result, sink, applicationId)
    if (!result.passed) {
      // No ir.Plan translation exists for a state change (there's no query to
      // translate): a plain description stands in for the rendered plan tree.
      throw new ContractViolationException(result, explain(contract, ir.UnknownPlan(description), result))
    }
  }

  /** The fail-closed response: an operation that looks like it writes or
    * otherwise mutates data, but that the adapter has no translation for, so
    * it was never actually checked against the contract. Always throws -
    * `ViolationType.UnverifiableWrite`.
    *
    * @param operation the engine's own name for the operation (Spark: the
    *   command's class name), named in the violation.
    * @param translatedPlan what the adapter did produce for it (typically an
    *   `ir.UnknownPlan`), rendered in the explanation.
    */
  def rejectUnverifiableWrite(
      contract: Contract,
      operation: String,
      translatedPlan: ir.Plan,
      sink: Option[NotificationSink] = None,
      applicationId: Option[String] = None
  ): Nothing = {
    val violation = Violations.unverifiableWrite(operation, s"${contract.id}@${contract.version}")
    val result = VerificationResult.of(s"${contract.id}@${contract.version}", List(violation))
    publishValidation(contract, result, sink, applicationId)
    throw new ContractViolationException(result, explain(contract, translatedPlan, result))
  }

  /** Throws if `contract` itself is structurally unsound per
    * `ContractValidator` (e.g. no declared outputs), or names a custom rule
    * type whose class cannot be resolved - the check every other rejection in
    * this object assumes has already passed.
    */
  private[sparkadapter] def requireValidContract(contract: Contract, sink: Option[NotificationSink], applicationId: Option[String]): Unit = {
    val validation = ContractValidator.validate(contract)
    // ContractValidator only checks customRuleTypes's shape (empty key/class
    // name, collision with a built-in RuleType) - it lives in `contract`, which
    // can't depend on CustomRuleVerifier, so it can never actually resolve a
    // named class. Resolving each entry here, once per write, fails the same
    // way an unresolvable NotificationSink class or CustomPolicyEvaluator class
    // does: loudly, at validation time, before any rule check runs - rather
    // than RuleVerifier silently treating every rule naming that class as
    // inapplicable.
    val unresolvableCustomRuleTypes = contract.customRuleTypes.toList.flatMap { case (ruleType, className) =>
      CustomRuleVerifierFactory.tryResolve(className).failed.toOption.map(e => (ruleType, className, e.getMessage))
    }
    if (!validation.isValid || unresolvableCustomRuleTypes.nonEmpty) {
      val contractRef = s"${contract.id}@${contract.version}"
      val validatorViolations = validation.errors.map(issue => Violations.invalidContract(contractRef, issue.path, issue.message))
      val customRuleTypeViolations = unresolvableCustomRuleTypes.map { case (ruleType, className, message) =>
        Violations.unresolvableCustomRuleType(contractRef, ruleType, className, message)
      }
      val result = VerificationResult.of(contractRef, validatorViolations ++ customRuleTypeViolations)
      publishValidation(contract, result, sink, applicationId)
      // Reads as a plain sentence rather than a parenthesized fragment:
      // PlanPrinter already wraps an UnknownPlan as "UnknownPlan(<description>)",
      // so an inner "(...)" too would render as a confusing doubled "((...))".
      val describedPlan =
        ir.UnknownPlan("no transformation plan exists yet - contract validation failed before any plan was checked")
      throw new ContractViolationException(result, explain(contract, describedPlan, result))
    }
  }

  /** Publishes `result` as a `ContractValidationEvent` to `sink`, if one is configured. */
  private[sparkadapter] def publishValidation(
      contract: Contract,
      result: VerificationResult,
      sink: Option[NotificationSink],
      applicationId: Option[String]
  ): Unit =
    sink.foreach { s =>
      s.publish(
        ContractValidationEvent(
          contract = result.contract,
          status = result.status,
          violations = result.violations,
          timestamp = System.currentTimeMillis(),
          metadata = contract.extensions,
          applicationId = applicationId,
          fingerprints = result.fingerprints,
          dataQuality = result.dataQuality,
          roleConformance = result.roleConformance,
          unverifiableInputs = result.unverifiableInputs
        )
      )
    }

  /** The complete, human-readable explanation a `ContractViolationException`
    * carries: what the contract expected, what the plan contains, why it
    * violates the contract, and how to correct it - a developer reading only
    * this text, with no other context, should be able to answer all four.
    */
  private[sparkadapter] def explain(contract: Contract, plan: ir.Plan, result: VerificationResult): String = {
    val sb = new StringBuilder

    sb.append(s"Contract violation: '${result.contract}' rejected this transformation. Write aborted.\n")

    sb.append("\nWhat the contract expects:\n")
    contract.inputs.foreach { input =>
      sb.append(s"  input  '${input.name}' at ${input.location}: ${describeFields(input.schema.fields)}\n")
    }
    contract.outputs.foreach { output =>
      val lineage = output.derivedFrom match {
        case None                         => ""
        case Some(names) if names.isEmpty => " (derived from no declared input)"
        case Some(names)                  => s" (derived from ${names.mkString(", ")})"
      }
      sb.append(s"  output '${output.name}' at ${output.location}$lineage: ${describeFields(output.schema.fields)}\n")
    }

    sb.append("\nWhat the plan contains:\n")
    PlanPrinter.render(plan).linesIterator.foreach(line => sb.append("  ").append(line).append("\n"))

    sb.append(s"\nWhy it violates the contract (${result.violations.size} " + (if (result.violations.size == 1) "violation" else "violations") + "):\n")
    result.violations.zipWithIndex.foreach { case (v, i) =>
      sb.append(s"  ${i + 1}. [${v.violationType}] ${v.message}\n")
    }

    sb.append("\nHow to correct it:\n")
    result.violations.zipWithIndex.foreach { case (v, i) =>
      sb.append(s"  ${i + 1}. ${v.remediation}\n")
    }

    // Only present when VerificationOptions.computeFingerprint was true for
    // this check and a real plan existed to fingerprint - see
    // docs/SEMANTIC_LINEAGE_FINGERPRINTING.md section 14.4. Only each output's
    // combined hash is printed, and this never claims "changed"/"unchanged":
    // there is no prior fingerprint here to compare against.
    result.fingerprints.foreach(appendFingerprints(sb, _))

    sb.toString()
  }

  private def appendFingerprints(sb: StringBuilder, fingerprints: TransformationFingerprint): Unit = {
    sb.append(s"\nFingerprints (v${fingerprints.version}, ${fingerprints.overall.algorithm}):\n")
    sb.append(s"  overall: ${fingerprints.overall.value}\n")
    if (fingerprints.outputs.nonEmpty) {
      sb.append("  outputs:\n")
      fingerprints.outputs.toList.sortBy(_._1).foreach { case (name, output) =>
        sb.append(s"    $name: ${output.combined.value}\n")
      }
    }
  }

  private def describeFields(fields: List[com.invaract.contract.Field]): String =
    fields
      .map(f => s"${f.name}: ${f.fieldType}" + (if (f.required) "" else " (optional)"))
      .mkString(", ")

  private def unverifiableDmlViolation(kind: MutationKind, location: Option[String]): Violation = {
    val kindName = kind match {
      case MutationKind.Merge  => "MERGE"
      case MutationKind.Update => "UPDATE"
      case MutationKind.Delete => "DELETE"
    }
    Violations.unverifiableDml(kindName, location)
  }
}
