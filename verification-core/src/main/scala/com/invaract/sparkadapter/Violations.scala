// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{DatasetType, Dataset}
import com.invaract.sparkadapter.SchemaChecker.Side

/** The one place a `Violation` is built: a named constructor per kind of finding,
  * so a kind's *shape* (which of `column`/`location`/`expected`/`actual` it fills)
  * and its wording are each decided once, not re-decided by whichever verifier
  * happens to raise it. `Violation` itself stays an open case class (it crosses a
  * JSON boundary and is public API), but code in this module builds violations
  * only through here; `ViolationsSpec` checks every constructor against the table
  * below and every `ViolationType` against the docs.
  *
  * ## What each kind carries
  *
  * `location` always means "the dataset this finding is about": the declared
  * location for a finding about a declared dataset that has no actual
  * counterpart (a missing input/output, an input's schema or catalog), and the
  * write's actual location for a finding about the write (an output's location,
  * format, save mode, catalog, schema, and every rule, data-quality and
  * unverifiable-operation finding). `column` is a field path (`address.zip` for a
  * nested one). `expected`/`actual` are each filled when the finding has that
  * value to report — a comparison fills both; a missing field reports only the
  * `expected` type, a stray column only its `actual` type. `rule` names the
  * contract rule type (or org policy id) that raised the finding.
  *
  * Rule findings are built here without `location`/`rule` — only the caller
  * (`RuleVerifier`/`PlanRuleVerifier`) knows the write and which rule it was
  * running, so it stamps them (`RuleVerifier.stamp`), custom rule verifiers' too.
  *
  * {{{
  * kind                                   location  column  expected / actual              rule
  * MISSING_INPUT                          declared  -       -                              -
  * UNDECLARED_INPUT                       read      -       -                              -
  * MISSING_OUTPUT                         declared  -       -                              -
  * OUTPUT_LOCATION_MISMATCH               write     -       declared locations / write     -
  * OUTPUT_FORMAT_MISMATCH                 write     -       format / format                -
  * OUTPUT_SAVE_MODE_MISMATCH              write     -       save mode / save mode          -
  * *_CATALOG_REGISTRATION / *_MISMATCH    dataset   -       (mismatch only) catalog / catalog  -
  * MISSING_*_FIELD                        dataset   path    declared type / -              -
  * *_UNDECLARED_COLUMN                    dataset   path    - / actual type                -
  * *_FIELD_TYPE_MISMATCH                  dataset   path    declared type / actual type    -
  * *_FIELD_NULLABILITY_MISMATCH           dataset   path    "not null" / "nullable"        -
  * ROLE_CONSISTENCY_VIOLATION             input     -       -                              -
  * DATA_QUALITY_VIOLATION                 write     field   the declared constraint / -    -
  * RULE_* (plan and DML rules)             write     -       the rule's columns / what the plan has  rule type
  * RULE_UNVERIFIABLE_DML                  write     -       - / the operation kind         -
  * ORG_POLICY_VIOLATION                   dataset   -       -                              policy id
  * UNVERIFIABLE_WRITE                     -         -       - / the command class          -
  * INVALID_CONTRACT                       -         -       -                              -
  * UNSUPPORTED_CONTRACT_FEATURE           -         -       capability id / unsupported by <adapter>  -
  * }}}
  */
private[sparkadapter] object Violations {

  // ---- inputs ---------------------------------------------------------------------------------

  /** `nudgeTowardDerivedFrom`: the write's output declares no `derivedFrom` in a
    * multi-output contract, so say where an input that feeds only some outputs belongs.
    */
  def missingInput(input: Dataset, nudgeTowardDerivedFrom: Boolean): Violation =
    Violation(
      ViolationType.MissingInput,
      s"declared input '${input.name}' (${input.location}) was not read by this plan",
      remediation =
        s"Add a read of '${input.location}' to the transformation, or remove '${input.name}' from the contract's inputs if it is no longer needed." +
          (if (nudgeTowardDerivedFrom)
             s" If '${input.name}' feeds only some of this contract's outputs, list the inputs each output is built from in that output's 'derivedFrom' instead."
           else ""),
      location = Some(input.location)
    )

  /** A read of a location the contract does not declare as an input at all. */
  def undeclaredInput(readLocation: String): Violation =
    Violation(
      ViolationType.UndeclaredInput,
      s"plan reads '$readLocation' which is not declared as a contract input",
      remediation = s"Declare '$readLocation' as an input in the contract, or remove this read from the transformation.",
      location = Some(readLocation)
    )

  /** A read of an input the contract declares, but not as a source of the output being written. */
  def undeclaredInputNotSourceOf(readLocation: String, declared: Dataset, output: Dataset): Violation =
    Violation(
      ViolationType.UndeclaredInput,
      s"plan reads '$readLocation' (input '${declared.name}'), which this contract declares but not as a source of " +
        s"output '${output.name}' (its derivedFrom lists ${output.derivedFrom.filter(_.nonEmpty).map(_.map(n => s"'$n'").mkString(", ")).getOrElse("no inputs")})",
      remediation =
        s"Add '${declared.name}' to output '${output.name}''s derivedFrom if it is genuinely one of its sources, or remove this read from the transformation.",
      location = Some(readLocation)
    )

  // ---- outputs --------------------------------------------------------------------------------

  def missingOutput(output: Dataset): Violation =
    Violation(
      ViolationType.MissingOutput,
      s"the plan does not produce a write; expected output '${output.name}' (${output.location})",
      remediation = s"Add a write to '${output.location}' to the transformation.",
      location = Some(output.location)
    )

  def outputLocationMismatch(declaredLocations: List[String], actualLocation: String): Violation = {
    val single = declaredLocations.size == 1
    Violation(
      ViolationType.OutputLocationMismatch,
      if (single)
        s"contract declares output location '${declaredLocations.head}' but the plan writes to '$actualLocation'"
      else
        s"the plan writes to '$actualLocation', which does not match any of the contract's " +
          s"${declaredLocations.size} declared output locations (${declaredLocations.mkString(", ")})",
      remediation =
        if (single)
          s"Write to '${declaredLocations.head}' instead, or update the contract's declared output location to '$actualLocation' if this location change is intentional."
        else
          s"Write to one of the contract's declared output locations (${declaredLocations.mkString(", ")}) instead, or add '$actualLocation' as a new declared output if this is intentional.",
      location = Some(actualLocation),
      expected = Some(declaredLocations.mkString(", ")),
      actual = Some(actualLocation)
    )
  }

  def outputFormatMismatch(expected: String, actual: String, writeLocation: String): Violation =
    Violation(
      ViolationType.OutputFormatMismatch,
      s"contract declares output format '$expected' but the plan writes in format '$actual'",
      remediation =
        s"Write in '$expected' format instead, or update the contract's declared format to '$actual' if this format change is intentional.",
      location = Some(writeLocation),
      expected = Some(expected),
      actual = Some(actual)
    )

  def outputSaveModeMismatch(expected: String, actual: String, writeLocation: String): Violation =
    Violation(
      ViolationType.OutputSaveModeMismatch,
      s"contract declares output save mode '$expected' but the plan writes with save mode '$actual'",
      remediation =
        s"Write with save mode '$expected' instead, or update the contract's declared saveMode to '$actual' if this change is intentional.",
      location = Some(writeLocation),
      expected = Some(expected),
      actual = Some(actual)
    )

  // ---- catalog registration (inputs and outputs) -------------------------------------------------

  def missingCatalogRegistration(side: Side, location: String): Violation =
    Violation(
      if (side == Side.Input) ViolationType.MissingInputCatalogRegistration else ViolationType.MissingOutputCatalogRegistration,
      s"contract requires the ${side.noun} at '$location' to be registered in a catalog, but it has no catalog registration",
      remediation =
        s"Register '$location' in a catalog (e.g. CREATE EXTERNAL TABLE, .saveAsTable(), or a DSv2 catalog read/write) instead of a bare path, or set catalog.required to false in the contract if registration isn't actually required.",
      location = Some(location)
    )

  /** `mismatches`: one description per disagreeing sub-field; `expected`/`actual`: the declared and actual identities. */
  def catalogMismatch(side: Side, location: String, mismatches: List[String], expected: String, actual: String): Violation =
    Violation(
      if (side == Side.Input) ViolationType.InputCatalogMismatch else ViolationType.OutputCatalogMismatch,
      s"contract's declared catalog registration for the ${side.noun} at '$location' does not match the actual registration: ${mismatches
        .mkString("; ")}",
      remediation =
        s"Update the catalog registration for '$location' to match the contract's declared catalog fields, or update the contract if this change is intentional.",
      location = Some(location),
      expected = Some(expected),
      actual = Some(actual)
    )

  // ---- schema (inputs and outputs) ---------------------------------------------------------------

  def missingField(side: Side, location: String, path: String, fieldType: String): Violation =
    Violation(
      side.missingField,
      s"required field '$path' is absent from the actual ${side.label} schema",
      remediation =
        s"Add a '$path' column (type '$fieldType') to the ${side.noun}, or mark it optional in the contract if it isn't always produced.",
      column = Some(path),
      location = Some(location),
      expected = Some(fieldType)
    )

  def fieldTypeMismatch(side: Side, location: String, path: String, declaredType: String, actualType: String): Violation =
    Violation(
      side.typeMismatch,
      s"field '$path' declares type '$declaredType' but the actual ${side.label} schema has type '$actualType'",
      remediation =
        s"Cast '$path' to '$declaredType' in the transformation, or update the contract to declare '$actualType' if the new type is intentional.",
      column = Some(path),
      location = Some(location),
      expected = Some(declaredType),
      actual = Some(actualType)
    )

  def fieldNullabilityMismatch(side: Side, location: String, path: String): Violation =
    Violation(
      side.nullabilityMismatch,
      s"field '$path' is declared non-nullable but the actual ${side.label} schema permits nulls",
      remediation =
        s"Filter or coalesce nulls out of '$path' before the ${side.noun} is produced, or relax the contract to allow nulls if they're expected.",
      column = Some(path),
      location = Some(location),
      expected = Some("not null"),
      actual = Some("nullable")
    )

  /** `actualType`: the stray column's type as Spark reports it. */
  def undeclaredColumn(side: Side, location: String, path: String, actualType: String): Violation =
    Violation(
      side.undeclaredColumn,
      s"column '$path' is present in the actual ${side.label} schema but not declared by the contract",
      remediation =
        s"Remove '$path' from the transformation's ${side.noun}, or add it to the contract's declared schema if it's intentional.",
      column = Some(path),
      location = Some(location),
      actual = Some(actualType)
    )

  // ---- row-level DML rules ------------------------------------------------------------------------

  def mergeConditionRule(declaredColumns: List[String], missing: List[String], pairedColumns: Set[String]): Violation =
    Violation(
      ViolationType.RuleMergeConditionViolation,
      s"contract requires the MERGE to match on ${declaredColumns.mkString(", ")}, but its ON condition " +
        s"does not include an equality match on ${missing.mkString(", ")}",
      remediation =
        s"Add a 'target.${missing.head} = source.${missing.head}'-style equality to the MERGE's ON " +
          s"condition for ${missing.mkString(", ")}, or update the contract's merge_condition rule if " +
          "matching on fewer columns is intentional.",
      expected = Some(declaredColumns.mkString(", ")),
      actual = Some(pairedColumns.mkString(", "))
    )

  def unconditionalDeleteRule(): Violation =
    Violation(
      ViolationType.RuleUnconditionalDelete,
      "contract forbids an unconditional DELETE, but this operation deletes every row it reaches with no filtering predicate",
      remediation =
        "Add a WHERE predicate to the DELETE, or remove the forbid_unconditional_delete rule if deleting every row is intentional."
    )

  def disallowedUpdateColumnRule(allowedColumns: List[String], disallowed: List[String], updatedColumns: List[String]): Violation =
    Violation(
      ViolationType.RuleDisallowedUpdateColumn,
      s"contract only allows UPDATE to assign ${allowedColumns.mkString(", ")}, but this operation also assigns ${disallowed.mkString(", ")}",
      remediation =
        s"Remove ${disallowed.mkString(", ")} from the UPDATE's SET clause, or add ${disallowed.mkString(", ")} " +
          "to the contract's allowed_update_columns rule if assigning them is intentional.",
      expected = Some(allowedColumns.mkString(", ")),
      actual = Some(updatedColumns.mkString(", "))
    )

  /** `location`: the write the operation targets; `kindName` is also reported as `actual`. */
  def unverifiableDml(kindName: String, location: Option[String]): Violation =
    Violation(
      ViolationType.RuleUnverifiableDml,
      s"this operation is a $kindName the active contract declares a rule for, but Invaract could not " +
        "extract the structural fact that rule needs to check, so it was never actually verified.",
      remediation =
        "This is likely a genuine gap in Invaract's support for this operation's exact shape (e.g. an " +
          "Iceberg merge-on-read UPDATE, whose rewritten plan doesn't expose which columns changed) - open " +
          "an issue/PR. If the rule doesn't need to apply to this operation, remove it from the contract.",
      location = location,
      actual = Some(kindName)
    )

  // ---- plan-shape rules ----------------------------------------------------------------------------

  /** `groupings`: for each aggregation in the plan, the columns it groups by; empty when the plan has none. */
  def requiredGroupByRule(columns: List[String], groupings: List[Set[String]]): Violation =
    Violation(
      ViolationType.RuleRequiredGroupByViolation,
      if (groupings.isEmpty)
        s"contract requires the output to be grouped by ${columns.mkString(", ")}, but the plan performs no aggregation at all"
      else
        s"contract requires the output to be grouped by ${columns.mkString(", ")}, but no aggregation in the plan groups by all of them",
      remediation =
        s"Add a GROUP BY on ${columns.mkString(", ")} to the transformation, or update the contract's " +
          "required_group_by rule if grouping by fewer/different columns is intentional.",
      expected = Some(columns.mkString(", ")),
      actual = Some(if (groupings.isEmpty) "no aggregation" else groupings.map(_.mkString("+")).mkString("; "))
    )

  def crossJoinRule(conditionlessJoins: Int): Violation =
    Violation(
      ViolationType.RuleCrossJoinViolation,
      s"contract forbids a cartesian-product join, but the plan contains $conditionlessJoins join(s) with no condition",
      remediation =
        "Add a join condition to every join in the transformation, or remove the forbid_cross_join rule if a cartesian product is intentional."
    )

  def requiredJoinColumnsRule(columns: List[String], planHasAnyJoin: Boolean): Violation =
    Violation(
      ViolationType.RuleRequiredJoinColumnsViolation,
      if (!planHasAnyJoin)
        s"contract requires a join matching on ${columns.mkString(", ")}, but the plan contains no join at all"
      else
        s"contract requires a join matching on ${columns.mkString(", ")}, but no join's condition establishes an equality match on all of them",
      remediation =
        s"Add a 'left.${columns.head} = right.${columns.head}'-style equality to a join's condition for " +
          s"${columns.mkString(", ")}, or update the contract's required_join_columns rule if matching on " +
          "fewer/different columns is intentional.",
      expected = Some(columns.mkString(", "))
    )

  def requiredFilterColumnsRule(columns: List[String], missing: List[String], filteredColumns: Set[String]): Violation =
    Violation(
      ViolationType.RuleRequiredFilterColumnsViolation,
      s"contract requires the plan to filter on ${columns.mkString(", ")}, but no filter references ${missing.mkString(", ")}",
      remediation =
        s"Add a filter referencing ${missing.mkString(", ")} to the transformation, or update the contract's " +
          "required_filter_columns rule if filtering on fewer columns is intentional.",
      expected = Some(columns.mkString(", ")),
      actual = Some(filteredColumns.mkString(", "))
    )

  // ---- static analysis ------------------------------------------------------------------------------

  /** `location`: the output the field belongs to; `constraint` (the declared property) is also reported as `expected`. */
  def dataQuality(field: String, constraint: String, location: Option[String]): Violation =
    Violation(
      ViolationType.DataQualityViolation,
      s"the transformation's own semantics prove that output field '$field' cannot always satisfy its declared $constraint property",
      remediation =
        s"Review the transformation logic producing '$field' — it can produce a value that violates the contract's declared constraint. " +
          "If the constraint is no longer correct, relax or remove it from the contract instead.",
      column = Some(field),
      location = location,
      expected = Some(constraint)
    )

  /** `inputName`/`inputLocation`: the declared input whose observed role contradicts its declared type. */
  def roleConsistency(inputName: String, inputLocation: String, datasetType: DatasetType, detail: String): Violation =
    Violation(
      ViolationType.RoleConsistencyViolation,
      s"input '$inputName' is declared ${datasetType.name} but $detail",
      remediation =
        s"Review the transformation logic deriving output data from '$inputName' - either declare it " +
          "DATA_ASSET/SOURCE instead of CONTROL if it genuinely is substantive business data, or remove the output " +
          "column(s) that derive from it if it is meant to remain control-only.",
      location = Some(inputLocation)
    )

  // ---- the whole contract or operation (no dataset, column or comparison) ----------------------------

  def invalidContract(contractRef: String, path: String, message: String): Violation =
    Violation(
      ViolationType.InvalidContract,
      s"contract '$contractRef' is invalid at '$path': $message",
      remediation = s"Fix the contract document (see the '$path' issue above) so it passes " +
        "ContractValidator.validate before it's used to verify any write."
    )

  def unresolvableCustomRuleType(contractRef: String, ruleType: String, className: String, reason: String): Violation =
    Violation(
      ViolationType.InvalidContract,
      s"contract '$contractRef' declares customRuleTypes['$ruleType'] = '$className', which could not be resolved: $reason",
      remediation = s"Fix or remove the customRuleTypes['$ruleType'] entry so '$className' names a class on the " +
        "classpath implementing CustomRuleVerifier with a public no-arg constructor."
    )

  def invalidInferredContract(path: String, message: String): Violation =
    Violation(
      ViolationType.InvalidContract,
      s"the inferred contract is invalid at '$path': $message",
      "Report this: an inferred contract should always be structurally valid."
    )

  /** `location`: the declared location of the dataset the policy found fault with, when it names one;
    * `policyId`: the policy that was violated (reported as `rule`).
    */
  def orgPolicy(message: String, remediation: String, location: Option[String], policyId: String): Violation =
    Violation(ViolationType.OrgPolicyViolation, message, remediation, location = location, rule = Some(policyId))

  /** `commandClassName` is also reported as `actual`. */
  def unverifiableWrite(commandClassName: String, contractRef: String): Violation =
    Violation(
      ViolationType.UnverifiableWrite,
      s"'$commandClassName' looks like it may write or otherwise mutate data, but Invaract has no " +
        s"translation for it, so it was never checked against contract '$contractRef'.",
      remediation =
        "If this command genuinely doesn't write data, add its class to FailClosedCommands' known-safe list " +
          "(with the same reasoning documented there) and open an issue/PR. If it does write data, that's a " +
          "real translation gap in SparkPlanAdapter - see docs/SPARK_ADAPTER.md's " +
          "\"Fail-closed on unverifiable writes\" section.",
      actual = Some(commandClassName)
    )

  /** `capability` is also reported as `expected`, and the declaring adapter as `actual`. `why` is
    * what in the contract needs it; `reason` is the adapter's own stated reason for not supporting it.
    */
  def unsupportedContractFeature(adapter: String, capability: Capability, why: String, reason: Option[String]): Violation =
    Violation(
      ViolationType.UnsupportedContractFeature,
      s"the contract relies on '${capability.id}' ($why), but the '$adapter' adapter declares it unsupported" +
        reason.map(r => s": $r").getOrElse("") + ", so it would not be verified.",
      remediation =
        s"Remove what needs '${capability.id}' from the contract (or turn off the option that asks for it), or run this " +
          "job through an adapter that supports it - see the engine capability matrix in the docs (reference/engine-capabilities). " +
          "A requirement the adapter cannot verify is never passed as if it had been.",
      expected = Some(capability.id),
      actual = Some(s"unsupported by $adapter")
    )
}
