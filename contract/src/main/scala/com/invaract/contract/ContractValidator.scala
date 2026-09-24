// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

sealed trait ValidationSeverity
object ValidationSeverity {
  case object Error extends ValidationSeverity
  case object Warning extends ValidationSeverity
}

case class ValidationIssue(severity: ValidationSeverity, path: String, message: String) {
  override def toString: String = s"[$severity] $path: $message"
}

case class ValidationResult(issues: List[ValidationIssue]) {
  def errors: List[ValidationIssue] = issues.filter(_.severity == ValidationSeverity.Error)
  def warnings: List[ValidationIssue] = issues.filter(_.severity == ValidationSeverity.Warning)
  def isValid: Boolean = errors.isEmpty
}

/** Validates that a parsed [[Contract]] is structurally sound.
  *
  * This is distinct from parsing: `ContractParser` fails fast on documents
  * it cannot interpret at all (missing required YAML keys). `ContractValidator`
  * runs on an already-parsed `Contract` and reports every issue it can find
  * in one pass (duplicate fields, contradictory flags, unknown types) rather
  * than stopping at the first problem.
  */
object ContractValidator {
  private val IdPattern = """^[a-zA-Z][a-zA-Z0-9_.-]*$""".r

  private val KnownTypes = Set(
    "string", "integer", "long", "short", "byte", "double", "float", "decimal",
    "boolean", "date", "timestamp", "binary", "struct", "array", "map"
  )

  def validate(contract: Contract): ValidationResult = {
    val issues = List.newBuilder[ValidationIssue]

    if (contract.id.trim.isEmpty) {
      issues += ValidationIssue(ValidationSeverity.Error, "id", "Contract id must not be empty")
    } else if (IdPattern.findFirstIn(contract.id).isEmpty) {
      issues += ValidationIssue(
        ValidationSeverity.Warning,
        "id",
        s"Contract id '${contract.id}' should start with a letter and contain only " +
          "alphanumerics, '.', '_', '-'"
      )
    }

    if (contract.outputs.isEmpty) {
      issues += ValidationIssue(
        ValidationSeverity.Error,
        "outputs",
        "Contract must declare at least one output dataset"
      )
    }

    val namedInputs = contract.inputs.zipWithIndex.map { case (d, i) => (s"inputs[$i]", d) }
    val namedOutputs = contract.outputs.zipWithIndex.map { case (d, i) => (s"outputs[$i]", d) }

    namedInputs.foreach { case (path, dataset) => issues ++= validateDataset(path, dataset) }
    namedOutputs.foreach { case (path, dataset) =>
      issues ++= validateDataset(path, dataset)
      // SOURCE means "data entering this contract's pipeline that this
      // contract does not claim responsibility for producing" - declaring
      // one as this same contract's own output contradicts that role by
      // definition (see docs/CONTRACT_MODEL.md's "Input and Output Types"
      // section: "a contract... does not define the SOURCE as an output
      // produced by that contract"). A Warning, not an Error: nothing in
      // this module or StructuralVerifier is actually broken by it, the
      // same "structurally fine, semantically questionable" treatment the
      // required-but-nullable Field check below already gets.
      if (dataset.datasetType.contains(DatasetType.Source)) {
        issues += ValidationIssue(
          ValidationSeverity.Warning,
          s"$path.type",
          s"Output dataset '${dataset.name}' is declared SOURCE, but a SOURCE represents data entering the " +
            "pipeline from outside this contract's own responsibility - it should not be declared as this " +
            "contract's own output. Did you mean DATA_ASSET or CONTROL?"
        )
      }
    }

    val duplicateInputs = duplicateNames(contract.inputs.map(_.name))
    duplicateInputs.foreach { name =>
      issues += ValidationIssue(ValidationSeverity.Error, "inputs", s"Duplicate input dataset name '$name'")
    }

    val duplicateOutputs = duplicateNames(contract.outputs.map(_.name))
    duplicateOutputs.foreach { name =>
      issues += ValidationIssue(ValidationSeverity.Error, "outputs", s"Duplicate output dataset name '$name'")
    }

    // A Warning, not an Error: `spark-adapter`'s StructuralVerifier matches
    // a plan's actual write against whichever declared output shares its
    // location (see its "Multi-output contracts" doc), so two outputs
    // declaring the same location are ambiguous - StructuralVerifier picks
    // whichever comes first rather than rejecting the contract outright,
    // and this warns the author rather than silently accepting it.
    val duplicateOutputLocations = duplicateNames(contract.outputs.map(_.location))
    duplicateOutputLocations.foreach { location =>
      issues += ValidationIssue(
        ValidationSeverity.Warning,
        "outputs",
        s"Multiple outputs declare the same location '$location'; verification will match whichever is declared first"
      )
    }

    contract.rules.zipWithIndex.foreach { case (rule, idx) =>
      if (rule.ruleType.trim.isEmpty) {
        issues += ValidationIssue(ValidationSeverity.Error, s"rules[$idx]", "Rule type must not be empty")
      } else if (RuleType.All.contains(rule.ruleType) && rule.interpret.isEmpty) {
        issues += ValidationIssue(
          ValidationSeverity.Error,
          s"rules[$idx]",
          s"Rule type '${rule.ruleType}' has malformed or missing properties for its shape" + ruleHint(rule.ruleType)
        )
      }
      // Deliberately no "unrecognized ruleType has no customRuleTypes
      // entry" Warning here, unlike OrgPolicyValidator's identical-shaped
      // check for customPolicyTypes: unlike org policy's seven-type closed
      // set, a contract's own `rules:` is documented (see this file's own
      // class doc, and docs/CONTRACT_MODEL.md's "Interpreted rules"
      // section) as routinely carrying rule types Invaract doesn't
      // interpret at all - `compatibility` (versioning mode) is a real,
      // pervasive example, present in essentially every demo/fixture
      // contract in this repo without ever being a mistake. Flagging that
      // as "will never be evaluated" would be a false positive against
      // this module's own established, intentional design - confirmed
      // directly: adding that check broke a real "well-formed contract,
      // no warnings" test over a `compatibility` rule, not a hypothetical.
    }

    // Only this module's own shape can be checked here: an empty
    // ruleType/class name, or a ruleType colliding with a built-in
    // RuleType (dead - RuleVerifier's own built-in lookup always wins,
    // see spark-adapter's RuleVerifier for why). Whether a named class
    // actually resolves needs CustomRuleVerifierFactory, which lives in
    // spark-adapter - this module has no Spark dependency to reach it
    // with, so that check happens there instead, the same moment
    // spark-adapter's ContractEnforcementRule already runs this
    // validator (see Contract.customRuleTypes's own doc).
    contract.customRuleTypes.toList.sortBy(_._1).foreach { case (ruleType, className) =>
      val path = s"customRuleTypes.$ruleType"
      if (ruleType.trim.isEmpty) {
        issues += ValidationIssue(ValidationSeverity.Error, "customRuleTypes", "A customRuleTypes key must not be empty")
      } else if (RuleType.All.contains(ruleType)) {
        issues += ValidationIssue(
          ValidationSeverity.Warning,
          path,
          s"customRuleTypes entry for '$ruleType' is dead - it's already a built-in RuleType, which always takes precedence"
        )
      }
      if (className.trim.isEmpty) {
        issues += ValidationIssue(ValidationSeverity.Error, path, "A customRuleTypes class name must not be empty")
      }
    }

    ValidationResult(issues.result())
  }

  private def validateDataset(path: String, dataset: Dataset): List[ValidationIssue] = {
    val issues = List.newBuilder[ValidationIssue]

    if (dataset.name.trim.isEmpty) {
      issues += ValidationIssue(ValidationSeverity.Error, s"$path.name", "Dataset name must not be empty")
    }

    if (dataset.location.trim.isEmpty) {
      issues += ValidationIssue(ValidationSeverity.Error, s"$path.location", "Dataset location must not be empty")
    }

    dataset.catalog.foreach { catalog =>
      val declaresIdentity =
        catalog.technology.isDefined || catalog.catalogName.isDefined || catalog.location.isDefined ||
          catalog.namespace.nonEmpty || catalog.table.isDefined
      if (!catalog.required && declaresIdentity) {
        issues += ValidationIssue(
          ValidationSeverity.Warning,
          s"$path.catalog",
          "catalog.required is false but an expected catalog identity is declared; it will not be " +
            "checked against anything until 'required' is set to true"
        )
      }
    }

    if (dataset.schema.fields.isEmpty) {
      issues += ValidationIssue(ValidationSeverity.Error, s"$path.schema", "Schema must declare at least one field")
    }

    duplicateNames(dataset.schema.fields.map(_.name)).foreach { name =>
      issues += ValidationIssue(ValidationSeverity.Error, s"$path.schema", s"Duplicate field name '$name'")
    }

    val topLevelFieldsByName: Map[String, Field] = dataset.schema.fields.map(f => f.name -> f).toMap
    dataset.schema.fields.foreach { field =>
      issues ++= validateField(s"$path.schema.${field.name}", field, Some(topLevelFieldsByName - field.name))
    }

    issues.result()
  }

  /** `siblingFields`: the other top-level fields of this same dataset,
    * keyed by name, available for a `fieldRange` constraint to reference —
    * `None` for a nested field (recursed into via `field.properties`
    * below), since cross-field comparison is only ever resolved by
    * `StaticDataQualityVerifier` for a top-level output field (see
    * `validateFieldRange`'s own doc).
    */
  private def validateField(path: String, field: Field, siblingFields: Option[Map[String, Field]]): List[ValidationIssue] = {
    val issues = List.newBuilder[ValidationIssue]

    if (field.name.trim.isEmpty) {
      issues += ValidationIssue(ValidationSeverity.Error, path, "Field name must not be empty")
    }

    if (field.fieldType.trim.isEmpty) {
      issues += ValidationIssue(ValidationSeverity.Error, path, "Field type must not be empty")
    } else if (!field.isStruct && !KnownTypes.contains(field.fieldType.toLowerCase)) {
      issues += ValidationIssue(
        ValidationSeverity.Warning,
        path,
        s"Unrecognized field type '${field.fieldType}'. Known types: ${KnownTypes.mkString(", ")}"
      )
    }

    if (field.required && field.nullable) {
      issues += ValidationIssue(
        ValidationSeverity.Warning,
        path,
        "Field is marked required but also nullable; a required field should not be nullable"
      )
    }

    duplicateNames(field.properties.map(_.name)).foreach { name =>
      issues += ValidationIssue(ValidationSeverity.Error, path, s"Duplicate nested field name '$name'")
    }

    field.properties.foreach { nested =>
      issues ++= validateField(s"$path.${nested.name}", nested, None)
    }

    field.constraints.zipWithIndex.foreach { case (constraint, idx) =>
      issues ++= validateFieldConstraint(s"$path.constraints[$idx]", field, constraint, siblingFields)
    }

    issues.result()
  }

  /** Mirrors `validate`'s own `rules`-malformed-properties check, scoped to
    * one field's `constraints` — see
    * docs/STATIC_DATA_QUALITY_VERIFICATION.md §5.
    */
  private def validateFieldConstraint(path: String, field: Field, constraint: FieldConstraint, siblingFields: Option[Map[String, Field]]): List[ValidationIssue] = {
    val issues = List.newBuilder[ValidationIssue]

    if (constraint.constraintType.trim.isEmpty) {
      issues += ValidationIssue(ValidationSeverity.Error, path, "Constraint type must not be empty")
    } else if (FieldConstraintType.All.contains(constraint.constraintType) && constraint.interpret.isEmpty) {
      issues += ValidationIssue(
        ValidationSeverity.Error,
        path,
        s"Constraint type '${constraint.constraintType}' has malformed or missing properties for its shape" + fieldConstraintHint(constraint.constraintType)
      )
    }
    // Deliberately no "unrecognized constraintType" warning here, mirroring
    // rules' own precedent above: a constraints entry outside this closed
    // set is simply never interpreted, not necessarily a mistake.

    (constraint.constraintType, constraint.interpret) match {
      case (FieldConstraintType.Equals, Some(InterpretedFieldConstraint.Equals(_, literalType))) if !typesCompatible(field.fieldType, literalType) =>
        issues += ValidationIssue(
          ValidationSeverity.Warning,
          path,
          s"constraint declares a value of type '$literalType' but the field's own declared type is '${field.fieldType}'"
        )
      case (FieldConstraintType.OneOf, Some(InterpretedFieldConstraint.OneOf(_, literalType))) if !typesCompatible(field.fieldType, literalType) =>
        issues += ValidationIssue(
          ValidationSeverity.Warning,
          path,
          s"constraint declares values of type '$literalType' but the field's own declared type is '${field.fieldType}'"
        )
      case (FieldConstraintType.Length, Some(InterpretedFieldConstraint.Length(_, _, _))) if field.fieldType.toLowerCase != "string" =>
        issues += ValidationIssue(
          ValidationSeverity.Warning,
          path,
          s"a length constraint is declared but the field's own declared type is '${field.fieldType}', not 'string'"
        )
      case (FieldConstraintType.FieldRange, Some(fr @ InterpretedFieldConstraint.FieldRange(_, _, _, _))) =>
        issues ++= validateFieldRange(path, field, fr, siblingFields)
      case _ => ()
    }

    issues.result()
  }

  /** `fieldRange`'s own checks beyond "well-formed shape" (already covered
    * above by the generic malformed-properties check): the constrained
    * field's own declared type must be numeric (the only kind
    * `StaticDataQualityVerifier` can ever prove a cross-field bound for —
    * see docs/STATIC_DATA_QUALITY_VERIFICATION.md), each referenced field
    * must actually exist as a *sibling* (same dataset, same nesting level)
    * and must itself be numeric, and a field referencing itself is
    * degenerate (never a meaningful comparison). `siblingFields = None`
    * (a nested field) can't be resolved at all — see this constraint's own
    * doc for why cross-field comparison is scoped to top-level fields.
    * Every issue here is a Warning, not an Error: exactly like `Length` on
    * a non-string field, none of these make the contract structurally
    * invalid — `StaticDataQualityVerifier` degrades safely to
    * `NotStaticallyVerifiable` for all of them, never a crash or a false
    * guarantee.
    */
  private def validateFieldRange(
    path: String,
    field: Field,
    fr: InterpretedFieldConstraint.FieldRange,
    siblingFields: Option[Map[String, Field]]
  ): List[ValidationIssue] = {
    val issues = List.newBuilder[ValidationIssue]

    if (!NumericFieldTypes.contains(field.fieldType.toLowerCase)) {
      issues += ValidationIssue(
        ValidationSeverity.Warning,
        path,
        s"a fieldRange constraint is declared but the field's own declared type is '${field.fieldType}', not a numeric type"
      )
    }

    val referencedNames = List(fr.gte, fr.gt, fr.lte, fr.lt).flatten
    siblingFields match {
      case None =>
        issues += ValidationIssue(
          ValidationSeverity.Warning,
          path,
          "a fieldRange constraint on a nested field is never resolved by static verification (always NotStaticallyVerifiable) - " +
            "cross-field comparison only supports top-level output fields"
        )
      case Some(bySiblingName) =>
        referencedNames.foreach { name =>
          if (name == field.name) {
            issues += ValidationIssue(ValidationSeverity.Warning, path, s"fieldRange constraint references its own field '$name' - this is never a meaningful comparison")
          } else
            bySiblingName.get(name) match {
              case None =>
                issues += ValidationIssue(ValidationSeverity.Warning, path, s"fieldRange constraint references field '$name', which does not exist in this dataset's schema")
              case Some(other) if !NumericFieldTypes.contains(other.fieldType.toLowerCase) =>
                issues += ValidationIssue(
                  ValidationSeverity.Warning,
                  path,
                  s"fieldRange constraint references field '$name' of type '${other.fieldType}', not a numeric type"
                )
              case _ => ()
            }
        }
    }

    issues.result()
  }

  private val NumericFieldTypes = Set("integer", "long", "short", "byte", "double", "float", "decimal")

  /** Loose on purpose: only flags a clear mismatch (a numeric constraint
    * value against a declared string field, or vice versa) rather than
    * requiring an exact type match (an `integer` constraint value against a
    * `long` field is not itself a mistake worth warning about).
    */
  private def typesCompatible(declaredFieldType: String, constraintLiteralType: String): Boolean = {
    val declaredNumeric = NumericFieldTypes.contains(declaredFieldType.toLowerCase)
    val literalNumeric = NumericFieldTypes.contains(constraintLiteralType.toLowerCase)
    declaredNumeric == literalNumeric
  }

  private def fieldConstraintHint(constraintType: String): String = constraintType match {
    case FieldConstraintType.Equals => " (expected a 'value' property)"
    case FieldConstraintType.OneOf  => " (expected a non-empty 'values' list)"
    case FieldConstraintType.Range  => " (expected at least one of gte/gt/lte/lt, and not both gte+gt or both lte+lt)"
    case FieldConstraintType.Length => " (expected 'exact', or at least one of 'min'/'max' with min <= max, and not 'exact' combined with 'min'/'max')"
    case FieldConstraintType.FieldRange => " (expected at least one of gte/gt/lte/lt naming another field in the same schema, and not both gte+gt or both lte+lt)"
    case _                           => ""
  }

  private def ruleHint(ruleType: String): String = ruleType match {
    case RuleType.MergeCondition        => " (expected a non-empty 'columns' list of column names)"
    case RuleType.AllowedUpdateColumns  => " (expected a non-empty 'columns' list of column names)"
    case RuleType.RequiredGroupBy       => " (expected a non-empty 'columns' list of column names)"
    case RuleType.RequiredJoinColumns   => " (expected a non-empty 'columns' list of column names)"
    case RuleType.RequiredFilterColumns => " (expected a non-empty 'columns' list of column names)"
    case _                              => ""
  }

  private def duplicateNames(names: List[String]): Set[String] =
    names.groupBy(identity).collect { case (name, occurrences) if occurrences.size > 1 => name }.toSet
}
