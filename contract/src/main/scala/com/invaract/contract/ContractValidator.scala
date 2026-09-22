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

    val namedDatasets =
      contract.inputs.zipWithIndex.map { case (d, i) => (s"inputs[$i]", d) } ++
        contract.outputs.zipWithIndex.map { case (d, i) => (s"outputs[$i]", d) }

    namedDatasets.foreach { case (path, dataset) => issues ++= validateDataset(path, dataset) }

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

    dataset.schema.fields.foreach { field =>
      issues ++= validateField(s"$path.schema.${field.name}", field)
    }

    issues.result()
  }

  private def validateField(path: String, field: Field): List[ValidationIssue] = {
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
      issues ++= validateField(s"$path.${nested.name}", nested)
    }

    issues.result()
  }

  private def ruleHint(ruleType: String): String = ruleType match {
    case RuleType.MergeCondition        => " (expected a non-empty 'columns' list of column names)"
    case RuleType.AllowedUpdateColumns  => " (expected a non-empty 'columns' list of column names)"
    case _                              => ""
  }

  private def duplicateNames(names: List[String]): Set[String] =
    names.groupBy(identity).collect { case (name, occurrences) if occurrences.size > 1 => name }.toSet
}
