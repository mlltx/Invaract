// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.contract.{Contract, ContractParser, LogicalField, LogicalSchema, LogicalType}
import com.invaract.verification.{Capability, VerificationOptions, ViolationType}

/** The conformance scenarios: each is one behaviour every adapter must show, written in no engine's
  * terms. They are small on purpose - one contract, one job, one expected verdict - so a failure
  * names exactly what an adapter does differently, and so a new adapter can pass them one by one.
  *
  * What they cover is deliberate and listed: a capability that is the `focus` of some scenario is
  * *verified* when an adapter claims it; one in `notCovered` is an honest gap in the kit (with the
  * reason), never a silent one - `Conformance.evaluate` reports an adapter's claims on those as
  * unverified, and the kit's own tests fail if a capability is in neither place.
  */
object Scenarios {
  import ColumnSource._

  private val Long = LogicalType.LongType
  private val Str = LogicalType.StringType

  private val orders = ScenarioInput(
    "in/orders",
    LogicalSchema(List(LogicalField("id", Long, nullable = true), LogicalField("amount", Long, nullable = true), LogicalField("label", Str, nullable = true)))
  )

  /** `(name, type, required)` -> the contract's field list, indented to sit under `fields:`. */
  private def fields(indent: Int, fs: (String, String, Boolean)*): String =
    fs.map { case (n, t, req) =>
      val pad = " " * indent
      s"$pad- name: $n\n$pad  type: $t" + (if (req) s"\n$pad  required: true" else "")
    }.mkString("\n")

  private val ordersContractFields = fields(8, ("id", "long", false), ("amount", "long", false), ("label", "string", false))
  private val defaultOutputFields = fields(8, ("id", "long", false), ("total", "long", true))

  /** The columns of a table changed in place: nothing is promised about nullability, because a table format
    * decides that for itself (Delta records every column as nullable). */
  private val dmlOutputFields = fields(8, ("id", "long", false), ("total", "long", false))

  private def contract(
      outputFields: String = defaultOutputFields,
      outputExtras: String = "",
      rules: String = "",
      inputs: String = s"""  - name: orders
                          |    location: in/orders
                          |    schema:
                          |      fields:
                          |$ordersContractFields""".stripMargin,
      outputLocation: String = "out/report"
  ): Contract = ContractParser.parse(
    s"""id: conformance
       |version: "1.0.0"
       |${if (inputs.isEmpty) "" else "inputs:\n" + inputs}
       |outputs:
       |  - name: report
       |    location: $outputLocation
       |${if (outputExtras.isEmpty) "" else outputExtras + "\n"}    schema:
       |      fields:
       |$outputFields
       |${if (rules.isEmpty) "" else "rules:\n" + rules + "\n"}""".stripMargin
  )

  private val passThrough = List(OutColumn("id", FromInput(0, "id")), OutColumn("total", NonNullLong(1)))

  private def job(
      columns: List[OutColumn] = passThrough,
      output: ScenarioOutput = ScenarioOutput("out/report"),
      inputs: List[ScenarioInput] = List(orders),
      join: Option[JoinOn] = None,
      filterColumn: Option[String] = None,
      rowChange: Option[RowChange] = None
  ) = ScenarioJob(inputs, columns, output, join, filterColumn, rowChange = rowChange)

  private val defaults = VerificationOptions()

  private val tagsType = LogicalType.ArrayType(Str)
  private val tagged = ScenarioInput(
    "in/tagged",
    LogicalSchema(List(LogicalField("id", Long, nullable = true), LogicalField("tags", tagsType, nullable = true)))
  )
  private def taggedContract(declaredTagsType: String) = contract(
    outputFields = fields(8, ("id", "long", false), ("tags", declaredTagsType, false)),
    inputs = s"""  - name: tagged
                |    location: in/tagged
                |    schema:
                |      fields:
                |${fields(8, ("id", "long", false), ("tags", "array<string>", false))}""".stripMargin
  )
  private val taggedJob = job(
    columns = List(OutColumn("id", FromInput(0, "id")), OutColumn("tags", FromInput(0, "tags"))),
    inputs = List(tagged)
  )

  private val customers = ScenarioInput("in/customers", LogicalSchema(List(LogicalField("id", Long, nullable = true))))


  private val profiles = ScenarioInput("in/profiles", LogicalSchema(List(LogicalField("id", Long, nullable = true), LogicalField("segment", Str, nullable = true))))

  /** An output whose fields carry static data-quality properties: `total` is required, `id` is not. */
  private def dqOutputFields(idConstraint: String, totalConstraint: String, totalRequired: Boolean): String =
    s"""        - name: id
       |          type: long$idConstraint
       |        - name: total
       |          type: long${if (totalRequired) "\n          required: true" else ""}$totalConstraint""".stripMargin
  private def rangeGte(bound: Int): String = s"\n          constraints:\n            - type: range\n              gte: $bound"
  private val staticDq = defaults.copy(staticDataQuality = true)

  /** The orders input declared with a role (`type:`), for the role-consistency scenarios. */
  private def roleInputs(role: String) =
    s"""  - name: orders
       |    location: in/orders
       |    type: $role
       |    schema:
       |      fields:
       |$ordersContractFields""".stripMargin
  private val roleCheck = defaults.copy(roleConsistency = true)

  /** The orders input with `amount` and `label` tagged sensitive, for the sensitivity scenario. */
  private val sensitiveInputs =
    s"""  - name: orders
       |    location: in/orders
       |    schema:
       |      fields:
       |        - name: id
       |          type: long
       |        - name: amount
       |          type: long
       |          sensitivityTags: [financial]
       |        - name: label
       |          type: string
       |          sensitivityTags: [pii]""".stripMargin

  private val twoInputs =
    s"""  - name: orders
       |    location: in/orders
       |    schema:
       |      fields:
       |$ordersContractFields
       |  - name: profiles
       |    location: in/profiles
       |    schema:
       |      fields:
       |${fields(8, ("id", "long", false), ("segment", "string", false))}""".stripMargin

  private val castAmount = List(OutColumn("id", FromInput(0, "id")), OutColumn("amount_text", CastInput(0, "amount", Str)), OutColumn("total", NonNullLong(1)))
  private val castAmountFields = fields(8, ("id", "long", false), ("amount_text", "string", false), ("total", "long", true))

  val all: List[Scenario] = List(
    Scenario(
      "conforming-write",
      "a write that matches its contract is allowed, and a PASSED validation is published",
      Set(Capability.CheckInputExistence, Capability.CheckLocation, Capability.CheckSchema, Capability.ReportingNotifications),
      contract(), job(), defaults, Expectation.Pass
    ),
    Scenario(
      "wrong-output-location",
      "a write to a location other than the contract's is blocked, and a FAILED validation is published",
      Set(Capability.CheckLocation, Capability.ReportingNotifications),
      contract(), job(output = ScenarioOutput("out/elsewhere")), defaults, Expectation.Reject(Set(ViolationType.OutputLocationMismatch))
    ),
    Scenario(
      "missing-output-field",
      "a declared output field the job does not produce is blocked",
      Set(Capability.CheckSchema),
      contract(outputFields = fields(8, ("id", "long", false), ("total", "long", true), ("customer_name", "string", true))),
      job(), defaults, Expectation.Reject(Set(ViolationType.MissingOutputField))
    ),
    Scenario(
      "output-type-mismatch",
      "an output column of a different type than declared is blocked",
      Set(Capability.CheckSchema),
      contract(), job(columns = List(OutColumn("id", CastInput(0, "id", Str)), OutColumn("total", NonNullLong(1)))),
      defaults, Expectation.Reject(Set(ViolationType.OutputFieldTypeMismatch))
    ),
    Scenario(
      "extended-type-not-produced",
      "a declared interval, json, geography or time column is not satisfied by a column of another type - a type an engine lacks is a mismatch, never a pass",
      Set(Capability.CheckSchema),
      contract(outputFields = fields(8, ("id", "long", false), ("total", "interval", true))),
      job(), defaults, Expectation.Reject(Set(ViolationType.OutputFieldTypeMismatch))
    ),
    Scenario(
      "row-level-delete-checked-as-write",
      "a row-level DELETE is recognized as a write to its table: aimed at a table the contract does not declare, it is blocked",
      Set(Capability.WriteRowLevelDml, Capability.CheckLocation),
      contract(outputFields = dmlOutputFields, inputs = "", outputLocation = "out/declared"),
      job(output = ScenarioOutput("out/elsewhere", format = "delta"), rowChange = Some(RowChange.FilteredDelete)),
      defaults, Expectation.Reject(Set(ViolationType.OutputLocationMismatch)),
      operations = Set(Capability.WriteRowLevelDml)
    ),
    Scenario(
      "row-level-delete-own-table-is-not-an-input",
      "with rejectUndeclaredInputs, a row-level DELETE is not blocked for reading the table it changes",
      Set(Capability.WriteRowLevelDml),
      contract(outputFields = dmlOutputFields, inputs = ""),
      job(output = ScenarioOutput("out/report", format = "delta"), rowChange = Some(RowChange.FilteredDelete)),
      VerificationOptions(rejectUndeclaredInputs = true), Expectation.Pass,
      operations = Set(Capability.WriteRowLevelDml)
    ),
    Scenario(
      "unconditional-delete-forbidden",
      "a DELETE with no predicate is blocked when the contract carries forbid_unconditional_delete",
      Set(Capability.RulesDml, Capability.WriteRowLevelDml),
      contract(outputFields = dmlOutputFields, inputs = "", rules = "  - type: forbid_unconditional_delete"),
      job(output = ScenarioOutput("out/report", format = "delta"), rowChange = Some(RowChange.UnconditionalDelete)),
      defaults, Expectation.Reject(Set(ViolationType.RuleUnconditionalDelete)),
      operations = Set(Capability.WriteRowLevelDml)
    ),
    Scenario(
      "filtered-delete-allowed",
      "a DELETE with a predicate passes a contract that carries forbid_unconditional_delete",
      Set(Capability.RulesDml, Capability.WriteRowLevelDml),
      contract(outputFields = dmlOutputFields, inputs = "", rules = "  - type: forbid_unconditional_delete"),
      job(output = ScenarioOutput("out/report", format = "delta"), rowChange = Some(RowChange.FilteredDelete)),
      defaults, Expectation.Pass,
      operations = Set(Capability.WriteRowLevelDml)
    ),
    Scenario(
      "catalog-required-output-unregistered",
      "an output the contract requires to be registered in a catalog is blocked when it is written to a bare path",
      Set(Capability.CheckCatalogRegistration),
      contract(outputExtras = "    catalog:\n      required: true"), job(), defaults,
      Expectation.Reject(Set(ViolationType.MissingOutputCatalogRegistration))
    ),
    Scenario(
      "catalog-required-output-registered",
      "an output the contract requires to be registered in a catalog is allowed when it is written as a catalog table",
      Set(Capability.CheckCatalogRegistration),
      contract(outputExtras = "    catalog:\n      required: true"),
      job(output = ScenarioOutput("out/report", saveMode = "append", registeredAs = Some("conformance_report"))), defaults, Expectation.Pass
    ),
    Scenario(
      "output-nullability-mismatch",
      "a column declared required that the job can leave null is blocked",
      Set(Capability.CheckSchema),
      contract(outputFields = fields(8, ("id", "long", true), ("total", "long", true))),
      job(), defaults, Expectation.Reject(Set(ViolationType.OutputFieldNullabilityMismatch))
    ),
    Scenario(
      "undeclared-output-column-allowed-by-default",
      "an output column the contract does not declare is allowed unless rejectUndeclaredFields is on",
      Set(Capability.CheckSchema),
      contract(), job(columns = passThrough :+ OutColumn("extra", FromInput(0, "label"))), defaults, Expectation.Pass
    ),
    Scenario(
      "undeclared-output-column-rejected",
      "with rejectUndeclaredFields, an output column the contract does not declare is blocked",
      Set(Capability.CheckSchema),
      contract(), job(columns = passThrough :+ OutColumn("extra", FromInput(0, "label"))),
      defaults.copy(rejectUndeclaredFields = true), Expectation.Reject(Set(ViolationType.UndeclaredOutputColumn))
    ),
    Scenario(
      "declared-input-not-read",
      "a declared input the job never reads is blocked",
      Set(Capability.CheckInputExistence),
      contract(inputs =
        s"""  - name: orders
           |    location: in/orders
           |    schema:
           |      fields:
           |$ordersContractFields
           |  - name: customers
           |    location: in/customers
           |    schema:
           |      fields:
           |${fields(8, ("id", "long", false))}""".stripMargin
      ),
      job(), defaults, Expectation.Reject(Set(ViolationType.MissingInput))
    ),
    Scenario(
      "undeclared-input-rejected",
      "with rejectUndeclaredInputs, reading a dataset the contract does not declare is blocked",
      Set(Capability.CheckInputExistence),
      contract(), job(inputs = List(orders, customers), join = Some(JoinOn("id", "id"))),
      defaults.copy(rejectUndeclaredInputs = true), Expectation.Reject(Set(ViolationType.UndeclaredInput))
    ),
    Scenario(
      "output-format-matches",
      "an output whose format is what the contract declares is allowed",
      Set(Capability.CheckFormat),
      contract(outputExtras = "    format: parquet"), job(), defaults, Expectation.Pass
    ),
    Scenario(
      "output-format-mismatch",
      "an output written in a different format than the contract declares is blocked",
      Set(Capability.CheckFormat),
      contract(outputExtras = "    format: parquet"), job(output = ScenarioOutput("out/report", format = "csv")),
      defaults, Expectation.Reject(Set(ViolationType.OutputFormatMismatch))
    ),
    Scenario(
      "output-save-mode-matches",
      "an output written with the save mode the contract declares is allowed",
      Set(Capability.CheckSaveMode),
      contract(outputExtras = "    saveMode: overwrite"), job(), defaults, Expectation.Pass
    ),
    Scenario(
      "output-save-mode-mismatch",
      "an output written with a different save mode than the contract declares is blocked",
      Set(Capability.CheckSaveMode),
      contract(outputExtras = "    saveMode: append"), job(), defaults, Expectation.Reject(Set(ViolationType.OutputSaveModeMismatch))
    ),
    Scenario(
      "nested-type-matches",
      "a nested output column of the declared structure is allowed",
      Set(Capability.CheckNestedTypes),
      taggedContract("array<string>"), taggedJob, defaults, Expectation.Pass
    ),
    Scenario(
      "nested-type-mismatch",
      "a nested output column whose element type differs from the declared one is blocked",
      Set(Capability.CheckNestedTypes),
      taggedContract("array<int>"), taggedJob, defaults, Expectation.Reject(Set(ViolationType.OutputFieldTypeMismatch))
    ),
    Scenario(
      "required-filter-column-present",
      "a required_filter_columns rule is satisfied when the job filters on that column",
      Set(Capability.RulesPlanShape),
      contract(rules = "  - type: required_filter_columns\n    columns: [amount]"), job(filterColumn = Some("amount")),
      defaults, Expectation.Pass
    ),
    Scenario(
      "required-filter-column-missing",
      "a required_filter_columns rule is violated when the job never filters on that column",
      Set(Capability.RulesPlanShape),
      contract(rules = "  - type: required_filter_columns\n    columns: [amount]"), job(),
      defaults, Expectation.Reject(Set(ViolationType.RuleRequiredFilterColumnsViolation))
    ),
    Scenario(
      "fingerprint-flags-nothing-for-a-deterministic-job",
      "with computeFingerprint, a job built only from deterministic columns reports no non-deterministic output",
      Set(Capability.AnalysisFingerprint, Capability.AnalysisFunctionCatalog),
      contract(), job(), defaults.copy(computeFingerprint = true), Expectation.Pass, expectNonDeterministic = Some(Set.empty)
    ),
    Scenario(
      "fingerprint-flags-a-generated-id",
      "a column the engine generates afresh per row is reported non-deterministic, whatever the engine calls the function",
      Set(Capability.AnalysisFingerprint, Capability.AnalysisFunctionCatalog),
      contract(outputFields = fields(8, ("id", "long", false), ("total", "long", true), ("token", "string", true))),
      job(columns = passThrough :+ OutColumn("token", UniqueId)), defaults.copy(computeFingerprint = true), Expectation.Pass,
      expectNonDeterministic = Some(Set("token"))
    ),
    Scenario(
      "static-data-quality-proves-a-constant",
      "with staticDataQuality, a required column built from a constant is proven to satisfy its declared range, and the proof is reported",
      Set(Capability.AnalysisStaticDataQuality),
      contract(outputFields = dqOutputFields("", rangeGte(0), totalRequired = true)), job(), staticDq, Expectation.Pass,
      analysis = Analysis(dataQuality = Some(Set("total" -> "Guaranteed")))
    ),
    Scenario(
      "static-data-quality-cannot-prove-a-passthrough",
      "with staticDataQuality, a range declared on a column copied from an unconstrained input is reported not guaranteed, and does not block",
      Set(Capability.AnalysisStaticDataQuality),
      contract(outputFields = dqOutputFields(rangeGte(0), "", totalRequired = true)), job(), staticDq, Expectation.Pass,
      analysis = Analysis(dataQuality = Some(Set("id" -> "NotGuaranteed", "total" -> "Guaranteed")))
    ),
    Scenario(
      "static-data-quality-violation-blocks",
      "with staticDataQuality, a constant the declared range provably excludes blocks the write",
      Set(Capability.AnalysisStaticDataQuality),
      contract(outputFields = dqOutputFields("", rangeGte(5), totalRequired = false)), job(), staticDq,
      Expectation.Reject(Set(ViolationType.DataQualityViolation)),
      analysis = Analysis(dataQuality = Some(Set("total" -> "Violated")))
    ),
    Scenario(
      "role-source-contributing-conforms",
      "with roleConsistency, a SOURCE input whose data reaches the output is consistent with its declared role",
      Set(Capability.AnalysisRoleConsistency),
      contract(inputs = roleInputs("SOURCE")), job(), roleCheck, Expectation.Pass,
      analysis = Analysis(roles = Some(Map("orders" -> "Conforms")))
    ),
    Scenario(
      "role-control-contributing-contradicts",
      "with roleConsistency, a CONTROL input whose data reaches the output contradicts its declared role and blocks the write",
      Set(Capability.AnalysisRoleConsistency),
      contract(inputs = roleInputs("CONTROL")), job(), roleCheck, Expectation.Reject(Set(ViolationType.RoleConsistencyViolation)),
      analysis = Analysis(roles = Some(Map("orders" -> "Contradicts")))
    ),
    Scenario(
      "lineage-passthrough-cast-and-constant",
      "each output column traces to the input column it is copied or cast from, and a constant traces to none",
      Set(Capability.LineageColumnLevel),
      contract(outputFields = castAmountFields), job(columns = castAmount), defaults, Expectation.Pass,
      analysis = Analysis(lineage = Some(Map("id" -> Set(0 -> "id"), "amount_text" -> Set(0 -> "amount"), "total" -> Set.empty)))
    ),
    Scenario(
      "lineage-through-join-and-filter",
      "a column read from the second input of a join traces to that input, and the join and filter do not add sources",
      Set(Capability.LineageColumnLevel),
      contract(outputFields = fields(8, ("id", "long", false), ("segment", "string", false), ("total", "long", true)), inputs = twoInputs),
      job(
        columns = List(OutColumn("id", FromInput(0, "id")), OutColumn("segment", FromInput(1, "segment")), OutColumn("total", NonNullLong(1))),
        inputs = List(orders, profiles), join = Some(JoinOn("id", "id")), filterColumn = Some("amount")
      ),
      defaults, Expectation.Pass,
      analysis = Analysis(lineage = Some(Map("id" -> Set(0 -> "id"), "segment" -> Set(1 -> "segment"), "total" -> Set.empty)))
    ),
    Scenario(
      "sensitivity-follows-a-cast",
      "tags declared on input fields reach the output columns derived from them, through a cast, and only those",
      Set(Capability.AnalysisSensitivityPropagation),
      contract(
        outputFields = fields(8, ("id", "long", false), ("amount_text", "string", false), ("label", "string", false), ("total", "long", true)),
        inputs = sensitiveInputs
      ),
      job(columns = castAmount.dropRight(1) ++ List(OutColumn("label", FromInput(0, "label")), OutColumn("total", NonNullLong(1)))),
      defaults, Expectation.Pass,
      analysis = Analysis(
        sensitivity = Some(Map("id" -> Set.empty, "amount_text" -> Set("financial"), "label" -> Set("pii"), "total" -> Set.empty))
      )
    ),
    Scenario(
      "boundary-filter-is-seen-through",
      "a filter made before a checkpoint or cache still satisfies a required_filter_columns rule, and no declared input is reported hidden",
      Set(Capability.LineageBoundaryResolution),
      contract(rules = "  - type: required_filter_columns\n    columns: [amount]"), job(filterColumn = Some("amount")).copy(boundary = true),
      defaults, Expectation.Pass,
      analysis = Analysis(unverifiableInputs = Some(Set.empty))
    ),
    Scenario(
      "boundary-does-not-excuse-a-missing-input",
      "a declared input that is genuinely never read is still reported missing when the job passes through a checkpoint or cache",
      Set(Capability.LineageBoundaryResolution),
      contract(inputs = twoInputs), job().copy(boundary = true), defaults, Expectation.Reject(Set(ViolationType.MissingInput)),
      analysis = Analysis(unverifiableInputs = Some(Set.empty))
    ),
    Scenario(
      "streaming-write-is-checked-like-a-batch-write",
      "a streaming job whose source is the declared input and whose sink is the declared output is allowed",
      Set(Capability.ReadStreaming, Capability.WriteStreaming),
      contract(), job().copy(streaming = true), defaults, Expectation.Pass,
      operations = Set(Capability.ReadStreaming, Capability.WriteStreaming)
    ),
    Scenario(
      "streaming-write-to-the-wrong-location",
      "a streaming job whose sink is not the declared output is blocked",
      Set(Capability.WriteStreaming),
      contract(), job(output = ScenarioOutput("out/elsewhere")).copy(streaming = true), defaults,
      Expectation.Reject(Set(ViolationType.OutputLocationMismatch)),
      operations = Set(Capability.ReadStreaming, Capability.WriteStreaming)
    ),
    Scenario(
      "untranslatable-write-fails-closed",
      "an operation that changes data but that the adapter cannot translate is blocked as unverifiable, never passed unchecked",
      Set(Capability.FailClosedUnverifiableWrites),
      contract(), ScenarioJob(List(orders), passThrough, ScenarioOutput("out/report"), untranslatableWrite = true), defaults,
      Expectation.Reject(Set(ViolationType.UnverifiableWrite)),
      operations = Set(Capability.WriteBatch)
    ),
    Scenario(
      "invalid-contract",
      "a contract that is itself unsound is rejected before the job is looked at",
      Set.empty,
      ContractParser.parse("id: broken\nversion: \"1.0.0\"\n"), job(), defaults, Expectation.Reject(Set(ViolationType.InvalidContract))
    )
  )

  /** Capabilities a job cannot be evidence for, by their nature: how an adapter attaches to a job, what it
    * applies before any job exists, what a mode of installing does. An adapter's claim on one of these is
    * attested by the adapter's own tests, never by this kit, and the report says so. */
  val attested: Map[Capability, String] = Map(
    Capability.PolicyOrganizational -> "applied to the contract at session start, not part of a job",
    Capability.ConfigZeroCodeInstall -> "how an adapter attaches to a job is engine-specific, not a job",
    Capability.ConfigLocationRefs -> "how an adapter attaches to a job is engine-specific, not a job",
    Capability.ConfigContractRegistry -> "how an adapter attaches to a job is engine-specific, not a job",
    Capability.ReportingDryRun -> "dry-run mode is a mode of installing, not a job",
    Capability.WriteStateChange -> "a state change is not a read-transform-write job; each engine has its own",
    Capability.RulesCustom -> "a custom rule type is a class resolved by the engine's own classpath; no neutral way to supply one"
  )

  /** Capabilities a job *could* be evidence for, but the kit cannot check yet - honest gaps in the kit, each with
    * why. An adapter's claim on one of these is reported as an unchecked gap, not as passing. */
  val gaps: Map[Capability, String] = Map.empty

  /** Everything the kit does not verify with a job: `attested` plus `gaps`. */
  val notCovered: Map[Capability, String] = attested ++ gaps

  /** Capabilities some scenario is the evidence for: a scenario's focus, and the operations every job needs. */
  val covered: Set[Capability] = all.flatMap(s => s.focus ++ s.operations).toSet
}
