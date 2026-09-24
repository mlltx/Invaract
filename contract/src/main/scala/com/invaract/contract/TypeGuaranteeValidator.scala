// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

/** The three-state verdict a `TypeGuaranteeCheck` reaches — the spec's own
  * vocabulary (docs/CONTRACT_MODEL.md's "Input and Output Types" section),
  * identical to `com.invaract.sparkadapter.RoleConformanceVerdict`'s three
  * names so the whole feature (role-consistency at the plan level, type
  * guarantees at the cross-contract level) shares one vocabulary rather than
  * two parallel ones for the same underlying idea: never collapse an
  * unproven contradiction into a proven one.
  */
sealed trait TypeGuaranteeVerdict
object TypeGuaranteeVerdict {

  /** The check found no evidence contradicting the declared type(s) involved. */
  case object Conforms extends TypeGuaranteeVerdict

  /** The check found a real, structural contradiction — becomes blocking
    * when the owning `TypeGuaranteeConfig.mode` is `Enforce` (see
    * `TypeGuaranteeValidator.evaluate`).
    */
  case object Contradicts extends TypeGuaranteeVerdict

  /** The check could not establish either way from the contracts it was
    * given — genuinely ambiguous, not a violation; never blocks regardless
    * of `mode`. E.g. a `DATA_ASSET` output never consumed by any *other*
    * contract in the same run may simply be consumed by a system outside
    * this run's view, not proof it's actually pipeline-only state.
    */
  case object CannotDetermine extends TypeGuaranteeVerdict
}

/** One `TypeGuaranteeCheck`'s finding for one location/relationship —
  * `involvedContracts` names every contract (as `"id@version"`) the finding
  * is about, so a caller can report "who needs to look at this" without
  * re-deriving it from `message`.
  */
case class TypeGuaranteeResult(
  checkType: String,
  verdict: TypeGuaranteeVerdict,
  location: String,
  message: String,
  remediation: String,
  involvedContracts: List[String] = Nil
) {
  def toMap: Map[String, Any] =
    Map(
      "checkType" -> checkType,
      "verdict" -> verdict.toString,
      "location" -> location,
      "message" -> message,
      "remediation" -> remediation,
      "involvedContracts" -> involvedContracts
    )
}

/** Extension point for a "stronger semantic and guarantee validation" check
  * outside the built-in `TypeGuaranteeType` set — the same reflective,
  * no-source-change escape hatch `CustomPolicyEvaluator`/
  * `com.invaract.sparkadapter.CustomRuleVerifier` already establish. Given
  * the *whole* set of contracts a run is considering (never just one): a
  * type guarantee is inherently a claim about how a dataset's declared role
  * holds up across everything Invaract can see about it, the same reason
  * `CrossContractValidator.validate` takes a `List[Contract]` rather than
  * one at a time. Required to be stateless, the same assumption every other
  * reflectively-resolved plugin trait in this codebase makes.
  */
trait TypeGuaranteeCheck {
  def check(contracts: List[Contract]): List[TypeGuaranteeResult]
}

/** The built-in `TypeGuaranteeCheck` names `TypeGuaranteeValidator` itself
  * implements — deliberately a narrow, closed set for the initial
  * implementation, mirroring `PolicyType`/`RuleType`'s own discipline. A
  * `TypeGuaranteeConfig.enabled` entry outside this set is not necessarily
  * inert: if `customTypeGuaranteeTypes` names a `TypeGuaranteeCheck` for it,
  * `TypeGuaranteeValidator.validate` dispatches to that instead.
  */
object TypeGuaranteeType {

  /** Every location more than one contract declares a `DATA_ASSET` at must
    * agree, field for field, on what it actually looks like — see
    * `DataAssetSchemaConsistencyCheck`'s own doc. Mechanical, high-confidence:
    * two independently-authored contracts disagreeing about the same
    * physical object's shape is a real structural fact, not a guess.
    */
  val DataAssetSchemaConsistency = "data_asset_schema_consistency"

  /** A `DATA_ASSET` output no other contract in the same run consumes is
    * visibility, not proof, of the spec's own "treated as pipeline-only
    * state" concern — see `DataAssetDownstreamConsumptionCheck`'s own doc.
    * Never reaches `Contradicts`; only `Conforms`/`CannotDetermine`.
    */
  val DataAssetDownstreamConsumption = "data_asset_downstream_consumption"

  val All: Set[String] = Set(DataAssetSchemaConsistency, DataAssetDownstreamConsumption)
}

/** Performs "stronger semantic and guarantee validation"
  * (docs/CONTRACT_MODEL.md's "Input and Output Types" section, its own
  * deferred Phase 6) across a set of governed contracts — pure,
  * engine-independent, the same `contract`-module-only reach
  * `CrossContractValidator` already has. Deliberately built as a registry of
  * independent, narrow checks (`TypeGuaranteeCheck`) rather than one
  * monolithic pass, so a new "type verification rule" — built-in
  * (`TypeGuaranteeType`) or a third party's own
  * (`OrgPolicy.typeGuarantees.customTypeGuaranteeTypes`) — can be added
  * without touching an existing check's own code, and so an organization
  * decides *which* checks actually run and whether a real contradiction
  * blocks anything at all (`TypeGuaranteeConfig`/`OrgPolicy.typeGuarantees`).
  *
  * Only two built-in checks exist today, corresponding to the two
  * "Contract Conformance" examples the spec itself gives that Phases 1-5
  * don't already cover (`docs/CONTRACT_MODEL.md`'s own accounting): "a
  * DATA_ASSET being produced from inputs inconsistent with its declared
  * contract" and "an object declared DATA_ASSET being treated as
  * pipeline-only state." Per the spec's own instruction for this phase
  * ("the priority should be high-confidence evidence first, rather than
  * maximising the number of things Invaract claims to understand"), neither
  * check invents a new inference technique — both reuse the identical
  * `Dataset`/`DatasetType`/location-matching machinery `CrossContractValidator`
  * already established.
  */
object TypeGuaranteeValidator {

  /** `results` from every check named in `policy.typeGuarantees.enabled`,
    * plus `blocking` — every `Contradicts` result, but only when
    * `policy.typeGuarantees.mode` is `Enforce` (an empty list in `Warn`
    * mode, or when nothing enabled produced a `Contradicts` at all). The
    * `OrgPolicyEvaluation`-shaped split every other policy-driven check in
    * this module already returns.
    */
  def evaluate(contracts: List[Contract], policy: OrgPolicy): TypeGuaranteeEvaluation = {
    val config = policy.typeGuarantees
    val results = validate(contracts, config)
    val blocking =
      if (config.mode == PolicyMode.Enforce) results.filter(_.verdict == TypeGuaranteeVerdict.Contradicts) else Nil
    TypeGuaranteeEvaluation(results, blocking)
  }

  /** Every result from every check `config.enabled` names — built-in first
    * where a name matches both (built-in always wins, the same one-directional
    * fallback `OrgPolicyEvaluator.resolveEvaluator` already establishes for
    * `customPolicyTypes`), in `config.enabled`'s own declared order. An
    * unresolvable name (neither built-in nor a working
    * `customTypeGuaranteeTypes` entry) contributes nothing — total/safe,
    * the same convention every other `*.interpret`/resolution in this
    * module follows; `OrgPolicyValidator` is where that becomes a reported
    * issue instead.
    */
  def validate(contracts: List[Contract], config: TypeGuaranteeConfig): List[TypeGuaranteeResult] =
    config.enabled.distinct.flatMap { checkType =>
      resolveCheck(checkType, config.customTypeGuaranteeTypes).map(_.check(contracts)).getOrElse(Nil)
    }

  private def resolveCheck(checkType: String, customTypeGuaranteeTypes: Map[String, String]): Option[TypeGuaranteeCheck] =
    builtinChecks.get(checkType).orElse {
      customTypeGuaranteeTypes.get(checkType).flatMap(className => TypeGuaranteeCheckFactory.tryResolve(className).toOption)
    }

  private val builtinChecks: Map[String, TypeGuaranteeCheck] = Map(
    TypeGuaranteeType.DataAssetSchemaConsistency -> DataAssetSchemaConsistencyCheck,
    TypeGuaranteeType.DataAssetDownstreamConsumption -> DataAssetDownstreamConsumptionCheck
  )

  /** For every physical location more than one *distinct* contract (by
    * `Contract.id` — two versions of the same contract are never compared,
    * the identical "not a real dependency" reasoning `CrossContractValidator`
    * already uses) declares a `DATA_ASSET` at (input or output side alike —
    * a DATA_ASSET consumed elsewhere ought to describe the same shape as
    * the DATA_ASSET it's consuming), every field name common to both
    * declared schemas must agree on `fieldType`, and a field `required` on
    * one side must exist on the other. A real mismatch there is a genuine
    * structural fact — two contracts governing the same physical object
    * disagree about what it looks like — not an inference, so it's the one
    * `Contradicts`-capable built-in check.
    */
  private[contract] object DataAssetSchemaConsistencyCheck extends TypeGuaranteeCheck {
    def check(contracts: List[Contract]): List[TypeGuaranteeResult] = {
      val declarations: List[(Contract, Dataset)] = for {
        c <- contracts
        d <- c.inputs ++ c.outputs
        if d.datasetType.contains(DatasetType.DataAsset)
      } yield (c, d)

      // Grouped by location FIRST, so a pair is only ever compared when both
      // sides genuinely claim the same physical object - a contract with
      // two DATA_ASSET declarations at two different locations must never
      // have those cross-compared against each other, or against an
      // unrelated contract's declaration at a third location.
      val byLocation: Map[String, List[(Contract, Dataset)]] =
        declarations.groupBy { case (_, d) => CrossContractValidator.normalize(d.location) }

      byLocation.toList.sortBy(_._1).flatMap { case (_, decls) =>
        // One declaration per *contract* within this location's group - a
        // contract is never compared against itself, the same "not a real
        // dependency" reasoning CrossContractValidator already uses.
        val distinctByContract = decls.groupBy(_._1.id).values.map(_.head).toList
        distinctByContract.combinations(2).toList.collect { case List((c1, d1), (c2, d2)) => resultsFor(c1, d1, c2, d2) }.flatten
      }
    }

    private def resultsFor(c1: Contract, d1: Dataset, c2: Contract, d2: Dataset): List[TypeGuaranteeResult] = {
      val mismatches = fieldMismatches(d1.schema, d2.schema)
      if (mismatches.isEmpty) Nil
      else
        List(
          TypeGuaranteeResult(
            TypeGuaranteeType.DataAssetSchemaConsistency,
            TypeGuaranteeVerdict.Contradicts,
            d1.location,
            s"contracts '${c1.id}@${c1.version}' and '${c2.id}@${c2.version}' both declare '${d1.location}' a " +
              s"DATA_ASSET but disagree: ${mismatches.mkString("; ")}.",
            s"Align the schemas '${c1.id}@${c1.version}' and '${c2.id}@${c2.version}' declare for this DATA_ASSET, " +
              "or confirm they genuinely represent two different physical objects that happen to share a location.",
            List(s"${c1.id}@${c1.version}", s"${c2.id}@${c2.version}")
          )
        )
    }

    private def fieldMismatches(a: Schema, b: Schema): List[String] = {
      val aByName = a.fields.map(f => f.name -> f).toMap
      val bByName = b.fields.map(f => f.name -> f).toMap
      val common = (aByName.keySet intersect bByName.keySet).toList.sorted
      val typeMismatches = common.flatMap { name =>
        val (fa, fb) = (aByName(name), bByName(name))
        if (fa.fieldType.equalsIgnoreCase(fb.fieldType)) None
        else Some(s"field '$name' is '${fa.fieldType}' in one declaration but '${fb.fieldType}' in the other")
      }
      val missingRequired =
        ((aByName.keySet -- bByName.keySet).toList.sorted.filter(n => aByName(n).required) ++
          (bByName.keySet -- aByName.keySet).toList.sorted.filter(n => bByName(n).required))
          .distinct
          .sorted
          .map(n => s"field '$n' is required in one declaration but absent from the other")
      typeMismatches ++ missingRequired
    }
  }

  /** For every `DATA_ASSET`-declared *output* in `contracts`, whether *any
    * other* contract (by `Contract.id`) in the same run declares an input at
    * the same location — `Conforms` if so, `CannotDetermine` (never
    * `Contradicts`) if not. This is deliberately never a blocking verdict:
    * the spec's own example for this shape ("read and used to filter
    * processing dates" doesn't prove CONTROL) cuts the other way here too —
    * a DATA_ASSET genuinely consumed only by a system outside this run's
    * view (an external, non-Invaract-governed consumer, or simply a
    * contract this run's `contracts` list didn't happen to include) is
    * completely legitimate, so "not observed being consumed" can only ever
    * be visibility for a human to review, never proof of the spec's
    * "treated as pipeline-only state" concern.
    */
  private[contract] object DataAssetDownstreamConsumptionCheck extends TypeGuaranteeCheck {
    def check(contracts: List[Contract]): List[TypeGuaranteeResult] =
      for {
        producer <- contracts
        output <- producer.outputs
        if output.datasetType.contains(DatasetType.DataAsset)
      } yield {
        val consumedElsewhere = contracts.exists { consumer =>
          consumer.id != producer.id && consumer.inputs.exists(i => CrossContractValidator.sameLocation(i.location, output.location))
        }
        if (consumedElsewhere)
          TypeGuaranteeResult(
            TypeGuaranteeType.DataAssetDownstreamConsumption,
            TypeGuaranteeVerdict.Conforms,
            output.location,
            s"'${output.location}' (DATA_ASSET output '${output.name}' of '${producer.id}@${producer.version}') " +
              "is consumed as an input by at least one other governed contract in this run.",
            "No action needed.",
            List(s"${producer.id}@${producer.version}")
          )
        else
          TypeGuaranteeResult(
            TypeGuaranteeType.DataAssetDownstreamConsumption,
            TypeGuaranteeVerdict.CannotDetermine,
            output.location,
            s"'${output.location}' (DATA_ASSET output '${output.name}' of '${producer.id}@${producer.version}') " +
              "is not consumed as an input by any other contract in this run - it may be consumed by a system " +
              "outside this run's view, or it may genuinely be pipeline-only state despite its declared type.",
            "If this output is meant to be a real, shared data product, confirm a consuming contract exists " +
              "(even one this run didn't scan); if it's actually internal/pipeline-only, consider declaring it " +
              "CONTROL instead.",
            List(s"${producer.id}@${producer.version}")
          )
      }
  }
}

/** `TypeGuaranteeValidator.evaluate`'s result — `results` is every finding
  * from every enabled check, whatever the verdict; `blocking` is the subset
  * that should actually stop a lint/pipeline run, already filtered by
  * `TypeGuaranteeConfig.mode`. Mirrors `OrgPolicyEvaluation`'s identical
  * `enforceViolations`/all-violations split.
  */
case class TypeGuaranteeEvaluation(results: List[TypeGuaranteeResult], blocking: List[TypeGuaranteeResult]) {
  def hasBlockingIssues: Boolean = blocking.nonEmpty
}
