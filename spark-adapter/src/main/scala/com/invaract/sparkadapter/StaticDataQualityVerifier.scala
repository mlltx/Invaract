// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, Field, FieldConstraint, InterpretedFieldConstraint}
import com.invaract.ir._

/** Attempts to *prove* — never merely predict — that a `Write`'s output
  * satisfies each output field's declared static data-quality properties
  * (`Field.nullable = false`, and each `FieldConstraint`), by running
  * `ir.PropertyAnalysis` over the translated plan, seeded with the
  * contract's own *input* fields' declared properties as trusted axioms.
  * See docs/STATIC_DATA_QUALITY_VERIFICATION.md for the full design.
  *
  * Lives in `spark-adapter`, not `ir`: this is the same division of labor
  * `SensitivityLineage` already establishes for `Lineage` — `ir.PropertyAnalysis`
  * knows nothing about `Contract`, and a type that needs both concepts
  * together belongs where both are already dependencies.
  *
  * The one thing this module adds beyond a bare call to `PropertyAnalysis.analyze`
  * is axiom seeding: a contract's declared input `location` is portable
  * (survives a rename, an environment move), while Spark reports a `Read`'s
  * actual scope as either an explicit alias or its absolute resolved
  * location — the exact same mismatch `StructuralVerifier`'s own
  * `locationsMatch` exists to bridge. `buildAxioms` below resolves that
  * mismatch by first discovering the plan's *real* `Read` scopes
  * (`StructuralVerifier.collectReads`), then matching each one against
  * `contract.inputs` by location — never the other way around, since
  * `PropertyAnalysis`'s own `Read` case looks axioms up by the scope
  * string Spark actually reports, not the contract's declared one.
  */
private[sparkadapter] object StaticDataQualityVerifier {

  /** One `DataQualityCheckResult` per output field with a declared
    * `nullable: false` and/or `constraints` property, against whichever of
    * `contract.outputs` `plan`'s `Write` matches (the same `matchOutput`
    * location-based rule `StructuralVerifier.verify` itself uses — a
    * `Write` matching no declared output contributes nothing here, since
    * `StructuralVerifier` already reports that mismatch as
    * `MissingOutput`/an unmatched write elsewhere). `Nil` for any plan that
    * isn't a `Write` at all.
    */
  def verify(contract: Contract, plan: Plan): List[DataQualityCheckResult] = plan match {
    case Write(dataset, _, _, _, _) =>
      contract.outputs.find(o => StructuralVerifier.locationsMatch(o.location, dataset.location)) match {
        case None => Nil
        case Some(output) =>
          val axioms = buildAxioms(contract, plan)
          val analyzed = PropertyAnalysis.analyze(plan, axioms).map(r => r.output.name -> r.state).toMap
          output.schema.fields.flatMap(field => checksFor(field, analyzed.getOrElse(field.name, ColumnPropertyState.Unknown)))
      }
    case _ => Nil
  }

  /** `Violation`s for every `Violated` entry in `results` — the only
    * verdict that ever blocks a write; see `ViolationType.DataQualityViolation`'s
    * own doc for why `NotGuaranteed`/`NotStaticallyVerifiable` never reach
    * here.
    */
  def violations(results: List[DataQualityCheckResult]): List[Violation] =
    results.filter(_.verdict == DataQualityVerdict.Violated).map(toViolation)

  private def toViolation(result: DataQualityCheckResult): Violation = Violation(
    ViolationType.DataQualityViolation,
    s"the transformation's own semantics prove that output field '${result.field}' cannot always satisfy its declared ${result.constraint} property",
    remediation =
      s"Review the transformation logic producing '${result.field}' — it can produce a value that violates the contract's declared constraint. " +
        "If the constraint is no longer correct, relax or remove it from the contract instead.",
    column = Some(result.field)
  )

  /** Trusted axioms for every real `Read` scope the plan contains, keyed
    * exactly the way `PropertyAnalysis`'s own `Read` case looks them up
    * (`ColumnRef(fieldName, Some(realScope))`, `realScope` being the
    * `Read`'s alias if it has one, else its dataset's actual reported
    * location) — never keyed by the contract's own declared, portable
    * location string, which would simply never match.
    */
  private def buildAxioms(contract: Contract, plan: Plan): Map[ColumnRef, ColumnPropertyState] =
    StructuralVerifier.collectReads(plan).flatMap { read =>
      val scope = read.alias.getOrElse(read.dataset.location)
      contract.inputs.find(input => StructuralVerifier.locationsMatch(input.location, read.dataset.location)) match {
        case Some(input) => input.schema.fields.map(f => ColumnRef(f.name, Some(scope)) -> fieldAxiomState(f))
        case None         => Nil
      }
    }.toMap

  private def fieldAxiomState(field: Field): ColumnPropertyState = {
    val interpreted = field.constraints.flatMap(_.interpret)
    ColumnPropertyState(
      notNull = if (!field.nullable) NullabilityFact.Proven else NullabilityFact.Unknown,
      equalsConstant = interpreted.collectFirst { case InterpretedFieldConstraint.Equals(v, t) => Property.EqualsConstant(v, t) },
      oneOf = interpreted.collectFirst { case InterpretedFieldConstraint.OneOf(vs, t) => Property.OneOf(vs, t) },
      range = interpreted.collectFirst { case InterpretedFieldConstraint.Range(gte, gt, lte, lt) => Property.Range(gte, gt, lte, lt) }
    )
  }

  private def checksFor(field: Field, state: ColumnPropertyState): List[DataQualityCheckResult] = {
    val notNullCheck = if (!field.nullable) List(DataQualityCheckResult(field.name, "NOT NULL", notNullVerdict(state))) else Nil
    val constraintChecks = field.constraints.flatMap(_.interpret).map {
      case InterpretedFieldConstraint.Equals(v, _) =>
        DataQualityCheckResult(field.name, s"= $v", setVerdict(state, Set(v)))
      case InterpretedFieldConstraint.OneOf(vs, _) =>
        DataQualityCheckResult(field.name, s"IN (${vs.mkString(", ")})", setVerdict(state, vs))
      case r @ InterpretedFieldConstraint.Range(_, _, _, _) =>
        val required = Property.Range(r.gte, r.gt, r.lte, r.lt)
        DataQualityCheckResult(field.name, describeRange(required), rangeVerdict(state, required))
    }
    notNullCheck ++ constraintChecks
  }

  private def describeRange(r: Property.Range): String =
    List(r.gte.map(v => s">= $v"), r.gt.map(v => s"> $v"), r.lte.map(v => s"<= $v"), r.lt.map(v => s"< $v")).flatten.mkString(" and ")

  private def notNullVerdict(state: ColumnPropertyState): DataQualityVerdict = state.notNull match {
    case NullabilityFact.Proven  => DataQualityVerdict.Guaranteed
    case NullabilityFact.Refuted => DataQualityVerdict.Violated
    case NullabilityFact.Unknown => if (state.unsupported) DataQualityVerdict.NotStaticallyVerifiable else DataQualityVerdict.NotGuaranteed
  }

  /** `proven` is a fully-known set of values the column is limited to
    * (`OneOf`, or a single-value `EqualsConstant` treated as a one-element
    * set) — `Guaranteed` when every one of those values is inside
    * `required`, `Violated` when a proven value falls outside it (the
    * transformation's own semantics rule out satisfying the contract for
    * that row), and `NotGuaranteed`/`NotStaticallyVerifiable` when nothing
    * is proven at all. Deliberately does not cross-check `required` against
    * `state.range` (e.g. a numeric range that happens to exclude every
    * value in `required`) — an intentional MVP simplification that only
    * ever under-claims `Violated` in favor of the "false guarantees are
    * worse than missed proofs" principle, not one that risks a false
    * `Violated`.
    */
  private def setVerdict(state: ColumnPropertyState, required: Set[Any]): DataQualityVerdict = {
    val proven: Option[Set[Any]] = state.oneOf.map(_.values).orElse(state.equalsConstant.map(e => Set(e.value)))
    proven match {
      case Some(vs) if vs.subsetOf(required) => DataQualityVerdict.Guaranteed
      case Some(_)                            => DataQualityVerdict.Violated
      case None                               => if (state.unsupported) DataQualityVerdict.NotStaticallyVerifiable else DataQualityVerdict.NotGuaranteed
    }
  }

  /** `p.tighten(required) == p` is exactly "`p`'s bounds are already at
    * least as tight as `required`'s on every side" (`Property.Range.tighten`
    * already picks the tighter of two bounds per side) — i.e. every value
    * `p` allows is inside `required`, the `Guaranteed` case. Otherwise,
    * `Violated` only when `p` provably *escapes* `required` on some side —
    * a strict, tie-conservative numeric comparison (an exact boundary match,
    * e.g. required `> 0` against a proven `>= 0`, is `NotGuaranteed`, never
    * `Violated`: `p` doesn't provably escape, it merely isn't proven to stay
    * inside).
    */
  private def rangeVerdict(state: ColumnPropertyState, required: Property.Range): DataQualityVerdict = state.range match {
    case Some(p) if p.tighten(required) == p                => DataQualityVerdict.Guaranteed
    case Some(p) if escapesBelow(p, required) || escapesAbove(p, required) => DataQualityVerdict.Violated
    case _                                                    => if (state.unsupported) DataQualityVerdict.NotStaticallyVerifiable else DataQualityVerdict.NotGuaranteed
  }

  private def lowerValue(r: Property.Range): Option[BigDecimal] = r.gte.orElse(r.gt)
  private def upperValue(r: Property.Range): Option[BigDecimal] = r.lte.orElse(r.lt)

  private def escapesBelow(p: Property.Range, required: Property.Range): Boolean = (lowerValue(p), lowerValue(required)) match {
    case (_, None)          => false // required has no lower bound: nothing to escape below
    case (None, Some(_))    => true  // p is unbounded below, required is not: p can escape
    case (Some(pv), Some(rv)) => pv < rv // strict: an exact tie is not a proven escape
  }

  private def escapesAbove(p: Property.Range, required: Property.Range): Boolean = (upperValue(p), upperValue(required)) match {
    case (_, None)          => false
    case (None, Some(_))    => true
    case (Some(pv), Some(rv)) => pv > rv
  }
}
