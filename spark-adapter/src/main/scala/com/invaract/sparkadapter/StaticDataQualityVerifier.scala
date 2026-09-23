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

  /** One `DataQualityCheckResult` per output field (and, recursively, per
    * nested struct field — see `checksForField`) with a declared
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
          output.schema.fields.flatMap { field =>
            val definingExpr = PropertyAnalysis.definingExpr(plan, field.name)
            checksForField(field, field.name, analyzed.getOrElse(field.name, ColumnPropertyState.Unknown), definingExpr, axioms)
          }
      }
    case _ => Nil
  }

  /** Checks `field` itself against `state` (the real, analyzed state for a
    * top-level field, or a nested one traced through `definingExpr` — see
    * below), then recurses into `field.properties` for a struct/record
    * field.
    *
    * `definingExpr` is `field`'s own raw `ir.Expr` (paired with the `Plan`
    * any `ColumnReference` inside it resolves against) when one was found —
    * `PropertyAnalysis.definingExpr` for a top-level field, or, for a
    * nested one, this call's own construction below. Each `child` in
    * `field.properties` gets its own defining `Expr` built by wrapping
    * `definingExpr`'s `Expr` in one more `StructField(_, child.name)`
    * access and resolving *that* — exactly the same
    * `StructField(StructConstruct(...), ...)` resolution
    * `ir.PropertyAnalysis.resolveExprT` already performs for a *flat*
    * output column extracting one of its own just-built struct's fields,
    * now reached for a field `Contract`'s schema model declares nested
    * obligations on (`Field.properties`, see docs/CONTRACT_MODEL.md).
    *
    * `definingExpr` is `None` whenever no single well-defined source `Expr`
    * exists for `field` at all — an `Aggregate`/`Window`/`Union`/`Join`
    * output, a struct read straight from an input `Read` with no
    * intervening `Project`, or (once nested) a struct-valued expression
    * this analysis doesn't trace through (anything but a `StructConstruct`
    * — see `PropertyAnalysis.definingExpr`'s own doc for why these stay out
    * of scope). In every such case the field gets
    * `ColumnPropertyState(unsupported = true)` — the same "genuinely
    * analyzed but this construct is opaque" signal a UDF or an
    * unrecognized function already produces, which resolves to
    * `NotStaticallyVerifiable` below (see `notNullVerdict`/`setVerdict`/
    * `rangeVerdict`) — never silently producing no `DataQualityCheckResult`
    * at all (indistinguishable from "this field declares no constraints")
    * and never `NotGuaranteed` (which would wrongly imply analysis was
    * attempted and simply inconclusive, rather than never attempted).
    * `path` is the dotted field path (`"address.zip"`) used as this
    * result's own `field` name, so a violation or report entry for a
    * nested field is still unambiguous.
    */
  private def checksForField(
    field: Field,
    path: String,
    state: ColumnPropertyState,
    definingExpr: Option[(Expr, Plan)],
    axioms: Map[ColumnRef, ColumnPropertyState]
  ): List[DataQualityCheckResult] =
    checksFor(field, path, state) ++
      field.properties.flatMap { child =>
        val childDefiningExpr = definingExpr.map { case (parentExpr, input) => (StructField(parentExpr, child.name): Expr, input) }
        val childState = childDefiningExpr match {
          case Some((expr, input)) => PropertyAnalysis.analyzeExpr(expr, input, axioms)
          case None                 => ColumnPropertyState(unsupported = true)
        }
        checksForField(child, s"$path.${child.name}", childState, childDefiningExpr, axioms)
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
      range = interpreted.collectFirst { case InterpretedFieldConstraint.Range(gte, gt, lte, lt) => Property.Range(gte, gt, lte, lt) },
      length = interpreted.collectFirst { case InterpretedFieldConstraint.Length(exact, min, max) => Property.Length(exact, min, max) }
    )
  }

  private def checksFor(field: Field, path: String, state: ColumnPropertyState): List[DataQualityCheckResult] = {
    val notNullCheck = if (!field.nullable) List(DataQualityCheckResult(path, "NOT NULL", notNullVerdict(state))) else Nil
    val constraintChecks = field.constraints.flatMap(_.interpret).map {
      case InterpretedFieldConstraint.Equals(v, _) =>
        DataQualityCheckResult(path, s"= $v", setVerdict(state, Set(v)))
      case InterpretedFieldConstraint.OneOf(vs, _) =>
        DataQualityCheckResult(path, s"IN (${vs.mkString(", ")})", setVerdict(state, vs))
      case r @ InterpretedFieldConstraint.Range(_, _, _, _) =>
        val required = Property.Range(r.gte, r.gt, r.lte, r.lt)
        DataQualityCheckResult(path, describeRange(required), rangeVerdict(state, required))
      case l @ InterpretedFieldConstraint.Length(_, _, _) =>
        val required = Property.Length(l.exact, l.min, l.max)
        DataQualityCheckResult(path, describeLength(required), lengthVerdict(state, required))
    }
    notNullCheck ++ constraintChecks
  }

  private def describeRange(r: Property.Range): String =
    List(r.gte.map(v => s">= $v"), r.gt.map(v => s"> $v"), r.lte.map(v => s"<= $v"), r.lt.map(v => s"< $v")).flatten.mkString(" and ")

  private def describeLength(l: Property.Length): String = l.exact match {
    case Some(n) => s"LENGTH = $n"
    case None    => List(l.min.map(v => s"LENGTH >= $v"), l.max.map(v => s"LENGTH <= $v")).flatten.mkString(" and ")
  }

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

  /** Exactly `rangeVerdict`'s own structure and tie-breaking conventions —
    * `Guaranteed` when `state.length`'s envelope is already at least as
    * tight as `required`'s (`p.tighten(required) == p`, the same
    * `Property.Range.tighten` idiom `Property.Length.tighten` mirrors),
    * `Violated` only when `p` provably *escapes* `required` on either
    * side, `NotGuaranteed`/`NotStaticallyVerifiable` otherwise. Simpler
    * than `rangeVerdict`: `Length` has no `gt`/`lt` exclusive-bound
    * variant, so `exact.orElse(min)`/`exact.orElse(max)` are the whole of
    * each side's own "lower"/"upper" value, with no inclusive/exclusive
    * distinction to carry through an escape check.
    */
  private def lengthVerdict(state: ColumnPropertyState, required: Property.Length): DataQualityVerdict = state.length match {
    case Some(p) if p.tighten(required) == p                                         => DataQualityVerdict.Guaranteed
    case Some(p) if lengthEscapesBelow(p, required) || lengthEscapesAbove(p, required) => DataQualityVerdict.Violated
    case _                                                                             => if (state.unsupported) DataQualityVerdict.NotStaticallyVerifiable else DataQualityVerdict.NotGuaranteed
  }

  private def lengthLowerValue(l: Property.Length): Option[Int] = l.exact.orElse(l.min)
  private def lengthUpperValue(l: Property.Length): Option[Int] = l.exact.orElse(l.max)

  // Each `case (_, None) => false` below is a genuinely equivalent mutant
  // to its own `=> true` flip, not an untested gap: whenever `required`
  // declares no bound at all on this axis, `Length.tighten`'s own `lo`/`hi`
  // computation always resolves to `p`'s own raw value there (nothing on
  // required's side to widen or narrow it), so `p.tighten(required) == p`
  // already succeeds on that axis by construction - `lengthVerdict` only
  // ever reaches this function's `required`-missing-this-axis case when
  // the *other* axis is what's keeping the verdict from being `Guaranteed`,
  // and that other axis's own escape check is what actually decides the
  // verdict. Confirmed by construction, not just asserted: no proven/
  // required pair exists where flipping this branch changes `lengthVerdict`
  // scoped Stryker output.
  private def lengthEscapesBelow(p: Property.Length, required: Property.Length): Boolean = (lengthLowerValue(p), lengthLowerValue(required)) match {
    case (_, None)            => false // required has no lower bound: nothing to escape below
    case (None, Some(_))      => true  // p is unbounded below, required is not: p can escape
    case (Some(pv), Some(rv)) => pv < rv // strict: an exact tie is not a proven escape
  }

  private def lengthEscapesAbove(p: Property.Length, required: Property.Length): Boolean = (lengthUpperValue(p), lengthUpperValue(required)) match {
    case (_, None)            => false // see lengthEscapesBelow's own doc - the same equivalence applies here
    case (None, Some(_))      => true
    case (Some(pv), Some(rv)) => pv > rv
  }
}
