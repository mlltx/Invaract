// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.ir

/** A single statically-provable value-domain fact about a column — the
  * closed vocabulary `PropertyAnalysis` reasons about. See
  * docs/STATIC_DATA_QUALITY_VERIFICATION.md §2.2 for the full design
  * rationale; this is deliberately small and closed, not a general
  * constraint language a caller can extend.
  */
sealed trait Property
object Property {

  /** `EqualsConstant` is not modeled as a one-element `OneOf` so that
    * "equals a constant" — a common, simply-stated rule — has its own
    * direct, unambiguous representation.
    */
  case class EqualsConstant(value: Any, literalType: String) extends Property

  case class OneOf(values: Set[Any], literalType: String) extends Property

  /** A numeric bound. At most one of `gte`/`gt`, and at most one of
    * `lte`/`lt`, is ever populated on one `Range` — normalized at
    * construction, not left to callers to keep consistent.
    */
  case class Range(
    gte: Option[BigDecimal] = None,
    gt: Option[BigDecimal] = None,
    lte: Option[BigDecimal] = None,
    lt: Option[BigDecimal] = None
  ) {
    require(gte.isEmpty || gt.isEmpty, "Range: at most one of gte/gt may be set")
    require(lte.isEmpty || lt.isEmpty, "Range: at most one of lte/lt may be set")

    /** The range a negated value (`-x`) satisfies, given `x` satisfies
      * this range — swaps which side each bound constrains and negates
      * its value: `x >= a` becomes `-x <= -a`, `x <= b` becomes
      * `-x >= -b`, and so on for the exclusive variants.
      */
    def negate: Range = Range(gte = lte.map(-_), gt = lt.map(-_), lte = gte.map(-_), lt = gt.map(-_))

    /** The range `x + c` satisfies, given `x` satisfies this range. */
    def shift(c: BigDecimal): Range = Range(gte.map(_ + c), gt.map(_ + c), lte.map(_ + c), lt.map(_ + c))

    /** The range `x * k` satisfies, given `x` satisfies this range and
      * `k >= 0` (a negative `k` is handled by composing `negate` with a
      * scale by the positive magnitude instead — scaling by a negative
      * factor also flips which side each bound constrains, which this
      * method does not attempt on its own).
      */
    def scale(k: BigDecimal): Range = {
      require(k >= 0, "Range.scale: k must be non-negative; compose with negate for a negative factor")
      Range(gte.map(_ * k), gt.map(_ * k), lte.map(_ * k), lt.map(_ * k))
    }

    /** The tightest range consistent with both `this` and `other` holding
      * simultaneously — used when a `Filter`/`CASE` branch condition adds
      * a new bound on top of whatever was already known. Each side
      * independently keeps the tighter (larger lower / smaller upper)
      * bound, preferring the exclusive variant on a tie (`> 0` is
      * tighter than `>= 0`).
      */
    def tighten(other: Range): Range = {
      def tighterLower(a: Option[(BigDecimal, Boolean)], b: Option[(BigDecimal, Boolean)]): Option[(BigDecimal, Boolean)] =
        (a, b) match {
          case (None, x)          => x
          case (x, None)          => x
          case (Some(x), Some(y)) =>
            if (x._1 > y._1) Some(x)
            else if (y._1 > x._1) Some(y)
            else Some((x._1, x._2 || y._2)) // equal value: exclusive wins on a tie
        }
      def tighterUpper(a: Option[(BigDecimal, Boolean)], b: Option[(BigDecimal, Boolean)]): Option[(BigDecimal, Boolean)] =
        (a, b) match {
          case (None, x)          => x
          case (x, None)          => x
          case (Some(x), Some(y)) =>
            if (x._1 < y._1) Some(x)
            else if (y._1 < x._1) Some(y)
            else Some((x._1, x._2 || y._2))
        }
      val lower = tighterLower(gte.map((_, false)).orElse(gt.map((_, true))), other.gte.map((_, false)).orElse(other.gt.map((_, true))))
      val upper = tighterUpper(lte.map((_, false)).orElse(lt.map((_, true))), other.lte.map((_, false)).orElse(other.lt.map((_, true))))
      Range(
        gte = lower.filterNot(_._2).map(_._1),
        gt = lower.filter(_._2).map(_._1),
        lte = upper.filterNot(_._2).map(_._1),
        lt = upper.filter(_._2).map(_._1)
      )
    }

    /** The loosest range that still covers both `this` and `other` — used
      * to combine `Union`/`CASE` branches, where the real value could come
      * from either. `None` (no bound) on either side wins, matching "we no
      * longer know a bound holds when even one branch doesn't guarantee
      * it."
      */
    def widen(other: Range): Range = {
      def looserLower(a: Option[(BigDecimal, Boolean)], b: Option[(BigDecimal, Boolean)]): Option[(BigDecimal, Boolean)] =
        (a, b) match {
          case (None, _) | (_, None) => None
          case (Some(x), Some(y))    =>
            if (x._1 < y._1) Some(x) else if (y._1 < x._1) Some(y) else Some((x._1, x._2 && y._2))
        }
      def looserUpper(a: Option[(BigDecimal, Boolean)], b: Option[(BigDecimal, Boolean)]): Option[(BigDecimal, Boolean)] =
        (a, b) match {
          case (None, _) | (_, None) => None
          case (Some(x), Some(y))    =>
            if (x._1 > y._1) Some(x) else if (y._1 > x._1) Some(y) else Some((x._1, x._2 && y._2))
        }
      val lower = looserLower(gte.map((_, false)).orElse(gt.map((_, true))), other.gte.map((_, false)).orElse(other.gt.map((_, true))))
      val upper = looserUpper(lte.map((_, false)).orElse(lt.map((_, true))), other.lte.map((_, false)).orElse(other.lt.map((_, true))))
      Range(
        gte = lower.filterNot(_._2).map(_._1),
        gt = lower.filter(_._2).map(_._1),
        lte = upper.filterNot(_._2).map(_._1),
        lt = upper.filter(_._2).map(_._1)
      )
    }
  }
}

/** Whether a column has been proven non-null, proven to always be null, or
  * neither — the same tri-state, metadata-only classification idiom
  * `com.invaract.fingerprint.NonDeterminism` already uses for a different
  * per-expression fact, generalized from `Option[Boolean]` to a named enum
  * here since "unknown" and "refuted" both matter for reporting, not just
  * for a single yes/no branch.
  */
sealed trait NullabilityFact
object NullabilityFact {
  case object Proven extends NullabilityFact
  case object Refuted extends NullabilityFact
  case object Unknown extends NullabilityFact
}

/** One column's fully-resolved, analyzed value-domain state — the
  * `PropertyAnalysis` counterpart to `Lineage`'s `Provenance`/
  * `ColumnLineage`. `equalsConstant`/`oneOf`/`range` are each `None` when
  * nothing is known (not "refuted" — absence of proof is not proof of
  * absence). Unlike `notNull`, there is no separate "refuted" state carried
  * *here* for `equalsConstant`/`oneOf`/`range`: whether an analyzed
  * envelope escapes a required bound (`Violated`, per
  * docs/STATIC_DATA_QUALITY_VERIFICATION.md §3.6) is derived once, by the
  * caller, from comparing this state's plain `range`/`oneOf`/`equalsConstant`
  * against the contract's declared obligation — computing and carrying a
  * pre-derived "is this violated" flag through every intermediate plan
  * node it wasn't yet being checked against would be premature.
  *
  * `unsupported` records whether resolution touched a construct with no
  * trusted semantic definition (`UDF`, an unrecognized `Function`'s
  * *argument* — never the unrecognized `Function` itself, whose own lack of
  * a rule is not by itself evidence of anything unusual — an
  * `UnknownExpression`, or a column sourced from an `Aggregate`/`Window`
  * this MVP doesn't reason about) anywhere on the path to this column —
  * see §3.7: it's what distinguishes `NotStaticallyVerifiable` from a
  * plain `NotGuaranteed`.
  */
case class ColumnPropertyState(
  notNull: NullabilityFact = NullabilityFact.Unknown,
  equalsConstant: Option[Property.EqualsConstant] = None,
  oneOf: Option[Property.OneOf] = None,
  range: Option[Property.Range] = None,
  unsupported: Boolean = false
) {

  /** Combines `this` with a *newly established* fact, where both are
    * known to hold simultaneously (an `AND` of predicate conjuncts, or a
    * `Filter`'s condition layered on top of its input's already-known
    * state) — the narrowing direction: a bound only ever gets tighter, a
    * `NotNull` fact only ever gets more certain, never less. Two
    * genuinely contradictory facts (e.g. `Proven` meeting `Refuted`) are
    * a real, if rare, sign of dead code (a filter that can never pass);
    * `Refuted` wins in that case, the same "prefer the fact that would
    * change enforcement behavior" bias `Violated` gets elsewhere in this
    * design (see docs/STATIC_DATA_QUALITY_VERIFICATION.md §3.6).
    */
  def tightenWith(other: ColumnPropertyState): ColumnPropertyState = ColumnPropertyState(
    notNull = (notNull, other.notNull) match {
      case (NullabilityFact.Refuted, _) | (_, NullabilityFact.Refuted) => NullabilityFact.Refuted
      case (NullabilityFact.Proven, _) | (_, NullabilityFact.Proven)   => NullabilityFact.Proven
      case _                                                            => NullabilityFact.Unknown
    },
    equalsConstant = equalsConstant.orElse(other.equalsConstant),
    oneOf = (oneOf, other.oneOf) match {
      case (Some(a), Some(b)) => Some(Property.OneOf(a.values.intersect(b.values), a.literalType))
      case (a, b)              => a.orElse(b)
    },
    range = (range, other.range) match {
      case (Some(a), Some(b)) => Some(a.tighten(b))
      case (a, b)              => a.orElse(b)
    },
    unsupported = unsupported || other.unsupported
  )

  /** Combines `this` with an *alternative* state — a different `Union`/
    * `CASE` branch that could equally be where the real value came from.
    * Unlike `tightenWith`, a fact only survives here if *both* sides
    * agree on it — the widening direction (see `Property.Range.widen`'s
    * own doc for the numeric case).
    */
  def unionWith(other: ColumnPropertyState): ColumnPropertyState = ColumnPropertyState(
    notNull = (notNull, other.notNull) match {
      case (NullabilityFact.Proven, NullabilityFact.Proven)   => NullabilityFact.Proven
      case (NullabilityFact.Refuted, NullabilityFact.Refuted) => NullabilityFact.Refuted
      case _                                                   => NullabilityFact.Unknown
    },
    equalsConstant = (equalsConstant, other.equalsConstant) match {
      case (Some(a), Some(b)) if a == b => Some(a)
      case _                             => None
    },
    oneOf =
      (oneOf.orElse(equalsConstant.map(e => Property.OneOf(Set(e.value), e.literalType))),
       other.oneOf.orElse(other.equalsConstant.map(e => Property.OneOf(Set(e.value), e.literalType)))) match {
        case (Some(a), Some(b)) => Some(Property.OneOf(a.values ++ b.values, a.literalType))
        case _                   => None
      },
    range = (range, other.range) match {
      case (Some(a), Some(b)) => Some(a.widen(b))
      case _                   => None
    },
    unsupported = unsupported || other.unsupported
  )
}

object ColumnPropertyState {
  val Unknown: ColumnPropertyState = ColumnPropertyState()

  def merge(states: Iterable[ColumnPropertyState]): ColumnPropertyState =
    states.foldLeft(ColumnPropertyState.Unknown)(_.tightenWith(_))

  def union(states: Iterable[ColumnPropertyState]): ColumnPropertyState = states.toList match {
    case Nil          => ColumnPropertyState.Unknown
    case head :: tail => tail.foldLeft(head)(_.unionWith(_))
  }
}

/** The `ir.Literal.literalType` names `PredicateFacts`/`PropertyAnalysis`
  * both treat as numeric, and the shared conversion to `BigDecimal` for
  * range arithmetic — kept in one place so the two modules can't drift
  * into disagreeing about which types are numeric.
  */
private[ir] object NumericLiterals {
  val Types: Set[String] = Set("integer", "long", "short", "byte", "double", "float", "decimal")

  def isNumeric(literalType: String): Boolean = Types.contains(literalType.toLowerCase)

  def toBigDecimal(value: Any): Option[BigDecimal] = value match {
    case null => None
    case _    => scala.util.Try(BigDecimal(value.toString)).toOption
  }
}
