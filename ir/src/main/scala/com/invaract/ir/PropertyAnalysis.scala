// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.ir

import scala.util.control.TailCalls._

/** Traces which statically-provable value-domain facts (`ColumnPropertyState`
  * — not null, equals/one-of a constant, a numeric bound) hold for each
  * column a `Write` ultimately produces, given a set of trusted axioms
  * seeded at the plan's `Read` nodes — the `PropertyAnalysis` counterpart to
  * `Lineage.trace`. See docs/STATIC_DATA_QUALITY_VERIFICATION.md for the
  * full design; §1 explains why this is its own traversal rather than a
  * consumer of `Lineage`'s `DerivationKind` (too coarse — two `Computed`
  * columns built from different operations can have completely different
  * provable properties).
  *
  * Resolution is purely structural, mirroring `Lineage`'s own approach
  * exactly (no symbol table, no exprIds, the same pass-through-node
  * fall-through, the same `Join`/`Union` ambiguous-name handling) — and,
  * for the same reason `Lineage` documents (a real generated pipeline's
  * chain of nested `Project`s from repeated `.withColumn()` calls has
  * already been measured to overflow a plain-recursive walk of this shape),
  * every genuinely recursive traversal here is trampolined via
  * `scala.util.control.TailCalls`.
  *
  * This module knows nothing about `Contract` — `axioms` is supplied by the
  * caller (`spark-adapter`'s `StaticDataQualityVerifier`, which is where a
  * type needing both `ir` and `contract` concepts belongs — see
  * `SensitivityLineage`'s identical division of labor for the equivalent
  * `Lineage`-consuming case).
  */
object PropertyAnalysis {

  case class ColumnPropertyResult(output: ColumnRef, state: ColumnPropertyState)

  def analyze(plan: Plan, axioms: Map[ColumnRef, ColumnPropertyState]): List[ColumnPropertyResult] =
    (plan match {
      case Write(_, input, _, _, _) => outputsOfT(input, axioms)
      case other                    => outputsOfT(other, axioms)
    }).result

  private def traverseT[A, B](xs: List[A])(f: A => TailRec[B]): TailRec[List[B]] = xs match {
    case Nil => done(Nil)
    case head :: tail =>
      for {
        b  <- tailcall(f(head))
        bs <- traverseT(tail)(f)
      } yield b :: bs
  }

  private def outputsOfT(plan: Plan, axioms: Map[ColumnRef, ColumnPropertyState]): TailRec[List[ColumnPropertyResult]] = plan match {
    case Project(input, columns) =>
      traverseT(columns) { case NamedExpr(name, expr) =>
        tailcall(resolveExprT(expr, input, axioms)).map(s => ColumnPropertyResult(ColumnRef(name), s))
      }

    case Aggregate(input, groupBy, aggregates) =>
      val groupByNames: Set[String] = groupBy.collect { case ColumnReference(ref) => ref.name }.toSet
      traverseT(aggregates) { nc => tailcall(resolveAggregationColumn(nc, groupByNames, input, axioms)) }

    case Window(input, windowExprs, _, _) =>
      for {
        base <- tailcall(outputsOfT(input, axioms))
        windowOnes <- traverseT(windowExprs) { case NamedExpr(name, _) =>
          // MVP: every windowed output is Unknown-with-unsupported — see
          // the design doc's §3.2/§9: a sound per-window-function rule
          // (does RANK ever produce null? does a partitioned SUM over an
          // empty partition?) is real, function-specific work deferred the
          // same way Aggregate's own per-function rules are.
          done(ColumnPropertyResult(ColumnRef(name), ColumnPropertyState.Unknown.copy(unsupported = true)))
        }
      } yield base ++ windowOnes

    case Filter(input, _)   => tailcall(outputsOfT(input, axioms))
    case Sort(input, _)     => tailcall(outputsOfT(input, axioms))
    case Limit(input, _, _) => tailcall(outputsOfT(input, axioms))

    case Union(inputs) =>
      traverseT(inputs)(outputsOfT(_, axioms)).map { branchResults =>
        // Output names follow the first branch (Union's own doc); align
        // every branch by position, not name, the same convention.
        branchResults.map(_.size) match {
          case Nil => Nil
          case sizes =>
            val width = sizes.min
            (0 until width).toList.map { i =>
              ColumnPropertyResult(branchResults.head(i).output, ColumnPropertyState.union(branchResults.map(_(i).state)))
            }
        }
      }

    case Join(left, right, joinType, _) =>
      for {
        l <- tailcall(outputsOfT(left, axioms))
        r <- tailcall(outputsOfT(right, axioms))
      } yield demoteForJoin(l, joinType, keepsThisSide = preservesLeft(joinType)) ++
        demoteForJoin(r, joinType, keepsThisSide = preservesRight(joinType))

    case Write(_, input, _, _, _) => tailcall(outputsOfT(input, axioms))

    // A bare Read declares no output list of its own — nothing to trace
    // until something downstream projects it (mirrors Lineage exactly).
    case Read(_, _, _)        => done(Nil)
    case UnknownPlan(_, _, _) => done(Nil)
  }

  /** Whether a `JoinType` preserves the left/right side's own proven facts
    * for a column sourced from it, or can synthetically null-fill that
    * whole side for an unmatched row — see
    * docs/STATIC_DATA_QUALITY_VERIFICATION.md §3.5's table (Example 6).
    */
  private def preservesLeft(joinType: JoinType): Boolean = joinType match {
    case JoinType.RightOuter | JoinType.FullOuter => false
    case _                                         => true
  }
  private def preservesRight(joinType: JoinType): Boolean = joinType match {
    case JoinType.LeftOuter | JoinType.FullOuter => false
    case JoinType.LeftSemi | JoinType.LeftAnti   => true // moot: these project no right-side columns at all
    case _                                        => true
  }

  private def demoteForJoin(results: List[ColumnPropertyResult], joinType: JoinType, keepsThisSide: Boolean): List[ColumnPropertyResult] =
    if (keepsThisSide) results
    else results.map(r => ColumnPropertyResult(r.output, ColumnPropertyState.Unknown.copy(unsupported = r.state.unsupported)))

  /** Shared by `outputsOfT`'s `Aggregate` case and `resolveInScopeT`'s —
    * a bare `groupBy` passthrough column keeps its own upstream state
    * (grouping doesn't alter a key column's own value domain); every other
    * aggregate output is `Unknown`-with-`unsupported` in MVP (see the
    * design doc's §3.2/§9 — a wrong per-aggregate-function rule is a false
    * guarantee, the one outcome this whole design exists to prevent).
    */
  private def resolveAggregationColumn(
    nc: NamedExpr,
    groupByNames: Set[String],
    input: Plan,
    axioms: Map[ColumnRef, ColumnPropertyState]
  ): TailRec[ColumnPropertyResult] = nc match {
    case NamedExpr(name, ColumnReference(ref)) if groupByNames.contains(ref.name) =>
      tailcall(resolveExprT(ColumnReference(ref), input, axioms)).map(s => ColumnPropertyResult(ColumnRef(name), s))
    case NamedExpr(name, _) =>
      done(ColumnPropertyResult(ColumnRef(name), ColumnPropertyState.Unknown.copy(unsupported = true)))
  }

  private def resolveExprT(expr: Expr, input: Plan, axioms: Map[ColumnRef, ColumnPropertyState]): TailRec[ColumnPropertyState] = expr match {
    case ColumnReference(ref) =>
      tailcall(resolveInScopeT(ref, input, axioms)).map(_.getOrElse(ColumnPropertyState.Unknown))

    case Literal(v, t) => done(literalState(v, t))

    case Alias(_, inner) => tailcall(resolveExprT(inner, input, axioms))

    case Cast(inner, _) =>
      // MVP: only NotNull survives a Cast — see the design doc's §3.3/§9
      // for why EqualsConstant/OneOf/Range don't (type-dependent, and a
      // wrong preservation rule risks a false Guaranteed).
      tailcall(resolveExprT(inner, input, axioms)).map(s => ColumnPropertyState(notNull = s.notNull, unsupported = s.unsupported))

    case Arithmetic(op, operands) =>
      traverseT(operands)(o => tailcall(resolveExprT(o, input, axioms)).map(s => (o, s))).map(pairs => arithmeticState(op, pairs))

    case Comparison(_, left, right) =>
      for {
        l <- tailcall(resolveExprT(left, input, axioms))
        r <- tailcall(resolveExprT(right, input, axioms))
      } yield ColumnPropertyState(notNull = combineNotNullAll(List(l, r)), unsupported = l.unsupported || r.unsupported)

    case BooleanExpr(_, operands) =>
      traverseT(operands)(o => tailcall(resolveExprT(o, input, axioms)))
        .map(states => ColumnPropertyState(notNull = combineNotNullAll(states), unsupported = states.exists(_.unsupported)))

    case Conditional(branches, elseValue) =>
      tailcall(resolveConditionalT(branches, elseValue, input, axioms))

    case Function(name, args) =>
      traverseT(args)(a => tailcall(resolveExprT(a, input, axioms)).map(s => (a, s))).map(pairs => functionState(name, pairs))

    case UDF(_, args, _) =>
      // Opaque unconditionally, regardless of what its arguments resolve
      // to — a UDF's body is opaque to this IR by design (see UDF's own
      // doc in Expr.scala), and per the design brief's own instruction,
      // never assumed to preserve anything without an explicit, trusted
      // semantic definition, which a UDF by construction doesn't have.
      traverseT(args)(a => tailcall(resolveExprT(a, input, axioms))).map(_ => ColumnPropertyState.Unknown.copy(unsupported = true))

    case AggregateCall(_, arg, _) =>
      // Only reachable when resolving an Aggregate/Window output directly
      // through resolveInScopeT's own dedicated handling; not separately
      // useful here, but resolved (and discarded) so a malformed hand-built
      // plan that reaches this node some other way still terminates safely
      // rather than crashing.
      tailcall(resolveExprT(arg, input, axioms)).map(_ => ColumnPropertyState.Unknown.copy(unsupported = true))

    case UnknownExpression(_, _, children) =>
      traverseT(children)(c => tailcall(resolveExprT(c, input, axioms))).map(_ => ColumnPropertyState.Unknown.copy(unsupported = true))

    case StructField(StructConstruct(fields), fieldName) =>
      // The one case this analysis can actually trace through: a struct
      // built right here (StructConstruct) and immediately having one of
      // its own named fields extracted back out - resolve straight
      // through to that field's own value expression, the same as any
      // other nested expression. This is deliberately narrow: it proves
      // exactly the "construct, then extract, in the same plan" pattern
      // (Example 3's own oneOf/CASE WHEN shape, now for a struct field
      // instead of a flat column) — see docs/STATIC_DATA_QUALITY_VERIFICATION.md
      // §3.8/§9 for why the harder case (an axiom for a nested field on an
      // *input* struct column, propagated through arbitrary plan shapes)
      // stays out of scope here.
      fields.find(_._1 == fieldName) match {
        case Some((_, value)) => tailcall(resolveExprT(value, input, axioms))
        case None              => done(ColumnPropertyState.Unknown.copy(unsupported = true))
      }

    case StructField(struct, _) =>
      // Any other struct-valued expression (a column reference to an
      // existing struct - e.g. one read from the input - the result of a
      // UDF, a nested StructField, ...): this analysis has no axiom
      // representation for a struct's own internal fields (axioms are
      // seeded per flat Read-scoped column only), so nothing here can be
      // proven or refuted about a field reached through it. Still resolves
      // (and discards) the struct expression itself so a well-formed plan
      // terminates safely rather than short-circuiting.
      tailcall(resolveExprT(struct, input, axioms)).map(_ => ColumnPropertyState.Unknown.copy(unsupported = true))

    case StructConstruct(fields) =>
      // A struct literal is itself never SQL NULL, even when one of its
      // own fields is - constructing the struct is what StructField above
      // actually reaches into; this case only handles a StructConstruct
      // resolved as a column's own top-level value (e.g. a `nullable:
      // false` check directly on a struct-typed output column), where
      // Proven is a real, sound fact, not a guess.
      traverseT(fields.map(_._2))(v => tailcall(resolveExprT(v, input, axioms)))
        .map(_ => ColumnPropertyState(notNull = NullabilityFact.Proven))
  }

  /** A `CASE WHEN ... END` with no `ELSE` produces SQL `NULL` for a
    * non-matching row — always `Unknown` in MVP (proving branch
    * exhaustiveness is out of scope; see the design doc's §3.3). With an
    * `ELSE`, each branch's value is resolved *narrowed* by that branch's
    * own condition, conjoined with every earlier branch's condition
    * negated (this is what makes Example 4's clamp provable: `ELSE amount`
    * is resolved knowing `NOT(amount < 0)`, i.e. `amount >= 0`, holds) —
    * but, as a deliberate MVP simplification the design doc's own
    * implementation notes should record, narrowing only applies when a
    * branch's *value* is itself a bare `ColumnReference` naming the
    * narrowed column directly, not an arbitrary expression that merely
    * *uses* it. The overall result is the union (§4.5) of every branch's
    * (narrowed) value state, `elseValue`'s included.
    */
  private def resolveConditionalT(
    branches: List[(Expr, Expr)],
    elseValue: Option[Expr],
    input: Plan,
    axioms: Map[ColumnRef, ColumnPropertyState]
  ): TailRec[ColumnPropertyState] = elseValue match {
    case None => done(ColumnPropertyState.Unknown)
    case Some(elseExpr) =>
      def resolveNarrowed(valueExpr: Expr, extraFacts: Map[ColumnRef, ColumnPropertyState]): TailRec[ColumnPropertyState] =
        valueExpr match {
          case ColumnReference(ref) if extraFacts.contains(ref) =>
            tailcall(resolveExprT(valueExpr, input, axioms)).map(_.tightenWith(extraFacts(ref)))
          case _ =>
            tailcall(resolveExprT(valueExpr, input, axioms))
        }

      def go(remaining: List[(Expr, Expr)], negatedSoFar: Map[ColumnRef, ColumnPropertyState]): TailRec[List[ColumnPropertyState]] =
        remaining match {
          case Nil => done(Nil)
          case (cond, value) :: rest =>
            val thisTrueFacts = PredicateFacts.merge(negatedSoFar, PredicateFacts.requiredFacts(cond, negated = false))
            val restNegated = PredicateFacts.merge(negatedSoFar, PredicateFacts.requiredFacts(cond, negated = true))
            for {
              v  <- tailcall(resolveNarrowed(value, thisTrueFacts))
              rs <- tailcall(go(rest, restNegated))
            } yield v :: rs
        }

      val elseFacts = branches.foldLeft(Map.empty[ColumnRef, ColumnPropertyState]) { (acc, branch) =>
        PredicateFacts.merge(acc, PredicateFacts.requiredFacts(branch._1, negated = true))
      }
      for {
        branchStates <- tailcall(go(branches, Map.empty))
        elseState    <- tailcall(resolveNarrowed(elseExpr, elseFacts))
      } yield ColumnPropertyState.union(branchStates :+ elseState)
  }

  private def resolveInScopeT(ref: ColumnRef, plan: Plan, axioms: Map[ColumnRef, ColumnPropertyState]): TailRec[Option[ColumnPropertyState]] = plan match {
    case Read(dataset, alias, _) =>
      val scope = alias.getOrElse(dataset.location)
      done(
        if (ref.qualifier.forall(_ == scope))
          Some(axioms.getOrElse(ColumnRef(ref.name, Some(scope)), ColumnPropertyState.Unknown))
        else
          None
      )

    case Project(input, columns) =>
      columns.find(_.name == ref.name) match {
        case Some(nc) => tailcall(resolveExprT(nc.expr, input, axioms)).map(Some(_))
        case None     => done(None)
      }

    case Aggregate(input, groupBy, aggregates) =>
      val groupByNames: Set[String] = groupBy.collect { case ColumnReference(gref) => gref.name }.toSet
      aggregates.find(_.name == ref.name) match {
        case Some(nc) => tailcall(resolveAggregationColumn(nc, groupByNames, input, axioms)).map(r => Some(r.state))
        case None     => done(None)
      }

    case Window(input, windowExprs, _, _) =>
      windowExprs.find(_.name == ref.name) match {
        case Some(_) => done(Some(ColumnPropertyState.Unknown.copy(unsupported = true)))
        case None    => tailcall(resolveInScopeT(ref, input, axioms))
      }

    case Filter(input, condition) =>
      tailcall(resolveInScopeT(ref, input, axioms)).map { base =>
        val facts = PredicateFacts.requiredFacts(condition)
        (base, facts.get(ref)) match {
          case (Some(b), Some(f)) => Some(b.tightenWith(f))
          case (Some(b), None)    => Some(b)
          case (None, _)          => None
        }
      }

    case Sort(input, _)     => tailcall(resolveInScopeT(ref, input, axioms))
    case Limit(input, _, _) => tailcall(resolveInScopeT(ref, input, axioms))

    case Union(inputs) =>
      traverseT(inputs)(resolveInScopeT(ref, _, axioms)).map { results =>
        val found = results.flatten
        if (found.isEmpty) None else Some(ColumnPropertyState.union(found))
      }

    case Join(left, right, joinType, _) =>
      for {
        l <- tailcall(resolveInScopeT(ref, left, axioms)).map(_.map(s => if (preservesLeft(joinType)) s else ColumnPropertyState.Unknown.copy(unsupported = s.unsupported)))
        r <- tailcall(resolveInScopeT(ref, right, axioms)).map(_.map(s => if (preservesRight(joinType)) s else ColumnPropertyState.Unknown.copy(unsupported = s.unsupported)))
      } yield (l, r) match {
        case (Some(lv), None)     => Some(lv)
        case (None, Some(rv))     => Some(rv)
        case (Some(lv), Some(rv)) => Some(ColumnPropertyState.union(List(lv, rv)))
        case (None, None)         => None
      }

    case Write(_, input, _, _, _) => tailcall(resolveInScopeT(ref, input, axioms))

    case UnknownPlan(_, _, _) => done(None)
  }

  private def combineNotNullAll(states: List[ColumnPropertyState]): NullabilityFact =
    if (states.forall(_.notNull == NullabilityFact.Proven)) NullabilityFact.Proven else NullabilityFact.Unknown

  private def literalState(v: Any, t: String): ColumnPropertyState =
    if (v == null) ColumnPropertyState(notNull = NullabilityFact.Refuted)
    else {
      val range =
        if (NumericLiterals.isNumeric(t)) NumericLiterals.toBigDecimal(v).map(bd => Property.Range(gte = Some(bd), lte = Some(bd))) else None
      val length = v match {
        case s: String if t.toLowerCase == "string" => Some(Property.Length(exact = Some(s.length)))
        case _                                        => None
      }
      ColumnPropertyState(notNull = NullabilityFact.Proven, equalsConstant = Some(Property.EqualsConstant(v, t)), range = range, length = length)
    }

  private def functionState(name: String, argPairs: List[(Expr, ColumnPropertyState)]): ColumnPropertyState = {
    val argStates = argPairs.map(_._2)
    val baseNotNull = combineNotNullAll(argStates)
    val unsupported = argStates.exists(_.unsupported)
    name match {
      case "ABS" =>
        // Established regardless of the argument's own range - the
        // narrowest sound statement about ABS's result is always >= 0.
        ColumnPropertyState(notNull = baseNotNull, range = Some(Property.Range(gte = Some(0))), unsupported = unsupported)
      case "COALESCE" =>
        val hasNonNullLiteralFallback = argPairs.exists {
          case (Literal(v, _), _) => v != null
          case _                   => false
        }
        ColumnPropertyState(notNull = if (hasNonNullLiteralFallback) NullabilityFact.Proven else baseNotNull, unsupported = unsupported)
      case "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" =>
        // LENGTH(...) returns an integer, not a string - its own Range
        // (not Length) is exactly its argument's already-known Length
        // envelope, carried over verbatim (exact -> exact, min/max ->
        // gte/lte). No rule at all when the argument's own length isn't
        // known - this is a bridge between the two property kinds, not a
        // fresh fact invented from nothing.
        val range = argStates.headOption.flatMap(_.length).map(lengthAsRange)
        ColumnPropertyState(notNull = baseNotNull, range = range, unsupported = unsupported)
      case "UPPER" | "LOWER" =>
        // Case conversion changes no character count - the result's own
        // Length envelope is identical to the argument's.
        ColumnPropertyState(notNull = baseNotNull, length = argStates.headOption.flatMap(_.length), unsupported = unsupported)
      case "TRIM" | "LTRIM" | "RTRIM" =>
        // Trimming can only shrink the string (or leave it unchanged) -
        // only an upper bound on length survives; the lower bound (how
        // much whitespace was actually removed) is never knowable
        // statically, so it's deliberately dropped rather than carried
        // over as a false floor.
        val trimmedLength = argStates.headOption.flatMap(_.length).flatMap(lengthUpperBound).map(ub => Property.Length(max = Some(ub)))
        ColumnPropertyState(notNull = baseNotNull, length = trimmedLength, unsupported = unsupported)
      case _ =>
        // Not flagged `unsupported` on its own - a Function is still a
        // claim this IR understands the *shape* of the computation (see
        // Expr.scala's own doc), just not one this analysis has a
        // value-domain rule for. It only becomes the reason a specific
        // rule can't be proven, exactly like any other Unknown - see the
        // design doc's §3.3/§3.7.
        ColumnPropertyState(notNull = baseNotNull, unsupported = unsupported)
    }
  }

  private def arithmeticState(operator: String, operandPairs: List[(Expr, ColumnPropertyState)]): ColumnPropertyState = {
    val states = operandPairs.map(_._2)
    val notNull = combineNotNullAll(states)
    val unsupported = states.exists(_.unsupported)
    val range: Option[Property.Range] = (operator, operandPairs) match {
      case ("NEGATE", List((_, s))) => s.range.map(_.negate)

      case ("+", List((le, ls), (re, rs))) =>
        numericLiteral(re).flatMap(rv => ls.range.map(_.shift(rv))).orElse(numericLiteral(le).flatMap(lv => rs.range.map(_.shift(lv))))

      case ("-", List((le, ls), (re, rs))) =>
        numericLiteral(re)
          .flatMap(rv => ls.range.map(_.shift(-rv)))
          .orElse(numericLiteral(le).flatMap(lv => rs.range.map(_.negate.shift(lv))))

      case ("*", List((le, ls), (re, rs))) =>
        numericLiteral(re)
          .filter(_ >= 0)
          .flatMap(rv => ls.range.map(_.scale(rv)))
          .orElse(numericLiteral(le).filter(_ >= 0).flatMap(lv => rs.range.map(_.scale(lv))))

      case _ => None
    }
    ColumnPropertyState(notNull = notNull, range = range, unsupported = unsupported)
  }

  private def numericLiteral(e: Expr): Option[BigDecimal] = e match {
    case Literal(v, t) if NumericLiterals.isNumeric(t) => NumericLiterals.toBigDecimal(v)
    case _                                              => None
  }

  /** `LENGTH(...)`'s own numeric-valued Range, carried over from its
    * argument's already-known `Length` envelope: an `exact` length
    * becomes a single-point range, a `min`/`max` envelope becomes
    * `gte`/`lte`.
    */
  private def lengthAsRange(l: Property.Length): Property.Range = {
    val (lo, hi) = if (l.exact.isDefined) (l.exact, l.exact) else (l.min, l.max)
    Property.Range(gte = lo.map(BigDecimal(_)), lte = hi.map(BigDecimal(_)))
  }

  private def lengthUpperBound(l: Property.Length): Option[Int] = if (l.exact.isDefined) l.exact else l.max
}
