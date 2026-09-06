// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.fingerprint

import com.invaract.ir._

import CanonicalNode._

/** One `Read` occurrence found while walking a `Plan`, in encounter order.
  * `positionalId` is the canonical, alias-string-independent label
  * (`"src0"`, `"src1"`, ...) that `ScopeInfo.substitution` maps this
  * occurrence's raw scope string to; `inputKey` (`"<location>#<index>"`,
  * indexed per distinct `location` — not deduplicated the way
  * `positionalId` is) is what `TransformationFingerprinter` keys the
  * `inputs` map by.
  */
final case class ReadOccurrence(location: String, positionalId: String, inputKey: String)

/** The result of one pre-order walk over a `Plan`, gathering everything
  * needed to make `ColumnRef.qualifier` alias-relabelling-invariant — see
  * docs/SEMANTIC_LINEAGE_FINGERPRINTING.md §2.3.
  *
  * `substitution` maps a raw scope string (`Read.alias.getOrElse(Read.
  * dataset.location)`, exactly what `ir.Lineage` itself uses as a
  * `ColumnRef.qualifier`) to a positional id assigned by first occurrence
  * of that *string* — so relabelling a self-join's aliases consistently
  * throughout a plan produces the same substitution targets, while two
  * genuinely different `Read`s (different `location`, or the same
  * `location` read twice under distinct aliases) always get distinct ids.
  */
final case class ScopeInfo(substitution: Map[String, String], reads: List[ReadOccurrence])

/** Turns an `ir.Plan`/`ir.Expr`/`ir.Lineage.ColumnLineage` value into a
  * `CanonicalNode` tree suitable for hashing — the canonicalisation
  * strategy specified in docs/SEMANTIC_LINEAGE_FINGERPRINTING.md. Pure and
  * total over every node kind in `ir` (including `UnknownPlan`/
  * `UnknownExpression`, which must never silently disappear — see §8).
  *
  * `ColumnRef.id` is never read anywhere in this file. That omission is
  * deliberate and load-bearing (§2.2): `id` is populated from Spark's own
  * `exprId.id`, a per-session runtime-assigned integer, and including it
  * anywhere in a canonical form would break the "same model → same
  * fingerprint, independent of runtime identifiers" guarantee this whole
  * module exists to provide.
  */
object Canonicalizer {

  // ---------------------------------------------------------------------
  // Scope table (§2.3)
  // ---------------------------------------------------------------------

  def buildScopeInfo(plan: Plan): ScopeInfo = {
    val substitution = scala.collection.mutable.LinkedHashMap.empty[String, String]
    val reads = scala.collection.mutable.ArrayBuffer.empty[ReadOccurrence]
    val locationCounts = scala.collection.mutable.HashMap.empty[String, Int].withDefaultValue(0)

    def walk(p: Plan): Unit = p match {
      case Read(dataset, alias) =>
        val scopeString = alias.getOrElse(dataset.location)
        val positionalId = substitution.getOrElseUpdate(scopeString, s"src${substitution.size}")
        val index = locationCounts(dataset.location)
        locationCounts(dataset.location) = index + 1
        reads += ReadOccurrence(dataset.location, positionalId, s"${dataset.location}#$index")
      case other =>
        other.children.foreach(walk)
    }
    walk(plan)
    ScopeInfo(substitution.toMap, reads.toList)
  }

  private def substituteQualifier(qualifier: String, scope: Map[String, String]): String =
    // A qualifier matching no known Read scope is left untouched rather
    // than treated as an error - a bare, not-yet-resolved reference (e.g.
    // one sitting in a Filter condition, resolved only by ir.Lineage's own
    // structural walk, not by this best-effort substitution table) is a
    // real, valid state, not a malformed one. See §2.3's own doc.
    scope.getOrElse(qualifier, qualifier)

  // ---------------------------------------------------------------------
  // Expressions
  // ---------------------------------------------------------------------

  def canonicalizeColumnRef(ref: ColumnRef, scope: Map[String, String]): CanonicalNode = {
    val qualifierNode = optionNode(ref.qualifier.map(q => stringLeaf(substituteQualifier(q, scope))))
    // ref.id is intentionally never referenced - see this object's own doc.
    CTag("ColumnRef", List(stringLeaf(ref.name), qualifierNode))
  }

  def canonicalizeExpr(expr: Expr, scope: Map[String, String]): CanonicalNode = expr match {
    case ColumnReference(ref) => CTag("ColumnReference", List(canonicalizeColumnRef(ref, scope)))
    case Literal(value, literalType) => LiteralEncoding.encode(value, literalType)
    case Alias(name, inner) => CTag("Alias", List(stringLeaf(name), canonicalizeExpr(inner, scope)))
    case Cast(inner, targetType) => CTag("Cast", List(canonicalizeExpr(inner, scope), stringLeaf(targetType)))
    // Operand order always preserved - never sorted, even for an
    // abstractly-commutative operator. See §2.4/§6: floating-point
    // rounding and evaluation-order hazards make blanket commutative
    // normalisation unsafe as a default.
    case Arithmetic(operator, operands) =>
      CTag("Arithmetic", stringLeaf(operator) :: operands.map(canonicalizeExpr(_, scope)))
    case Comparison(operator, left, right) =>
      CTag("Comparison", List(stringLeaf(operator), canonicalizeExpr(left, scope), canonicalizeExpr(right, scope)))
    case BooleanExpr(operator, operands) =>
      CTag("BooleanExpr", stringLeaf(operator) :: operands.map(canonicalizeExpr(_, scope)))
    case Conditional(branches, elseValue) =>
      // Branch order always preserved - first-matching-branch-wins makes
      // it observable, unlike Aggregate.groupBy's set-like keys.
      val branchNodes = branches.map { case (cond, value) =>
        CTag("Branch", List(canonicalizeExpr(cond, scope), canonicalizeExpr(value, scope)))
      }
      CTag("Conditional", branchNodes :+ CTag("Else", optionNode(elseValue.map(canonicalizeExpr(_, scope))) :: Nil))
    case Function(name, args) =>
      CTag("Function", stringLeaf(name) :: args.map(canonicalizeExpr(_, scope)))
    case UDF(name, args, _engineType) =>
      // engineType is deliberately never read here - reported as metadata
      // elsewhere (see NonDeterminism/TransformationFingerprinter), never
      // hash-affecting. See §7's rationale (translator-classification
      // noise risk).
      CTag(
        "UDF",
        List(
          optionNode(name.map(stringLeaf)),
          CTag("Args", args.map(canonicalizeExpr(_, scope)))
        )
      )
    case AggregateCall(function, arg, distinct) =>
      CTag("AggregateCall", List(stringLeaf(function), boolLeaf(distinct), canonicalizeExpr(arg, scope)))
    case UnknownExpression(_description, sourceType, children) =>
      // description is deliberately never read here - free text, excluded
      // from the hash per §8 (surfaced as metadata elsewhere, never hash
      // input). sourceType (a stable structural label for *what kind* of
      // construct this is) and children are always included.
      CTag("UnknownExpression", List(stringLeaf(sourceType), CTag("Children", children.map(canonicalizeExpr(_, scope)))))
  }

  private def canonicalizeNamedExpr(ne: NamedExpr, scope: Map[String, String]): CanonicalNode =
    CTag("NamedExpr", List(stringLeaf(ne.name), canonicalizeExpr(ne.expr, scope)))

  private def canonicalizeSortOrder(order: SortOrder, scope: Map[String, String]): CanonicalNode =
    CTag("SortOrder", List(canonicalizeExpr(order.expr, scope), boolLeaf(order.ascending), boolLeaf(order.nullsFirst)))

  /** Sorts a set-like list of already-canonicalized expressions by their
    * own encoded bytes (§2.4) - used only for `Aggregate.groupBy`/
    * `Window.partitionBy`, whose declared order carries no relational
    * meaning (unlike every other ordered field in the IR).
    */
  private def canonicallySorted(nodes: List[CanonicalNode]): List[CanonicalNode] =
    nodes.sortBy(Encoding.sortKey)

  // ---------------------------------------------------------------------
  // Plans
  // ---------------------------------------------------------------------

  def canonicalizePlan(plan: Plan, scope: Map[String, String]): CanonicalNode = plan match {
    case Read(dataset, _alias) =>
      // alias is deliberately never read here - it never affects the hash
      // directly, only (via ScopeInfo.substitution, applied at every
      // ColumnRef site) which positional label downstream references are
      // normalized to. See §2.3.
      CTag("Read", List(stringLeaf(dataset.location)))
    case Write(dataset, input, format, saveMode) =>
      CTag(
        "Write",
        List(
          stringLeaf(dataset.location),
          canonicalizePlan(input, scope),
          optionNode(format.map(stringLeaf)),
          optionNode(saveMode.map(stringLeaf))
        )
      )
    case Project(input, columns) =>
      CTag("Project", canonicalizePlan(input, scope) :: columns.map(canonicalizeNamedExpr(_, scope)))
    case Filter(input, condition) =>
      CTag("Filter", List(canonicalizePlan(input, scope), canonicalizeExpr(condition, scope)))
    case Join(left, right, joinType, condition) =>
      // left/right order always preserved - never reordered, even for a
      // relationally-commutative Inner/Cross join. See §2.3/§2.4/§11: this
      // is a deliberately accepted conservatism, not an oversight.
      CTag(
        "Join",
        List(
          canonicalizePlan(left, scope),
          canonicalizePlan(right, scope),
          stringLeaf(joinType.toString),
          optionNode(condition.map(canonicalizeExpr(_, scope)))
        )
      )
    case Aggregate(input, groupBy, aggregates) =>
      // groupBy is set-like (a plain GROUP BY's key order doesn't change
      // which rows fall in which group - this IR models no ROLLUP/CUBE/
      // GROUPING SETS) - canonically sorted, unlike every ordered field
      // above. See §2.4.
      val sortedGroupBy = canonicallySorted(groupBy.map(canonicalizeExpr(_, scope)))
      CTag(
        "Aggregate",
        canonicalizePlan(input, scope) :: CTag("GroupBy", sortedGroupBy) :: aggregates.map(canonicalizeNamedExpr(_, scope))
      )
    case Union(inputs) =>
      // Branch order always preserved - the IR's own doc: "Output column
      // names follow the first branch."
      CTag("Union", inputs.map(canonicalizePlan(_, scope)))
    case Sort(input, order) =>
      CTag("Sort", canonicalizePlan(input, scope) :: order.map(canonicalizeSortOrder(_, scope)))
    case Limit(input, limit, offset) =>
      CTag("Limit", List(canonicalizePlan(input, scope), intLeaf(limit), intLeaf(offset)))
    case Window(input, windowExprs, partitionBy, orderBy) =>
      // partitionBy is set-like, same reasoning as Aggregate.groupBy above;
      // orderBy is not (it defines window-function ordering, e.g. RANK) -
      // preserved, mirroring Sort.order.
      val sortedPartitionBy = canonicallySorted(partitionBy.map(canonicalizeExpr(_, scope)))
      CTag(
        "Window",
        canonicalizePlan(input, scope) ::
          CTag("PartitionBy", sortedPartitionBy) ::
          CTag("OrderBy", orderBy.map(canonicalizeSortOrder(_, scope))) ::
          windowExprs.map(canonicalizeNamedExpr(_, scope))
      )
    case UnknownPlan(_description, sourceType, children) =>
      // Same description/sourceType split as UnknownExpression above.
      CTag("UnknownPlan", stringLeaf(sourceType) :: children.map(canonicalizePlan(_, scope)))
  }

  // ---------------------------------------------------------------------
  // Lineage summary layer (§3's "lineage" fingerprint)
  // ---------------------------------------------------------------------

  def canonicalizeLineage(lineage: ColumnLineage, scope: Map[String, String]): CanonicalNode = {
    // sources/aggregations are Scala Sets - never iterated directly (their
    // iteration order is hash-based, not guaranteed stable - see §2.2/
    // §2.4), always canonically sorted by encoded bytes first.
    val sortedSources = canonicallySorted(lineage.sources.toList.map(canonicalizeColumnRef(_, scope)))
    val sortedAggregations = canonicallySorted(
      lineage.aggregations.toList.map(a => CTag("AggregationDetail", List(stringLeaf(a.function), boolLeaf(a.distinct))))
    )
    // lineage.output is not included here - in TransformationFingerprinter
    // it is the output map's own key, not part of any node's content.
    CTag(
      "ColumnLineage",
      List(
        CTag("Sources", sortedSources),
        stringLeaf(lineage.derivation.toString),
        CTag("Aggregations", sortedAggregations)
      )
    )
  }

  // ---------------------------------------------------------------------
  // Deep expression resolution, for the per-output "expression" layer
  // ---------------------------------------------------------------------
  //
  // Because Invaract translates Spark's *analyzed* (not optimized) plan,
  // a chain of `.withColumn()` calls produces nested Projects - an
  // outermost Project's own declaration for an untouched column is often
  // nothing more than a bare passthrough reference to an inner Project's
  // real computation (see docs/TRANSFORMATION_IR.md's own "Derivation
  // classification" section, and ir.Lineage's identical resolution
  // reasoning). Canonicalizing only the outermost NamedExpr.expr would
  // therefore frequently produce a bare ColumnReference for a column whose
  // real computation lives several plan stages down - missing exactly the
  // "amount * 1.20 -> 1.25" kind of change this module exists to catch.
  //
  // This mirrors ir.Lineage's own resolveExpr/resolveInScope structural
  // walk, but reconstructs an actual (passthrough-inlined) Expr rather
  // than a Provenance summary - it does not claim any two expressions are
  // *equivalent*; it reconstructs what one specific column's one specific
  // computation actually is, the same descriptive (not rewriting) move
  // ir.Lineage's DerivationKind classification already makes. Duplicated
  // here rather than added to ir.Lineage itself (whose private resolution
  // logic isn't part of that module's public API) - a small, stable
  // structural pattern match over Plan's own public case classes, the same
  // kind of plan-shape-aware logic spark-adapter itself already writes
  // against ir's public surface.

  def resolveExprDeep(expr: Expr, input: Plan): Expr = expr match {
    case ColumnReference(ref) => resolveRefDeep(ref, input).getOrElse(expr)
    case literal: Literal     => literal
    case Alias(name, inner)   => Alias(name, resolveExprDeep(inner, input))
    case Cast(inner, targetType) => Cast(resolveExprDeep(inner, input), targetType)
    case Arithmetic(operator, operands) => Arithmetic(operator, operands.map(resolveExprDeep(_, input)))
    case Comparison(operator, left, right) => Comparison(operator, resolveExprDeep(left, input), resolveExprDeep(right, input))
    case BooleanExpr(operator, operands) => BooleanExpr(operator, operands.map(resolveExprDeep(_, input)))
    case Conditional(branches, elseValue) =>
      Conditional(
        branches.map { case (cond, value) => (resolveExprDeep(cond, input), resolveExprDeep(value, input)) },
        elseValue.map(resolveExprDeep(_, input))
      )
    case Function(name, args) => Function(name, args.map(resolveExprDeep(_, input)))
    case UDF(name, args, engineType) => UDF(name, args.map(resolveExprDeep(_, input)), engineType)
    case AggregateCall(function, arg, distinct) => AggregateCall(function, resolveExprDeep(arg, input), distinct)
    case UnknownExpression(description, sourceType, children) =>
      UnknownExpression(description, sourceType, children.map(resolveExprDeep(_, input)))
  }

  private def resolveRefDeep(ref: ColumnRef, plan: Plan): Option[Expr] = plan match {
    case Read(dataset, alias) =>
      val scope = alias.getOrElse(dataset.location)
      if (ref.qualifier.forall(_ == scope)) Some(ColumnReference(ColumnRef(ref.name, Some(scope)))) else None
    case Project(input, columns) => columns.find(_.name == ref.name).map(nc => resolveExprDeep(nc.expr, input))
    case Aggregate(input, _, aggregates) => aggregates.find(_.name == ref.name).map(nc => resolveExprDeep(nc.expr, input))
    case Window(input, windowExprs, _, _) =>
      windowExprs.find(_.name == ref.name).map(nc => resolveExprDeep(nc.expr, input)).orElse(resolveRefDeep(ref, input))
    case Filter(input, _)   => resolveRefDeep(ref, input)
    case Sort(input, _)     => resolveRefDeep(ref, input)
    case Limit(input, _, _) => resolveRefDeep(ref, input)
    case Union(inputs) =>
      // Unlike ir.Lineage's combineUnion (which merges every plausible
      // candidate's *summary* into one Provenance), reconstructing a
      // single Expr means picking one concrete candidate when more than
      // one branch resolves the name - the first, in declared branch
      // order, deterministically. A narrower limitation than Lineage's own
      // resolution for this one ambiguous case; the "lineage" fingerprint
      // layer (built from ir.Lineage.trace directly) is unaffected and
      // still unions every candidate's real sources.
      inputs.view.flatMap(resolveRefDeep(ref, _)).headOption
    case Join(left, right, _, _) =>
      (resolveRefDeep(ref, left), resolveRefDeep(ref, right)) match {
        case (Some(l), None) => Some(l)
        case (None, Some(r)) => Some(r)
        // Both sides match an unqualified name - genuinely ambiguous.
        // Same deterministic-but-narrower-than-Lineage choice as Union
        // above: prefer the left side.
        case (Some(l), Some(_)) => Some(l)
        case (None, None)       => None
      }
    case Write(_, input, _, _) => resolveRefDeep(ref, input)
    case UnknownPlan(_, _, _)  => None
  }

  /** The per-output deep-resolved expression for every output name a
    * `Write`'s (or a bare plan's) top-level `Project`/`Aggregate`/`Window`
    * declares - the same top-level dispatch `ir.Lineage.trace`/`outputsOf`
    * uses, kept structurally parallel to it deliberately (see this
    * object's own "Deep expression resolution" doc above for why this
    * isn't reused directly from `ir.Lineage`).
    */
  def resolvedOutputs(plan: Plan): Map[String, Expr] = {
    def outputsOf(p: Plan): List[(String, Expr)] = p match {
      case Project(input, columns)        => columns.map(nc => nc.name -> resolveExprDeep(nc.expr, input))
      case Aggregate(input, _, aggregates) => aggregates.map(nc => nc.name -> resolveExprDeep(nc.expr, input))
      case Window(input, windowExprs, _, _) =>
        outputsOf(input) ++ windowExprs.map(nc => nc.name -> resolveExprDeep(nc.expr, input))
      case Filter(input, _)   => outputsOf(input)
      case Sort(input, _)     => outputsOf(input)
      case Limit(input, _, _) => outputsOf(input)
      case Union(inputs)      => inputs.headOption.map(outputsOf).getOrElse(Nil)
      case Join(left, right, _, _) => outputsOf(left) ++ outputsOf(right)
      case Write(_, input, _, _)   => outputsOf(input)
      case Read(_, _)          => Nil
      case UnknownPlan(_, _, _) => Nil
    }
    // Later entries win on a duplicate name, matching Map's own
    // to-Map-from-list convention - a real, well-formed plan does not
    // declare the same output name twice at one boundary regardless.
    outputsOf(plan).toMap
  }
}
