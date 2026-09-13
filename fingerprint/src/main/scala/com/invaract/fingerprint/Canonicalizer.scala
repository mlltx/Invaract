// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.fingerprint

import com.invaract.ir._

import CanonicalNode._
import scala.util.control.TailCalls._

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
  *
  * Every genuinely recursive traversal here (`canonicalizeExpr`,
  * `canonicalizePlan`, `resolveExprDeep`/`resolveRefDeep`, and
  * `resolvedOutputs`'s own dispatch) is implemented via
  * `scala.util.control.TailCalls` internally, trampolining the recursion
  * onto the heap instead of the JVM call stack. This is load-bearing, not
  * defensive-only: measured directly (a forked JVM, default stack size),
  * a plain-recursive version of this code stack-overflowed on a chain of
  * roughly 700-1700 nested nodes — well within reach of a real generated
  * pipeline with hundreds of chained `.withColumn()` calls (exactly the
  * shape docs/TRANSFORMATION_IR.md's own "Derivation classification"
  * section describes as an ordinary, expected translation output, not an
  * edge case). The public API below is unaffected by this — every method
  * still takes and returns plain values (`CanonicalNode`, `Expr`,
  * `Option[Expr]`, `Map[String, Expr]`); the trampoline is purely an
  * internal implementation detail, run to completion via `.result` at
  * each public entry point, never exposed as `TailRec` itself.
  */
object Canonicalizer {

  // ---------------------------------------------------------------------
  // Scope table (§2.3)
  // ---------------------------------------------------------------------

  def buildScopeInfo(plan: Plan): ScopeInfo = {
    val substitution = scala.collection.mutable.LinkedHashMap.empty[String, String]
    val reads = scala.collection.mutable.ArrayBuffer.empty[ReadOccurrence]
    val locationCounts = scala.collection.mutable.HashMap.empty[String, Int].withDefaultValue(0)

    // A plain `foreach`-based walk over `children`, not a recursive
    // descent this object defines itself - `Plan.children`'s own
    // depth is exactly the same "long .withColumn() chain" shape the
    // rest of this file trampolines, so this walk inherits the same
    // stack-safety concern. Rewritten as an explicit worklist (a
    // mutable stack, LIFO) rather than recursion for exactly that
    // reason - `scala.collection.mutable.Stack`, not `ArrayDeque`
    // (2.13-only; this module targets 2.12).
    val worklist = scala.collection.mutable.Stack(plan)
    while (worklist.nonEmpty) {
      worklist.pop() match {
        case Read(dataset, alias, _) =>
          val scopeString = alias.getOrElse(dataset.location)
          val positionalId = substitution.getOrElseUpdate(scopeString, s"src${substitution.size}")
          val index = locationCounts(dataset.location)
          locationCounts(dataset.location) = index + 1
          reads += ReadOccurrence(dataset.location, positionalId, s"${dataset.location}#$index")
        case other =>
          // Pushed in reverse so popping (LIFO) still visits children in
          // the same left-to-right, top-to-bottom pre-order a recursive
          // walk would.
          worklist.pushAll(other.children.reverse)
      }
    }
    ScopeInfo(substitution.toMap, reads.toList)
  }

  private def substituteQualifier(qualifier: String, scope: Map[String, String]): String =
    // A qualifier matching no known Read scope is left untouched rather
    // than treated as an error - a bare, not-yet-resolved reference (e.g.
    // one sitting in a Filter condition, resolved only by ir.Lineage's own
    // structural walk, not by this best-effort substitution table) is a
    // real, valid state, not a malformed one. See §2.3's own doc.
    scope.getOrElse(qualifier, qualifier)

  /** Runs `f` over every element of `xs`, left to right, trampolined -
    * building the whole `TailRec[List[B]]` value is itself O(1) JVM stack
    * regardless of `xs`'s length (each recursive call to `traverseT`
    * returns immediately, deferring its own recursive step inside a
    * closure `TailRec.flatMap` stores rather than invokes); the actual
    * work only unwinds, iteratively, once `.result` runs the whole
    * trampoline. Shared by every list-shaped field this object
    * canonicalizes/resolves (`Expr` argument lists, `Plan` children,
    * branches, ...).
    */
  private def traverseT[A, B](xs: List[A])(f: A => TailRec[B]): TailRec[List[B]] = xs match {
    case Nil => done(Nil)
    case head :: tail =>
      for {
        b <- tailcall(f(head))
        bs <- traverseT(tail)(f)
      } yield b :: bs
  }

  // ---------------------------------------------------------------------
  // Expressions
  // ---------------------------------------------------------------------

  /** `rand()`/`random()`/`randn()`, called with no explicit seed, get a
    * fresh random `Long` baked in as a genuine child `Expression` by
    * Spark's own analyzer (Catalyst's `ResolveRandomSeed` rule) - confirmed
    * directly against a real Spark session: analyzing the exact same
    * `.withColumn("r", rand())` twice in one JVM produces two different
    * seed `Literal`s every time, and there is no way to tell "the analyzer
    * assigned this" apart from "the user wrote this exact literal" once
    * the plan is analyzed - Invaract translates only the analyzed plan
    * (see ARCHITECTURE.md), so that distinction is already gone by the
    * time this canonicalizer ever sees it. Hashing that seed like any
    * other `Function` argument would make the fingerprint of the exact
    * same code - arguably the single most common non-deterministic
    * construct in real jobs - different on every run, the "same model ->
    * same fingerprint" guarantee this whole module exists to provide,
    * broken by the one function family whose own analyzer-injected
    * argument masquerades as ordinary user-written data.
    *
    * `uuid()`/`shuffle()` do *not* need this treatment - confirmed
    * directly too: Catalyst's `Uuid`/`Shuffle` expressions store their own
    * analyzer-assigned seed in a field that is never exposed via
    * `Expression.children`, so it never reaches `Function.args` at all
    * (`uuid()` always translates to `Function("UUID", Nil)`, seed or not).
    * `rand`/`randn`'s seed is different: `Rand`/`Randn` model the seed as
    * a real child `Expression`, so it does reach `args` - confirmed by
    * inspecting the actual translated `ir.Expr` both ways.
    *
    * Both the plain SQL name and Spark's own `random` alias for `rand`
    * are listed - confirmed directly that calling `random()` produces a
    * `Function` node whose own name is `"RANDOM"`, not `"RAND"` (Catalyst
    * reports a different `prettyName` per call-site alias for the
    * identical `Rand` expression class), matching `NonDeterminism`'s own
    * allowlist listing both separately for the same reason.
    *
    * The accepted trade-off (§9 of docs/SEMANTIC_LINEAGE_FINGERPRINTING.md):
    * changing an *explicit* seed (`rand(42)` -> `rand(43)`) is no longer
    * detected either, since nothing post-analysis can tell that case apart
    * from the unseeded one. A much smaller loss than the alternative - a
    * fingerprint that never matches itself for the overwhelmingly more
    * common unseeded call.
    */
  private val SeedBearingFunctionNames: Set[String] = Set("rand", "random", "randn")

  def canonicalizeColumnRef(ref: ColumnRef, scope: Map[String, String]): CanonicalNode = {
    val qualifierNode = optionNode(ref.qualifier.map(q => stringLeaf(substituteQualifier(q, scope))))
    // ref.id is intentionally never referenced - see this object's own doc.
    CTag("ColumnRef", List(stringLeaf(ref.name), qualifierNode))
  }

  private def canonicalizeExprT(expr: Expr, scope: Map[String, String]): TailRec[CanonicalNode] = expr match {
    case ColumnReference(ref) => done(CTag("ColumnReference", List(canonicalizeColumnRef(ref, scope))))
    case Literal(value, literalType) => done(LiteralEncoding.encode(value, literalType))
    case Alias(name, inner) =>
      tailcall(canonicalizeExprT(inner, scope)).map(n => CTag("Alias", List(stringLeaf(name), n)))
    case Cast(inner, targetType) =>
      tailcall(canonicalizeExprT(inner, scope)).map(n => CTag("Cast", List(n, stringLeaf(targetType))))
    // Operand order always preserved - never sorted, even for an
    // abstractly-commutative operator. See §2.4/§6: floating-point
    // rounding and evaluation-order hazards make blanket commutative
    // normalisation unsafe as a default.
    case Arithmetic(operator, operands) =>
      traverseT(operands)(canonicalizeExprT(_, scope)).map(nodes => CTag("Arithmetic", stringLeaf(operator) :: nodes))
    case Comparison(operator, left, right) =>
      for {
        l <- tailcall(canonicalizeExprT(left, scope))
        r <- tailcall(canonicalizeExprT(right, scope))
      } yield CTag("Comparison", List(stringLeaf(operator), l, r))
    case BooleanExpr(operator, operands) =>
      traverseT(operands)(canonicalizeExprT(_, scope)).map(nodes => CTag("BooleanExpr", stringLeaf(operator) :: nodes))
    case Conditional(branches, elseValue) =>
      // Branch order always preserved - first-matching-branch-wins makes
      // it observable, unlike Aggregate.groupBy's set-like keys.
      for {
        branchNodes <- traverseT(branches) { case (cond, value) =>
          for {
            c <- tailcall(canonicalizeExprT(cond, scope))
            v <- tailcall(canonicalizeExprT(value, scope))
          } yield CTag("Branch", List(c, v)): CanonicalNode
        }
        elseNode <- elseValue match {
          case Some(e) => tailcall(canonicalizeExprT(e, scope)).map(n => CTag("Else", List(CTag("Option", List(n)))): CanonicalNode)
          case None    => done(CTag("Else", List(CTag("Option"))): CanonicalNode)
        }
      } yield CTag("Conditional", branchNodes :+ elseNode)
    // See SeedBearingFunctionNames' own doc above: rand/random/randn's
    // sole argument is an analyzer-injected random seed when the call is
    // unseeded, indistinguishable post-analysis from a genuinely
    // user-written literal - excluded from the hash entirely for exactly
    // these three names, never for any other Function.
    case Function(name, _) if SeedBearingFunctionNames.contains(name.toLowerCase) =>
      done(CTag("Function", List(stringLeaf(name))))
    case Function(name, args) =>
      traverseT(args)(canonicalizeExprT(_, scope)).map(nodes => CTag("Function", stringLeaf(name) :: nodes))
    case UDF(name, args, _engineType) =>
      // engineType is deliberately never read here - reported as metadata
      // elsewhere (see NonDeterminism/TransformationFingerprinter), never
      // hash-affecting. See §7's rationale (translator-classification
      // noise risk).
      traverseT(args)(canonicalizeExprT(_, scope)).map { nodes =>
        CTag("UDF", List(optionNode(name.map(stringLeaf)), CTag("Args", nodes)))
      }
    case AggregateCall(function, arg, distinct) =>
      tailcall(canonicalizeExprT(arg, scope)).map(n => CTag("AggregateCall", List(stringLeaf(function), boolLeaf(distinct), n)))
    case UnknownExpression(_description, sourceType, children) =>
      // description is deliberately never read here - free text, excluded
      // from the hash per §8 (surfaced as metadata elsewhere, never hash
      // input). sourceType (a stable structural label for *what kind* of
      // construct this is) and children are always included.
      traverseT(children)(canonicalizeExprT(_, scope)).map { nodes =>
        CTag("UnknownExpression", List(stringLeaf(sourceType), CTag("Children", nodes)))
      }
  }

  def canonicalizeExpr(expr: Expr, scope: Map[String, String]): CanonicalNode = canonicalizeExprT(expr, scope).result

  private def canonicalizeNamedExprT(ne: NamedExpr, scope: Map[String, String]): TailRec[CanonicalNode] =
    tailcall(canonicalizeExprT(ne.expr, scope)).map(n => CTag("NamedExpr", List(stringLeaf(ne.name), n)))

  private def canonicalizeSortOrderT(order: SortOrder, scope: Map[String, String]): TailRec[CanonicalNode] =
    tailcall(canonicalizeExprT(order.expr, scope)).map(n => CTag("SortOrder", List(n, boolLeaf(order.ascending), boolLeaf(order.nullsFirst))))

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

  /** `catalog`'s registration identity (technology/catalogName/location/
    * namespace/table) folded into the hash the same way `Write.format`/
    * `saveMode` already are - it describes where data is registered, not
    * any column's business logic, the same "report vs. hash" bucket those
    * two fields are already documented in (see docs/SEMANTIC_LINEAGE_FINGERPRINTING.md
    * §2.2/§3). Applied identically on `Read` and `Write`: two reads (or
    * writes) of the same `location` but genuinely different catalog
    * registrations are treated as observably different, the same way
    * `Write.format`/`saveMode` already distinguish two writes to the same
    * location under a different format/mode.
    */
  private def canonicalizeCatalogIdentity(catalog: Option[CatalogIdentity]): CanonicalNode =
    optionNode(catalog.map { c =>
      CTag(
        "CatalogIdentity",
        List(
          optionNode(c.technology.map(stringLeaf)),
          optionNode(c.catalogName.map(stringLeaf)),
          optionNode(c.location.map(stringLeaf)),
          CTag("Namespace", c.namespace.map(stringLeaf)),
          optionNode(c.table.map(stringLeaf))
        )
      )
    })

  private def canonicalizePlanT(plan: Plan, scope: Map[String, String]): TailRec[CanonicalNode] = plan match {
    case Read(dataset, _alias, catalog) =>
      // alias is deliberately never read here - it never affects the hash
      // directly, only (via ScopeInfo.substitution, applied at every
      // ColumnRef site) which positional label downstream references are
      // normalized to. See §2.3. catalog, unlike alias, does affect the
      // hash - see canonicalizeCatalogIdentity's own doc.
      done(CTag("Read", List(stringLeaf(dataset.location), canonicalizeCatalogIdentity(catalog))))
    case Write(dataset, input, format, saveMode, catalog) =>
      tailcall(canonicalizePlanT(input, scope)).map { inputNode =>
        CTag(
          "Write",
          List(
            stringLeaf(dataset.location),
            inputNode,
            optionNode(format.map(stringLeaf)),
            optionNode(saveMode.map(stringLeaf)),
            canonicalizeCatalogIdentity(catalog)
          )
        )
      }
    case Project(input, columns) =>
      for {
        inputNode <- tailcall(canonicalizePlanT(input, scope))
        columnNodes <- traverseT(columns)(canonicalizeNamedExprT(_, scope))
      } yield CTag("Project", inputNode :: columnNodes)
    case Filter(input, condition) =>
      for {
        inputNode <- tailcall(canonicalizePlanT(input, scope))
        conditionNode <- tailcall(canonicalizeExprT(condition, scope))
      } yield CTag("Filter", List(inputNode, conditionNode))
    case Join(left, right, joinType, condition) =>
      // left/right order always preserved - never reordered, even for a
      // relationally-commutative Inner/Cross join. See §2.3/§2.4/§11: this
      // is a deliberately accepted conservatism, not an oversight.
      for {
        leftNode <- tailcall(canonicalizePlanT(left, scope))
        rightNode <- tailcall(canonicalizePlanT(right, scope))
        conditionNode <- condition match {
          case Some(c) => tailcall(canonicalizeExprT(c, scope)).map(n => optionNode(Some(n)))
          case None    => done(optionNode(None))
        }
      } yield CTag("Join", List(leftNode, rightNode, stringLeaf(joinType.toString), conditionNode))
    case Aggregate(input, groupBy, aggregates) =>
      // groupBy is set-like (a plain GROUP BY's key order doesn't change
      // which rows fall in which group - this IR models no ROLLUP/CUBE/
      // GROUPING SETS) - canonically sorted, unlike every ordered field
      // above. See §2.4.
      for {
        inputNode <- tailcall(canonicalizePlanT(input, scope))
        groupByNodes <- traverseT(groupBy)(canonicalizeExprT(_, scope))
        aggregateNodes <- traverseT(aggregates)(canonicalizeNamedExprT(_, scope))
      } yield CTag("Aggregate", inputNode :: CTag("GroupBy", canonicallySorted(groupByNodes)) :: aggregateNodes)
    case Union(inputs) =>
      // Branch order always preserved - the IR's own doc: "Output column
      // names follow the first branch."
      traverseT(inputs)(canonicalizePlanT(_, scope)).map(nodes => CTag("Union", nodes))
    case Sort(input, order) =>
      for {
        inputNode <- tailcall(canonicalizePlanT(input, scope))
        orderNodes <- traverseT(order)(canonicalizeSortOrderT(_, scope))
      } yield CTag("Sort", inputNode :: orderNodes)
    case Limit(input, limit, offset) =>
      tailcall(canonicalizePlanT(input, scope)).map(n => CTag("Limit", List(n, intLeaf(limit), intLeaf(offset))))
    case Window(input, windowExprs, partitionBy, orderBy) =>
      // partitionBy is set-like, same reasoning as Aggregate.groupBy above;
      // orderBy is not (it defines window-function ordering, e.g. RANK) -
      // preserved, mirroring Sort.order.
      for {
        inputNode <- tailcall(canonicalizePlanT(input, scope))
        partitionNodes <- traverseT(partitionBy)(canonicalizeExprT(_, scope))
        orderNodes <- traverseT(orderBy)(canonicalizeSortOrderT(_, scope))
        windowNodes <- traverseT(windowExprs)(canonicalizeNamedExprT(_, scope))
      } yield CTag(
        "Window",
        inputNode :: CTag("PartitionBy", canonicallySorted(partitionNodes)) :: CTag("OrderBy", orderNodes) :: windowNodes
      )
    case UnknownPlan(_description, sourceType, children) =>
      // Same description/sourceType split as UnknownExpression above.
      traverseT(children)(canonicalizePlanT(_, scope)).map(nodes => CTag("UnknownPlan", stringLeaf(sourceType) :: nodes))
  }

  def canonicalizePlan(plan: Plan, scope: Map[String, String]): CanonicalNode = canonicalizePlanT(plan, scope).result

  // ---------------------------------------------------------------------
  // Lineage summary layer (§3's "lineage" fingerprint)
  // ---------------------------------------------------------------------

  def canonicalizeLineage(lineage: ColumnLineage, scope: Map[String, String]): CanonicalNode = {
    // sources/aggregations are Scala Sets - never iterated directly (their
    // iteration order is hash-based, not guaranteed stable - see §2.2/
    // §2.4), always canonically sorted by encoded bytes first. Both are
    // small in practice (one entry per source column/aggregate function a
    // single output touches, not per plan node), so this is left as plain
    // (non-trampolined) recursion via canonicalizeColumnRef, which is
    // itself non-recursive.
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
  // Row mutation facts (MERGE/UPDATE/DELETE) - see
  // docs/SEMANTIC_LINEAGE_FINGERPRINTING.md's RowMutation section
  // ---------------------------------------------------------------------

  /** Canonicalizes an `ir.RowMutation` - the MERGE `ON` condition, DELETE
    * predicate, and UPDATE-touched-column facts that `spark-adapter`'s
    * `RowMutationSupport` extracts *separately* from `ir.Plan` (a `Write`'s
    * own `input` never contains them - see `WriteCommandSupport`'s own
    * doc: for MERGE, `query = source`, never the ON condition; for
    * UPDATE/DELETE, `query` is a bare target reference with no predicate
    * at all). Left as plain (non-trampolined) recursion via
    * `canonicalizeExpr`/`canonicalizePlan`, whose own trampolines already
    * make each individual call stack-safe - `RowMutation` itself has no
    * recursive structure of its own to trampoline.
    *
    * `updatedColumns` is set-like (an UPDATE's assigned-column list carries
    * no meaningful order - the affected columns, not the order Spark
    * happened to declare them in), so it is canonically sorted, like
    * `Aggregate.groupBy`/`Window.partitionBy` above.
    */
  def canonicalizeRowMutation(mutation: RowMutation, scope: Map[String, String]): CanonicalNode = {
    val matchConditionNode = optionNode(mutation.matchCondition.map(canonicalizeExpr(_, scope)))
    val deleteNode = mutation.delete match {
      case DeleteScope.NotApplicable     => CTag("NotApplicable")
      case DeleteScope.Unconditional     => CTag("Unconditional")
      case DeleteScope.Conditional(cond) => CTag("Conditional", List(canonicalizeExpr(cond, scope)))
    }
    val updatedColumnsNode = CTag("UpdatedColumns", canonicallySorted(mutation.updatedColumns.map(stringLeaf)))
    CTag("RowMutation", List(matchConditionNode, deleteNode, updatedColumnsNode))
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

  private def resolveExprDeepT(expr: Expr, input: Plan): TailRec[Expr] = expr match {
    case ColumnReference(ref) => tailcall(resolveRefDeepT(ref, input)).map(_.getOrElse(expr))
    case literal: Literal     => done(literal)
    case Alias(name, inner)   => tailcall(resolveExprDeepT(inner, input)).map(Alias(name, _))
    case Cast(inner, targetType) => tailcall(resolveExprDeepT(inner, input)).map(Cast(_, targetType))
    case Arithmetic(operator, operands) => traverseT(operands)(resolveExprDeepT(_, input)).map(Arithmetic(operator, _))
    case Comparison(operator, left, right) =>
      for {
        l <- tailcall(resolveExprDeepT(left, input))
        r <- tailcall(resolveExprDeepT(right, input))
      } yield Comparison(operator, l, r)
    case BooleanExpr(operator, operands) => traverseT(operands)(resolveExprDeepT(_, input)).map(BooleanExpr(operator, _))
    case Conditional(branches, elseValue) =>
      for {
        resolvedBranches <- traverseT(branches) { case (cond, value) =>
          for {
            c <- tailcall(resolveExprDeepT(cond, input))
            v <- tailcall(resolveExprDeepT(value, input))
          } yield (c, v)
        }
        resolvedElse <- elseValue match {
          case Some(e) => tailcall(resolveExprDeepT(e, input)).map(Some(_): Option[Expr])
          case None    => done(None: Option[Expr])
        }
      } yield Conditional(resolvedBranches, resolvedElse)
    case Function(name, args) => traverseT(args)(resolveExprDeepT(_, input)).map(Function(name, _))
    case UDF(name, args, engineType) => traverseT(args)(resolveExprDeepT(_, input)).map(UDF(name, _, engineType))
    case AggregateCall(function, arg, distinct) => tailcall(resolveExprDeepT(arg, input)).map(AggregateCall(function, _, distinct))
    case UnknownExpression(description, sourceType, children) =>
      traverseT(children)(resolveExprDeepT(_, input)).map(UnknownExpression(description, sourceType, _))
  }

  def resolveExprDeep(expr: Expr, input: Plan): Expr = resolveExprDeepT(expr, input).result

  private def resolveRefDeepT(ref: ColumnRef, plan: Plan): TailRec[Option[Expr]] = plan match {
    case Read(dataset, alias, _) =>
      val scope = alias.getOrElse(dataset.location)
      done(if (ref.qualifier.forall(_ == scope)) Some(ColumnReference(ColumnRef(ref.name, Some(scope)))) else None)
    case Project(input, columns) =>
      columns.find(_.name == ref.name) match {
        case Some(nc) => tailcall(resolveExprDeepT(nc.expr, input)).map(Some(_))
        case None     => done(None)
      }
    case Aggregate(input, _, aggregates) =>
      aggregates.find(_.name == ref.name) match {
        case Some(nc) => tailcall(resolveExprDeepT(nc.expr, input)).map(Some(_))
        case None     => done(None)
      }
    case Window(input, windowExprs, _, _) =>
      windowExprs.find(_.name == ref.name) match {
        case Some(nc) => tailcall(resolveExprDeepT(nc.expr, input)).map(Some(_))
        case None     => tailcall(resolveRefDeepT(ref, input))
      }
    case Filter(input, _)   => tailcall(resolveRefDeepT(ref, input))
    case Sort(input, _)     => tailcall(resolveRefDeepT(ref, input))
    case Limit(input, _, _) => tailcall(resolveRefDeepT(ref, input))
    case Union(inputs) =>
      // Unlike ir.Lineage's combineUnion (which merges every plausible
      // candidate's *summary* into one Provenance), reconstructing a
      // single Expr means picking one concrete candidate when more than
      // one branch resolves the name - the first, in declared branch
      // order, deterministically. A narrower limitation than Lineage's own
      // resolution for this one ambiguous case; the "lineage" fingerprint
      // layer (built from ir.Lineage.trace directly) is unaffected and
      // still unions every candidate's real sources. Trampolined the same
      // way as everywhere else in this file - a Union with many branches
      // is exactly as real-world-plausible as a long Project chain.
      def firstMatch(remaining: List[Plan]): TailRec[Option[Expr]] = remaining match {
        case Nil => done(None)
        case head :: tail =>
          tailcall(resolveRefDeepT(ref, head)).flatMap {
            case found @ Some(_) => done(found)
            case None            => firstMatch(tail)
          }
      }
      firstMatch(inputs)
    case Join(left, right, _, _) =>
      for {
        l <- tailcall(resolveRefDeepT(ref, left))
        r <- tailcall(resolveRefDeepT(ref, right))
      } yield (l, r) match {
        case (Some(lv), None) => Some(lv)
        case (None, Some(rv)) => Some(rv)
        // Both sides match an unqualified name - genuinely ambiguous.
        // Same deterministic-but-narrower-than-Lineage choice as Union
        // above: prefer the left side.
        case (Some(lv), Some(_)) => Some(lv)
        case (None, None)        => None
      }
    case Write(_, input, _, _, _) => tailcall(resolveRefDeepT(ref, input))
    case UnknownPlan(_, _, _)     => done(None)
  }

  /** The per-output deep-resolved expression for every output name a
    * `Write`'s (or a bare plan's) top-level `Project`/`Aggregate`/`Window`
    * declares - the same top-level dispatch `ir.Lineage.trace`/`outputsOf`
    * uses, kept structurally parallel to it deliberately (see this
    * object's own "Deep expression resolution" doc above for why this
    * isn't reused directly from `ir.Lineage`).
    */
  def resolvedOutputs(plan: Plan): Map[String, Expr] = {
    def outputsOfT(p: Plan): TailRec[List[(String, Expr)]] = p match {
      case Project(input, columns) =>
        traverseT(columns)(nc => tailcall(resolveExprDeepT(nc.expr, input)).map(nc.name -> _))
      case Aggregate(input, _, aggregates) =>
        traverseT(aggregates)(nc => tailcall(resolveExprDeepT(nc.expr, input)).map(nc.name -> _))
      case Window(input, windowExprs, _, _) =>
        for {
          base <- tailcall(outputsOfT(input))
          windowOnes <- traverseT(windowExprs)(nc => tailcall(resolveExprDeepT(nc.expr, input)).map(nc.name -> _))
        } yield base ++ windowOnes
      case Filter(input, _)   => tailcall(outputsOfT(input))
      case Sort(input, _)     => tailcall(outputsOfT(input))
      case Limit(input, _, _) => tailcall(outputsOfT(input))
      case Union(inputs) =>
        inputs.headOption match {
          case Some(p) => tailcall(outputsOfT(p))
          case None    => done(Nil)
        }
      case Join(left, right, _, _) =>
        for {
          l <- tailcall(outputsOfT(left))
          r <- tailcall(outputsOfT(right))
        } yield l ++ r
      case Write(_, input, _, _, _) => tailcall(outputsOfT(input))
      case Read(_, _, _)            => done(Nil)
      case UnknownPlan(_, _, _)     => done(Nil)
    }
    // Later entries win on a duplicate name, matching Map's own
    // to-Map-from-list convention - a real, well-formed plan does not
    // declare the same output name twice at one boundary regardless.
    outputsOfT(plan).result.toMap
  }
}
