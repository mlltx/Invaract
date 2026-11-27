// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.ir

import scala.util.control.TailCalls._

/** How an output column's *fully resolved* computation relates to its
  * source columns — a coarse, human-auditable classification, not a full
  * replay of the expression tree (see `Expr.scala`'s own doc for why the
  * expression algebra itself already distinguishes operation categories by
  * node kind — this classification is a further compression of that,
  * purpose-built for lineage consumers that want one summary judgment per
  * column rather than the whole tree).
  *
  * "Fully resolved" matters: Invaract translates Spark's *analyzed* plan,
  * not the optimized one (see ARCHITECTURE.md's ADR-002), so a chain of
  * `.withColumn()` calls produces *nested* `Project` nodes rather than one
  * flat one — the outermost `Project`'s own declaration for an untouched
  * column is often nothing more than a bare passthrough reference to an
  * inner `Project`'s real computation (`value_squared = value_squared`,
  * where the actual `value * value` lives one level down). Classifying
  * only the outermost declaring expression's own syntax would misreport
  * that column as `Direct` — this classification is instead computed
  * *through* `Lineage`'s own source resolution, the same traversal that
  * already resolves `sources`, so it reflects the column's real,
  * end-to-end derivation, not just its last hop.
  *
  * The distinction that matters most for a human (or a downstream policy)
  * auditing a transformation: is this column's derivation fully understood
  * by this IR, or does it pass through something the IR cannot see inside
  * — a `UDF` (opaque by design, see `Expr.scala`'s `UDF` doc) or an
  * `UnknownExpression` (a construct the front-end translator itself could
  * not represent)? `Opaque` wins over every other classification for a
  * column whose resolved computation contains one anywhere along the way,
  * even nested arbitrarily deep beneath otherwise-understood operations or
  * behind several layers of passthrough — a `CASE WHEN` whose result calls
  * a UDF is opaque overall, not "mostly computed."
  */
sealed trait DerivationKind
object DerivationKind {

  /** The output is exactly one source column, possibly renamed, through
    * any number of pure passthrough hops — no computation anywhere along
    * the way (`id = id`, `customer_id = orders.customer_id`, or the same
    * column re-declared unchanged across several nested `Project`s).
    */
  case object Direct extends DerivationKind

  /** The output has no source columns at all — a literal, or an
    * expression built entirely from literals (`region = 'US'`).
    */
  case object Constant extends DerivationKind

  /** The output is computed from one or more source columns using only
    * operations this IR fully understands (`Cast`/`Arithmetic`/
    * `Comparison`/`BooleanExpr`/`Conditional`/`Function`/`AggregateCall`)
    * — auditable: a human (or tooling built on this IR) can read exactly
    * what this column computes, even if that computation happened several
    * plan stages before the column's final declaration. A `CASE WHEN`
    * (`Conditional`) built from a `Comparison` is the canonical example:
    * every operation involved has known, named semantics, unlike a
    * `UDF`'s opaque body.
    */
  case object Computed extends DerivationKind

  /** The output's resolved computation contains a `UDF` or
    * `UnknownExpression` node somewhere along the way — this IR knows
    * what columns it structurally depends on, but not what it actually
    * computes from them. Distinct from `Computed` specifically so a
    * contract author or a lineage consumer can flag "this column's
    * meaning is not fully auditable from the transformation plan alone"
    * without having to separately re-walk the expression tree looking for
    * a `UDF` themselves.
    */
  case object Opaque extends DerivationKind
}

/** One aggregate function contributing to an output column's derivation —
  * e.g. `SUM`, `COUNT DISTINCT`. An output column's derivation can combine
  * more than one aggregate function (`sum(x) / count(y)`), so
  * `ColumnLineage.aggregations` is a `Set`, not a single value — collapsing
  * that down to one boolean ("is this aggregated at all") loses exactly
  * the detail (which function, over what) a human auditing *how* a column
  * changed meaning across a schema-compatible contract revision needs.
  */
case class AggregationDetail(function: String, distinct: Boolean = false)

/** The provenance of a single output column: which `Read` columns it
  * ultimately derives from, how (`derivation`), and which aggregate
  * function(s), if any, collapse many source rows into this one output
  * value (`aggregations`).
  *
  * This is "verified lineage" in the sense of MISSION.md §5: derived from
  * the transformation plan itself, not observed at runtime or merely
  * declared by metadata.
  */
case class ColumnLineage(
  output: ColumnRef,
  sources: Set[ColumnRef],
  derivation: DerivationKind,
  aggregations: Set[AggregationDetail] = Set.empty
) {

  /** Convenience for "was this column derived through any aggregation at
    * all," without inspecting `aggregations` directly.
    */
  def aggregated: Boolean = aggregations.nonEmpty
}

/** How an input column shapes an output without being (part of) its value - the OpenLineage column lineage facet's
  * `INDIRECT` transformation subtypes, which a lineage consumer uses to see, say, that a report's rows depend on a
  * customer's country (a filter) though no output column holds it.
  */
sealed trait IndirectKind
object IndirectKind {

  /** A filter's condition: which rows are kept. */
  case object Filter extends IndirectKind

  /** A join's condition: which rows of each side meet. */
  case object Join extends IndirectKind

  /** An aggregate's grouping: which rows are collapsed together. */
  case object GroupBy extends IndirectKind

  /** A sort's order: the order of the rows (and, under a limit, which are kept). */
  case object Sort extends IndirectKind

  /** A window's partitioning and ordering: which rows one output column's value is computed over. */
  case object Window extends IndirectKind

  /** A condition (`CASE WHEN`, `IF`): which of several values one output column takes. */
  case object Conditional extends IndirectKind

  val all: List[IndirectKind] = List(Filter, Join, GroupBy, Sort, Window, Conditional)
}

/** One input column shaping the output the way `kind` says: the whole output (`output` empty - a filter, a join, a
  * grouping, a sort), or one output column (`output` that column - a condition, a window). */
case class IndirectLineage(source: ColumnRef, kind: IndirectKind, output: Option[String] = None)

/** One output column's lineage in full: `lineage` exactly as `Lineage.trace` gives it, and beside it
  *
  * @param values the sources whose value reaches the column (OpenLineage's `DIRECT` edges): `lineage.sources` without
  *   a column that only decides which value is taken (a `CASE WHEN`'s test), which `indirect` names instead.
  * @param indirect the sources that decide which value the column takes (a condition) or which rows it is computed
  *   over (a window's partitioning and ordering).
  * @param masked the sources of `values` that reach the column only through a masking function (a hash such as
  *   `sha2` or `md5`, or `mask`): the column carries no readable copy of them.
  * @param copied the sources of `values` that reach the column unchanged on some path through the plan, and
  * @param transformed those that reach it through a computation on some path: each source is in one or both. A union
  *   whose branches differ (a MERGE's clauses: one copies a column, another computes it) gives each source its own;
  *   `lineage.derivation` says it for the column as a whole.
  */
case class ColumnDependencies(
  lineage: ColumnLineage,
  values: Set[ColumnRef],
  indirect: Set[IndirectLineage],
  masked: Set[ColumnRef],
  copied: Set[ColumnRef] = Set.empty,
  transformed: Set[ColumnRef] = Set.empty
)

/** What `Lineage.traceDetailed` finds in a plan: each output column's dependencies, and the sources that shape the
  * whole output (filters, joins, groupings, sorts), each resolved through the plan to the read columns it comes from.
  */
case class DetailedLineage(columns: List[ColumnDependencies], indirect: List[IndirectLineage])

/** Traces column-level lineage through a transformation plan: for each
  * column a `Write` ultimately produces, which `Read` columns it derives
  * from.
  *
  * Resolution is purely structural — no symbol table, no exprIds. A bare
  * column name is looked up against the nearest plan node that declares
  * output names (`Project`/`Aggregate`/`Window`), falling through
  * pass-through nodes (`Filter`/`Sort`) until it either matches a declared
  * name or bottoms out at a `Read`. `Join` tries both branches and unions
  * the result on an ambiguous unqualified name rather than resolving
  * arbitrarily, so lineage tracing degrades to "attributed to all
  * plausible sources" instead of failing or guessing.
  *
  * Every genuinely recursive traversal below (`outputsOf`/`resolveExpr`/
  * `resolveInScope`) is implemented via `scala.util.control.TailCalls`,
  * trampolining the recursion onto the heap instead of the JVM call stack.
  * This is load-bearing, not defensive-only: measured directly, a plain-
  * recursive version of this code (and of `com.invaract.fingerprint`'s own
  * structurally analogous walks, which found this) stack-overflowed on a
  * chain of roughly 700-1700 nested nodes on a forked, default-stack JVM —
  * well within reach of a real generated pipeline with hundreds of chained
  * `.withColumn()` calls, exactly the shape this class's own doc above
  * already calls out as an ordinary, expected translation output, not an
  * edge case. `trace`'s public signature is unaffected — the trampoline is
  * purely an internal implementation detail, run to completion via
  * `.result` once, at `trace` itself.
  */
object Lineage {

  /** The functions whose result reveals nothing readable of their input (OpenLineage's `masking`). */
  val MaskingFunctions: Set[String] = Set("sha", "sha1", "sha2", "md5", "crc32", "hash", "xxhash64", "mask")

  /** The aggregates whose result reveals nothing of the values aggregated, only how many there are. */
  val MaskingAggregates: Set[String] = Set("count", "approx_count_distinct")

  /** No read's columns are known: a read is taken to have whatever column a name asks of it. */
  val NoReadColumns: Read => Option[Set[String]] = _ => None

  def trace(plan: Plan): List[ColumnLineage] = trace(plan, NoReadColumns)

  /** `trace`, told the columns each read has (`readColumns`; `None` for a read whose columns are not known). A name a
    * read does not have is then not taken from it: across a join, a column of one side is no longer also attributed to
    * a read on the other side that only happens to be unqualified too. Names are matched as an engine matches them by
    * default, ignoring case.
    */
  def trace(plan: Plan, readColumns: Read => Option[Set[String]]): List[ColumnLineage] =
    traceDetailed(plan, readColumns).columns.map(_.lineage)

  /** Each output column's full dependencies, and the plan's own indirect lineage: every filter's, join's, grouping's
    * and sort's columns, resolved to the read columns they come from (a filter on a computed column is a filter on
    * what it was computed from). A source is listed once per kind, in plan order.
    */
  def traceDetailed(plan: Plan, readColumns: Read => Option[Set[String]] = NoReadColumns): DetailedLineage = {
    val run = new Run(readColumns)
    val columns = (plan match {
      case Write(_, input, _, _, _) => run.dependenciesOfT(input)
      case other                    => run.dependenciesOfT(other)
    }).result
    DetailedLineage(columns, run.planIndirect(plan))
  }

  /** An expression's fully resolved provenance: which Read columns it
    * depends on, how it was derived (following passthrough references
    * through however many plan stages, not just the immediate syntax —
    * see `DerivationKind`'s own doc), and which aggregate function(s), if
    * any, it aggregates them through. `sources` are the columns whose value reaches it, `indirect` those that only
    * decide which value it takes or what rows it is computed over, `masked` the sources reaching it only through a
    * masking function, `copied` the sources reaching it unchanged on some path, `changed` those reaching it through a
    * computation on some path.
    */
  private case class Provenance(
    sources: Set[ColumnRef],
    derivation: DerivationKind,
    aggregations: Set[AggregationDetail],
    indirect: Set[(ColumnRef, IndirectKind)] = Set.empty,
    masked: Set[ColumnRef] = Set.empty,
    copied: Set[ColumnRef] = Set.empty,
    changed: Set[ColumnRef] = Set.empty
  ) {
    /** Every column it depends on, how ever: what `trace` reports as a column's sources. A window's partitioning is not
      * among them, as it never was. */
    def all: Set[ColumnRef] = sources ++ indirect.collect { case (r, IndirectKind.Conditional) => r }
    def everything: Set[ColumnRef] = sources ++ indirect.map(_._1)
  }

  private val NoSource = Provenance(Set.empty, DerivationKind.Direct, Set.empty)

  /** Runs `f` over every element of `xs`, left to right, trampolined - see
    * `com.invaract.fingerprint.Canonicalizer`'s identical helper for why
    * this is stack-safe to *construct* regardless of `xs`'s length, not
    * only to eventually run.
    */
  private def traverseT[A, B](xs: List[A])(f: A => TailRec[B]): TailRec[List[B]] = xs match {
    case Nil => done(Nil)
    case head :: tail =>
      for {
        b <- tailcall(f(head))
        bs <- traverseT(tail)(f)
      } yield b :: bs
  }

  /** One trace: the walk below, with what it has already worked out for each node. A node shared by several parents
    * (see `Plan`'s doc), or a column several expressions refer to (`v = v + v`, repeated), is resolved once - walked
    * afresh each time, `k` such steps in a row would cost `2^k`.
    */
  /** Which of `names` a reference to `name` means: the same name, else the one name equal to it ignoring case. An engine
    * resolves names ignoring case by default, so a job that refers to `AMOUNT` means a column named `amount`; with
    * names that differ only in case (a case-sensitive engine), the exact name is the only match. */
  private def matching(name: String, names: List[String]): Option[String] =
    if (names.contains(name)) Some(name)
    else names.filter(_.equalsIgnoreCase(name)).distinct match {
      case only :: Nil => Some(only)
      case _           => None
    }

  private def named(name: String, columns: List[NamedExpr]): Option[NamedExpr] =
    matching(name, columns.map(_.name)).flatMap(n => columns.find(_.name == n))

  private final class Run(readColumns: Read => Option[Set[String]]) {
    private val outputs = new IdentityMemo[Plan, List[(String, Provenance)]]
    private val scopes = new IdentityMemo[Plan, scala.collection.mutable.HashMap[ColumnRef, Option[Provenance]]]
    private val knownColumns = new IdentityMemo[Plan, Option[List[String]]]

    def dependenciesOfT(plan: Plan): TailRec[List[ColumnDependencies]] =
      outputsOfT(plan).map(_.map { case (name, p) =>
        val output = ColumnRef(name)
        ColumnDependencies(
          ColumnLineage(output, p.all, p.derivation, p.aggregations),
          p.sources,
          p.indirect.map { case (r, k) => IndirectLineage(r, k, Some(name)) },
          p.masked intersect p.sources,
          p.copied intersect p.sources,
          p.changed intersect p.sources
        )
      })

    /** The indirect lineage of the plan's own nodes, every node once (a shared subtree is walked once). */
    def planIndirect(plan: Plan): List[IndirectLineage] = {
      val seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap[Plan, java.lang.Boolean]())
      val found = scala.collection.mutable.LinkedHashSet.empty[IndirectLineage]
      def resolve(exprs: List[Expr], scope: Plan, kind: IndirectKind): Unit =
        exprs.foreach(e => resolveExprT(e, scope).result.everything.toList.sortBy(_.toString).foreach(r => found += IndirectLineage(r, kind)))
      var pending: List[Plan] = List(plan)
      while (pending.nonEmpty) {
        val node = pending.head
        pending = pending.tail
        if (seen.add(node)) {
          node match {
            case Filter(input, condition)     => resolve(List(condition), input, IndirectKind.Filter)
            case j @ Join(_, _, _, condition) => resolve(condition.toList, j, IndirectKind.Join)
            case Aggregate(input, groupBy, _) => resolve(groupBy, input, IndirectKind.GroupBy)
            case Sort(input, order)           => resolve(order.map(_.expr), input, IndirectKind.Sort)
            case _                            => ()
          }
          pending = node.children ++ pending
        }
      }
      found.toList
    }

    private def outputsOfT(plan: Plan): TailRec[List[(String, Provenance)]] = outputs.getOrElseUpdateT(plan)(outputsOfUncachedT(plan))

    private def outputsOfUncachedT(plan: Plan): TailRec[List[(String, Provenance)]] = plan match {
      case Project(input, columns) =>
        traverseT(columns) { case NamedExpr(name, expr) => tailcall(resolveExprT(expr, input)).map(name -> _) }

      case Aggregate(input, _, aggregates) =>
        traverseT(aggregates) { case NamedExpr(name, expr) => tailcall(resolveExprT(expr, input)).map(name -> _) }

      case w @ Window(input, windowExprs, _, _) =>
        for {
          base <- tailcall(outputsOfT(input))
          windowOnes <- traverseT(windowExprs) { case NamedExpr(name, _) => tailcall(windowColumnT(name, w)).map(name -> _) }
        } yield base ++ windowOnes

      case g @ Generate(input, _, generated, _) =>
        for {
          base <- tailcall(outputsOfT(input))
          own <- traverseT(generated)(name => tailcall(resolveInScopeT(ColumnRef(name), g)).map(p => name -> p.getOrElse(NoSource)))
        } yield base ++ own

      case e @ Expand(_, _, names) =>
        traverseT(names.distinct)(name => tailcall(resolveInScopeT(ColumnRef(name), e)).map(p => name -> p.getOrElse(NoSource)))

      case Filter(input, _)   => tailcall(outputsOfT(input))
      case Sort(input, _)     => tailcall(outputsOfT(input))
      case Limit(input, _, _) => tailcall(outputsOfT(input))
      case u @ Union(inputs) =>
        inputs.headOption match {
          case Some(p) =>
            tailcall(outputsOfT(p)).flatMap { first =>
              traverseT(first.zipWithIndex) { case ((name, _), i) => tailcall(unionColumnT(u, i, ColumnRef(name))).map(name -> _) }
            }
          case None => done(Nil)
        }
      case Join(left, right, _, _) =>
        for {
          l <- tailcall(outputsOfT(left))
          r <- tailcall(outputsOfT(right))
        } yield l ++ r
      case Write(_, input, _, _, _) => tailcall(outputsOfT(input))

      // A bare Read declares no output list of its own (see Plan.scala) —
      // there is nothing to trace until something downstream projects it.
      case Read(_, _, _) => done(Nil)

      // An untranslated construct declares no known output list either —
      // there is nothing to trace past it.
      case UnknownPlan(_, _, _) => done(Nil)
    }

    /** Column `i` of a union: each input's column `i`, or, from an input that declares no columns, its column named as
      * `ref` names it. */
    private def unionColumnT(u: Union, i: Int, ref: ColumnRef): TailRec[Provenance] =
      traverseT(u.inputs) { input =>
        tailcall(outputsOfT(input)).flatMap { columns =>
          columns.lift(i) match {
            case Some((_, provenance)) => done(Some(provenance))
            case None                  => tailcall(resolveInScopeT(ref.copy(qualifier = None), input))
          }
        }
      }.map(found => combineUnion(found.flatten))

    /** A union whose columns cannot be told by position: each input asked for the name. `combineUnion` of nothing is
      * `NoSource`, so an empty `found` could give that as well as `None`: every caller treats the two alike. */
    private def unionByNameT(ref: ColumnRef, inputs: List[Plan]): TailRec[Option[Provenance]] =
      traverseT(inputs)(resolveInScopeT(ref, _)).map { results =>
        val found = results.flatten
        if (found.isEmpty) None else Some(combineUnion(found))
      }

    /** A window column: its expression's provenance, with the window's partitioning and ordering columns as the
      * `Window` indirect lineage of it. */
    private def windowColumnT(name: String, w: Window): TailRec[Provenance] =
      w.windowExprs.find(_.name == name) match {
        case Some(nc) =>
          for {
            value <- tailcall(resolveExprT(nc.expr, w.input))
            spec <- traverseT(w.partitionBy ++ w.orderBy.map(_.expr))(resolveExprT(_, w.input))
          } yield value.copy(indirect = value.indirect ++ spec.flatMap(_.everything).map(_ -> IndirectKind.Window))
        case None => done(NoSource)
      }

    private def resolveExprT(expr: Expr, input: Plan): TailRec[Provenance] = expr match {
      case ColumnReference(ref) =>
        // Direct is the right fallback for a reference this plan can't
        // resolve at all (e.g. it sits on an UnknownPlan): with nothing to
        // inherit a real computation from, "just this one (unresolvable)
        // column, unchanged" is the honest, syntax-level answer — the same
        // one a resolvable pure passthrough chain would eventually bottom
        // out at via Read's own base case below.
        tailcall(resolveInScopeT(ref, input)).map(_.getOrElse(NoSource))
      case Literal(_, _) =>
        done(Provenance(Set.empty, DerivationKind.Constant, Set.empty))
      case Alias(_, inner) =>
        // A rename is not a computation - inherits the inner expression's
        // resolved provenance verbatim, Direct included.
        tailcall(resolveExprT(inner, input))
      case Cast(inner, _) =>
        tailcall(resolveExprT(inner, input)).map(p => combineOperation(List(p)))
      case Arithmetic(_, operands) =>
        traverseT(operands)(resolveExprT(_, input)).map(combineOperation)
      case Comparison(_, left, right) =>
        for {
          l <- tailcall(resolveExprT(left, input))
          r <- tailcall(resolveExprT(right, input))
        } yield combineOperation(List(l, r))
      case BooleanExpr(_, operands) =>
        traverseT(operands)(resolveExprT(_, input)).map(combineOperation)
      case Conditional(branches, elseValue) =>
        // The values are the column's sources; what the conditions test only decides which value it takes - its
        // Conditional indirect lineage. (`trace` still counts both as sources, as it always has.)
        for {
          conditions <- traverseT(branches.map(_._1))(resolveExprT(_, input))
          values <- traverseT(branches.map(_._2) ++ elseValue.toList)(resolveExprT(_, input))
        } yield {
          val combined = combineOperation(conditions ++ values)
          val tested = conditions.flatMap(_.everything).map(_ -> (IndirectKind.Conditional: IndirectKind)).toSet
          combined.copy(
            sources = values.flatMap(_.sources).toSet,
            changed = values.flatMap(_.sources).toSet,
            indirect = values.flatMap(_.indirect).toSet ++ tested,
            masked = values.flatMap(_.masked).toSet -- values.flatMap(v => v.sources -- v.masked)
          )
        }
      case Function(name, args) =>
        traverseT(args)(resolveExprT(_, input)).map { ps =>
          val combined = combineOperation(ps)
          if (MaskingFunctions(name.toLowerCase)) combined.copy(masked = combined.sources) else combined
        }
      case UDF(_, args, _) =>
        // Opaque unconditionally, regardless of what its arguments resolve
        // to - a UDF's body is opaque to this IR by design (see UDF's own
        // doc in Expr.scala), never a function of its arguments' own
        // derivation.
        traverseT(args)(resolveExprT(_, input)).map(ps => combineOperation(ps).copy(derivation = DerivationKind.Opaque))
      case AggregateCall(function, arg, distinct) =>
        // combineOperation handles sources/derivation (an aggregate call is
        // always a real operation, never Direct); aggregations is replaced
        // outright, not merged with the argument's own - the argument isn't
        // itself expected to already be aggregated (nested aggregates
        // aren't valid SQL), and this call is the one aggregation that
        // matters at this level.
        tailcall(resolveExprT(arg, input)).map { p =>
          val combined = combineOperation(List(p)).copy(aggregations = Set(AggregationDetail(function, distinct)))
          if (MaskingAggregates(function.toLowerCase)) combined.copy(masked = combined.sources) else combined
        }
      case UnknownExpression(_, _, children) =>
        // Opaque unconditionally, regardless of whether any children
        // resolved real, understood sources - an unrepresentable construct
        // is opaque by definition, the same "opaque anywhere wins" rule a
        // nested UDF gets.
        traverseT(children)(resolveExprT(_, input)).map(ps => combineOperation(ps).copy(derivation = DerivationKind.Opaque))
      // A field of a struct the plan itself builds (`joinWith`'s `_1.id`, a `struct(...)` read back) is that field's own
      // value, copied: not every column the struct was built from.
      case StructField(struct, field) if builtField(struct, field, input).isDefined =>
        val (value, scope) = builtField(struct, field, input).get
        tailcall(resolveExprT(value, scope))
      case StructField(struct, _) =>
        // A field projection is a real operation on the struct's own
        // provenance (never Direct, even when the struct itself is), the
        // same treatment Cast already gets - accessing .zip is itself a
        // computation step, not a pure passthrough of the whole struct.
        tailcall(resolveExprT(struct, input)).map(p => combineOperation(List(p)))
      case StructConstruct(fields) =>
        // Building a struct from its field values, the same treatment
        // Function's own argument list gets.
        traverseT(fields.map(_._2))(resolveExprT(_, input)).map(combineOperation)
    }

    /** Combines the resolved provenances of a real operation's operands
      * (`Cast`/`Arithmetic`/`Comparison`/`BooleanExpr`/`Conditional`/
      * `Function`/`AggregateCall`'s own argument) into that operation's own
      * provenance. Never produces `Direct`, even when every operand does —
      * applying an actual operator is itself a computation, not a pure
      * passthrough (`Cast(ColumnReference(x), "double")` is `Computed`, not
      * `Direct`, despite depending on exactly one already-Direct column).
      * Contrast `combineUnion` below, used where multiple *candidate*
      * resolutions of the *same* reference are being combined, not multiple
      * *operands* of one real operation. A source is masked when it is masked in an operand and reaches the result
      * unmasked through none.
      */
    private def combineOperation(provenances: List[Provenance]): Provenance = {
      val sources = provenances.flatMap(_.sources).toSet
      val aggregations = provenances.flatMap(_.aggregations).toSet
      val derivation =
        if (provenances.exists(_.derivation == DerivationKind.Opaque)) DerivationKind.Opaque
        else if (provenances.forall(_.all.isEmpty)) DerivationKind.Constant
        else DerivationKind.Computed
      Provenance(sources, derivation, aggregations, provenances.flatMap(_.indirect).toSet, maskedOf(provenances), changed = sources)
    }

    private def maskedOf(provenances: List[Provenance]): Set[ColumnRef] =
      provenances.flatMap(_.masked).toSet -- provenances.flatMap(p => p.sources -- p.masked)

    /** Combines multiple *candidate* resolutions of one ambiguous reference
      * — `Union`'s branches, or `Join`'s two sides when both plausibly
      * declare the same unqualified name — into one Provenance. Unlike
      * `combineOperation`, this preserves `Direct` when every candidate
      * agrees it's a pure passthrough: the reference genuinely is just one
      * of several possible raw columns, with no computation on any branch,
      * so `Direct` (structurally ambiguous about origin, but computation-
      * free either way) is the accurate label, not `Computed`. An empty
      * input list is never actually reached with observable effect (see the
      * `Union` case below), but is defined to combine to `Direct` with no
      * sources — a mathematically consistent choice (vacuously "every
      * candidate is Direct").
      */
    private def combineUnion(provenances: List[Provenance]): Provenance = {
      val sources = provenances.flatMap(_.sources).toSet
      val aggregations = provenances.flatMap(_.aggregations).toSet
      val derivation =
        if (provenances.exists(_.derivation == DerivationKind.Opaque)) DerivationKind.Opaque
        else if (provenances.forall(_.derivation == DerivationKind.Direct)) DerivationKind.Direct
        else if (provenances.forall(_.all.isEmpty)) DerivationKind.Constant
        else DerivationKind.Computed
      Provenance(sources, derivation, aggregations, provenances.flatMap(_.indirect).toSet, maskedOf(provenances),
        provenances.flatMap(_.copied).toSet, provenances.flatMap(_.changed).toSet)
    }

    /** The expression a struct is built from for its field `field`, and the plan it is resolved against: through an
      * alias, and through a reference to a column a projection below builds (following references and the operators
      * that pass columns through). `None` for anything else - a struct read from an input, a function's result - and
      * for a name both sides of a join declare. Walks in a loop: no deeper a stack for a deep plan. */
    private def builtField(struct: Expr, field: String, input: Plan): Option[(Expr, Plan)] = {
      var expr = struct
      var scope = input
      var answer: Option[Option[(Expr, Plan)]] = None
      while (answer.isEmpty) expr match {
        case StructConstruct(fields) =>
          answer = Some(matching(field, fields.map(_._1)).flatMap(name => fields.find(_._1 == name)).map(f => (f._2, scope)))
        case Alias(_, inner) => expr = inner
        case ColumnReference(ref) =>
          definitionOf(ref, scope) match {
            case Some((definition, below)) => expr = definition; scope = below
            case None                      => answer = Some(None)
          }
        case _ => answer = Some(None)
      }
      answer.get
    }

    /** The expression a projection in `plan` declares `ref`'s column with, and that projection's input. */
    private def definitionOf(ref: ColumnRef, plan: Plan): Option[(Expr, Plan)] = {
      var current = plan
      var answer: Option[Option[(Expr, Plan)]] = None
      while (answer.isEmpty) current match {
        case Project(input, columns) => answer = Some(named(ref.name, columns).map(nc => (nc.expr, input)))
        case Filter(input, _)        => current = input
        case Sort(input, _)          => current = input
        case Limit(input, _, _)      => current = input
        case Join(left, right, _, _) =>
          answer = Some((definitionOf(ref, left), definitionOf(ref, right)) match {
            case (Some(found), None) => Some(found)
            case (None, Some(found)) => Some(found)
            case _                   => None
          })
        case _ => answer = Some(None)
      }
      answer.get
    }

    /** Resolves a bare column reference against a plan: does `ref` name a
      * column this plan (re)declares, or does it pass through to whatever
      * produced this plan's input? Returns `None` when `ref`'s qualifier
      * rules out this branch entirely (used by `Join` to avoid attributing a
      * qualified reference to the wrong side).
      */
    private def resolveInScopeT(ref: ColumnRef, plan: Plan): TailRec[Option[Provenance]] = {
      val byRef = scopes.getOrElseUpdate(plan, scala.collection.mutable.HashMap.empty)
      byRef.get(ref) match {
        case Some(resolved) => done(resolved)
        case None           => resolveInScopeUncachedT(ref, plan).map { resolved => byRef(ref) = resolved; resolved }
      }
    }

    /** Whether `plan`, which resolves `ref`, does so only by name: its column `ref.name` is a projection passing another
      * qualifier's column through. Anything else is `false`: a computed column, a join, an operator that makes its own
      * columns, or a read (which resolves a qualified reference only under its own scope). Walks down one chain of
      * inputs, in a loop: no deeper a stack for a deep plan. A Stryker mutant turning `exists` into `forall` is an
      * equivalent: an unqualified reference then gives both sides the same answer, and the join keeps both either way. */
    private def namesAnother(ref: ColumnRef, plan: Plan): Boolean = ref.qualifier.exists { qualifier =>
      var current: Plan = plan
      var answer: Option[Boolean] = None
      while (answer.isEmpty) current match {
        case Project(_, columns) =>
          answer = Some(columns.find(_.name == ref.name) match {
            case Some(NamedExpr(_, ColumnReference(passed))) => passed.qualifier.exists(_ != qualifier)
            case _                                         => false
          })
        case Filter(input, _)   => current = input
        case Sort(input, _)     => current = input
        case Limit(input, _, _) => current = input
        case _                  => answer = Some(false)
      }
      answer.get
    }

    private def columnsOf(read: Read): Option[List[String]] =
      knownColumns.getOrElseUpdate(read, readColumns(read).map(_.toList.sorted))

    private def resolveInScopeUncachedT(ref: ColumnRef, plan: Plan): TailRec[Option[Provenance]] = plan match {
      case read @ Read(dataset, alias, _) =>
        val scope = alias.getOrElse(dataset.location)
        val columns = columnsOf(read)
        val has = columns.forall(_.exists(_.equalsIgnoreCase(ref.name)))
        // the column as the read has it: a reference that differs from it only in case (`AMOUNT` for `amount`) names
        // that column, not one the dataset does not have
        val source = ColumnRef(columns.flatMap(matching(ref.name, _)).getOrElse(ref.name), Some(scope))
        done(
          if (ref.qualifier.forall(_ == scope) && has)
            Some(Provenance(Set(source), DerivationKind.Direct, Set.empty, copied = Set(source)))
          else
            None
        )

      case Project(input, columns) =>
        named(ref.name, columns) match {
          case Some(nc) => tailcall(resolveExprT(nc.expr, input)).map(Some(_))
          case None     => done(None)
        }

      case Aggregate(input, _, aggregates) =>
        named(ref.name, aggregates) match {
          case Some(nc) => tailcall(resolveExprT(nc.expr, input)).map(Some(_))
          case None     => done(None)
        }

      case w @ Window(input, windowExprs, _, _) =>
        named(ref.name, windowExprs) match {
          case Some(nc) => tailcall(windowColumnT(nc.name, w)).map(Some(_))
          case None     => tailcall(resolveInScopeT(ref, input))
        }

      // A generated column comes from the generator's arguments (every generated column from all of them: which
      // argument fills which output is the generator's own business); any other passes through from the input.
      case Generate(input, generator, generated, _) =>
        if (matching(ref.name, generated).isDefined) tailcall(resolveExprT(generator, input)).map(p => Some(combineOperation(List(p))))
        else tailcall(resolveInScopeT(ref, input))

      // Each row of an expansion takes its value of column i from one projection's i-th expression: the candidates are
      // all of them, as a union's branches are. A name declared twice (an engine keeps a grouping key's original
      // column beside the expanded one) is its first declaration's.
      case Expand(input, projections, names) =>
        matching(ref.name, names).fold(-1)(names.indexOf(_)) match {
          case -1 => done(None)
          case i  => traverseT(projections.flatMap(_.lift(i)))(resolveExprT(_, input)).map(ps => Some(combineUnion(ps)))
        }

      case Filter(input, _)   => tailcall(resolveInScopeT(ref, input))
      case Sort(input, _)     => tailcall(resolveInScopeT(ref, input))
      case Limit(input, _, _) => tailcall(resolveInScopeT(ref, input))

      // A union matches its inputs' columns by position: its column i is each input's column i. An input that declares
      // no columns of its own (a read) is asked for the name instead, as is every input when the first declares none. A
      // union of reads alone is one read of several paths: each gives the name, whatever the reference's qualifier.
      case u @ Union(inputs) if !inputs.forall(_.isInstanceOf[Read]) =>
        tailcall(outputsOfT(inputs.head)).flatMap { first =>
          matching(ref.name, first.map(_._1)) match {
            case Some(name) => tailcall(unionColumnT(u, first.indexWhere(_._1 == name), ref)).map(Some(_))
            case None       => tailcall(unionByNameT(ref, inputs))
          }
        }

      case Union(inputs) =>
        tailcall(unionByNameT(ref.copy(qualifier = None), inputs))

      // A name both sides have is the side whose column it is. A projection matches a name whatever its qualifier, so a
      // qualified reference (a USING join's key, `orders.id`) also resolves on a side that projects another read's column
      // of that name (`events.id`): such a side is ruled out. When neither or both sides are, the reference stays theirs.
      case Join(left, right, _, _) =>
        for {
          l <- tailcall(resolveInScopeT(ref, left))
          r <- tailcall(resolveInScopeT(ref, right))
        } yield (l, r) match {
          case (Some(lv), None) => Some(lv)
          case (None, Some(rv)) => Some(rv)
          case (Some(lv), Some(rv)) =>
            (namesAnother(ref, left), namesAnother(ref, right)) match {
              case (true, false) => Some(rv)
              case (false, true) => Some(lv)
              case _             => Some(combineUnion(List(lv, rv)))
            }
          case (None, None) => None
        }

      case Write(_, input, _, _, _) => tailcall(resolveInScopeT(ref, input))

      case UnknownPlan(_, _, _) => done(None)
    }
  }
}
