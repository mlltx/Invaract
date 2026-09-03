// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.ir

/** Renders a `Plan` as an indented ASCII tree, for debugging and for
  * demonstrating that the IR captures a transformation's real structure.
  * Rendering is a separate concern from the plan/expression model itself —
  * `Plan.scala` and `Expr.scala` carry no display logic.
  */
object PlanPrinter {

  def render(plan: Plan): String = {
    val sb = new StringBuilder
    sb.append(label(plan)).append("\n")
    renderChildren(contentOf(plan), sb, "")
    sb.toString()
  }

  /** The lines directly nested under a node: for `Project`/`Aggregate`/
    * `Window`, its input plan followed by its declared output columns
    * (each shown fully on one line rather than expanded into its own
    * subtree); for every other node, its child plans.
    */
  private def contentOf(plan: Plan): List[Either[String, Plan]] = plan match {
    case Project(input, columns) =>
      Right(input) :: columns.map(nc => Left(s"${nc.name} = ${renderExpr(nc.expr)}"))

    case Aggregate(input, groupBy, aggregates) =>
      val groupLine =
        if (groupBy.nonEmpty) List(Left(s"GROUP BY ${groupBy.map(renderExpr).mkString(", ")}")) else Nil
      Right(input) :: groupLine ++ aggregates.map(nc => Left(s"${nc.name} = ${renderExpr(nc.expr)}"))

    case Window(input, windowExprs, _, _) =>
      Right(input) :: windowExprs.map(nc => Left(s"${nc.name} = ${renderExpr(nc.expr)}"))

    case other => other.children.map(Right(_))
  }

  private def renderChildren(items: List[Either[String, Plan]], sb: StringBuilder, prefix: String): Unit =
    items.zipWithIndex.foreach { case (item, idx) =>
      val isLast = idx == items.length - 1
      val branch = if (isLast) "└─ " else "├─ "
      val childPrefix = prefix + (if (isLast) "   " else "│  ")
      item match {
        case Left(line) =>
          sb.append(prefix).append(branch).append(line).append("\n")
        case Right(child) =>
          sb.append(prefix).append(branch).append(label(child)).append("\n")
          renderChildren(contentOf(child), sb, childPrefix)
      }
    }

  private val InfixOperators = Set("=", "!=", "<", "<=", ">", ">=", "+", "-", "*", "/", "AND", "OR")

  private def renderExpr(expr: Expr): String = expr match {
    case ColumnReference(ref) => ref.toString
    case Literal(value, _)    => String.valueOf(value)
    case FunctionCall(name, List(left, right)) if InfixOperators.contains(name) =>
      s"${renderExpr(left)} $name ${renderExpr(right)}"
    case FunctionCall(name, args) =>
      s"$name(${args.map(renderExpr).mkString(", ")})"
    case AggregateCall(function, arg, distinct) =>
      val d = if (distinct) "DISTINCT " else ""
      s"$function($d${renderExpr(arg)})"
    case UnsupportedExpr(description) => s"<unsupported: $description>"
  }

  private def label(plan: Plan): String = plan match {
    case Read(dataset, alias) => s"Read(${dataset.location}${alias.map(a => s" AS $a").getOrElse("")})"
    case Write(dataset, _, format, saveMode) =>
      val details = format.map(f => s", format=$f").getOrElse("") + saveMode.map(m => s", saveMode=$m").getOrElse("")
      s"Write(${dataset.location}$details)"
    case Project(_, _)        => "Project"
    case Filter(_, condition) => s"Filter(${renderExpr(condition)})"
    case Join(_, _, joinType, condition) =>
      s"Join($joinType${condition.map(c => s", ${renderExpr(c)}").getOrElse("")})"
    case Aggregate(_, _, _) => "Aggregate"
    case Union(_)           => "Union"
    case Sort(_, order) =>
      s"Sort(${order.map(o => s"${renderExpr(o.expr)} ${if (o.ascending) "ASC" else "DESC"}").mkString(", ")})"
    case Window(_, _, partitionBy, orderBy) =>
      val p = if (partitionBy.nonEmpty) s"PARTITION BY ${partitionBy.map(renderExpr).mkString(", ")}" else ""
      val o = if (orderBy.nonEmpty) s"ORDER BY ${orderBy.map(so => renderExpr(so.expr)).mkString(", ")}" else ""
      val spec = List(p, o).filter(_.nonEmpty).mkString(" ")
      s"Window($spec)"
    case Unsupported(description, _) => s"Unsupported($description)"
  }
}
