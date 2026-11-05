// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{Dataset, InterpretedFieldConstraint}
import com.invaract.contract.InterpretedFieldConstraint.{Equals, FieldRange, Length, OneOf, Range}

/** One check of the values a write puts in an output field: one of the field's declared `constraints`, for an adapter that
  * sees the data before it is published to evaluate (`RuntimeDataQuality`).
  *
  * A check judges each row on its own and never fails a row whose value is null, as SQL's `CHECK` does: whether a field may
  * hold nulls is its `nullable`/`required` declaration, checked with the schema. A `fieldRange` row passes when either field
  * is null.
  */
final case class DataCheck(field: String, constraint: InterpretedFieldConstraint) {

  /** The constraint as the contract spells it (`range gte 0, lt 100`), for a violation's `expected`. */
  def describe: String = constraint match {
    case Equals(value, _)  => s"equals ${DataCheck.show(value)}"
    case OneOf(values, _)  => s"oneOf [${DataCheck.sorted(values).map(DataCheck.show).mkString(", ")}]"
    case r: Range          => s"range ${DataCheck.bounds(r.gte, r.gt, r.lte, r.lt)}"
    case l: Length         => s"length ${List("exact" -> l.exact, "min" -> l.min, "max" -> l.max).collect { case (k, Some(v)) => s"$k $v" }.mkString(", ")}"
    case f: FieldRange     => s"fieldRange ${DataCheck.bounds(f.gte, f.gt, f.lte, f.lt)}"
  }

  /** The check as an adapter outside the JVM receives it: `field`, `type` (the contract's constraint type) and the
    * constraint's own properties, spelled as in the contract (`value`, `values`, `gte`/`gt`/`lte`/`lt`, `exact`/`min`/`max`). */
  def toMap: Map[String, Any] = {
    val properties: Map[String, Any] = constraint match {
      case Equals(value, _) => Map("type" -> "equals", "value" -> value)
      case OneOf(values, _) => Map("type" -> "oneOf", "values" -> DataCheck.sorted(values))
      case r: Range =>
        Map("type" -> "range") ++ List("gte" -> r.gte, "gt" -> r.gt, "lte" -> r.lte, "lt" -> r.lt).collect { case (k, Some(v)) => k -> v }
      case l: Length => Map("type" -> "length") ++ List("exact" -> l.exact, "min" -> l.min, "max" -> l.max).collect { case (k, Some(v)) => k -> v }
      case f: FieldRange =>
        Map("type" -> "fieldRange") ++ List("gte" -> f.gte, "gt" -> f.gt, "lte" -> f.lte, "lt" -> f.lt).collect { case (k, Some(v)) => k -> v }
    }
    properties + ("field" -> field)
  }

  /** Whether `row` (field name to value; a missing field reads as null) breaks this check. */
  def fails(row: Map[String, Any]): Boolean = row.get(field).filter(_ != null) match {
    case None => false
    case Some(value) =>
      constraint match {
        case Equals(expected, _) => !DataCheck.same(value, expected)
        case OneOf(allowed, _)   => !allowed.exists(DataCheck.same(value, _))
        case r: Range            => DataCheck.number(value).forall(n => !DataCheck.within(n, r.gte, r.gt, r.lte, r.lt))
        case l: Length =>
          val s = value.toString
          val n = s.codePointCount(0, s.length)
          l.exact.exists(_ != n) || l.min.exists(n < _) || l.max.exists(n > _)
        case f: FieldRange =>
          // a null on the other side is no bound at all, so it passes, as a null on this side does
          def other(name: Option[String]): Option[BigDecimal] = name.flatMap(n => row.get(n).filter(_ != null).flatMap(DataCheck.number))
          DataCheck.number(value).forall(n => !DataCheck.within(n, other(f.gte), other(f.gt), other(f.lte), other(f.lt)))
      }
  }
}

private object DataCheck {

  def show(value: Any): String = value match {
    case s: String => s"'$s'"
    case other     => String.valueOf(other)
  }

  def sorted(values: Set[Any]): List[Any] = values.toList.sortBy(String.valueOf)

  def bounds[A](gte: Option[A], gt: Option[A], lte: Option[A], lt: Option[A]): String =
    List("gte" -> gte, "gt" -> gt, "lte" -> lte, "lt" -> lt).collect { case (k, Some(v)) => s"$k $v" }.mkString(", ")

  def number(value: Any): Option[BigDecimal] = value match {
    case n: java.math.BigDecimal => Some(BigDecimal(n))
    case n: BigDecimal           => Some(n)
    case n: java.lang.Number     => scala.util.Try(BigDecimal(n.toString)).toOption
    case _                       => None
  }

  /** Numbers compare by value whatever their types (`1`, `1L` and `1.0` are the same), anything else by equality. */
  def same(a: Any, b: Any): Boolean = (number(a), number(b)) match {
    case (Some(x), Some(y)) => x.compare(y) == 0
    case _                  => a == b
  }

  def within(n: BigDecimal, gte: Option[BigDecimal], gt: Option[BigDecimal], lte: Option[BigDecimal], lt: Option[BigDecimal]): Boolean =
    gte.forall(n >= _) && gt.forall(n > _) && lte.forall(n <= _) && lt.forall(n < _)
}

/** What one check found in one write: how many rows broke it, and a few of the values those rows held (as text). */
final case class DataCheckResult(check: DataCheck, failed: Long, examples: List[String] = Nil)

/** The values a write puts in an output, held to its fields' declared `constraints` (`VerificationOptions.runtimeDataQuality`,
  * capability `check.runtimeDataQuality`). An adapter that sees the data before it is published evaluates `checks` with its own
  * engine (or with `evaluate`, over rows it holds in memory) and hands the results over as `CheckedWrite.dataChecks`; the
  * pipeline turns every check with a failing row into a `RUNTIME_DATA_QUALITY_VIOLATION`.
  *
  * Only the five constraint types the contract format interprets are checked; a `constraints` entry of another type is an
  * extension nothing in Invaract interprets (`ContractValidator` accepts it), here as in static analysis. Only top-level fields
  * are checked: a constraint on a field inside a struct is not, which an adapter's declaration says (`partial`).
  */
object RuntimeDataQuality {

  /** How many failing values a result carries, at most. */
  val MaxExamples = 5

  /** Every check of `output`'s top-level fields, in field order, then in the order each field declares its constraints. */
  def checks(output: Dataset): List[DataCheck] =
    output.schema.fields.flatMap(f => f.constraints.flatMap(_.interpret).map(DataCheck(f.name, _)))

  /** Evaluates `checks` over `rows` in memory, one result per check, in order. */
  def evaluate(checks: List[DataCheck], rows: Iterable[Map[String, Any]]): List[DataCheckResult] =
    checks.map { check =>
      val failing = rows.filter(check.fails)
      DataCheckResult(check, failing.size.toLong, failing.flatMap(_.get(check.field)).map(String.valueOf).toList.distinct.take(MaxExamples))
    }

  /** One violation per result with a failing row; `location` is the write's. */
  def violations(results: List[DataCheckResult], location: Option[String]): List[Violation] =
    results.filter(_.failed > 0).map(r => Violations.runtimeDataQuality(r.check.field, r.check.describe, r.failed, r.examples, location))
}
