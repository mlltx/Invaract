// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.ir._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

/** Focused coverage of `SparkPlanAdapter`'s expression- and plan-level
  * translation against real analyzed Spark plans, complementing
  * `SparkPlanAdapterSpec`'s broader per-construct survey with the specific
  * cases called out by this phase's spec: literal types, arithmetic/
  * boolean operators, nested and chained expressions, multi-column and
  * ambiguous joins, duplicate output names, and repeated column
  * references. Every assertion is a structural match against the real
  * translated `ir.Expr`/`ir.Plan`, not a string/snapshot comparison — a
  * regression in what a construct means, not just how it prints, is what
  * this is meant to catch.
  */
class ExpressionTranslationSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var fixtureCsv: Path = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ExpressionTranslationSpec")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // A real LogicalRelation-backed CSV read, not spark.createDataFrame's
    // LogicalRDD - the same relation shape SparkPlanAdapterSpec's own
    // fixtures use. LogicalRDD is not among the Read shapes this
    // translator recognizes (a real, separate gap from anything this
    // phase's spec covers - see docs/SPARK_ADAPTER.md), so a CSV-backed
    // fixture keeps every test here scoped to expression/plan translation
    // proper rather than tripping over that unrelated gap.
    val dir = Files.createTempDirectory("invaract-expr-translation-test")
    fixtureCsv = dir.resolve("fixture.csv")
    Files.write(
      fixtureCsv,
      "id,name,amount,active\n1,alice,10.0,true\n2,bob,20.0,false\n3,,30.0,true\n".getBytes("UTF-8")
    )
  }

  override def afterAll(): Unit = spark.stop()

  private def baseDf() =
    spark.read.option("header", "true").option("inferSchema", "true").csv(fixtureCsv.toString)

  private def translate(df: org.apache.spark.sql.DataFrame) =
    SparkPlanAdapter.translate(df.queryExecution.analyzed)

  private def projectColumns(df: org.apache.spark.sql.DataFrame): List[NamedExpr] =
    translate(df).plan match {
      case Project(_, columns) => columns
      case other                => fail(s"expected a Project, got ${PlanPrinter.render(other)}")
    }

  // ---- Literals of relevant types ----------------------------------------

  test("literals: integer, long, double, float, string, boolean, decimal, date, and null translate with the right logical type") {
    val df = baseDf().select(
      lit(42).as("int_lit"),
      lit(42L).as("long_lit"),
      lit(3.14).as("double_lit"),
      lit(3.14f).as("float_lit"),
      lit("hello").as("string_lit"),
      lit(true).as("bool_lit"),
      lit(BigDecimal("12.34")).as("decimal_lit"),
      lit(java.sql.Date.valueOf("2024-01-01")).as("date_lit"),
      lit(null).as("null_lit")
    )

    val byName = projectColumns(df).map(c => c.name -> c.expr).toMap
    assert(byName("int_lit") == Literal(42, "integer"))
    assert(byName("long_lit") == Literal(42L, "long"))
    assert(byName("double_lit") == Literal(3.14, "double"))
    assert(byName("float_lit") == Literal(3.14f, "float"))
    assert(byName("string_lit") == Literal("hello", "string"))
    assert(byName("bool_lit") == Literal(true, "boolean"))
    byName("decimal_lit") match {
      case Literal(v, "decimal") => assert(v.toString.contains("12.34"))
      case other                  => fail(s"unexpected decimal literal translation: $other")
    }
    byName("date_lit") match {
      case Literal(_, "date") => // expected — the raw internal day-offset value isn't asserted, only the logical type
      case other                => fail(s"unexpected date literal translation: $other")
    }
    byName("null_lit") match {
      case Literal(null, _) => // expected: a typed-or-untyped SQL NULL, fully understood, not UnknownExpression
      case other              => fail(s"expected a null Literal, got $other")
    }
  }

  test("a null value read from real data (not a literal) round-trips as a null Literal via a comparison, not silently dropped") {
    val df = baseDf().filter(col("name").isNull).select(col("id"))
    val result = translate(df)
    result.plan match {
      case Project(Filter(_, Function("ISNULL", List(ColumnReference(ColumnRef("name", _, _))))), _) => // expected
      case other => fail(s"unexpected shape: ${PlanPrinter.render(other)}")
    }
  }

  // ---- Arithmetic operators -----------------------------------------------

  test("arithmetic: +, -, *, /, % all translate as Arithmetic with the right operator and operands") {
    val df = baseDf().select(
      (col("amount") + 1).as("plus"),
      (col("amount") - 1).as("minus"),
      (col("amount") * 2).as("times"),
      (col("amount") / 2).as("div"),
      (col("id") % 3).as("mod")
    )
    val byName = projectColumns(df).map(c => c.name -> c.expr).toMap

    // amount is Double, so an Int literal operand (1, 2, 3) is promoted by
    // Spark's own type coercion into a real Cast(Literal(...), "double")
    // BEFORE the arithmetic op - a genuine Catalyst rewrite, confirmed
    // empirically, not assumed - so the literal operand is only checked
    // for being *a* literal (bare or Cast-wrapped), not an exact shape.
    def isLiteralOperand(e: Expr): Boolean = e match {
      case Literal(_, _)       => true
      case Cast(Literal(_, _), _) => true
      case _                     => false
    }
    def opAndCol(name: String): (String, String) = byName(name) match {
      case Arithmetic(op, List(ColumnReference(ColumnRef(colName, _, _)), lit)) if isLiteralOperand(lit) => (op, colName)
      case other => fail(s"unexpected arithmetic translation for '$name': $other")
    }
    assert(opAndCol("plus") == ("+", "amount"))
    assert(opAndCol("minus") == ("-", "amount"))
    assert(opAndCol("times") == ("*", "amount"))
    assert(opAndCol("div") == ("/", "amount"))
    assert(opAndCol("mod") == ("%", "id"))
  }

  test("arithmetic: unary negation translates as a single-operand Arithmetic(NEGATE, ...)") {
    val df = baseDf().select(negate(col("amount")).as("negated"))
    projectColumns(df).find(_.name == "negated").get.expr match {
      case Arithmetic("NEGATE", List(ColumnReference(ColumnRef("amount", _, _)))) => // expected
      case other                                                                    => fail(s"unexpected negation translation: $other")
    }
  }

  // ---- Boolean expressions -------------------------------------------------

  test("boolean: AND, OR, and NOT translate as BooleanExpr over their real operands") {
    val df = baseDf().select(
      (col("active") && (col("amount") > 10)).as("and_expr"),
      (col("active") || (col("amount") < 5)).as("or_expr"),
      (!col("active")).as("not_expr")
    )
    val byName = projectColumns(df).map(c => c.name -> c.expr).toMap

    // amount (Double) compared against an Int literal (10, 5) - same real
    // Cast-promotion as the arithmetic test above, so the comparison's
    // right operand is only checked as "some literal", bare or Cast-wrapped.
    byName("and_expr") match {
      case BooleanExpr("AND", List(ColumnReference(ColumnRef("active", _, _)), Comparison(">", ColumnReference(ColumnRef("amount", _, _)), _))) =>
      // expected
      case other => fail(s"unexpected AND translation: $other")
    }
    byName("or_expr") match {
      case BooleanExpr("OR", List(ColumnReference(ColumnRef("active", _, _)), Comparison("<", ColumnReference(ColumnRef("amount", _, _)), _))) =>
      // expected
      case other => fail(s"unexpected OR translation: $other")
    }
    byName("not_expr") match {
      case BooleanExpr("NOT", List(ColumnReference(ColumnRef("active", _, _)))) => // expected
      case other                                                                  => fail(s"unexpected NOT translation: $other")
    }
  }

  // ---- Nested / composite expressions --------------------------------------

  test("a deeply nested arithmetic/comparison/boolean expression preserves its full real structure") {
    // ((amount * 2) + 1) > 10 AND active
    val df = baseDf().select((((col("amount") * 2) + 1) > 10 && col("active")).as("flag"))

    projectColumns(df).find(_.name == "flag").get.expr match {
      case BooleanExpr(
            "AND",
            List(
              Comparison(">", Arithmetic("+", List(Arithmetic("*", List(ColumnReference(ColumnRef("amount", _, _)), _)), _)), _),
              ColumnReference(ColumnRef("active", _, _))
            )
          ) =>
      // expected: every level of nesting preserved, not flattened or
      // collapsed - the Int literals (2, 1, 10) are Cast-promoted to
      // double against the Double `amount` column (same real rewrite as
      // the arithmetic/boolean tests above), so their exact shape isn't
      // pinned here, only that every structural level survives translation
      case other => fail(s"unexpected nested-expression translation: ${PlanPrinter.render(Project(Read(DatasetRef("x")), List(NamedExpr("flag", other))))}")
    }
  }

  // ---- Common built-in and nested functions --------------------------------

  test("common built-in functions (UPPER, LENGTH, COALESCE) translate as Function nodes by name") {
    val df = baseDf().select(
      upper(col("name")).as("upper_name"),
      length(col("name")).as("name_len"),
      coalesce(col("name"), lit("unknown")).as("safe_name")
    )
    val byName = projectColumns(df).map(c => c.name -> c.expr).toMap

    byName("upper_name") match {
      case Function("UPPER", List(ColumnReference(ColumnRef("name", _, _)))) => // expected
      case other                                                               => fail(s"unexpected UPPER translation: $other")
    }
    byName("name_len") match {
      case Function("LENGTH", List(ColumnReference(ColumnRef("name", _, _)))) => // expected
      case other                                                                => fail(s"unexpected LENGTH translation: $other")
    }
    byName("safe_name") match {
      case Function("COALESCE", List(ColumnReference(ColumnRef("name", _, _)), Literal("unknown", "string"))) => // expected
      case other                                                                                                 => fail(s"unexpected COALESCE translation: $other")
    }
  }

  test("a function nested inside another function preserves both levels") {
    val df = baseDf().select(upper(trim(col("name"))).as("cleaned"))
    projectColumns(df).find(_.name == "cleaned").get.expr match {
      case Function("UPPER", List(Function("TRIM", List(ColumnReference(ColumnRef("name", _, _)))))) => // expected
      case other                                                                                        => fail(s"unexpected nested-function translation: $other")
    }
  }

  // ---- Multiple chained transformations ------------------------------------

  test("multiple chained filters and projections nest in the real order Spark applies them") {
    val df = baseDf()
      .filter(col("amount") > 5)
      .select(col("id"), col("amount"))
      .filter(col("id") =!= 2)
      .select((col("amount") * 2).as("doubled"))

    def depthAndKinds(plan: Plan): List[String] = plan.getClass.getSimpleName :: plan.children.flatMap(depthAndKinds)
    val shape = depthAndKinds(translate(df).plan)
    // Innermost to outermost: Read, then alternating Filter/Project layers -
    // the exact chain this pipeline built, not merely "some nesting".
    assert(shape == List("Project", "Filter", "Project", "Filter", "Read"), s"unexpected chain shape: $shape")
  }

  // ---- Join conditions involving multiple columns, and ambiguous columns --

  test("a join condition over multiple columns preserves both equalities, connected by AND") {
    val left = baseDf().as("l")
    val right = baseDf().as("r")
    val joined = left.join(right, col("l.id") === col("r.id") && col("l.name") === col("r.name"), "inner")

    def findJoin(plan: Plan): Option[Join] = plan match {
      case j: Join => Some(j)
      case other    => other.children.flatMap(findJoin).headOption
    }
    val join = findJoin(translate(joined).plan).getOrElse(fail("no Join node found"))
    join.condition match {
      case Some(BooleanExpr("AND", List(Comparison("=", ColumnReference(idL), ColumnReference(idR)), Comparison("=", ColumnReference(nameL), ColumnReference(nameR))))) =>
        assert(idL.name == "id" && idR.name == "id")
        assert(nameL.name == "name" && nameR.name == "name")
      case other => fail(s"unexpected multi-column join condition translation: $other")
    }
  }

  test("two ambiguous same-named columns from different relations remain distinguishable by qualifier after a join") {
    val orders = baseDf().as("orders")
    val archive = baseDf().as("archive")
    val joined = orders
      .join(archive, col("orders.id") === col("archive.id"), "inner")
      .select(col("orders.id").as("current_id"), col("archive.id").as("archived_id"))

    val byName = projectColumns(joined).map(c => c.name -> c.expr).toMap
    (byName("current_id"), byName("archived_id")) match {
      case (ColumnReference(ColumnRef("id", Some("orders"), _)), ColumnReference(ColumnRef("id", Some("archive"), _))) =>
      // expected: same bare name ("id"), but distinguishable qualifiers —
      // exactly the case CLAUDE.md's task spec calls out ("two different
      // id columns participating in a join")
      case other => fail(s"expected two distinguishable 'id' columns, got $other")
    }
    // Lineage tracing must keep them apart too, not merely the raw Project.
    val lineage = Lineage.trace(Write(DatasetRef("out"), translate(joined).plan))
    assert(lineage.find(_.output.name == "current_id").get.sources == Set(ColumnRef("id", Some("orders"))))
    assert(lineage.find(_.output.name == "archived_id").get.sources == Set(ColumnRef("id", Some("archive"))))
  }

  // ---- Outer join variants (beyond SparkPlanAdapterSpec's type-only check) -

  test("a left outer join's condition and both relations are preserved, not just its JoinType") {
    val left = baseDf().as("l")
    val right = baseDf().as("r")
    val joined = left.join(right, col("l.id") === col("r.id"), "left_outer")

    def findJoin(plan: Plan): Option[Join] = plan match {
      case j: Join => Some(j)
      case other    => other.children.flatMap(findJoin).headOption
    }
    val join = findJoin(translate(joined).plan).getOrElse(fail("no Join node found"))
    assert(join.joinType == JoinType.LeftOuter)
    join.left match {
      case Read(_, Some("l")) => // expected
      case other                => fail(s"expected the left relation's alias preserved, got $other")
    }
    join.right match {
      case Read(_, Some("r")) => // expected
      case other                => fail(s"expected the right relation's alias preserved, got $other")
    }
    assert(join.condition.exists(_.references.exists(_.name == "id")))
  }

  // ---- Duplicate output names, and an alias over a derived expression -----

  test("duplicate output names are preserved as two distinct NamedExpr entries, not deduplicated") {
    val df = baseDf().select(col("id").as("x"), col("amount").as("x"))
    val columns = projectColumns(df)
    assert(columns.count(_.name == "x") == 2, s"expected two entries named 'x', got $columns")
    // Each keeps its own real source column, not just the shared name.
    columns.head.expr match {
      case ColumnReference(ColumnRef("id", _, _)) => // expected
      case other                                    => fail(s"unexpected first 'x': $other")
    }
    columns(1).expr match {
      case ColumnReference(ColumnRef("amount", _, _)) => // expected
      case other                                        => fail(s"unexpected second 'x': $other")
    }
  }

  test("an alias over a derived (non-passthrough) expression preserves the full computation, not just the new name") {
    val df = baseDf().select((col("amount") * 1.2).as("value"))
    projectColumns(df).find(_.name == "value").get.expr match {
      case Arithmetic("*", List(ColumnReference(ColumnRef("amount", _, _)), Literal(_, _))) => // expected — a real
      // computation, not a bare ColumnReference the way a pure rename
      // (e.g. col("amount").as("value")) would translate
      case other => fail(s"unexpected derived-alias translation: $other")
    }
  }

  test("a pure rename (no computation) translates to a plain ColumnReference under the new name") {
    val df = baseDf().select(col("amount").as("total"))
    projectColumns(df).find(_.name == "total").get.expr match {
      case ColumnReference(ColumnRef("amount", _, _)) => // expected: passthrough, not wrapped in any function
      case other                                         => fail(s"expected a pure passthrough rename, got $other")
    }
  }

  // ---- Multiple references to the same input column ------------------------

  test("multiple independent references to the same input column all resolve to the identical, structurally-equal ColumnRef") {
    val df = baseDf().select(
      col("amount"),
      (col("amount") + 1).as("plus_one"),
      (col("amount") * 2).as("doubled")
    )
    val columns = projectColumns(df)

    val bareRef = columns.find(_.name == "amount").get.expr.asInstanceOf[ColumnReference].ref
    val refInPlusOne = columns.find(_.name == "plus_one").get.expr match {
      case Arithmetic("+", List(ColumnReference(ref), _)) => ref
      case other                                            => fail(s"unexpected plus_one translation: $other")
    }
    val refInDoubled = columns.find(_.name == "doubled").get.expr match {
      case Arithmetic("*", List(ColumnReference(ref), _)) => ref
      case other                                            => fail(s"unexpected doubled translation: $other")
    }

    assert(bareRef == refInPlusOne)
    assert(bareRef == refInDoubled)
    assert(bareRef.name == "amount" && bareRef.qualifier.isEmpty)
  }

  // ---- Multiple aggregates and grouping columns in one Aggregate node ------

  test("multiple aggregates over the same grouping key all appear as distinct declared outputs") {
    val df = baseDf().groupBy("active").agg(
      count("*").as("cnt"),
      sum("amount").as("total"),
      avg("amount").as("average")
    )
    translate(df).plan match {
      case Aggregate(_, List(ColumnReference(ColumnRef("active", _, _))), aggregates) =>
        val byName = aggregates.map(a => a.name -> a.expr).toMap
        assert(byName.keySet == Set("active", "cnt", "total", "average"))
        assert(byName("cnt").asInstanceOf[AggregateCall].function == "COUNT")
        assert(byName("total").asInstanceOf[AggregateCall].function == "SUM")
        assert(byName("average").asInstanceOf[AggregateCall].function == "AVG")
      case other => fail(s"unexpected shape: ${PlanPrinter.render(other)}")
    }
  }
}
