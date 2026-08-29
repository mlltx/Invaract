// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter

import com.example.ir._

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

/** Property-based fuzzing of `SparkPlanAdapter.translate`, complementing
  * `SparkPlanAdapterSpec`'s hand-picked, single-construct examples.
  *
  * `SparkPlanAdapter`'s own class doc promises it "never throws" — an
  * unrecognized plan node degrades to `ir.Unsupported` plus a `Diagnostic`
  * instead of raising an exception. That promise is only as trustworthy as
  * what's been thrown at it, and the hand-written spec only exercises each
  * translated construct in isolation, never in the combinations and
  * nesting depths a real, evolving pipeline eventually produces. This spec
  * generates random *chains* of the very same operations
  * `SparkPlanAdapterSpec` covers individually — filters, recomputed
  * columns, sorts, aggregates, self-joins, unions, distinct, limit,
  * repartition, CASE WHEN — composed in random order and depth against a
  * real `local[*]` `SparkSession`, and checks the "never throws" promise
  * (plus a few related invariants) holds for all of them, not just the
  * examples someone thought to write down.
  *
  * Every generated chain preserves a fixed canonical schema
  * (`id: Int, value: Int, name: String, active: Boolean`) at every step —
  * including through `AggregateStep` and `SelfJoinStep`, which re-project
  * back down to it — so steps can be composed in arbitrary order without
  * the generator needing to track a live schema. This keeps every
  * generated chain a *valid* Spark query (Spark's own analyzer still does
  * the real work of resolving it); the randomness is in plan *shape*, not
  * in producing intentionally-broken SQL.
  */
class SparkPlanAdapterFuzzSpec extends AnyFunSuite with BeforeAndAfterAll with ScalaCheckDrivenPropertyChecks {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[*]").appName("SparkPlanAdapterFuzzSpec")
      // See ContractEnforcementRuleSpec's beforeAll for why - this
      // property-based suite runs many small Spark actions per case, so
      // the default 200-task shuffle overhead compounds badly here.
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 200)

  // forAll is used here with an explicit Gen (not the Arbitrary-based
  // overload), so ScalaCheck does not attempt to shrink a failing case —
  // there's no Shrink[Step] instance for this ADT to shrink with anyway.
  // A failure instead reports the full generated chain and the resulting
  // analyzed plan directly (see the `fail(...)` call below), which is
  // enough to reproduce and debug it without shrinking.

  private val canonicalSchema = StructType(
    Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("active", BooleanType, nullable = false)
    )
  )

  private def baseDf(): DataFrame = {
    val rows = (1 to 5).map(i => Row(i, i * 10, s"row$i", i % 2 == 0))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), canonicalSchema)
  }

  // ---- The step ADT: every step preserves the canonical schema ----------

  sealed trait Step
  case class FilterStep(useId: Boolean, threshold: Int) extends Step
  case class RecomputeValueStep(op: Int) extends Step
  case class SortStep(useId: Boolean, ascending: Boolean) extends Step
  case object DistinctStep extends Step
  case class LimitStep(n: Int) extends Step
  case class RepartitionStep(n: Int, byColumn: Boolean, coalesce: Boolean) extends Step
  case object AggregateStep extends Step
  case object SelfJoinStep extends Step
  case object UnionSelfStep extends Step
  case class CaseWhenStep(threshold: Int) extends Step

  private val stepGen: Gen[Step] = Gen.oneOf(
    for {
      useId     <- Gen.oneOf(true, false)
      threshold <- Gen.choose(0, 60)
    } yield FilterStep(useId, threshold),
    Gen.choose(0, 3).map(RecomputeValueStep),
    for {
      useId <- Gen.oneOf(true, false)
      asc   <- Gen.oneOf(true, false)
    } yield SortStep(useId, asc),
    Gen.const(DistinctStep),
    Gen.choose(1, 10).map(LimitStep),
    for {
      n         <- Gen.choose(1, 4)
      byColumn  <- Gen.oneOf(true, false)
      coalesce  <- Gen.oneOf(true, false)
    } yield RepartitionStep(n, byColumn, coalesce),
    Gen.const(AggregateStep),
    Gen.const(SelfJoinStep),
    Gen.const(UnionSelfStep),
    Gen.choose(0, 60).map(CaseWhenStep)
  )

  // Bounded depth: this is a plan-shape fuzzer, not a load test — a few
  // steps is already enough combinatorial variety to exceed what the
  // hand-written spec exercises, and keeps each property run's ~200 cases
  // fast (no Spark job ever executes; only analysis, via .analyzed below).
  private val chainGen: Gen[List[Step]] = Gen.choose(1, 6).flatMap(n => Gen.listOfN(n, stepGen))

  private def applyStep(df: DataFrame, step: Step): DataFrame = step match {
    case FilterStep(useId, threshold) =>
      df.filter(col(if (useId) "id" else "value") > lit(threshold))

    case RecomputeValueStep(op) =>
      val recomputed = op match {
        case 0 => col("value") + col("id")
        case 1 => col("value") - lit(1)
        case 2 => col("value") * lit(2)
        case _ => col("value") % lit(7)
      }
      df.withColumn("value", recomputed.cast("int"))

    case SortStep(useId, ascending) =>
      val c = col(if (useId) "id" else "value")
      df.orderBy(if (ascending) c.asc else c.desc)

    case DistinctStep =>
      df.distinct()

    case LimitStep(n) =>
      df.limit(n)

    case RepartitionStep(n, byColumn, coalesce) =>
      if (coalesce) df.coalesce(math.max(1, n))
      else if (byColumn) df.repartition(n, col("id"))
      else df.repartition(n)

    case AggregateStep =>
      df.groupBy("id")
        .agg(
          sum("value").cast("int").as("value"),
          first("name").as("name"),
          first("active").as("active")
        )
        .select(col("id"), col("value"), col("name"), col("active"))

    case SelfJoinStep =>
      val left  = df.as("a")
      val right = df.as("b")
      left
        .join(right, col("a.id") === col("b.id"), "inner")
        .select(
          col("a.id").as("id"),
          col("a.value").as("value"),
          col("a.name").as("name"),
          col("a.active").as("active")
        )

    case UnionSelfStep =>
      df.union(df.filter(col("id") >= lit(0)))

    case CaseWhenStep(threshold) =>
      df.withColumn("value", when(col("value") > lit(threshold), lit(threshold)).otherwise(col("value")).cast("int"))
  }

  private def buildPlan(steps: List[Step]): DataFrame =
    steps.foldLeft(baseDf())(applyStep)

  /** Walks a translated `ir.Plan` and asserts that wherever the adapter
    * gave up (`Unsupported`), it also left a `Diagnostic` behind — the
    * pairing `SparkPlanAdapter`'s class doc promises ("Both paths are
    * recorded as Diagnostics"). A generator this constrained shouldn't hit
    * `Unsupported` at all in practice, but the check costs nothing and
    * directly verifies the documented contract if it ever does.
    */
  private def assertUnsupportedIsDiagnosed(plan: Plan, diagnostics: List[Diagnostic]): Unit = {
    def walk(p: Plan): Unit = {
      p match {
        case Unsupported(description, _) =>
          assert(
            diagnostics.nonEmpty,
            s"plan contains Unsupported($description) but no Diagnostic was recorded"
          )
        case _ => ()
      }
      p.children.foreach(walk)
    }
    walk(plan)
  }

  test("SparkPlanAdapter never throws on arbitrary compositions of translated operations (property-based)") {
    forAll(chainGen) { steps: List[Step] =>
      val df = buildPlan(steps)
      val analyzed = df.queryExecution.analyzed

      try {
        val result = SparkPlanAdapter.translateAsWrite(analyzed, DatasetRef("fuzz.output"))
        // Rendering and lineage tracing are the other two things every
        // translated plan is expected to survive unconditionally — both
        // are exercised by the real reporting path in DemoJobHarness.
        PlanPrinter.render(result.plan)
        Lineage.trace(result.plan)
        assertUnsupportedIsDiagnosed(result.plan, result.diagnostics)
      } catch {
        case e: Throwable =>
          fail(
            s"SparkPlanAdapter threw on a generated chain of ${steps.size} step(s): $steps\n" +
              s"Analyzed plan:\n${analyzed.toString}\n" +
              s"Exception: ${e.getClass.getName}: ${e.getMessage}",
            e
          )
      }
    }
  }
}
