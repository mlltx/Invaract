// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.ContractParser
import com.invaract.verification.{ContractViolationException, ViolationType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation}
import org.apache.spark.sql.execution.command.ShowTablesCommand
import org.apache.spark.sql.functions.col

import java.nio.file.{Files, Path}

/** A plan too deep for the analyzer thread's stack must never crash a job (`DeepPlans`): it is checked again on a
  * thread with a large stack, and only a write too deep even for that is refused.
  *
  * The retry is exercised deterministically by running the check on a thread with a deliberately tiny stack, rather
  * than by building a plan just deep enough to overflow the test thread - how deep that is depends on the JIT.
  */
class DeepPlansSpec extends ConnectorSpecBase {
  private var dir: Path = _

  override def beforeAll(): Unit = {
    dir = Files.createTempDirectory("invaract-deep-plans")
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("DeepPlansSpec")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .withExtensions(injectContractCheck)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  private def depth(n: Int): Int = if (n == 0) 0 else 1 + depth(n - 1)

  /** `body` on a thread with a 128 KB stack, too small to translate a plan of a few hundred nodes; what it threw, if
    * anything. The active session is inherited, as it is by Spark's own threads. */
  private def onSmallStack(body: => Unit): Option[Throwable] = {
    @volatile var thrown: Option[Throwable] = None
    val t = new Thread(null, () => try body catch { case e: Throwable => thrown = Some(e) }, "small-stack", 128L * 1024)
    t.start()
    t.join()
    thrown
  }

  private def contract(out: String) = ContractParser.parse(
    s"""id: deep
       |version: "1.0.0"
       |outputs:
       |  - name: out
       |    location: $out
       |    schema:
       |      fields:
       |        - { name: id, type: long }
       |        - { name: v, type: long }
       |""".stripMargin
  )

  private def deep(levels: Int): DataFrame =
    (1 to levels).foldLeft(spark.range(3).withColumn("v", col("id")))((df, i) => df.withColumn("v", col("v") + i))

  /** The fewest levels whose translation overflows the small stack. How deep that is depends on the JVM and on how much
    * of the translation the JIT has compiled by then (compiled frames are smaller), so it is measured rather than assumed. */
  private lazy val tooDeepForSmallStack: Int =
    Iterator.iterate(150)(_ * 2).takeWhile(_ <= 2400).find { levels =>
      val plan = deep(levels).queryExecution.analyzed
      onSmallStack(SparkPlanAdapter.translate(plan)).exists(_.isInstanceOf[StackOverflowError])
    }.getOrElse(fail("even 2400 levels fit a 128 KB stack"))

  /** The analyzed plan of a real write of `df` to `path`, captured as the check rule saw it (no contract active). */
  private def analyzedWriteOf(df: DataFrame, path: String): LogicalPlan = {
    capturedPlans.clear()
    df.write.mode("overwrite").parquet(path)
    capturedPlans.find(p => WriteCommandSupport.combined.isDefinedAt(p)).getOrElse(fail("no write captured"))
  }

  test("attempt: Right for work that fits on the stack, Left for work that does not; other errors are thrown") {
    assert(DeepPlans.attempt(depth(200)) == Right(200))
    assert(DeepPlans.attempt(depth(Int.MaxValue)).isLeft)
    intercept[IllegalStateException](DeepPlans.attempt[Int](throw new IllegalStateException("not a stack overflow")))
  }

  test("onLargeStack: work too deep for this thread fits; work with no end still overflows") {
    assert(DeepPlans.attempt(depth(1000000)).isLeft, "a million frames do not fit the test thread's stack")
    assert(DeepPlans.onLargeStack(depth(1000000)) == Right(1000000))
    assert(DeepPlans.onLargeStack(depth(Int.MaxValue)).isLeft)
  }

  test("onLargeStack: an exception arrives on the calling thread as itself, and the active session is the caller's") {
    val e = intercept[IllegalStateException](DeepPlans.onLargeStack[Unit](throw new IllegalStateException("as itself")))
    assert(e.getMessage == "as itself")
    SparkSession.setActiveSession(spark)
    assert(DeepPlans.onLargeStack(SparkSession.active) == Right(spark))
  }

  test("a write whose check overflows the analyzer thread's stack is checked on a large one: a wrong location is still refused") {
    val declared = dir.resolve("declared").toString
    val plan = analyzedWriteOf(deep(tooDeepForSmallStack), dir.resolve("elsewhere").toString)
    assert(onSmallStack(SparkPlanAdapter.translate(plan)).exists(_.isInstanceOf[StackOverflowError]), "the small stack really is too small")
    val thrown = onSmallStack(ContractEnforcementRule.verifyOrThrow(contract(declared), plan, com.invaract.verification.VerificationOptions()))
    thrown match {
      case Some(e: ContractViolationException) => assert(e.result.violations.map(_.violationType) == List(ViolationType.OutputLocationMismatch))
      case other                               => fail(s"expected the write refused for its location, got $other")
    }
  }

  test("... and a write to the declared location passes") {
    val declared = dir.resolve("declared_ok").toString
    val plan = analyzedWriteOf(deep(tooDeepForSmallStack), declared)
    assert(onSmallStack(ContractEnforcementRule.verifyOrThrow(contract(declared), plan, com.invaract.verification.VerificationOptions())).isEmpty)
  }

  test("the check of a plan that writes nothing never fails the query, however deep") {
    val plan = deep(tooDeepForSmallStack).queryExecution.analyzed
    assert(onSmallStack(ContractEnforcementRule.verifyOrThrow(contract(dir.resolve("x").toString), plan, com.invaract.verification.VerificationOptions())).isEmpty)
  }

  test("log-only dry-run never throws on a plan too deep to infer from") {
    val plan = analyzedWriteOf(deep(tooDeepForSmallStack), dir.resolve("dry").toString)
    var inferred = 0
    assert(onSmallStack(ContractEnforcementRule.inferOrIgnore(plan, _ => inferred += 1)).isEmpty)
  }

  test("a plan too deep even for the large stack: a write is refused, anything else is let through") {
    val c = contract(dir.resolve("y").toString)
    val e = intercept[ContractViolationException](ContractEnforcementRule.tooDeep(c, ShowTablesCommand(None, None, Nil), None, None, None))
    assert(e.result.violations.map(_.violationType) == List(ViolationType.UnverifiableWrite))
    assert(e.getMessage.contains("too deeply nested") && e.getMessage.contains(".checkpoint()"))
    val write = analyzedWriteOf(spark.range(1).toDF("id").withColumn("v", col("id")), dir.resolve("z").toString)
    intercept[ContractViolationException](ContractEnforcementRule.tooDeep(c, write, None, None, None))
    ContractEnforcementRule.tooDeep(c, OneRowRelation(), None, None, None) // must not throw
  }

  test("a plan too deep that writes nothing is warned about once per JVM, not on every query") {
    val c = contract(dir.resolve("w").toString)
    ContractEnforcementRule.warnedTooDeep.set(false)
    val (_, logged) = LogCapture.messages(LogCapture.EnforcementRule, org.apache.logging.log4j.Level.WARN) {
      ContractEnforcementRule.tooDeep(c, OneRowRelation(), None, None, None)
      ContractEnforcementRule.tooDeep(c, OneRowRelation(), None, None, None)
    }
    assert(logged.count(_.contains("too deeply nested to translate")) == 1, logged)
  }

  test("end to end: a count() over a thousand withColumn calls runs under a contract") {
    val df = (1 to 1000).foldLeft(spark.range(3).toDF("v"))((d, i) => d.withColumn("v", col("v") + i))
    assume(scala.util.Try(df.count()).isSuccess, "Spark itself cannot analyze this plan on this JVM")
    withContract(
      s"""id: deep
         |version: "1.0.0"
         |outputs:
         |  - { name: out, location: ${dir.resolve("never")}, schema: { fields: [ { name: v, type: long } ] } }
         |""".stripMargin
    )(assert(df.count() == 3))
  }
}
