// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Contract, ContractParser}
import com.invaract.verification.{ContractViolationException, VerificationOptions, ViolationType}
import com.invaract.verification.notification.NotificationSink
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}
import scala.collection.mutable.ListBuffer

/** A stream into `foreachBatch` is checked through the writes its function makes, each a batch write
  * (docs/SPARK_ADAPTER_HARDENING_PLAN.md, H-11). The stream itself used to be judged as a write to a place called
  * `ForeachBatchSink`, which refused every such stream under any contract.
  *
  * Each test installs Invaract the way a job does (`forContract`, or `dryRun`), in a session of its own: the batch
  * DataFrame the function receives is an RDD Spark builds from the micro-batch, and it is the session's own checkpoint
  * registry that ties it back to the files the micro-batch read.
  */
class ForeachBatchSpec extends AnyFunSuite {

  @volatile private var armed = false

  /** Test set-up that is no part of the job under test. */
  private def unchecked[T](body: => T): T = { armed = false; try body finally armed = true }

  /** A session in a fresh folder, with Invaract installed as `install(folder)` builds it. */
  private def withSession[T](install: Path => (SparkSession => LogicalPlan => Unit))(body: (SparkSession, Path) => T): T = {
    val dir = Files.createTempDirectory("invaract-foreach-batch")
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("ForeachBatchSpec")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", dir.resolve("warehouse").toString)
      // Armed once the stream's source exists: writing that fixture is no part of the job under test.
      .withExtensions(_.injectCheckRule { session =>
        val rule = install(dir)(session)
        plan => if (armed) rule(plan)
      })
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try {
      // The stream's source: a folder of parquet files, written before Invaract is asked anything about it.
      val s = spark
      import s.implicits._
      Seq((1L, 10L), (2L, 20L)).toDF("id", "value").write.parquet(dir.resolve("events").toString)
      armed = true
      body(spark, dir)
    } finally {
      armed = false
      spark.stop()
    }
  }

  private def contract(dir: Path, output: String, extraInputs: String = ""): Contract = ContractParser.parse(
    s"""id: foreach_batch
       |version: "1.0.0"
       |inputs:
       |  - { name: events, location: ${dir.resolve("events")}, schema: { fields: [ { name: id, type: long } ] } }
       |$extraInputs
       |outputs:
       |  - { name: out, location: $output, schema: { fields: [ { name: id, type: long }, { name: value, type: long } ] } }
       |""".stripMargin
  )

  private def enforcing(dir: Path, output: String, options: VerificationOptions = VerificationOptions(), extraInputs: String = "") =
    ContractEnforcementRule.forContract(contract(dir, output, extraInputs), options)

  /** Runs a stream over the events folder into `f` once, and returns what it failed with, if anything. */
  private def runStream(spark: SparkSession, dir: Path)(f: DataFrame => Unit): Option[Throwable] = {
    val q = spark.readStream.schema("id long, value long").parquet(dir.resolve("events").toString).writeStream
      .foreachBatch((batch: DataFrame, _: Long) => f(batch))
      .option("checkpointLocation", dir.resolve("stream-checkpoint").toString)
      .trigger(Trigger.AvailableNow())
      .start()
    try { q.awaitTermination(); None }
    catch { case e: Throwable => Some(e) }
  }

  private def violationOf(e: Throwable): Option[ContractViolationException] =
    Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).collectFirst { case c: ContractViolationException => c }

  test("PASS: a foreachBatch function writing each batch to the declared output") {
    val out = Files.createTempDirectory("fb-out").resolve("out").toString
    withSession(dir => enforcing(dir, out)) { (spark, dir) =>
      assert(runStream(spark, dir)(_.write.mode("append").parquet(out)).isEmpty)
      assert(spark.read.parquet(out).count() == 2)
    }
  }

  test("FAIL: a foreachBatch function writing to an undeclared place: the stream fails on that write, which never lands") {
    val out = Files.createTempDirectory("fb-out").resolve("out").toString
    withSession(dir => enforcing(dir, out)) { (spark, dir) =>
      val elsewhere = dir.resolve("elsewhere").toString
      val failure = runStream(spark, dir)(_.write.mode("append").parquet(elsewhere))
      val v = failure.flatMap(violationOf).getOrElse(fail(s"expected a contract violation, got $failure"))
      assert(v.result.violations.map(_.violationType) == List(ViolationType.OutputLocationMismatch))
      // the message names the location as Spark does (file:/C:/... on Windows), so compare with forward slashes
      val message = v.result.violations.head.message
      assert(message.replace('\\', '/').contains(elsewhere.replace('\\', '/')) && !message.contains("ForeachBatchSink"))
      assert(!v.getMessage.contains("WriteToMicroBatchDataSourceV1"), "the batch is translated as its query")
      assert(!Files.exists(dir.resolve("elsewhere")))
    }
  }

  test("FAIL: under rejectUndeclaredInputs, a function joining each batch with an undeclared table") {
    val out = Files.createTempDirectory("fb-out").resolve("out").toString
    withSession(dir => enforcing(dir, out, VerificationOptions(rejectUndeclaredInputs = true))) {
      (spark, dir) =>
        val secret = dir.resolve("secret").toString
        val s = spark
        import s.implicits._
        unchecked(Seq(1L).toDF("id").write.parquet(secret))
        val failure = runStream(spark, dir)(batch => batch.join(spark.read.parquet(secret), "id").write.mode("append").parquet(out))
        val v = failure.flatMap(violationOf).getOrElse(fail(s"expected a contract violation, got $failure"))
        assert(v.result.violations.map(_.violationType) == List(ViolationType.UndeclaredInput))
    }
  }

  test("a function that writes nothing: the stream runs, and the declared output is reported never written") {
    val out = Files.createTempDirectory("fb-out").resolve("out").toString
    withSession(dir => enforcing(dir, out)) { (spark, dir) =>
      assert(runStream(spark, dir)(_ => ()).isEmpty)
      val missing = DeclaredOutputs.finishAll(spark.sparkContext).flatMap(_.result.violations.map(_.violationType))
      assert(missing == List(ViolationType.MissingOutput))
    }
  }

  test("PASS: a Delta table written inside the function") {
    val out = Files.createTempDirectory("fb-out").resolve("delta_out").toString
    withSession(dir => enforcing(dir, out)) { (spark, dir) =>
      assert(runStream(spark, dir)(_.write.format("delta").mode("append").save(out)).isEmpty)
      assert(spark.read.format("delta").load(out).count() == 2)
    }
  }

  test("dry-run drafts one contract from the function's write: the real output and the stream's source folder") {
    val drafts = ListBuffer.empty[Contract]
    val out = Files.createTempDirectory("fb-out").resolve("out").toString
    withSession(_ => ContractEnforcementRule.dryRun(c => drafts.synchronized(drafts += c))) { (spark, dir) =>
      assert(runStream(spark, dir)(_.write.mode("append").parquet(out)).isEmpty)
      val draft = drafts.synchronized(drafts.toList) match {
        case List(only) => only
        case other      => fail(s"expected one draft, got ${other.size}: ${other.map(_.outputs.map(_.location))}")
      }
      // a draft names a location as Spark does (/C:/... on Windows for the path C:\...), so compare by its end
      def same(drafted: List[String], path: String) =
        drafted.size == 1 && { val d = drafted.head.stripPrefix("file:"); val p = path.replace('\\', '/'); d == p || d.endsWith("/" + p) }
      assert(same(draft.outputs.map(_.location), out), draft.outputs.map(_.location))
      assert(same(draft.inputs.map(_.location), dir.resolve("events").toString), draft.inputs.map(_.location))
    }
  }

  test("only a stream into foreachBatch is let through as not itself a write: a console stream is still judged") {
    val out = Files.createTempDirectory("fb-out").resolve("out").toString
    withSession(dir => enforcing(dir, out)) { (spark, dir) =>
      val q = scala.util.Try(
        spark.readStream.schema("id long, value long").parquet(dir.resolve("events").toString).writeStream
          .format("console").option("checkpointLocation", dir.resolve("console-checkpoint").toString)
          .trigger(Trigger.AvailableNow()).start().awaitTermination()
      )
      val v = q.failed.toOption.flatMap(violationOf).getOrElse(fail(s"expected the console stream refused, got $q"))
      assert(v.result.violations.map(_.violationType) == List(ViolationType.OutputLocationMismatch))
      assert(!WriteCommandSupport.isForeachBatch(spark.range(1).queryExecution.analyzed))
    }
  }

  test("a non-file source inside foreachBatch (rate): its batches have no location, so it cannot satisfy a declared input") {
    // Records what happens, rather than what should: the rate source's micro-batch reads no dataset with a location, so
    // the function's write passes a contract that declares no input, and fails one that declares the source as an input.
    def firstBatchOutcome(spark: SparkSession, dir: Path, out: String): Option[Throwable] = {
      val outcome = new java.util.concurrent.atomic.AtomicReference[Option[Throwable]](null)
      val q = spark.readStream.format("rate").option("rowsPerSecond", "5").load().selectExpr("value AS id", "value")
        .writeStream
        .foreachBatch { (batch: DataFrame, _: Long) =>
          if (outcome.get == null) outcome.set(scala.util.Try(batch.write.mode("append").parquet(out)).failed.toOption)
        }
        .option("checkpointLocation", dir.resolve("rate-checkpoint").toString)
        .start()
      try {
        val deadline = System.currentTimeMillis() + 60000
        while (outcome.get == null && System.currentTimeMillis() < deadline) Thread.sleep(100)
      } finally q.stop()
      Option(outcome.get).getOrElse(fail("no batch arrived"))
    }
    val out = Files.createTempDirectory("fb-out").resolve("out").toString
    val noInputs = ContractParser.parse(
      s"""id: rate
         |version: "1.0.0"
         |outputs:
         |  - { name: out, location: $out, schema: { fields: [ { name: id, type: long }, { name: value, type: long } ] } }
         |""".stripMargin
    )
    withSession(_ => ContractEnforcementRule.forContract(noInputs)) { (spark, dir) =>
      assert(firstBatchOutcome(spark, dir, out).isEmpty, "no input declared: the write passes")
    }
    withSession(dir => enforcing(dir, out)) { (spark, dir) =>
      val types = firstBatchOutcome(spark, dir, out).flatMap(violationOf).map(_.result.violations.map(_.violationType))
      assert(types.contains(List(ViolationType.MissingInput)), s"the declared folder is not what a rate stream reads: $types")
    }
  }
}
