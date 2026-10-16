// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.Contract
import com.invaract.ir.UnknownPlan
import com.invaract.verification.{ContractViolationException, InvaractConf, VerificationPipeline}
import com.invaract.verification.notification.{JobInfo, NotificationSink}
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/** Spark's half of `check.outputsWritten`: a declared output the job never writes (docs/SPARK_ADAPTER.md, "Declared outputs
  * never written").
  *
  * Spark checks each write as it runs, from that write's own plan, and builds no plan of the whole job: when one write is
  * checked, a later one has no plan yet, and the driver code may never get to it. So whether every declared output was
  * written is known only when the job ends. Each write the enforcement rule checks is recorded here, and when the
  * application ends a declared output none of them landed on is reported - one `MISSING_OUTPUT` each, in a FAILED
  * validation event and in the driver's log. The writes have happened by then; nothing is blocked. With
  * `spark.invaract.failOnMissingOutput=true` the driver also exits with status `ExitStatus`, so whatever runs the job (a
  * scheduler, a CI step) sees it fail and does not go on to what depends on it.
  *
  * One record per application and contract, shared by every session that enforces that contract, so a job that writes
  * through two sessions is judged on both.
  */
private[sparkadapter] final class DeclaredOutputs(
    contract: Contract,
    sink: Option[NotificationSink],
    applicationId: Option[String],
    job: Option[JobInfo]
) {
  private val written = new ConcurrentLinkedQueue[String]()
  private var outcome: Option[Option[ContractViolationException]] = None

  /** A write the enforcement rule checked, wherever it landed. */
  def record(location: String): Unit = written.add(location)

  /** The locations recorded so far, each once, in the order first written. */
  def locations: List[String] = written.asScala.toList.distinct

  /** Whether every declared output was written, decided once: the first call publishes the FAILED validation (when one
    * is due) and later calls return the same answer without publishing again. */
  def finish(): Option[ContractViolationException] = synchronized {
    if (outcome.isEmpty) outcome = Some(check())
    outcome.get
  }

  private def check(): Option[ContractViolationException] = {
    val writes = locations
    try {
      VerificationPipeline.requireOutputsWritten(
        contract, writes, UnknownPlan(s"the job's writes: ${if (writes.isEmpty) "none" else writes.mkString(", ")}", "SparkApplication"), sink, applicationId, job
      )
      None
    } catch { case e: ContractViolationException => Some(e) }
  }
}

private[sparkadapter] object DeclaredOutputs {
  private val logger = LoggerFactory.getLogger(classOf[DeclaredOutputs])

  /** The driver's exit status with `spark.invaract.failOnMissingOutput=true` when a declared output was never written. */
  val ExitStatus = 1

  /** Runs after Spark's own shutdown (its hooks run at 40 and the file systems close at 10, highest first). */
  private val ShutdownHookPriority = 0

  private final case class Attached(outputs: DeclaredOutputs, listener: SparkListener, hook: Option[Runnable])

  private val byJob = new ConcurrentHashMap[(SparkContext, Contract), Attached]()

  /** The record for `session`'s application and `contract`, created and attached on first use: a listener reports at
    * application end, and with `failOnMissingOutput` a shutdown hook turns a report into the exit status. */
  def forJob(
      session: SparkSession,
      contract: Contract,
      sink: Option[NotificationSink],
      applicationId: Option[String],
      job: Option[JobInfo],
      halt: Int => Unit = status => Runtime.getRuntime.halt(status)
  ): DeclaredOutputs =
    byJob.computeIfAbsent((session.sparkContext, contract), _ => attach(session, new DeclaredOutputs(contract, sink, applicationId, job), halt)).outputs

  private def attach(session: SparkSession, outputs: DeclaredOutputs, halt: Int => Unit): Attached = {
    val listener = new SparkListener {
      override def onApplicationEnd(end: SparkListenerApplicationEnd): Unit = report(outputs)
    }
    session.sparkContext.addSparkListener(listener)
    val hook = if (failOnMissingOutput(session)) Some(exitHook(outputs, halt)) else None
    hook.foreach(h => org.apache.hadoop.util.ShutdownHookManager.get().addShutdownHook(h, ShutdownHookPriority))
    Attached(outputs, listener, hook)
  }

  /** At shutdown, after Spark has stopped: the application-end report (made now if it never came) decides the exit status. */
  private[sparkadapter] def exitHook(outputs: DeclaredOutputs, halt: Int => Unit): Runnable = () =>
    if (report(outputs).isDefined) {
      logger.error(s"Invaract: exiting with status $ExitStatus because the job never wrote an output its contract declares (spark.invaract.failOnMissingOutput)")
      halt(ExitStatus)
    }

  private def failOnMissingOutput(session: SparkSession): Boolean =
    SparkConfigSource(session).get(InvaractConf.FailOnMissingOutput).exists(_.trim.equalsIgnoreCase("true"))

  /** Decides `outputs` (once) and logs a declared output never written; never throws into Spark's listener bus. */
  private[sparkadapter] def report(outputs: DeclaredOutputs): Option[ContractViolationException] =
    try {
      val missing = outputs.finish()
      missing.foreach(e => logger.error(s"Invaract: ${e.getMessage}"))
      missing
    } catch {
      case NonFatal(e) =>
        logger.warn(s"Invaract could not check that every declared output was written: $e")
        None
    }

  /** Ends the job for every contract `sc` enforces, as application end does, and forgets them: what a caller running
    * several jobs on one application (the conformance kit) uses between them. */
  def finishAll(sc: SparkContext): List[ContractViolationException] = detach(sc).flatMap(a => report(a.outputs))

  /** Whether an exit hook waits on any contract `sc` enforces (`failOnMissingOutput` was set). */
  private[sparkadapter] def exitHookAttached(sc: SparkContext): Boolean =
    byJob.asScala.exists { case ((context, _), attached) => (context eq sc) && attached.hook.isDefined }

  /** Forgets every contract `sc` enforces without judging it (a job that already failed). */
  def discardAll(sc: SparkContext): Unit = detach(sc)

  private def detach(sc: SparkContext): List[Attached] = {
    val keys = byJob.keySet.asScala.filter(_._1 eq sc).toList
    keys.flatMap(k => Option(byJob.remove(k))).map { a =>
      sc.removeSparkListener(a.listener)
      a.hook.foreach(h => org.apache.hadoop.util.ShutdownHookManager.get().removeShutdownHook(h))
      a
    }
  }
}
