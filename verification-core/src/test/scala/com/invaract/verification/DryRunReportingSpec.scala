// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{Contract, ContractVersion, Dataset, Field, Schema}
import com.invaract.verification.notification.{
  ContractInferenceEvent, DryRunSummaryEvent, InferenceStatus, JobInfo, MergeStatus, NotificationEvent, NotificationSink, TestNotificationSink
}
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.Logger

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import scala.collection.mutable

/** How an adapter that judges writes as it sees them reports its dry run (`DryRunReporting`): published to the sink, or logged
  * when there is none, and a sink that cannot flush never fails the run. */
class DryRunReportingSpec extends AnyFunSuite {

  /** A logger that records each `info` and `warn` message, as `level: message`. */
  private final class RecordingLogger {
    val lines: mutable.Buffer[String] = mutable.Buffer.empty
    val logger: Logger = Proxy
      .newProxyInstance(
        getClass.getClassLoader,
        Array(classOf[Logger]),
        new InvocationHandler {
          def invoke(proxy: AnyRef, method: Method, args: Array[AnyRef]): AnyRef = {
            if (Set("info", "warn").contains(method.getName) && args != null && args.length == 1) lines += s"${method.getName}: ${args(0)}"
            if (method.getReturnType == java.lang.Boolean.TYPE) java.lang.Boolean.TRUE else null
          }
        }
      )
      .asInstanceOf[Logger]
  }

  private val job = JobInfo(name = Some("orders_job.main"), engine = Some("toy"))

  private def draft(output: String): Contract =
    Contract(
      "inferred_contract",
      ContractVersion(0, 1, 0),
      "draft",
      List(Dataset("input", "in/orders", None, Schema(List(Field("id", "long", required = true, nullable = false))))),
      List(Dataset("output", output, Some("parquet"), Schema(List(Field("id", "long", required = true, nullable = false))), derivedFrom = Some(List("input")))),
      Nil,
      Map.empty
    )

  private val selfCheck = ("PASSED", List.empty[Violation], List("checked against its own write"))

  test("inference: one ContractInferenceEvent carrying the outcome, the draft and its self-check, and nothing logged") {
    val sink = new TestNotificationSink
    val log = new RecordingLogger
    DryRunReporting.inference(Some(sink), log.logger, job, InferenceStatus.Inferred, None, Some("out/a"), Some("id: inferred_contract"), Some(selfCheck))
    val e = sink.events.collect { case e: ContractInferenceEvent => e }
    assert(e.size == 1 && sink.events.size == 1)
    assert((e.head.status, e.head.reason, e.head.writeLocation, e.head.contractYaml) == ((InferenceStatus.Inferred, None, Some("out/a"), Some("id: inferred_contract"))))
    assert((e.head.selfCheck, e.head.selfCheckViolations, e.head.diagnostics) == ((Some("PASSED"), Nil, List("checked against its own write"))))
    assert(e.head.job == job)
    assert(log.lines.isEmpty)
  }

  test("inference: with no self-check the event says none") {
    val sink = new TestNotificationSink
    DryRunReporting.inference(Some(sink), new RecordingLogger().logger, job, InferenceStatus.SkippedUnrecognized, Some("why"), None, None, None)
    val e = sink.events.collect { case e: ContractInferenceEvent => e }.head
    assert((e.status, e.reason, e.writeLocation, e.contractYaml, e.selfCheck, e.selfCheckViolations, e.diagnostics) ==
      ((InferenceStatus.SkippedUnrecognized, Some("why"), None, None, None, Nil, Nil)))
  }

  test("inference: with no sink the outcome is logged at INFO, the draft when there is one, else the reason") {
    val log = new RecordingLogger
    DryRunReporting.inference(None, log.logger, job, InferenceStatus.Inferred, Some("ignored"), Some("out/a"), Some("id: x"), None)
    DryRunReporting.inference(None, log.logger, job, InferenceStatus.SkippedUnrecognized, Some("a table with no stream"), Some("out/b"), None, None)
    DryRunReporting.inference(None, log.logger, job, InferenceStatus.InferenceError, None, None, None, None)
    assert(log.lines.toList == List(
      "info: Invaract dry run: INFERRED out/a\nid: x",
      "info: Invaract dry run: SKIPPED_UNRECOGNIZED out/b - a table with no stream",
      "info: Invaract dry run: INFERENCE_ERROR "
    ))
  }

  test("summary: one DryRunSummaryEvent with each status counted and every draft merged, and nothing logged") {
    val sink = new TestNotificationSink
    val log = new RecordingLogger
    val outcomes = List(
      InferenceStatus.Inferred -> Some(draft("out/a")),
      InferenceStatus.SkippedUnrecognized -> None,
      InferenceStatus.Inferred -> Some(draft("out/b"))
    )
    DryRunReporting.summary(Some(sink), log.logger, job, outcomes)
    val e = sink.events.collect { case e: DryRunSummaryEvent => e }
    assert(e.size == 1 && sink.events.size == 1)
    assert(e.head.statusCounts == Map(InferenceStatus.Inferred -> 2L, InferenceStatus.SkippedUnrecognized -> 1L))
    assert(e.head.mergeStatus == MergeStatus.Merged)
    assert(e.head.mergedContractYaml.exists(y => y.contains("out/a") && y.contains("out/b")))
    assert(e.head.job == job)
    assert(log.lines.isEmpty)
  }

  test("summary: with no sink the counts, in order, and the merged draft are logged at INFO") {
    val log = new RecordingLogger
    DryRunReporting.summary(None, log.logger, job, List(InferenceStatus.SkippedUnrecognized -> None, InferenceStatus.Inferred -> Some(draft("out/a"))))
    assert(log.lines.size == 1)
    val line = log.lines.head
    assert(line.startsWith("info: Invaract dry run summary: INFERRED=1, SKIPPED_UNRECOGNIZED=1; merge MERGED\n"))
    assert(line.contains("out/a"))
  }

  test("summary: with no sink and no draft the log says so, with no contract") {
    val log = new RecordingLogger
    DryRunReporting.summary(None, log.logger, job, Nil)
    assert(log.lines.toList == List(s"info: Invaract dry run summary: ${InferenceStatus.NoWritesObserved}=1; merge ${MergeStatus.NoDrafts}"))
  }

  test("flush: waits on the sink for the given time; a sink that cannot flush is logged, never thrown") {
    var waited = List.empty[Long]
    val patient = new NotificationSink {
      def publish(event: NotificationEvent): Unit = ()
      override def flush(timeoutMs: Long): Unit = waited :+= timeoutMs
    }
    val log = new RecordingLogger
    DryRunReporting.flush(Some(patient), log.logger, 1234L)
    assert(waited == List(1234L) && log.lines.isEmpty)
    val broken = new NotificationSink {
      def publish(event: NotificationEvent): Unit = ()
      override def flush(timeoutMs: Long): Unit = throw new IllegalStateException("endpoint gone")
    }
    DryRunReporting.flush(Some(broken), log.logger, 10L)
    assert(log.lines.toList == List("warn: Invaract could not flush its notification sink: java.lang.IllegalStateException: endpoint gone"))
    DryRunReporting.flush(None, log.logger, 10L)
    assert(log.lines.size == 1)
  }
}
