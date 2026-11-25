// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.verification.{Capability, CapabilityEntry, Support}
import org.scalatest.funsuite.AnyFunSuite

import java.net.{HttpURLConnection, URI}
import java.nio.charset.StandardCharsets

/** The OpenLineage judge on events made by hand: each rule it holds a run to, broken one at a time. */
class OpenLineageConformanceTest extends AnyFunSuite {

  private val scenario = Scenarios.all.find(_.id == "openlineage-run").get
  private val refusedScenario = Scenarios.all.find(_.id == "openlineage-refused").get
  private val caps = new ReferenceAdapter().capabilities
  private def declaring(s: Support) = caps.copy(entries = caps.entries.updated(Capability.LineageColumnLevel, CapabilityEntry(s)))

  private val runId = "c3cef2ba-db3b-5736-bfcc-1ec79ef4f4d7"
  private val columns = ReferenceAdapter.outputSchema(scenario.job).fields.map(_.name)
  private val inputColumn = scenario.job.inputs.head.schema.fields.head.name

  private def event(
      eventType: String,
      outputs: String = "[]",
      inputs: String = "[]",
      runFacets: String = "{}",
      namespace: String = OpenLineageConformance.Namespace,
      id: String = runId,
      time: String = "2026-11-25T10:00:00Z"
  ): String =
    s"""{"eventType":"$eventType","eventTime":"$time","producer":"p","schemaURL":"s","run":{"runId":"$id","facets":$runFacets},""" +
      s""""job":{"namespace":"$namespace","name":"j"},"inputs":$inputs,"outputs":$outputs}"""

  private def output(location: String = "/w/out/report", fields: Seq[String] = columns, status: String = "PASSED", lineage: Option[String] = None): String = {
    val schema = fields.map(f => s"""{"name":"$f"}""").mkString("[", ",", "]")
    val col = lineage.fold("")(l => s""","columnLineage":$l""")
    s"""[{"namespace":"file","name":"$location","facets":{"schema":{"fields":$schema},"invaract_contract":{"status":"$status"}$col}}]"""
  }

  private def lineageOf(name: String = "/w/in/orders", field: String = inputColumn): String =
    s"""{"fields":{"${columns.head}":{"inputFields":[{"namespace":"file","name":"$name","field":"$field"}]}}}"""

  private val inputs = """[{"namespace":"file","name":"/w/in/orders"}]"""

  private def run(running: String = event("RUNNING", output(lineage = Some(lineageOf())), inputs), end: String = event("COMPLETE")): List[String] =
    List(event("START"), running, end)

  private def judged(events: List[String], s: Scenario = scenario, support: Support = Support.Supported, refused: Boolean = false): Option[String] =
    OpenLineageConformance.judgeEvents(events.map(OpenLineageConformance.parse), s, declaring(support), refused)

  private def problem(events: List[String], s: Scenario = scenario, support: Support = Support.Supported, refused: Boolean = false): String =
    judged(events, s, support, refused).getOrElse(fail("expected a problem, got none"))

  test("a complete, faithful run holds") {
    assert(judged(run()).isEmpty)
    assert(judged(run(), support = Support.Partial).isEmpty)
    assert(judged(run(running = event("RUNNING", output(), inputs)), support = Support.Unsupported).isEmpty)
    assert(judged(run(running = event("RUNNING", output(), inputs)), support = Support.NotApplicable).isEmpty)
  }

  test("a refused write: FAILED verdict, a FAIL end with an errorMessage") {
    val running = event("RUNNING", output("/w/out/elsewhere", status = "FAILED", lineage = Some(lineageOf())), inputs, runFacets = """{"errorMessage":{"message":"m"}}""")
    assert(judged(run(running, event("FAIL")), refusedScenario, refused = true).isEmpty)
    val endOnly = event("RUNNING", output("/w/out/elsewhere", status = "FAILED", lineage = Some(lineageOf())), inputs)
    assert(judged(run(endOnly, event("FAIL", runFacets = """{"errorMessage":{"message":"m"}}""")), refusedScenario, refused = true).isEmpty, "on the end alone is enough")
    assert(problem(run(endOnly, event("FAIL")), refusedScenario, refused = true).contains("errorMessage"))
    assert(problem(run(running, event("COMPLETE")), refusedScenario, refused = true).contains("ends FAIL, but it ended COMPLETE"))
    val passedVerdict = event("RUNNING", output("/w/out/elsewhere", lineage = Some(lineageOf())), inputs, runFacets = """{"errorMessage":{}}""")
    assert(problem(run(passedVerdict, event("FAIL")), refusedScenario, refused = true).contains("FAILED, but it records PASSED"))
    assert(problem(run(end = event("FAIL"))).contains("ends COMPLETE, but it ended FAIL"))
  }

  test("nothing arrived: the endpoint was never set") {
    assert(problem(Nil).contains("no OpenLineage event arrived"))
    assert(problem(Nil).startsWith("OpenLineage: "))
  }

  test("an event missing what every run event has, a run id that is not a UUID, a time without an offset") {
    assert(problem(run(end = event("COMPLETE").replace(""""producer":"p",""", ""))).contains("missing producer"))
    assert(problem(run(end = event("COMPLETE").replace("\"name\":\"j\"", "\"name\":\"\""))).contains("missing job.name"))
    assert(problem(run(end = event("COMPLETE").replace(""""eventType":"COMPLETE",""", ""))).contains("missing eventType"))
    assert(problem(List(event("START", id = "nope"))).contains("is not a UUID"))
    assert(problem(List(event("START", time = "2026-11-25T10:00:00"))).contains("not an ISO-8601 time"))
    assert(problem(List("not json at all")).contains("missing eventType"))
  }

  test("one run: one run id, START first, one terminal event and it last") {
    assert(problem(run(end = event("COMPLETE", id = "01890b4c-bb6b-7a8c-8f5b-2a6c35dd2a0e"))).contains("2 run ids"))
    assert(problem(run().reverse).contains("first event is START"))
    assert(problem(run().init).contains("never ended"))
    assert(problem(run() :+ event("COMPLETE")).contains("one terminal event"))
    assert(problem(run().init :+ event("COMPLETE") :+ event("RUNNING")).contains("one terminal event"))
    assert(judged(run(end = event("ABORT"))).exists(_.contains("ends COMPLETE, but it ended ABORT")), "ABORT is terminal")
  }

  test("the namespace the configuration set") {
    assert(problem(run(end = event("COMPLETE", namespace = "default"))).contains("namespace 'default'"))
  }

  test("the output and inputs, named by their locations") {
    assert(problem(run(event("RUNNING", output("/w/out/other"), inputs))).contains("no output dataset is named for the location the job wrote (out/report)"))
    assert(problem(run(event("RUNNING", output(), "[]"))).contains("no input dataset is named for the location the job read (in/orders)"))
    assert(OpenLineageConformance.names("out/report", "out/report") && OpenLineageConformance.names("/w/out/report/", "./out/report/"))
    assert(OpenLineageConformance.names("db.out/report", "out/report") && OpenLineageConformance.names("C:/w/out/report", "out\\report"))
    assert(!OpenLineageConformance.names("/w/xout/report", "out/report") && !OpenLineageConformance.names("/w/out/report2", "out/report"))
  }

  test("the output's schema facet and contract verdict") {
    assert(problem(run(event("RUNNING", output(fields = columns.init, lineage = Some(lineageOf())), inputs))).contains("should list the columns written"))
    assert(judged(run(event("RUNNING", output(fields = columns.map(_.toUpperCase), lineage = Some(lineageOf())), inputs))).isEmpty, "names compare without case")
    val noSchema = output(lineage = Some(lineageOf())).replace(""""schema":{"fields":""", """"other":{"fields":""")
    assert(problem(run(event("RUNNING", noSchema, inputs))).contains("schema facet"))
    assert(problem(run(event("RUNNING", output(status = "FAILED", lineage = Some(lineageOf())), inputs))).contains("PASSED, but it records FAILED"))
    val noVerdict = output(lineage = Some(lineageOf())).replace("invaract_contract", "other")
    assert(problem(run(event("RUNNING", noVerdict, inputs))).contains("records none"))
  }

  test("column lineage exactly when declared, naming real input columns") {
    assert(problem(run(event("RUNNING", output(), inputs))).contains("declares lineage.columnLevel, but the output has no columnLineage"))
    assert(problem(run(), support = Support.Unsupported).contains("claims what it does not trace"))
    assert(problem(run(event("RUNNING", output(lineage = Some(lineageOf(name = "/w/in/other"))), inputs))).contains("not an input the job read"))
    assert(problem(run(event("RUNNING", output(lineage = Some(lineageOf(field = "nope"))), inputs))).contains("has no such column"))
    assert(judged(run(event("RUNNING", output(lineage = Some(lineageOf(field = inputColumn.toUpperCase))), inputs))).isEmpty)
  }

  test("judge waits for the terminal event, and no longer than it is told") {
    val received = new java.util.concurrent.ConcurrentLinkedQueue[String]()
    run().init.foreach(received.add)
    val t = new Thread(() => { Thread.sleep(300); received.add(event("COMPLETE")) })
    t.start()
    assert(OpenLineageConformance.judge(received, scenario, declaring(Support.Supported), refused = false, waitMs = 10000).isEmpty)
    val open = new java.util.concurrent.ConcurrentLinkedQueue[String]()
    run().init.foreach(open.add)
    val started = System.nanoTime()
    assert(OpenLineageConformance.judge(open, scenario, declaring(Support.Supported), refused = false, waitMs = 200).exists(_.contains("never ended")))
    assert((System.nanoTime() - started) / 1000000 < 5000)
  }

  test("the endpoint records what is POSTed to the OpenLineage path, and nothing else; the unreachable URL refuses") {
    val endpoint = OpenLineageConformance.endpoint()
    try {
      def send(method: String, path: String): Int = {
        val c = URI.create(endpoint.url + path).toURL.openConnection().asInstanceOf[HttpURLConnection]
        c.setRequestMethod(method)
        if (method == "POST") { c.setDoOutput(true); c.getOutputStream.write("{}".getBytes(StandardCharsets.UTF_8)) }
        try c.getResponseCode finally c.disconnect()
      }
      assert(send("POST", OpenLineageConformance.Path) == 200 && send("POST", "/elsewhere") == 404 && send("GET", OpenLineageConformance.Path) == 404)
      assert(endpoint.received.size == 1 && endpoint.received.peek() == "{}")
    } finally endpoint.server.stop(0)
    val dead = URI.create(OpenLineageConformance.unreachableUrl() + "/").toURL.openConnection().asInstanceOf[HttpURLConnection]
    assert(scala.util.Try(dead.getResponseCode).isFailure)
  }
}
