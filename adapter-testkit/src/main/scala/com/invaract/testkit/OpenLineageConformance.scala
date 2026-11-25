// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.verification.{AdapterCapabilities, Capability, Support}
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.yaml.snakeyaml.{LoaderOptions, Yaml}
import org.yaml.snakeyaml.constructor.SafeConstructor

import java.net.{InetSocketAddress, ServerSocket}
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._
import scala.util.Try

/** Judges what an adapter publishes to OpenLineage (`reporting.openLineage`) for a job attached through configuration with
  * `openLineageUrl` set to an endpoint the kit runs (`Attachment.OpenLineage`).
  *
  * The events themselves are built by `verification-core`'s OpenLineage sink, whose tests hold them to the official
  * OpenLineage JSON Schema; what an adapter can get wrong is what it gives that sink, and whether it installs it at all. So
  * this holds the run to what the job really did: one run, `START` first and one terminal event last (the adapter ending
  * the run, `NotificationSink.endRun`, when the job is done), the namespace the configuration set, the job's inputs and
  * output named by their locations, the output's schema facet equal to the columns written, the contract's verdict on
  * the output (`invaract_contract`), a refused write ending the run `FAIL` with its `errorMessage`, and a `columnLineage`
  * facet exactly when the adapter declares `lineage.columnLevel` supported or partial, naming input columns that exist.
  */
object OpenLineageConformance {

  /** The namespace the kit's configuration sets (`openLineageNamespace`): the events must carry it. */
  val Namespace = "conformance"

  /** The path the kit's endpoint is reached at: the OpenLineage default (`api/v1/lineage` under `openLineageUrl`). */
  val Path = "/api/v1/lineage"

  /** How long the kit waits for the run's terminal event once the job has returned. The kit's own tests shorten it for an
    * adapter that never ends its run. */
  @volatile private[testkit] var WaitMs: Long = 30000L

  private val Terminal = Set("COMPLETE", "FAIL", "ABORT")

  /** An endpoint on a free local port that records the body of every event POSTed to it. */
  final class Endpoint private[OpenLineageConformance] (val server: HttpServer, val received: ConcurrentLinkedQueue[String]) {
    def url: String = s"http://127.0.0.1:${server.getAddress.getPort}"
  }

  def endpoint(): Endpoint = {
    val received = new ConcurrentLinkedQueue[String]()
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/", (exchange: HttpExchange) => {
      val body = new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
      val ok = exchange.getRequestMethod == "POST" && exchange.getRequestURI.getPath == Path
      if (ok) received.add(body)
      exchange.sendResponseHeaders(if (ok) 200 else 404, -1)
      exchange.close()
    })
    server.start()
    new Endpoint(server, received)
  }

  /** A URL nothing answers at: a port that was free a moment ago. */
  def unreachableUrl(): String = {
    val socket = new ServerSocket(0)
    val port = try socket.getLocalPort finally socket.close()
    s"http://127.0.0.1:$port"
  }

  /** Waits (at most `waitMs`) for a terminal event among `received`, then judges them: `None` when they hold, else what
    * is wrong. `refused`: the write was expected to be blocked. */
  def judge(
      received: ConcurrentLinkedQueue[String],
      scenario: Scenario,
      capabilities: AdapterCapabilities,
      refused: Boolean,
      waitMs: Long = WaitMs
  ): Option[String] = {
    val deadline = System.nanoTime() + waitMs * 1000000L
    def events = received.asScala.toList.map(parse)
    while (!events.exists(e => Terminal(str(e, "eventType"))) && System.nanoTime() < deadline) Thread.sleep(50)
    judgeEvents(events, scenario, capabilities, refused)
  }

  /** The judgement on events already received, in arrival order. */
  private[testkit] def judgeEvents(events: List[Map[String, Any]], scenario: Scenario, capabilities: AdapterCapabilities, refused: Boolean): Option[String] = {
    val job = scenario.job
    val types = events.map(str(_, "eventType"))
    val runIds = events.map(e => str(map(e.get("run")), "runId")).distinct
    val outputs = events.flatMap(e => list(e.get("outputs")).map(map))
    val inputs = events.flatMap(e => list(e.get("inputs")).map(map))
    val written = outputs.filter(d => names(str(d, "name"), job.output.location))
    val facetsOfWritten = written.map(d => map(d.get("facets")))
    val traces = Set[Support](Support.Supported, Support.Partial).contains(capabilities.supportOf(Capability.LineageColumnLevel))
    val terminal = events.lastOption.filter(e => Terminal(str(e, "eventType")))

    val checks: List[() => Option[String]] = List(
      () => when(events.isEmpty)(
        s"no OpenLineage event arrived at the endpoint openLineageUrl names: the adapter does not set openLineageUrl on the engine, or does not install the sink AttachSetup builds"
      ),
      () => events.flatMap(malformed).headOption,
      () => when(runIds.size != 1)(s"one job is one OpenLineage run, but the events name ${runIds.size} run ids (${runIds.mkString(", ")})"),
      () => when(types.headOption != Some("START"))(s"a run's first event is START, but the events were ${types.mkString(", ")}"),
      () => when(types.count(Terminal) == 0)(
        s"the run was never ended: the events were ${types.mkString(", ")}, with no COMPLETE or FAIL within ${WaitMs / 1000} seconds of the job returning. " +
          "The adapter must call NotificationSink.endRun on its sink when the job is done"
      ),
      () => when(types.count(Terminal) > 1 || terminal.isEmpty)(s"a run has one terminal event, and it is the last, but the events were ${types.mkString(", ")}"),
      () => events.map(e => str(map(e.get("job")), "namespace")).find(_ != Namespace).map(ns =>
        s"the events are in namespace '$ns', not '$Namespace' as openLineageNamespace says: the setting does not reach the adapter"
      ),
      () => when(written.isEmpty)(
        s"no output dataset is named for the location the job wrote (${job.output.location}); outputs named: ${outputs.map(str(_, "name")).distinct.mkString(", ")}"
      ),
      () => job.inputs.map(_.location).find(l => !inputs.exists(d => names(str(d, "name"), l))).map(l =>
        s"no input dataset is named for the location the job read ($l); inputs named: ${inputs.map(str(_, "name")).distinct.mkString(", ")}"
      ),
      () => {
        val expected = ReferenceAdapter.outputSchema(job).fields.map(_.name.toLowerCase)
        val schemas = facetsOfWritten.flatMap(f => f.get("schema")).map(s => list(map(s).get("fields")).map(f => str(map(f), "name").toLowerCase))
        when(!schemas.contains(expected))(
          s"the output's schema facet should list the columns written, ${expected.mkString("[", ", ", "]")}, but it lists ${schemas.map(_.mkString("[", ", ", "]")).mkString(" and ")}"
        )
      },
      () => {
        val verdicts = facetsOfWritten.flatMap(_.get("invaract_contract")).map(c => str(map(c), "status"))
        val want = if (refused) "FAILED" else "PASSED"
        when(!verdicts.contains(want))(s"the output's invaract_contract facet should record the contract's verdict, $want, but it records ${if (verdicts.isEmpty) "none" else verdicts.mkString(", ")}")
      },
      () => {
        val want = if (refused) "FAIL" else "COMPLETE"
        val got = terminal.map(str(_, "eventType")).getOrElse("")
        when(got != want)(s"a run whose write was ${if (refused) "refused" else "allowed"} ends $want, but it ended $got")
      },
      () => when(refused && !terminal.exists(e => map(map(e.get("run")).get("facets")).contains("errorMessage")) &&
        !events.exists(e => map(map(e.get("run")).get("facets")).contains("errorMessage")))(
        "a refused write's run carries an errorMessage facet saying why, but none does"
      ),
      () => {
        val lineage = facetsOfWritten.flatMap(_.get("columnLineage")).map(map)
        if (!traces) when(lineage.nonEmpty)("the output has a columnLineage facet, but the adapter declares lineage.columnLevel unsupported: it claims what it does not trace")
        else if (lineage.isEmpty) Some("the adapter declares lineage.columnLevel, but the output has no columnLineage facet")
        else lineage.flatMap(l => map(l.get("fields")).values.flatMap(f => list(map(f).get("inputFields")).map(map))).flatMap { field =>
          val source = job.inputs.find(i => names(str(field, "name"), i.location))
          val column = str(field, "field")
          if (source.isEmpty) Some(s"a columnLineage edge names '${str(field, "name")}', which is not an input the job read")
          else when(!source.get.schema.fields.exists(_.name.equalsIgnoreCase(column)))(s"a columnLineage edge names column '$column' of ${source.get.location}, which has no such column")
        }.headOption
      }
    )
    checks.iterator.map(_()).collectFirst { case Some(problem) => s"OpenLineage: $problem" }
  }

  /** What is wrong with one event's required fields (`RunEvent` in the OpenLineage spec), if anything. */
  private def malformed(event: Map[String, Any]): Option[String] = {
    val run = map(event.get("run"))
    val jobOf = map(event.get("job"))
    val missing = List("eventType" -> event, "eventTime" -> event, "producer" -> event, "schemaURL" -> event).collect {
      case (k, m) if str(m, k).isEmpty => k
    } ++ List("run.runId" -> str(run, "runId"), "job.namespace" -> str(jobOf, "namespace"), "job.name" -> str(jobOf, "name")).collect {
      case (k, v) if v.isEmpty => k
    }
    if (missing.nonEmpty) Some(s"an event is missing ${missing.mkString(", ")}")
    else if (Try(UUID.fromString(str(run, "runId"))).isFailure) Some(s"run id '${str(run, "runId")}' is not a UUID")
    else if (Try(java.time.OffsetDateTime.parse(str(event, "eventTime"))).isFailure) Some(s"eventTime '${str(event, "eventTime")}' is not an ISO-8601 time with an offset")
    else None
  }

  /** Whether a dataset name stands for `location`: the name is the location itself, made absolute and normalized by the
    * naming conventions, so it ends with the location's own path. */
  private[testkit] def names(name: String, location: String): Boolean = {
    val path = location.replace('\\', '/').stripPrefix("./").stripSuffix("/")
    val n = name.stripSuffix("/")
    n == path || n.endsWith("/" + path) || n.endsWith("." + path)
  }

  private def when(condition: Boolean)(problem: => String): Option[String] = if (condition) Some(problem) else None

  private val yaml = new ThreadLocal[Yaml] { override def initialValue(): Yaml = new Yaml(new SafeConstructor(new LoaderOptions)) }

  private[testkit] def parse(json: String): Map[String, Any] = Try(map(yaml.get().load[AnyRef](json))).getOrElse(Map.empty)

  private def str(m: Map[String, Any], key: String): String = m.get(key).collect { case s: String => s }.getOrElse("")

  private def map(value: Any): Map[String, Any] = value match {
    case Some(v)                => map(v)
    case m: java.util.Map[_, _] => m.asScala.map { case (k, v) => String.valueOf(k) -> (v: Any) }.toMap
    case _                      => Map.empty
  }

  private def list(value: Any): List[Any] = value match {
    case Some(v)              => list(v)
    case l: java.util.List[_] => l.asScala.toList
    case _                    => Nil
  }
}
