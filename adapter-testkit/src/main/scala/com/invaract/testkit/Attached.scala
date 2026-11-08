// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

import com.invaract.contract.{Contract, ContractParser, ContractVersion}
import com.invaract.verification.{AdapterCapabilities, InvaractConf}
import com.invaract.verification.location.{ContractLocationResolution, LocationRef}
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.yaml.snakeyaml.{LoaderOptions, Yaml}
import org.yaml.snakeyaml.constructor.SafeConstructor

import java.net.{HttpURLConnection, InetSocketAddress, URI, URLEncoder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.{Properties => JProperties}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/** Runs a scenario whose job is attached through configuration (`Scenario.attach`): the kit stands up what the attachment
  * points at - a contract registry, a location service, the contract and location map files - gives the adapter the settings
  * that name them (`ConformanceAdapter.attached`), and reads the run's verdict from the validation events the file sink
  * those settings configure wrote. Nothing is handed to the adapter in code but the settings, so a scenario shows the path
  * a platform's configuration really takes: a registry client missing from the shipped form, a setting the engine never
  * reads, or a registry's own error lost on the way to the job all show up as a divergence.
  */
object Attached {

  /** The verdict on `scenario` (which has an `attach`) for `adapter`, held to `expected`. */
  def judge(adapter: ConformanceAdapter, capabilities: AdapterCapabilities, scenario: Scenario, expected: Conformance.Expected): ScenarioVerdict = {
    val attachment = scenario.attach.getOrElse(throw new IllegalArgumentException(s"scenario '${scenario.id}' is not attached through configuration"))
    val root = Files.createTempDirectory(s"invaract-attach-${scenario.id}-")
    val events = root.resolve("events.jsonl")
    val sink = write(root, "sink.properties", s"sink.enabled=true\nsink.class=com.invaract.verification.notification.FileNotificationSink\nsink.property.path=${slashes(events)}\n")
    val (services, settings, resolved) = prepare(attachment, scenario.contract, root, adapter.bundlesRegistryClient)
    try {
      adapter.attached(scenario.id, resolved, scenario.job, settings + (InvaractConf.NotifyConfig -> slashes(sink))) match {
        case None =>
          ScenarioVerdict.Diverges(
            s"the adapter declares ${scenario.focus.map(_.id).toList.sorted.mkString(", ")}, which only a job attached through the engine's own " +
              "configuration can show, but it cannot run one (ConformanceAdapter.attached)"
          )
        case Some(AttachedRun.Stopped(message)) =>
          expected match {
            case Conformance.Expected.Stopped(mentioning) =>
              val missing = mentioning.filterNot(message.contains)
              if (missing.isEmpty) ScenarioVerdict.Conforms
              else ScenarioVerdict.Diverges(s"Invaract stopped the job, but its message does not name ${show(missing)}: '$message'")
            case _ => ScenarioVerdict.Diverges(s"Invaract stopped the job before it ran: '$message'")
          }
        case Some(AttachedRun.Ran) =>
          expected match {
            case Conformance.Expected.Stopped(mentioning) =>
              ScenarioVerdict.Diverges(s"expected Invaract to stop the job before it ran, naming ${show(mentioning)}, but the job ran")
            case other => Conformance.judge(other, outcomeOf(events), capabilities, None, Analysis(), scenario.job.writes)
          }
      }
    } finally {
      services.foreach(_.stop(0))
      deleteQuietly(root)
    }
  }

  /** The services `attachment` needs, the settings that name them, and the contract the settings lead to (its `ref://`
    * locations resolved), for an adapter that names its datasets by the contract's names. */
  private def prepare(attachment: Attachment, contract: Contract, root: Path, bundlesClient: Boolean): (List[HttpServer], Map[String, String], Contract) =
    attachment match {
      case Attachment.Registry(held) =>
        val path = s"/contracts/${encode(contract.id)}/versions/${encode(contract.version.toString)}"
        val registry = serve(if (held) Map(path -> (200 -> ContractParser.write(contract))) else Map.empty)
        val client = if (bundlesClient) Map.empty else Map(InvaractConf.RegistryClientClass -> classOf[KitRegistryClient].getName)
        val settings = Map(InvaractConf.Contract -> s"registry://${contract.id}@${contract.version}", InvaractConf.RegistryUrl -> url(registry)) ++ client
        (List(registry), settings, contract)
      case Attachment.LocationMap(locations) =>
        val map = new JProperties()
        locations.foreach { case (id, location) => map.setProperty(id, location) }
        val file = root.resolve("locations.properties")
        val out = Files.newOutputStream(file)
        try map.store(out, null)
        finally out.close()
        val settings = Map(InvaractConf.Contract -> slashes(contractFile(root, contract)), InvaractConf.LocationMap -> slashes(file))
        (Nil, settings, resolve(contract, locations))
      case Attachment.LocationService(locations) =>
        val service = serve(locations.map { case (id, location) => s"/locations/${encode(id)}" -> (200 -> json(location)) })
        val settings = Map(InvaractConf.Contract -> slashes(contractFile(root, contract)), InvaractConf.LocationUrl -> url(service))
        (List(service), settings, resolve(contract, locations))
    }

  /** The verdict the validation events in `events` record: blocked with the violations of the FAILED one, else allowed. */
  private[testkit] def outcomeOf(events: Path): ScenarioOutcome = {
    val validations =
      if (!Files.exists(events)) Nil
      else
        Files.readAllLines(events, StandardCharsets.UTF_8).asScala.toList.filter(_.trim.nonEmpty).map(parse)
          .filter(_.get("eventType").contains("CONTRACT_VALIDATION"))
    val statuses = validations.map(_.get("status").fold("")(String.valueOf))
    val engines = validations.map(v => mapOf(v.get("job").orNull).get("engine").map(String.valueOf))
    validations.find(_.get("status").contains("FAILED")) match {
      case Some(failed) =>
        val types = listOf(failed.get("violations").orNull).map(v => String.valueOf(mapOf(v).getOrElse("type", ""))).toSet
        ScenarioOutcome.Rejected(types, statuses, eventEngines = engines)
      case None => ScenarioOutcome.Passed(statuses, eventEngines = engines)
    }
  }

  private def resolve(contract: Contract, locations: Map[String, String]): Contract =
    ContractLocationResolution.resolve(contract, id => locations.getOrElse(id, LocationRef.Scheme + id))

  private def contractFile(root: Path, contract: Contract): Path = write(root, "contract.yaml", ContractParser.write(contract))

  /** A server on a free local port answering each path in `answers` with its `(status, body)`, and 404 for any other. */
  private def serve(answers: Map[String, (Int, String)]): HttpServer = {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/", (exchange: HttpExchange) => {
      val (status, body) = answers.getOrElse(exchange.getRequestURI.getRawPath, 404 -> "")
      val bytes = body.getBytes(StandardCharsets.UTF_8)
      exchange.sendResponseHeaders(status, if (bytes.isEmpty) -1 else bytes.length.toLong)
      if (bytes.nonEmpty) exchange.getResponseBody.write(bytes)
      exchange.close()
    })
    server.start()
    server
  }

  private def url(server: HttpServer): String = s"http://127.0.0.1:${server.getAddress.getPort}"

  private def json(location: String): String =
    "{\"location\": \"" + location.flatMap {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case c    => c.toString
    } + "\"}"

  private[testkit] def encode(segment: String): String = URLEncoder.encode(segment, StandardCharsets.UTF_8.name).replace("+", "%20")

  private def write(root: Path, name: String, text: String): Path = Files.write(root.resolve(name), text.getBytes(StandardCharsets.UTF_8))

  /** A path as a setting names it: forward slashes, which every engine and a `.properties` file read the same way. */
  private def slashes(path: Path): String = path.toAbsolutePath.toString.replace('\\', '/')

  private def parse(line: String): Map[String, Any] = mapOf(new Yaml(new SafeConstructor(new LoaderOptions)).load[AnyRef](line))

  private def mapOf(value: Any): Map[String, Any] = value match {
    case m: java.util.Map[_, _] => m.asScala.map { case (k, v) => String.valueOf(k) -> (v: Any) }.toMap
    case _                      => Map.empty
  }

  private def listOf(value: Any): List[Any] = value match {
    case l: java.util.List[_] => l.asScala.toList
    case _                    => Nil
  }

  private def show(names: Set[String]): String = names.toList.sorted.map(n => s"'$n'").mkString(", ")

  private def deleteQuietly(root: Path): Unit =
    try Files.walk(root).iterator().asScala.toList.reverse.foreach(p => Files.deleteIfExists(p))
    catch { case NonFatal(_) => () }
}

/** The kit's own contract registry client, for an adapter whose users add a registry client themselves
  * (`ConformanceAdapter.bundlesRegistryClient` `false`): it speaks the registry's protocol (`GET <url>/contracts/<id>/versions/
  * <version>`, the contract's YAML in the body) as `registry-client`'s `HttpContractRegistryClient` does, and is named with
  * `registryClientClass` as such a user names theirs. A contract the registry does not hold fails with the same message.
  */
final class KitRegistryClient {
  private var base = ""

  def configure(registryUrl: String): Unit = base = registryUrl.trim.stripSuffix("/")

  def get(contractId: String, version: ContractVersion): Contract = fetch(contractId, version.toString)

  def getLatest(contractId: String): Contract = fetch(contractId, "latest")

  private def fetch(contractId: String, version: String): Contract = {
    val connection = URI.create(s"$base/contracts/${Attached.encode(contractId)}/versions/${Attached.encode(version)}").toURL.openConnection()
      .asInstanceOf[HttpURLConnection]
    try connection.getResponseCode match {
      case 200 => ContractParser.parse(new String(connection.getInputStream.readAllBytes(), StandardCharsets.UTF_8))
      case 404 => throw new IllegalStateException(s"No contract registered for id '$contractId' at version '$version'")
      case other => throw new IllegalStateException(s"The contract registry answered HTTP $other for '$contractId' at version '$version'")
    } finally connection.disconnect()
  }
}
