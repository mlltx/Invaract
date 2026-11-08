// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import org.scalatest.funsuite.AnyFunSuite
import org.yaml.snakeyaml.{LoaderOptions, Yaml}
import org.yaml.snakeyaml.constructor.SafeConstructor

import com.sun.net.httpserver.HttpServer

import java.io.{BufferedReader, ByteArrayOutputStream, File, PrintStream, StringReader}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._

/** The checker's recorded protocol cases (`src/test/resources/protocol/<case>/`), run through `PythonIoCheck.serve` inside the
  * JVM. `native/cases.py` runs the same cases against the native program built from the checker (milestone 0,
  * docs/PYTHON_IO_ADAPTER_DESIGN.md), so the two are held to the same replies.
  *
  * A case is copied to a fresh directory, and `${CASE}` in any of its files becomes that directory; its `requests.jsonl` is
  * answered line by line, and each reply must equal the one in `replies.jsonl` (compared as JSON, with the directory written
  * back as `${CASE}`). When the case has an `events.txt`, the file sink it configures must have written those event types, in
  * that order, to `events.jsonl`, each as its type and what it concluded (`CONTRACT_VALIDATION FAILED`); when it has an
  * `http-events.txt`, its HTTP sink must have posted those, in any order (it delivers asynchronously), to `${HTTP}`, a local
  * endpoint the run provides.
  */
class PythonIoProtocolCasesSpec extends AnyFunSuite {
  import PythonIoProtocolCasesSpec._

  test("every case directory holds the requests and the replies it is checked against") {
    assert(cases.size >= 20)
    cases.foreach(c => assert(new File(c, "requests.jsonl").isFile && new File(c, "replies.jsonl").isFile, c.getName))
  }

  cases.foreach(c => test(s"protocol case: ${c.getName}") { assertCase(c.getName); () })

  test(OrganizationalPolicy) {
    // a contract that breaks the policy is refused when the run opens; one that keeps it opens and is checked
    assertCase("org-policy-refused")
    assertCase("org-policy-satisfied")
  }

  test(LocationRefs) {
    // the contract the run opens on names the resolved location, not the ref:// one
    assert(assertCase("location-refs").replies.head.toString.contains("s3://lake/reports/orders"))
  }

  test(ContractRegistry)(assertCase("registry"))

  test(WriteEvents) {
    val events = assertCase("published-write-events").eventObjects.filter(e => Set("WRITE", "JOB_SUMMARY")(String.valueOf(e.get("eventType"))))
    val List(report, snapshot, summary) = events
    // one event per output, however many writes staged it, with what the package says it put in place
    assert(report.get("location") == "data/out/report" && report.get("rowCount") == 12 && report.get("fileCount") == 3 &&
      report.get("bytesWritten") == 4096 && report.get("durationMs") == 41 && report.get("icebergSnapshotId") == null)
    assert(report.get("partitionColumns").toString == "[region]" && report.get("datasetType") == "DATA_ASSET" &&
      report.get("contract") == "orders_report@1.0.0" && report.get("saveMode") == "overwrite" && report.get("format") == "parquet")
    // the columns as staged: a required column that held no empty value is written not-null
    val columns = report.get("schema").asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]].asScala.map(c => (c.get("name"), c.get("type"), c.get("nullable")))
    // a column nullable in any write of the output is nullable in its event: `note` is not in the first write, and is in the second
    assert(columns == List(("region", "string", false), ("total", "long", false), ("note", "string", true)))
    val catalog = snapshot.get("catalog").asInstanceOf[java.util.Map[String, AnyRef]]
    assert(snapshot.get("icebergSnapshotId") == 3051729675574597004L && snapshot.get("fileCount") == null &&
      (catalog.get("technology"), catalog.get("catalogName"), catalog.get("namespace").toString, catalog.get("table")) == (("iceberg", "lake", "[sales]", "snapshot")))
    assert(report.get("metadata").asInstanceOf[java.util.Map[String, AnyRef]].get("owner") == "orders-team")
    assert((summary.get("totalWrites"), summary.get("checksPassed"), summary.get("checksFailed"), summary.get("totalViolations")) == ((2, 3, 0, 0)))
    events.foreach(e => assert(e.get("job").asInstanceOf[java.util.Map[String, AnyRef]].get("engine") == PythonIoCheck.Engine))
  }

  test(JobSummaries) {
    def summary(name: String) = assertCase(name).eventObjects.filter(e => String.valueOf(e.get("eventType")) == "JOB_SUMMARY") match {
      case List(only) => List("totalWrites", "checksPassed", "checksFailed", "totalViolations").map(only.get(_))
      case other      => fail(s"$name: ${other.size} summaries")
    }
    // one write checked and passed, one refused with a single violation; a refused run publishes nothing
    assert(summary("notifications") == List(0, 1, 1, 1))
    assert(summary("published-misuse") == List(0, 1, 0, 0))
  }

  test(JobIdentity) {
    val events = assertCase("job-identity").eventObjects
    assert(events.map(e => String.valueOf(e.get("eventType"))) == List("CONTRACT_VALIDATION", "WRITE", "JOB_SUMMARY"))
    events.foreach { e =>
      val job = e.get("job").asInstanceOf[java.util.Map[String, AnyRef]]
      val attributes = job.get("attributes").asInstanceOf[java.util.Map[String, AnyRef]]
      assert(job.get("jobId") == "orders_nightly" && job.get("name") == "orders_job.main" && job.get("runId") == "run-1")
      assert(attributes.size == 2 && attributes.get("team") == "orders" && attributes.get("dag") == "orders_daily")
    }
  }

  test(DryRunInference) {
    // every write is let through and gets a draft contract (events.txt: INFERRED, self-check PASSED); the drafts are merged
    assertCase("dry-run-infers")
    assertCase("dry-run-unverifiable-write")
  }

  test(ContractMetadata) {
    val events = assertCase("dry-run-contract-metadata").eventObjects
    def metadata(e: java.util.Map[String, AnyRef]) = e.get("metadata").asInstanceOf[java.util.Map[String, AnyRef]].asScala.toMap
    val expected = Map[String, AnyRef]("owner" -> "orders-team", "source" -> "erp")
    val List(inference, summary) = events.filter(e => Set("CONTRACT_INFERENCE", "DRY_RUN_SUMMARY")(String.valueOf(e.get("eventType"))))
    assert(metadata(inference) == expected && metadata(summary) == expected)
    // the draft and the merged contract both carry it as their extensions, written into the YAML a reviewer adopts
    List(inference.get("contractYaml"), summary.get("mergedContractYaml")).foreach { yaml =>
      assert(String.valueOf(yaml).contains("owner: orders-team") && String.valueOf(yaml).contains("source: erp"), yaml)
    }
  }
}

object PythonIoProtocolCasesSpec {

  val OrganizationalPolicy =
    "organizational policy: a contract that breaks INVARACT_ORG_POLICY is refused when the run opens, before the job's code runs; one that keeps it opens"
  val LocationRefs =
    "location refs: a ref:// location is resolved through INVARACT_LOCATION_MAP when the run opens, and the write to it is checked"
  val ContractRegistry =
    "contract registry: INVARACT_CONTRACT naming a registry:// reference is fetched from INVARACT_REGISTRY_URL's registry and enforced"
  val WriteEvents =
    "write events: after a finish that says to publish, each output the package published gets one WRITE event, and the run ends with a JOB_SUMMARY"
  val JobSummaries =
    "job summaries: every run that opened ends with one JOB_SUMMARY, counting its validation events by verdict, their violations, and its writes"
  val JobIdentity =
    "job identity: INVARACT_JOB_ID and INVARACT_JOB_ATTRIBUTES name the job in every event; malformed attributes are not checked"
  val ContractMetadata =
    "contract metadata: INVARACT_CONTRACT_METADATA goes into every dry-run draft's extensions and every dry-run event's metadata; malformed pairs are not checked"
  val DryRunInference =
    "dry-run inference: with INVARACT_DRY_RUN=true every write is let through, gets a draft contract, and the drafts are merged at the end"

  private val Placeholder = "${CASE}"
  private val HttpPlaceholder = "${HTTP}"

  private val root: File = new File(getClass.getResource("/protocol").toURI)

  /** Every case, by name. */
  val cases: List[File] = root.listFiles().filter(_.isDirectory).sortBy(_.getName).toList

  /** What a run of a case did: its replies, and the events its file and HTTP sinks received (each file event also whole). */
  final case class Outcome(replies: List[AnyRef], events: List[String], httpEvents: List[String], eventObjects: List[java.util.Map[String, AnyRef]] = Nil)

  private def json(line: String): AnyRef = new Yaml(new SafeConstructor(new LoaderOptions)).load[AnyRef](line)

  private def lines(file: File): List[String] =
    if (!file.isFile) Nil else new String(Files.readAllBytes(file.toPath), StandardCharsets.UTF_8).split("\n").map(_.trim).filter(_.nonEmpty).toList

  /** An event as `events.txt` records it: its type, then what it concluded (as `native/cases.py` describes it). */
  private def eventType(line: String): String = {
    val event = json(line).asInstanceOf[java.util.Map[String, AnyRef]]
    (event.get("eventType") :: List("status", "selfCheck", "mergeStatus").map(event.get).filter(_ != null)).mkString(" ")
  }

  private def copy(from: Path, to: Path, url: String): Unit =
    Files.walk(from).iterator().asScala.foreach { source =>
      val target = to.resolve(from.relativize(source).toString)
      if (Files.isDirectory(source)) Files.createDirectories(target)
      else {
        val text = new String(Files.readAllBytes(source), StandardCharsets.UTF_8)
        Files.write(target, text.replace(Placeholder, to.toString).replace(HttpPlaceholder, url).getBytes(StandardCharsets.UTF_8))
      }
    }

  /** Runs the case `name` in a fresh copy of its directory. */
  def runCase(name: String): Outcome = {
    val source = new File(root, name)
    val work = Files.createTempDirectory("invaract-case-").resolve(name)
    val posted = new ConcurrentLinkedQueue[String]
    val receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    receiver.createContext(
      "/events",
      exchange => {
        posted.add(eventType(new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)))
        exchange.sendResponseHeaders(200, -1)
        exchange.close()
      }
    )
    receiver.start()
    try {
      copy(source.toPath, work, s"http://127.0.0.1:${receiver.getAddress.getPort}/events")
      val out = new ByteArrayOutputStream
      val requests = new String(Files.readAllBytes(work.resolve("requests.jsonl")), StandardCharsets.UTF_8)
      PythonIoCheck.serve(new BufferedReader(new StringReader(requests)), new PrintStream(out, true, "UTF-8"))
      val replies = new String(out.toByteArray, StandardCharsets.UTF_8).replace(work.toString, Placeholder)
      val fileEvents = lines(work.resolve("events.jsonl").toFile)
      Outcome(replies.split("\n").map(_.trim).filter(_.nonEmpty).map(json).toList, fileEvents.map(eventType), posted.asScala.toList,
        fileEvents.map(json(_).asInstanceOf[java.util.Map[String, AnyRef]]))
    } finally receiver.stop(0)
  }

  /** Runs the case `name` and holds it to its recorded replies and events; returns what it did. */
  def assertCase(name: String): Outcome = {
    val source = new File(root, name)
    val outcome = runCase(name)
    val expected = lines(new File(source, "replies.jsonl")).map(json)
    assert(outcome.replies.size == expected.size, s"$name: ${outcome.replies.size} replies, expected ${expected.size}")
    outcome.replies.zip(expected).zipWithIndex.foreach { case ((actual, wanted), i) => assert(actual == wanted, s"$name: reply ${i + 1}") }
    assert(outcome.events == lines(new File(source, "events.txt")), s"$name: events")
    // an HTTP sink delivers asynchronously, so its events arrive in no fixed order
    assert(outcome.httpEvents.sorted == lines(new File(source, "http-events.txt")).sorted, s"$name: HTTP events")
    outcome
  }

}
