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
  * `http-events.txt`, its HTTP sink must have posted those to `${HTTP}`, a local endpoint the run provides.
  */
class PythonIoProtocolCasesSpec extends AnyFunSuite {
  import PythonIoProtocolCasesSpec._

  test("every case directory holds the requests and the replies it is checked against") {
    assert(cases.size >= 20)
    cases.foreach(c => assert(new File(c, "requests.jsonl").isFile && new File(c, "replies.jsonl").isFile, c.getName))
  }

  cases.foreach(c => test(s"protocol case: ${c.getName}")(assertCase(c.getName)))

  test(OrganizationalPolicy) {
    // a contract that breaks the policy is refused when the run opens; one that keeps it opens and is checked
    assertCase("org-policy-refused")
    assertCase("org-policy-satisfied")
  }

  test(LocationRefs) {
    assertCase("location-refs")
    assert(runCase("location-refs").replies.head.toString.contains("s3://lake/reports/orders"))
  }

  test(ContractRegistry)(assertCase("registry"))

  test(DryRunInference) {
    // every write is let through and gets a draft contract; the job's drafts are merged when it finishes
    assertCase("dry-run-infers")
    assert(runCase("dry-run-infers").events == List("CONTRACT_INFERENCE INFERRED PASSED", "CONTRACT_INFERENCE INFERRED PASSED", "DRY_RUN_SUMMARY MERGED"))
    assertCase("dry-run-unverifiable-write")
  }
}

object PythonIoProtocolCasesSpec {

  val OrganizationalPolicy =
    "organizational policy: a contract that breaks INVARACT_ORG_POLICY is refused when the run opens, before the job's code runs; one that keeps it opens"
  val LocationRefs =
    "location refs: a ref:// location is resolved through INVARACT_LOCATION_MAP when the run opens, and the write to it is checked"
  val ContractRegistry =
    "contract registry: INVARACT_CONTRACT naming a registry:// reference is fetched from INVARACT_REGISTRY_URL's registry and enforced"
  val DryRunInference =
    "dry-run inference: with INVARACT_DRY_RUN=true every write is let through, gets a draft contract, and the drafts are merged at the end"

  private val Placeholder = "${CASE}"
  private val HttpPlaceholder = "${HTTP}"

  private val root: File = new File(getClass.getResource("/protocol").toURI)

  /** Every case, by name. */
  val cases: List[File] = root.listFiles().filter(_.isDirectory).sortBy(_.getName).toList

  final case class Outcome(
      replies: List[AnyRef],
      expected: List[AnyRef],
      events: List[String],
      expectedEvents: List[String],
      httpEvents: List[String],
      expectedHttpEvents: List[String]
  )

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
        posted.add(eventType(new String(readAll(exchange.getRequestBody), StandardCharsets.UTF_8)))
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
      Outcome(
        replies.split("\n").map(_.trim).filter(_.nonEmpty).map(json).toList,
        lines(new File(source, "replies.jsonl")).map(json),
        lines(work.resolve("events.jsonl").toFile).map(eventType),
        lines(new File(source, "events.txt")),
        posted.asScala.toList,
        lines(new File(source, "http-events.txt"))
      )
    } finally receiver.stop(0)
  }

  private def readAll(in: java.io.InputStream): Array[Byte] = {
    val out = new ByteArrayOutputStream
    val buffer = new Array[Byte](8192)
    Iterator.continually(in.read(buffer)).takeWhile(_ != -1).foreach(n => out.write(buffer, 0, n))
    out.toByteArray
  }

  def assertCase(name: String): Unit = {
    val outcome = runCase(name)
    assert(outcome.replies.size == outcome.expected.size, s"$name: ${outcome.replies.size} replies, expected ${outcome.expected.size}")
    outcome.replies.zip(outcome.expected).zipWithIndex.foreach { case ((actual, expected), i) =>
      assert(actual == expected, s"$name: reply ${i + 1}")
    }
    assert(outcome.events == outcome.expectedEvents, s"$name: events")
    assert(outcome.httpEvents == outcome.expectedHttpEvents, s"$name: HTTP events")
  }

}
