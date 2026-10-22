// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.ContractParser
import com.invaract.verification.notification.{NotificationSink, RecordingTestSink}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

/** `AttachSetup` decides, from configuration alone, what a job is attached to. Everything here goes through a
  * plain-map `ConfigSource`; the Spark specs prove the same decisions through a real `SparkSession`'s conf.
  */
class AttachSetupSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var scratch: Path = _

  override def beforeAll(): Unit = scratch = Files.createTempDirectory("invaract-attach-test")

  override def afterAll(): Unit = {
    Files.list(scratch).forEach(p => Files.delete(p))
    Files.delete(scratch)
  }

  private def file(name: String, content: String): String = {
    val p = scratch.resolve(name)
    Files.write(p, content.getBytes("UTF-8"))
    p.toString
  }

  private def config(entries: (String, String)*): ConfigSource = ConfigSource.fromMap(entries.toMap)

  private lazy val contractPath: String = file("contract.yaml", ContractParser.write(FakeRegistryClient.contract("attached")))
  private lazy val goodSink: String = file("sink.properties", s"sink.enabled=true\nsink.class=${classOf[RecordingTestSink].getName}\n")
  private lazy val disabledSink: String = file("off.properties", "sink.enabled=false\n")
  private lazy val badSink: String = file("bad.properties", "sink.enabled=true\nsink.class=com.invaract.verification.NoSuchSink\n")
  private val missing = "/definitely/not/there.properties"

  // --- mode ---

  test("with nothing about dry-run set, a contract is required and the plan is Enforce") {
    val plan = AttachSetup.select(config(InvaractConf.Contract -> contractPath))
    plan match {
      case AttachPlan.Enforce(contract, sink) =>
        assert(contract.id == "attached")
        assert(sink.isEmpty)
      case other => fail(s"expected Enforce, got $other")
    }
  }

  test("dryRun=true is a DryRun plan and ignores the contract entirely, even an unreadable one") {
    val plan = AttachSetup.select(config(InvaractConf.DryRun -> "true", InvaractConf.Contract -> missing))
    assert(plan == AttachPlan.DryRun(None))
  }

  test("dryRun=false is Enforce") {
    val plan = AttachSetup.select(config(InvaractConf.DryRun -> "false", InvaractConf.Contract -> contractPath))
    assert(plan.isInstanceOf[AttachPlan.Enforce])
  }

  test("enforcement without a contract fails naming the setting as the engine spells it, and the dry-run alternative") {
    val spelled = new ConfigSource {
      def get(name: String): Option[String] = None
      override def describe(name: String): String = "engine." + name
    }
    val ex = intercept[IllegalStateException](AttachSetup.select(spelled))
    assert(ex.getMessage == "A contract is required: set 'engine.contract' (or 'engine.dryRun=true' for dry-run mode, which needs no contract at all).")
  }

  test("a registry reference is resolved through ContractReference") {
    FakeRegistryClient.reset()
    FakeRegistryClient.stubLatest("https://r.test", FakeRegistryClient.contract("from_registry"))
    val plan = AttachSetup.select(
      config(
        InvaractConf.Contract -> "registry://from_registry@latest",
        InvaractConf.RegistryUrl -> "https://r.test",
        InvaractConf.RegistryClientClass -> classOf[FakeRegistryClient].getName
      )
    )
    assert(plan.asInstanceOf[AttachPlan.Enforce].contract.id == "from_registry")
  }

  // --- sinks ---

  test("a configured sink is carried by an Enforce plan and by a DryRun plan") {
    val enforce = AttachSetup.select(config(InvaractConf.Contract -> contractPath, InvaractConf.NotifyConfig -> goodSink))
    assert(enforce.sink.isDefined)
    val dry = AttachSetup.select(config(InvaractConf.DryRun -> "true", InvaractConf.NotifyConfig -> goodSink))
    assert(dry.sink.isDefined)
  }

  test("a sink config with sink.enabled=false yields no sink in either mode") {
    assert(AttachSetup.select(config(InvaractConf.Contract -> contractPath, InvaractConf.NotifyConfig -> disabledSink)).sink.isEmpty)
    assert(AttachSetup.select(config(InvaractConf.DryRun -> "true", InvaractConf.NotifyConfig -> disabledSink)).sink.isEmpty)
  }

  test("enforcement fails loudly on a sink it cannot set up: a typo'd class, and an unreadable file") {
    intercept[Exception](AttachSetup.select(config(InvaractConf.Contract -> contractPath, InvaractConf.NotifyConfig -> badSink)))
    intercept[Exception](AttachSetup.select(config(InvaractConf.Contract -> contractPath, InvaractConf.NotifyConfig -> missing)))
  }

  test("dry-run never fails on a sink it cannot set up: the plan has no sink") {
    assert(AttachSetup.select(config(InvaractConf.DryRun -> "true", InvaractConf.NotifyConfig -> badSink)) == AttachPlan.DryRun(None))
    assert(AttachSetup.select(config(InvaractConf.DryRun -> "true", InvaractConf.NotifyConfig -> missing)) == AttachPlan.DryRun(None))
  }

  test("strictSink and lenientSink are None with no notifyConfig, and differ only in how failure is handled") {
    assert(AttachSetup.strictSink(ConfigSource.empty).isEmpty)
    assert(AttachSetup.lenientSink(ConfigSource.empty).isEmpty)
    intercept[Exception](AttachSetup.strictSink(config(InvaractConf.NotifyConfig -> badSink)))
    assert(AttachSetup.lenientSink(config(InvaractConf.NotifyConfig -> badSink)).isEmpty)
    val built: Option[NotificationSink] = AttachSetup.lenientSink(config(InvaractConf.NotifyConfig -> goodSink))
    assert(built.isDefined)
  }

  // --- the mistakes people make, and what they are told ---

  /** What `body` writes to standard error, where the tests' slf4j-simple logs. */
  private def stderrOf(body: => Unit): String = {
    val captured = new java.io.ByteArrayOutputStream()
    val original = System.err
    System.setErr(new java.io.PrintStream(captured, true, "UTF-8"))
    try body
    finally System.setErr(original)
    captured.toString("UTF-8")
  }

  test("dryRun accepts true and false in any case and with spaces around, and refuses anything else by name") {
    assert(AttachSetup.select(config(InvaractConf.DryRun -> " TRUE ")) == AttachPlan.DryRun(None))
    assert(AttachSetup.select(config(InvaractConf.DryRun -> "False", InvaractConf.Contract -> contractPath)).isInstanceOf[AttachPlan.Enforce])
    val e = intercept[IllegalArgumentException](AttachSetup.select(config(InvaractConf.DryRun -> "yes")))
    assert(e.getMessage == "'dryRun' must be true or false, not 'yes'.")
    val spark = ConfigSource.prefixed("spark.invaract.")(Map("spark.invaract.dryRun" -> "1").get)
    assert(intercept[IllegalArgumentException](AttachSetup.select(spark)).getMessage == "'spark.invaract.dryRun' must be true or false, not '1'.")
  }

  test("a notifyConfig that is not a file names the setting and the path, in either mode") {
    val message = s"'notifyConfig' names '$missing', which is not a file that exists."
    val enforce = intercept[IllegalArgumentException](
      AttachSetup.select(config(InvaractConf.Contract -> contractPath, InvaractConf.NotifyConfig -> missing))
    )
    assert(enforce.getMessage == message)
    val directory = intercept[IllegalArgumentException](AttachSetup.strictSink(config(InvaractConf.NotifyConfig -> scratch.toString)))
    assert(directory.getMessage == s"'notifyConfig' names '$scratch', which is not a file that exists.")
    val log = stderrOf(assert(AttachSetup.lenientSink(config(InvaractConf.NotifyConfig -> missing)).isEmpty))
    assert(log.contains(message))
  }

  test("a sink class that does not exist is reported as not found") {
    val e = intercept[IllegalArgumentException](AttachSetup.strictSink(config(InvaractConf.NotifyConfig -> badSink)))
    assert(e.getMessage.contains("Class 'com.invaract.verification.NoSuchSink' was not found"))
  }

  private val typoContract =
    """id: typos
      |version: "1.0.0"
      |outputs:
      |  - name: out
      |    location: /out
      |    format: parqet
      |    schema:
      |      fields:
      |        - name: id
      |          type: string
      |          nulable: false
      |""".stripMargin

  test("contractWarnings for a file: near-miss keys first, then the validator's warnings") {
    val path = file("typos.yaml", typoContract)
    val warnings = AttachSetup.contractWarnings(ContractParser.parseFile(path), path)
    assert(warnings.map(_.path) == List("outputs[0].schema.fields[0].nulable", "outputs[0].format"))
    assert(warnings.last.message.endsWith("Did you mean 'parquet'?"))
  }

  test("contractWarnings for a registry reference has only the validator's: there is no file to read") {
    val warnings = AttachSetup.contractWarnings(ContractParser.parse(typoContract), "registry://typos@1.0.0")
    assert(warnings.map(_.path) == List("outputs[0].format"))
    assert(AttachSetup.contractWarnings(FakeRegistryClient.contract("clean"), "registry://clean@latest").isEmpty)
  }

  test("an enforcing select logs each contract warning once, naming the contract") {
    val path = file("typos-logged.yaml", typoContract)
    val log = stderrOf(assert(AttachSetup.select(config(InvaractConf.Contract -> path)).isInstanceOf[AttachPlan.Enforce]))
    assert(log.contains("Invaract: contract typos@1.0.0: [Warning] outputs[0].schema.fields[0].nulable: Unknown key 'nulable' is ignored."))
    assert(log.split("\n").count(_.contains("outputs[0].format")) == 1)
    assert(!stderrOf(AttachSetup.select(config(InvaractConf.Contract -> contractPath))).contains("Invaract: contract"))
  }
}
