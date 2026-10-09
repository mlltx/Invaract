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
}
