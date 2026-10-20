// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.verification.InvaractConf
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

/** The checker the Python package runs (`PortableCheck`, docs/BEAM_PYTHON_ADAPTER.md, D4), called here as the package calls it,
  * on real Python pipelines (the recordings `record_fixtures.py` makes). */
class PortableCheckSpec extends AnyFunSuite {
  import PortableCheckSpec._

  test("a pipeline its contract allows: exit 0, nothing printed") {
    val root = freshRoot()
    assert(check(s"--pipeline=${recording(root, "parquet_job")}", s"--invaract_contract=${file(root, "c.yaml", ReportContract)}") == ((PortableCheck.Passed, "")))
  }

  test("a pipeline its contract refuses: exit 1, with the reason every adapter gives") {
    val root = freshRoot()
    val moved = file(root, "c.yaml", ReportContract.replace("location: data/out/report\n", "location: data/out/report_v2\n"))
    val (status, out) = check(s"--pipeline=${recording(root, "parquet_job")}", s"--invaract_contract=$moved")
    assert(status == PortableCheck.Refused)
    assert(out.contains("[OUTPUT_LOCATION_MISMATCH]") && out.contains("data/out/report_v2") && out.contains("Read(data/in/orders)"))
  }

  test("a contract that asks for what the Python adapter declares it cannot check is refused for that, before any write is judged") {
    val root = freshRoot()
    val shaped = file(root, "c.yaml", ReportContract + "rules:\n  - type: forbid_cross_join\n")
    val (status, out) = check(s"--pipeline=${recording(root, "joined")}", s"--invaract_contract=$shaped")
    assert(status == PortableCheck.Refused)
    assert(out.contains("[UNSUPPORTED_CONTRACT_FEATURE]") && out.contains("rules.planShape"))
    assert(!out.contains("[UNVERIFIABLE_WRITE]"), "the combine the rule cannot see into is not what refuses it")
  }

  test("dry-run mode needs no contract and lets the pipeline through, publishing a draft that names the engine, its version and the runner") {
    val root = freshRoot()
    val events = root.resolve("events.jsonl")
    val sink = file(root, "sink.properties",
      s"sink.enabled=true\nsink.class=com.invaract.verification.notification.FileNotificationSink\nsink.property.path=${events.toString.replace('\\', '/')}\n")
    val (status, out) = check(s"--pipeline=${recording(root, "parquet_job")}", "--invaract_dry_run=true", s"--invaract_notify_config=$sink",
      "--sdk_version=2.77.0", "--runner=DataflowRunner", "--job_name=nightly-orders")
    assert((status, out) == ((PortableCheck.Passed, "")))
    val published = new String(Files.readAllBytes(events), StandardCharsets.UTF_8).replace(" ", "")
    assert(published.contains("CONTRACT_INFERENCE") && published.contains("data/out/report"))
    List("\"engine\":\"beam-python\"", "\"engineVersion\":\"2.77.0\"", "\"runner\":\"DataflowRunner\"", "\"name\":\"nightly-orders\"")
      .foreach(fragment => assert(published.contains(fragment), fragment))
  }

  test("dry-run mode lists a Python read that states no columns (ReadFromParquet of dictionaries) as an input of the draft") {
    val root = freshRoot()
    val events = root.resolve("events.jsonl")
    val sink = file(root, "sink.properties",
      s"sink.enabled=true\nsink.class=com.invaract.verification.notification.FileNotificationSink\nsink.property.path=${events.toString.replace('\\', '/')}\n")
    assert(check(s"--pipeline=${recording(root, "parquet_dicts")}", "--invaract_dry_run=true", s"--invaract_notify_config=$sink")._1 == PortableCheck.Passed)
    val published = new String(Files.readAllBytes(events), StandardCharsets.UTF_8)
    assert(published.contains("INFERRED_DEGRADED"), published)
    assert(published.contains("- name: input\\n  location: data/in/orders"), "the read is the draft's input")
    assert(published.contains("the pipeline states no columns for the read of data/in/orders, so the draft declares none for it"))
  }

  test("a pipeline only checked (--check_only=true) is marked so in its events; a value other than true or false is not checked") {
    val root = freshRoot()
    val events = root.resolve("events.jsonl")
    val sink = file(root, "sink.properties",
      s"sink.enabled=true\nsink.class=com.invaract.verification.notification.FileNotificationSink\nsink.property.path=${events.toString.replace('\\', '/')}\n")
    val contract = s"--invaract_contract=${file(root, "c.yaml", ReportContract)}"
    val pipeline = s"--pipeline=${recording(root, "parquet_job")}"
    assert(check(pipeline, contract, s"--invaract_notify_config=$sink", "--check_only=true")._1 == PortableCheck.Passed)
    assert(new String(Files.readAllBytes(events), StandardCharsets.UTF_8).replace(" ", "").contains("\"checkOnly\":\"true\""))
    // without the argument, or with false, the pipeline is to run, and its events do not say otherwise
    for (flags <- Seq(Nil, Seq("--check_only=false"))) {
      Files.deleteIfExists(events)
      assert(check(Seq(pipeline, contract, s"--invaract_notify_config=$sink") ++ flags: _*)._1 == PortableCheck.Passed)
      val published = new String(Files.readAllBytes(events), StandardCharsets.UTF_8)
      assert(published.contains("CONTRACT_VALIDATION") && !published.contains("checkOnly"), flags)
    }
    val (status, out) = check(pipeline, contract, "--check_only=yes")
    assert(status == PortableCheck.NotChecked && out.contains("--check_only=yes must be true or false"), out)
  }

  test("the settings are read with the Python package's spelling: --invaract_reject_undeclared_inputs") {
    val root = freshRoot()
    // the write reads a CSV through pandas, which states no location: allowed by default, refused in strict mode
    val contract = file(root, "c.yaml", ReportContract.replace("inputs:\n  - { name: orders, location: data/in/orders, format: parquet }\n", ""))
    val pipeline = s"--pipeline=${recording(root, "unlocated_read")}"
    assert(check(pipeline, s"--invaract_contract=$contract")._1 == PortableCheck.Passed)
    val (status, out) = check(pipeline, s"--invaract_contract=$contract", "--invaract_reject_undeclared_inputs=true")
    assert(status == PortableCheck.Refused && out.contains("ReadCsv"))
  }

  test("a setting or an argument the checker does not know, or a malformed one, is not checked: exit 2, and the job does not start") {
    val root = freshRoot()
    val pipeline = s"--pipeline=${recording(root, "parquet_job")}"
    val (s1, o1) = check(pipeline, "--invaract_contrcat=x")
    assert(s1 == PortableCheck.NotChecked && o1.contains("--invaract_contrcat") && o1.contains("so it is not submitted"))
    val (s2, o2) = check(pipeline, "--invaract_contract=x", "--colour=blue")
    assert(s2 == PortableCheck.NotChecked && o2.contains("--colour"))
    assert(check(pipeline, "invaract_contract=x")._2.contains("an argument must be --name=value, not 'invaract_contract=x'"))
    assert(check(pipeline, "--=x")._2.contains("an argument must be --name=value, not '--=x'"))
    assert(check(pipeline, "--invaract_dry_run")._2.contains("an argument must be --name=value, not '--invaract_dry_run'"))
    assert(check(pipeline, "--a=x")._2.contains("does not know the argument(s) --a"), "a one-letter name is a name")
    assert(check("--invaract_dry_run=true")._2.contains("--pipeline=<file>"))
  }

  test("a value may itself hold '=' (a URL with a query)") {
    val root = freshRoot()
    val contract = file(root, "c=1.yaml", ReportContract)
    assert(check(s"--pipeline=${recording(root, "parquet_job")}", s"--invaract_contract=$contract")._1 == PortableCheck.Passed)
  }

  test("a file that is not a pipeline, or a contract that cannot be read, is not checked: exit 2") {
    val root = freshRoot()
    val notAPipeline = file(root, "p.pb", "not a pipeline")
    assert(check(s"--pipeline=$notAPipeline", "--invaract_dry_run=true")._1 == PortableCheck.NotChecked)
    assert(check(s"--pipeline=${root.resolve("missing.pb")}", "--invaract_dry_run=true")._1 == PortableCheck.NotChecked)
    assert(check(s"--pipeline=${recording(root, "parquet_job")}", s"--invaract_contract=${root.resolve("missing.yaml")}")._1 == PortableCheck.NotChecked)
  }

  test("an empty setting is no setting") {
    val root = freshRoot()
    // with no contract and no dry run, there is nothing to attach: the shared setup says so, and nothing is checked
    assert(check(s"--pipeline=${recording(root, "parquet_job")}", "--invaract_contract= ")._1 == PortableCheck.NotChecked)
  }

  test("the Python spelling of each setting, and how the checker names one in a message") {
    assert(PythonConfigSource.flag(InvaractConf.RejectUndeclaredInputs) == "reject_undeclared_inputs")
    assert(PythonConfigSource.flag(InvaractConf.Contract) == "contract")
    assert(new PythonConfigSource(Map.empty).describe(InvaractConf.DryRun) == "--invaract_dry_run")
    assert(new PythonConfigSource(Map("dry_run" -> "true")).get(InvaractConf.DryRun).contains("true"))
  }

  test("a Python pipeline can be given every setting a Java one can, and none that only an adapter checking after the job reads") {
    assert(PythonConfigSource.Flags == BeamSettings.Names.map(PythonConfigSource.flag).toSet)
    assert(!BeamSettings.Names.contains(InvaractConf.FailOnMissingOutput))
    assert(BeamSettings.Names.toSet == InvaractConf.All.toSet - InvaractConf.FailOnMissingOutput)
  }

  test("every setting a Beam pipeline can be given is a Beam option (a new neutral setting needs one in InvaractBeamOptions)") {
    BeamSettings.Names.foreach { name =>
      val options = PipelineOptionsFactory.fromArgs(s"--invaract${name.capitalize}=v").as(classOf[InvaractBeamOptions])
      assert(new BeamConfigSource(options).get(name).contains("v"), name)
    }
  }

  test("every setting a Beam pipeline can be given is a flag of the Python package (a new one needs adding to invaract_beam/options.py)") {
    val source = new String(Files.readAllBytes(java.nio.file.Paths.get("python/invaract_beam/options.py")), StandardCharsets.UTF_8)
    val block = source.substring(source.indexOf("SETTINGS = {"), source.indexOf("\n}", source.indexOf("SETTINGS = {")))
    val declared = "(?m)^\\s+\"([a-z_]+)\":".r.findAllMatchIn(block).map(_.group(1)).toSet
    assert(declared == PythonConfigSource.Flags)
  }
}

private[beamadapter] object PortableCheckSpec {

  /** The survey's contract: the Parquet job of `record_fixtures.py`, as its author meant it. */
  val ReportContract: String =
    """id: orders_report
      |version: "1.0.0"
      |inputs:
      |  - { name: orders, location: data/in/orders, format: parquet }
      |outputs:
      |  - name: report
      |    location: data/out/report
      |    format: parquet
      |    schema:
      |      fields:
      |        - { name: id, type: long, required: true }
      |        - { name: amount, type: long }
      |""".stripMargin

  /** The checker, called as the Python package calls it: its exit status and what it printed. */
  def check(args: String*): (Int, String) = {
    val bytes = new ByteArrayOutputStream()
    val status = PortableCheck.run(args.toArray, new PrintStream(bytes, true, "UTF-8"))
    (status, new String(bytes.toByteArray, StandardCharsets.UTF_8))
  }

  def freshRoot(): Path = Files.createTempDirectory("portable-check-")

  def file(root: Path, name: String, text: String): Path = Files.write(root.resolve(name), text.getBytes(StandardCharsets.UTF_8))

  def recording(root: Path, name: String): Path = Files.write(root.resolve(s"$name.pb"), PortablePipelineSpec.fixtureBytes(name))
}
