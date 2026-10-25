// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

/** How the Python adapter is switched on without touching a job's source (CLAUDE.md's External Attachability Requirement): a
  * platform team adds `--runner=invaract_beam.InvaractRunner` and the `--invaract_*` flags to an unchanged Python job
  * (`python/tests/example_job.py`). The tests here are the ones `BeamPythonAttestedClaimsSpec` names as the evidence for the
  * capabilities no neutral job can show.
  *
  * The ones that run a Python job need the Python environment (`BeamPythonConformanceAdapter`) and are canceled without it
  * (failed under `INVARACT_CONFORMANCE_STRICT=true`). The settings that only change what the checker does with a pipeline are
  * shown by running the checker as the package runs it, with the flags the package forwards (the package's own tests show
  * it forwards each one unchanged), on the same job recorded (`portable/example_job.pb`), so they run everywhere.
  */
class BeamPythonAttachSpec extends AnyFunSuite {
  import BeamPythonAttachSpec._
  import BeamPythonConformanceAdapter.{Python, environment, javaCommand, checkerClasspath, python, slashes}
  import PortableCheckSpec.{check, file, recording}

  private def needsPython(): Unit = environment.left.foreach { why =>
    if (sys.env.get("INVARACT_CONFORMANCE_STRICT").contains("true")) fail(why) else cancel(why)
  }

  /** A folder holding an `in/orders` dataset with two rows. */
  private def freshRoot(): Path = ExampleJob.withOrders(Files.createTempDirectory("beam-python-attach-"), (1L, 10L), (2L, 20L))

  /** The Parquet files the job wrote under `out/` whose names start with `report`. */
  private def written(root: Path, prefix: String = "report"): List[String] = {
    val out = root.resolve("out").toFile
    Option(out.listFiles()).toList.flatten.map(_.getName).filter(n => n.startsWith(prefix) && n.endsWith(".parquet")).sorted
  }

  private def runJob(root: Path, flags: List[String], pythonPath: String = BeamPythonConformanceAdapter.PythonPath): (Int, String) =
    python(List(new File("python/tests/example_job.py").getAbsolutePath, s"--root=${slashes(root)}") ++ flags, pythonPath)

  private def platform(root: Path, contract: Path): List[String] = List(
    "--runner=invaract_beam.InvaractRunner",
    s"--invaract_java=$javaCommand",
    s"--invaract_java_options=@${slashes(checkerClasspath)}",
    s"--invaract_contract=${slashes(contract)}"
  )

  // --- zero-code install ---

  test(ZeroCodeInstall) {
    needsPython()
    val root = freshRoot()
    val contract = file(root, "contract.yaml", ExampleJob.contractYaml())
    val (refused, why) = runJob(root, "--output=out/elsewhere" :: platform(root, contract))
    assert(refused != 0 && why.contains("ContractViolation") && why.contains("[OUTPUT_LOCATION_MISMATCH]"), why)
    assert(written(root, "elsewhere").isEmpty, "the pipeline was never submitted, so nothing was written")

    // the same flags let a job that keeps to the contract run to completion on the real runner
    val (status, output) = runJob(root, platform(root, contract))
    assert(status == 0, output)
    assert(written(root).nonEmpty)
  }

  test("zero-code install: a job that picks its own runner in code is not reached by --runner, which is the limit the declaration states") {
    needsPython()
    val root = freshRoot()
    val contract = file(root, "contract.yaml", ExampleJob.contractYaml())
    val (status, output) = runJob(root, List("--output=out/elsewhere", "--pin_runner") ++ platform(root, contract))
    assert(status == 0, output) // it breaks the contract, and nobody looked
    assert(written(root, "elsewhere").nonEmpty)
  }

  test("zero-code install: a job submitted where the package is not installed does not start") {
    needsPython()
    // the package reaches the jobs here through PYTHONPATH only; installed into the interpreter, it is found anyway
    val (installed, _) = python(List("-c", "import invaract_beam"), BeamPythonConformanceAdapter.WithoutPackage)
    if (installed == 0) {
      val why = s"$Python has invaract_beam installed, so a machine without it cannot be shown: run this test with an interpreter where it is not (pip uninstall invaract-beam)"
      if (sys.env.get("INVARACT_CONFORMANCE_STRICT").contains("true")) fail(why) else cancel(why)
    }
    val root = freshRoot()
    val contract = file(root, "contract.yaml", ExampleJob.contractYaml())
    val (status, output) = runJob(root, platform(root, contract), BeamPythonConformanceAdapter.WithoutPackage)
    assert(status != 0 && output.contains("No module named 'invaract_beam'"), output)
    assert(written(root).isEmpty)
  }

  // --- check only ---

  test(CheckOnly) {
    needsPython()
    val root = freshRoot()
    val contract = file(root, "contract.yaml", ExampleJob.contractYaml())
    // the production delegate is not even created, so one that cannot be found where CI runs does not matter
    val ci = platform(root, contract) ++ List("--invaract_delegate=no_such_module.NoSuchRunner", "--invaract_check_only=true")
    val (status, output) = runJob(root, ci)
    assert(status == 0, output)
    assert(written(root).isEmpty, "nothing ran, so nothing was written")
    val (refused, why) = runJob(root, "--output=out/elsewhere" :: ci)
    assert(refused != 0 && why.contains("ContractViolation") && why.contains("[OUTPUT_LOCATION_MISMATCH]"), why)
  }

  // --- dry-run mode ---

  test(DryRunInference) {
    needsPython()
    val root = freshRoot()
    val events = root.resolve("events.jsonl")
    val sink = file(root, "sink.properties",
      s"sink.enabled=true\nsink.class=com.invaract.verification.notification.FileNotificationSink\nsink.property.path=${slashes(events)}\n")
    val flags = platform(root, root.resolve("unused")).filterNot(_.startsWith("--invaract_contract=")) ++
      List("--output=out/anywhere", "--invaract_dry_run=true", s"--invaract_notify_config=${slashes(sink)}")
    val (status, output) = runJob(root, flags)
    assert(status == 0, output)
    assert(written(root, "anywhere").nonEmpty, "dry-run mode enforces nothing, so the write went ahead")
    val published = Files.readAllLines(events, StandardCharsets.UTF_8).asScala.mkString("\n").replace(" ", "")
    // ReadFromParquet of dictionaries states no columns (Python reads the file schema when the job runs), so the draft lists the
    // read as an input with none, and is degraded for saying so
    assert(published.contains("CONTRACT_INFERENCE") && published.contains("\"status\":\"INFERRED_DEGRADED\""))
    assert(published.contains("inputs:\\n-name:input\\nlocation:") && published.contains("/in/orders"), "the draft names the job's read")
    assert(published.contains("thepipelinestatesnocolumnsforthereadof"))
    assert(published.contains("out/anywhere"), "the inferred contract names the write's location")
    assert(published.contains("\"engine\":\"beam-python\""))
  }

  // --- what the checker applies, with the flags the package forwards ---

  test(LocationRefs) {
    val root = Files.createTempDirectory("beam-python-attach-")
    val pipeline = s"--pipeline=${recording(root, "example_job")}"
    val contract = file(root, "contract.yaml", ExampleJob.contractYaml(outputLocation = "ref://report_dir", inputLocation = "ref://orders_dir"))
    def map(reportDir: String) = s"--invaract_location_map=${file(root, "locations.properties", s"orders_dir=data/in/orders\nreport_dir=$reportDir\n")}"
    assert(check(pipeline, s"--invaract_contract=$contract", map("data/out/report")) == ((PortableCheck.Passed, "")))
    val (status, out) = check(pipeline, s"--invaract_contract=$contract", map("data/out/audit"))
    assert(status == PortableCheck.Refused && out.contains("[OUTPUT_LOCATION_MISMATCH]") && out.contains("data/out/audit"))
  }

  test(ContractRegistry) {
    val root = Files.createTempDirectory("beam-python-attach-")
    val registry = List("--invaract_contract=registry://orders_report@1.0.0", "--invaract_registry_url=stub-registry",
      "--invaract_registry_client_class=com.invaract.beamadapter.StubRegistryClient")
    assert(check(s"--pipeline=${recording(root, "example_job")}" :: registry: _*) == ((PortableCheck.Passed, "")))
    // the same contract, fetched for a job that writes its report as text
    val (status, out) = check(s"--pipeline=${recording(root, "rows_to_text")}" :: registry: _*)
    assert(status == PortableCheck.Refused && out.contains("orders_report@1.0.0") && out.contains("[OUTPUT_FORMAT_MISMATCH]"))
  }

  test(OrganizationalPolicy) {
    val root = Files.createTempDirectory("beam-python-attach-")
    val pipeline = s"--pipeline=${recording(root, "example_job")}"
    val policy = s"--invaract_org_policy=${file(root, "policy.yaml", "version: \"1.0\"\npolicies:\n  - id: catalog-required\n    type: require_catalog\n    scope: outputs\n")}"
    val (status, out) = check(pipeline, s"--invaract_contract=${file(root, "breaking.yaml", ExampleJob.contractYaml())}", policy)
    assert(status == PortableCheck.Refused && out.contains("[ORG_POLICY_VIOLATION]") && out.contains("catalog-required"))
    val satisfying = file(root, "satisfying.yaml",
      ExampleJob.contractYaml().replace("    location: out/report\n    format: parquet\n", "    location: out/report\n    format: parquet\n    catalog:\n      required: true\n"))
    // the policy is satisfied; the file write is then refused for what the contract now asks: a catalog table a folder is not
    val (then, why) = check(pipeline, s"--invaract_contract=$satisfying", policy)
    assert(then == PortableCheck.Refused && !why.contains("[ORG_POLICY_VIOLATION]") && why.contains("[MISSING_OUTPUT_CATALOG_REGISTRATION]"))
  }
}

/** The names of the tests `BeamPythonAttestedClaimsSpec` points at, shared so a rename cannot leave an attestation dangling. */
object BeamPythonAttachSpec {
  val ZeroCodeInstall = "zero-code install: an unmodified Python job is blocked by the runner and contract a platform names on its command line, before it writes anything"
  val LocationRefs = "location refs: a ref:// location in the contract is resolved through --invaract_location_map and the pipeline is checked against it"
  val ContractRegistry = "contract registry: a registry:// reference is fetched from the registry the flags name and enforced"
  val OrganizationalPolicy = "organizational policy: a contract that breaks a policy given by --invaract_org_policy is refused; one that satisfies it is checked"
  val DryRunInference = "dry-run inference: --invaract_dry_run lets a real Python job write and reports a contract inferred from it"
  val CheckOnly = "check only: --invaract_check_only checks an unmodified Python job in CI without running it or creating its runner, and fails the build when the job breaks its contract"
}
