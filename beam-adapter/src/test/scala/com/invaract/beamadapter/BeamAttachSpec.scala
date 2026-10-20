// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.verification.{ContractViolationException, ViolationType}
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

/** How the adapter is switched on without touching a job's source (CLAUDE.md's External Attachability Requirement), shown on
  * a real job (`ExampleJob`) that knows nothing about Invaract: a platform team names `--runner=...InvaractRunner` and the
  * `--invaract...` options, and Beam finds both. The tests here are the ones `BeamAttestedClaimsSpec` names as the evidence for the
  * capabilities no neutral job can show.
  */
class BeamAttachSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var scratch: Path = _
  private val runnerFlag = "--runner=com.invaract.beamadapter.InvaractRunner"

  override def beforeAll(): Unit = scratch = Files.createTempDirectory("beam-attach-")
  override def afterAll(): Unit = BeamConformanceAdapter.deleteQuietly(scratch)

  /** A fresh working folder holding an `in/orders` dataset with two rows. */
  private def freshRoot(): Path = ExampleJob.withOrders(Files.createTempDirectory(scratch, "root-"), (1L, 10L), (2L, 20L))

  private def file(root: Path, name: String, content: String): String = {
    val path = root.resolve(name)
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
    path.toString
  }

  private def blockedWith(job: => Unit): Set[String] =
    intercept[ContractViolationException](job).result.violations.map(_.violationType).toSet

  // --- zero-code install ---

  test(BeamAttachSpec.ZeroCodeInstall) {
    val root = freshRoot()
    val contract = file(root, "contract.yaml", ExampleJob.contractYaml())
    val violating = new ExampleJob(root, "out/elsewhere")
    assert(blockedWith(violating.main(Array(runnerFlag, s"--invaractContract=$contract"))) == Set(ViolationType.OutputLocationMismatch))
    assert(violating.written.isEmpty, "the pipeline was never submitted, so nothing was written")

    // the same flags let a job that keeps to the contract run to completion
    val conforming = new ExampleJob(root, "out/report")
    conforming.main(Array(runnerFlag, s"--invaractContract=$contract"))
    assert(conforming.written.nonEmpty && conforming.written.forall(_.endsWith(".parquet")))
  }

  test("zero-code install: a job that picks its own runner in code is not reached by --runner, which is the limit the declaration states") {
    val root = freshRoot()
    val contract = file(root, "contract.yaml", ExampleJob.contractYaml())
    val job = new ExampleJob(root, "out/elsewhere", pinRunner = true)
    job.main(Array(runnerFlag, s"--invaractContract=$contract")) // violates the contract, and nobody looked
    assert(job.written.nonEmpty)
  }

  test("zero-code install: the explicit call checks a job the command line cannot reach") {
    val root = freshRoot()
    val contract = file(root, "contract.yaml", ExampleJob.contractYaml())
    val job = new ExampleJob(root, "out/elsewhere", pinRunner = true)
    val options = PipelineOptionsFactory.fromArgs(s"--invaractContract=$contract").create()
    options.setRunner(classOf[DirectRunner])
    val pipeline = Pipeline.create(options)
    job.build(pipeline)
    assert(blockedWith(InvaractBeam.verify(pipeline)) == Set(ViolationType.OutputLocationMismatch))
  }

  test("a delegate that is not a runner is refused with a message that names the setting") {
    val root = freshRoot()
    val contract = file(root, "contract.yaml", ExampleJob.contractYaml())
    val unknown = intercept[IllegalArgumentException](new ExampleJob(root).main(Array(runnerFlag, s"--invaractContract=$contract", "--invaractDelegate=NoSuchRunner")))
    assert(unknown.getMessage.contains("--invaractDelegate=NoSuchRunner"))
    val notARunner = intercept[IllegalArgumentException](new ExampleJob(root).main(Array(runnerFlag, s"--invaractContract=$contract", "--invaractDelegate=java.lang.String")))
    assert(notARunner.getMessage.contains("not a Beam PipelineRunner"))
  }

  test("without a contract or dry-run mode the runner refuses to start, naming the option") {
    val root = freshRoot()
    val e = intercept[IllegalStateException](new ExampleJob(root).main(Array(runnerFlag)))
    assert(e.getMessage.contains("--invaractContract"))
    assert(e.getMessage.contains("--invaractDryRun=true"))
  }

  // --- location references ---

  test(BeamAttachSpec.LocationRefs) {
    val root = freshRoot()
    val contract = file(root, "contract.yaml", ExampleJob.contractYaml(outputLocation = "ref://report_dir", inputLocation = "ref://orders_dir"))
    def map(reportDir: String) =
      file(root, "locations.properties", s"orders_dir=${root.resolve("in/orders").toString.replace('\\', '/')}\nreport_dir=${root.resolve(reportDir).toString.replace('\\', '/')}\n")
    val flags = (mapPath: String) => Array(runnerFlag, s"--invaractContract=$contract", s"--invaractLocationMap=$mapPath")

    val conforming = new ExampleJob(root, "out/report")
    conforming.main(flags(map("out/report")))
    assert(conforming.written.nonEmpty)

    val elsewhere = new ExampleJob(root, "out/elsewhere")
    assert(blockedWith(elsewhere.main(flags(map("out/report")))) == Set(ViolationType.OutputLocationMismatch))
  }

  // --- the contract registry ---

  test(BeamAttachSpec.ContractRegistry) {
    val root = freshRoot()
    val flags = Array(
      runnerFlag,
      "--invaractContract=registry://orders_report@1.0.0",
      "--invaractRegistryUrl=stub-registry",
      "--invaractRegistryClientClass=com.invaract.beamadapter.StubRegistryClient"
    )
    assert(blockedWith(new ExampleJob(root, "out/elsewhere").main(flags)) == Set(ViolationType.OutputLocationMismatch))
    val conforming = new ExampleJob(root, "out/report")
    conforming.main(flags)
    assert(conforming.written.nonEmpty)
  }

  // --- organizational policy ---

  test(BeamAttachSpec.OrganizationalPolicy) {
    val root = freshRoot()
    val policy = file(root, "policy.yaml", "version: \"1.0\"\npolicies:\n  - id: catalog-required\n    type: require_catalog\n    scope: outputs\n")
    val breaking = file(root, "breaking.yaml", ExampleJob.contractYaml())
    val rejected = intercept[ContractViolationException](
      InvaractBeam.fromOptions(PipelineOptionsFactory.fromArgs(s"--invaractContract=$breaking", s"--invaractOrgPolicy=$policy").create())
    )
    assert(rejected.result.violations.exists(_.violationType == ViolationType.OrgPolicyViolation))
    assert(rejected.getMessage.contains("catalog-required"))

    val satisfying = file(root, "satisfying.yaml", ExampleJob.contractYaml().replace("    location: out/report\n    format: parquet\n", "    location: out/report\n    format: parquet\n    catalog:\n      required: true\n"))
    val installed = InvaractBeam.fromOptions(PipelineOptionsFactory.fromArgs(s"--invaractContract=$satisfying", s"--invaractOrgPolicy=$policy").create())
    assert(installed.enforces)
  }

  // --- check only: a CI build checks the job with its production command line and one more flag ---

  private def sinkTo(root: Path): (Path, String) = {
    val events = root.resolve("events.jsonl")
    (events, file(root, "sink.properties", s"sink.enabled=true\nsink.class=com.invaract.verification.notification.FileNotificationSink\nsink.property.path=${events.toString.replace('\\', '/')}\n"))
  }

  private def published(events: Path): String = new String(Files.readAllBytes(events), StandardCharsets.UTF_8).replace(" ", "")

  test(BeamAttachSpec.CheckOnly) {
    val root = freshRoot()
    val contract = file(root, "contract.yaml", ExampleJob.contractYaml())
    val (events, sink) = sinkTo(root)
    val production = Array(runnerFlag, s"--invaractContract=$contract", "--invaractDelegate=com.invaract.beamadapter.CannotBeBuiltRunner", s"--invaractNotifyConfig=$sink")
    // as in production, the delegate is built, and here that fails
    assert(intercept[IllegalStateException](new ExampleJob(root).main(production)).getMessage.contains("credentials"))
    // with --invaractCheckOnly the job's own main runs to its end: the pipeline is checked, and handed to no runner
    val conforming = new ExampleJob(root)
    conforming.main(production :+ "--invaractCheckOnly=true")
    assert(conforming.written.isEmpty, "nothing ran, so nothing was written")
    assert(published(events).contains("\"status\":\"PASSED\"") && published(events).contains("\"checkOnly\":\"true\""), published(events))
    // and a job that breaks its contract fails the build, as it would have been refused in production
    val violating = new ExampleJob(root, "out/elsewhere")
    assert(blockedWith(violating.main(production :+ "--invaractCheckOnly=true")) == Set(ViolationType.OutputLocationMismatch))
    assert(violating.written.isEmpty)
  }

  test("check only: the delegate is not built but its name is still resolved, so a misspelt one fails the check") {
    val root = freshRoot()
    val contract = file(root, "contract.yaml", ExampleJob.contractYaml())
    val e = intercept[IllegalArgumentException](
      new ExampleJob(root).main(Array(runnerFlag, s"--invaractContract=$contract", "--invaractCheckOnly=true", "--invaractDelegate=DataflowRuner"))
    )
    assert(e.getMessage.contains("--invaractDelegate=DataflowRuner"))
  }

  test("check only: with dry-run mode it reports the contract the job would need, and runs nothing") {
    val root = freshRoot()
    val (events, sink) = sinkTo(root)
    val job = new ExampleJob(root, "out/anywhere")
    job.main(Array(runnerFlag, "--invaractDryRun=true", "--invaractCheckOnly=true", s"--invaractNotifyConfig=$sink"))
    assert(job.written.isEmpty)
    assert(published(events).contains("CONTRACT_INFERENCE") && published(events).contains("anywhere"))
    assert(published(events).contains("\"checkOnly\":\"true\""))
  }

  test("check only: a value that is neither true nor false stops the job, naming the flag; false runs it as usual") {
    val root = freshRoot()
    val contract = file(root, "contract.yaml", ExampleJob.contractYaml())
    val e = intercept[IllegalArgumentException](new ExampleJob(root).main(Array(runnerFlag, s"--invaractContract=$contract", "--invaractCheckOnly=yes")))
    assert(e.getMessage.contains("--invaractCheckOnly=yes must be true or false"))
    val (events, sink) = sinkTo(root)
    val job = new ExampleJob(root)
    job.main(Array(runnerFlag, s"--invaractContract=$contract", "--invaractCheckOnly= FALSE ", s"--invaractNotifyConfig=$sink"))
    assert(job.written.nonEmpty)
    assert(!published(events).contains("checkOnly"), "a run is not marked as a check")
  }

  test("check only: what run returns is finished at once, cannot be cancelled, and has no metrics") {
    val result = CheckedOnly
    import org.apache.beam.sdk.PipelineResult.State
    assert(result.getState == State.DONE && result.waitUntilFinish() == State.DONE && result.cancel() == State.DONE)
    assert(result.waitUntilFinish(org.joda.time.Duration.ZERO) == State.DONE)
    val metrics = result.metrics().allMetrics()
    assert(!metrics.getCounters.iterator.hasNext && !metrics.getDistributions.iterator.hasNext && !metrics.getGauges.iterator.hasNext)
  }

  // --- dry-run mode ---

  test(BeamAttachSpec.DryRunInference) {
    val root = freshRoot()
    val events = root.resolve("events.jsonl")
    val sinkConfig = file(root, "sink.properties", s"sink.enabled=true\nsink.class=com.invaract.verification.notification.FileNotificationSink\nsink.property.path=${events.toString.replace('\\', '/')}\n")
    val job = new ExampleJob(root, "out/anywhere")
    job.main(Array(runnerFlag, "--invaractDryRun=true", s"--invaractNotifyConfig=$sinkConfig"))
    assert(job.written.nonEmpty, "dry-run mode enforces nothing, so the write went ahead")
    val published = new String(Files.readAllBytes(events), StandardCharsets.UTF_8)
    assert(published.contains("CONTRACT_INFERENCE") && published.contains("INFERRED"))
    assert(published.contains("anywhere"), "the inferred contract names the write's location")
    assert(published.contains("\"engine\":\"beam\"") || published.contains("\"engine\": \"beam\""))
  }
}

/** The names of the tests `BeamAttestedClaimsSpec` points at, shared so a rename cannot leave an attestation dangling. */
object BeamAttachSpec {
  val ZeroCodeInstall = "zero-code install: an unmodified job is blocked by the runner and contract a platform names on its command line, before it writes anything"
  val LocationRefs = "location refs: a ref:// location in the contract is resolved through --invaractLocationMap and a real write is checked against it"
  val ContractRegistry = "contract registry: a registry:// reference is fetched from the registry named in the options and enforced"
  val OrganizationalPolicy = "organizational policy: a contract that breaks a policy is rejected when the runner is set up, before any job runs; one that satisfies it installs"
  val DryRunInference = "dry-run inference: dry-run mode lets a real write through and reports a contract inferred from it"
  val CheckOnly = "check only: --invaractCheckOnly checks an unmodified job in CI without running it or building its runner, and fails the build when the job breaks its contract"
}
