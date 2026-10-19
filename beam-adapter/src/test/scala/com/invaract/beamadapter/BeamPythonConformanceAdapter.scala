// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.contract.{Contract, ContractParser, LogicalSchema}
import com.invaract.ir.Plan
import com.invaract.testkit.{ColumnSource, ConformanceAdapter, EnvironmentUnavailable, ReferenceAdapter, ScenarioInput, ScenarioJob, ScenarioOutcome}
import com.invaract.verification.{AdapterCapabilities, VerificationOptions}
import com.invaract.verification.notification.NotificationJson
import org.yaml.snakeyaml.Yaml

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

/** Beam's Python SDK as an adapter for the conformance kit: each neutral `ScenarioJob` becomes a real Python Beam pipeline
  * (`python/tests/conformance_job.py`, the Python counterpart of `BeamScenarioJobs`), run as a team's job would be, with
  * `--runner=invaract_beam.InvaractRunner` and the `--invaract_*` flags - the adapter's real path: the Python package writes the
  * pipeline's portable form, runs the Java checker on it (`PortableCheck`, here from this module's own test classpath rather
  * than the assembled jar), and hands the pipeline to the real runner only if it passes.
  *
  * What happened is read back from what the checker published: its validation events, written by the file sink a platform
  * would configure, and the job's exit status. A job the checker allows really runs on Python's in-process runner, over empty
  * data, unless its sink has no in-process stand-in (BigQuery: Python has no in-memory BigQuery like Java's) or it never
  * ends (a stream); those are handed to a runner that runs nothing, after the same check.
  *
  * The Python environment is `INVARACT_TEST_PYTHON` (default `python3`) with `apache-beam[gcp]` and pyarrow installed; without
  * it every scenario is `EnvironmentUnavailable` (canceled, or failed under `INVARACT_CONFORMANCE_STRICT=true`, as CI's Python job runs).
  */
class BeamPythonConformanceAdapter extends ConformanceAdapter {
  import BeamPythonConformanceAdapter._

  override def capabilities: AdapterCapabilities =
    BeamCapabilities.python.getOrElse(throw new IllegalStateException("the Beam Python declaration did not load"))

  override def run(scenarioId: String, contract: Contract, job: ScenarioJob, options: VerificationOptions): ScenarioOutcome = {
    environment.left.foreach(why => throw new EnvironmentUnavailable(why))
    val root = Files.createTempDirectory("beam-python-conformance-")
    try {
      val events = root.resolve("events.jsonl")
      val sinkConfig = write(root, "sink.properties",
        s"sink.enabled=true\nsink.class=com.invaract.verification.notification.FileNotificationSink\nsink.property.path=${slashes(events)}\n")
      val target = if (job.output.registeredAs.isEmpty && !contract.outputs.exists(_.saveMode.isDefined)) "files" else "bigquery"
      val delegate = if (target == "bigquery" || job.streaming) "conformance_job.CheckOnlyRunner" else "DirectRunner"
      val flags = List(
        "--runner=invaract_beam.InvaractRunner",
        s"--invaract_delegate=$delegate",
        s"--invaract_java=$javaCommand",
        s"--invaract_java_options=@${slashes(checkerClasspath)}",
        s"--invaract_contract=${slashes(write(root, "contract.yaml", ContractParser.write(contract)))}",
        s"--invaract_notify_config=${slashes(sinkConfig)}"
      ) ++ optionFlags(options) ++
        (if (target == "bigquery") List("--project=conformance", "--temp_location=gs://conformance/tmp") else Nil) ++
        (if (job.streaming) List("--streaming") else Nil)
      val (status, output) = python(List(new File("python/tests/conformance_job.py").getAbsolutePath, slashes(write(root, "job.json", jobJson(job, root, target)))) ++ flags)
      val validations = readValidations(events)
      def outcome[A](make: (List[String], Set[String], List[Option[String]]) => A): A =
        make(validations.map(_.status), validations.lastOption.toList.flatMap(_.unverifiableInputs).toSet, validations.map(_.engine))
      status match {
        case 0 => outcome((statuses, unverifiable, engines) => ScenarioOutcome.Passed(statuses, unverifiableInputs = unverifiable, eventEngines = engines))
        case Refused =>
          val types = validations.lastOption.map(_.violationTypes).getOrElse(Set.empty)
          outcome((statuses, unverifiable, engines) => ScenarioOutcome.Rejected(types, statuses, unverifiableInputs = unverifiable, eventEngines = engines))
        case other => throw new IllegalStateException(s"the Python job stopped with exit status $other (not a refusal by its contract):\n$output")
      }
    } finally BeamConformanceAdapter.deleteQuietly(root)
  }

  /** The functions of a Python pipeline are pickled code, and lineage is declared unsupported, so the kit never asks. */
  override def translation(scenarioId: String, job: ScenarioJob): Option[Plan] = None
}

private[beamadapter] object BeamPythonConformanceAdapter {

  /** What the job script exits with when the contract refuses it (`conformance_job.REFUSED`). */
  val Refused = 10

  final case class Validation(status: String, violationTypes: Set[String], unverifiableInputs: List[String], engine: Option[String])

  private def asMap(o: AnyRef): Map[String, AnyRef] = Option(o).collect { case m: java.util.Map[_, _] => m.asInstanceOf[java.util.Map[String, AnyRef]].asScala.toMap }.getOrElse(Map.empty)
  private def asList(o: AnyRef): List[AnyRef] = Option(o).collect { case l: java.util.List[_] => l.asScala.toList.map(_.asInstanceOf[AnyRef]) }.getOrElse(Nil)

  /** The validation events the checker published, in order (JSON lines, which the YAML reader reads). */
  def readValidations(events: Path): List[Validation] =
    if (!Files.exists(events)) Nil
    else
      Files.readAllLines(events, StandardCharsets.UTF_8).asScala.toList.filter(_.trim.nonEmpty).map(line => asMap(new Yaml().load[AnyRef](line)))
        .filter(_.get("eventType").contains("CONTRACT_VALIDATION"))
        .map { e =>
          Validation(
            e("status").toString,
            asList(e.getOrElse("violations", null)).map(v => asMap(v)("type").toString).toSet,
            asList(e.getOrElse("unverifiableInputs", null)).map(u => asMap(u)("inputName").toString),
            asMap(e.getOrElse("job", null)).get("engine").map(_.toString)
          )
        }

  /** The flags for the verification options a scenario runs with, as a platform would give them. */
  def optionFlags(options: VerificationOptions): List[String] =
    List(
      "reject_undeclared_inputs" -> options.rejectUndeclaredInputs,
      "reject_undeclared_fields" -> options.rejectUndeclaredFields,
      "compute_fingerprint" -> options.computeFingerprint,
      "static_data_quality" -> options.staticDataQuality,
      "role_consistency" -> options.roleConsistency
    ).collect { case (flag, true) => s"--invaract_$flag=true" }

  /** `job` as `conformance_job.py` reads it. */
  def jobJson(job: ScenarioJob, root: Path, target: String): String = {
    def fields(schema: LogicalSchema) = schema.fields.map(f => Map("name" -> f.name, "type" -> f.dataType.catalogString, "nullable" -> f.nullable))
    def dataset(input: ScenarioInput) = Map("location" -> input.location, "fields" -> fields(input.schema))
    def source(s: ColumnSource): Map[String, Any] = s match {
      case ColumnSource.FromInput(i, c)    => Map("kind" -> "FromInput", "input" -> i, "column" -> c)
      case ColumnSource.CastInput(i, c, t) => Map("kind" -> "CastInput", "input" -> i, "column" -> c, "to" -> t.catalogString)
      case ColumnSource.NonNullLong(v)     => Map("kind" -> "NonNullLong", "value" -> v)
      case ColumnSource.UniqueId           => Map("kind" -> "UniqueId")
      case ColumnSource.RowCount           => Map("kind" -> "RowCount")
    }
    NotificationJson.anyToJson(Map(
      "root" -> slashes(root),
      "target" -> target,
      "inputs" -> job.inputs.map(dataset),
      "columns" -> job.columns.map(c => Map("name" -> c.name, "source" -> source(c.source))),
      "outputFields" -> fields(ReferenceAdapter.outputSchema(job)),
      "output" -> Map("location" -> job.output.location, "format" -> job.output.format, "saveMode" -> job.output.saveMode),
      "join" -> job.join.map(j => Map("left" -> j.leftColumn, "right" -> j.rightColumn)),
      "filterColumn" -> job.filterColumn,
      "untranslatableWrite" -> job.untranslatableWrite,
      "streaming" -> job.streaming,
      "opaqueStep" -> job.opaqueStep,
      "crossJoin" -> job.crossJoin,
      "groupBy" -> job.groupBy,
      "extraWrites" -> job.extraWrites.map(e => Map("input" -> dataset(e.input), "output" -> Map("location" -> e.output.location)))
    ))
  }

  // --- the Python environment, and the checker's JVM ---

  val Python: String = sys.env.getOrElse("INVARACT_TEST_PYTHON", "python3")

  /** The Python package and the scenario jobs, found by the job as a team's job finds an installed package. */
  val PythonPath: String = List(new File("python"), new File("python/tests")).map(_.getAbsolutePath).mkString(File.pathSeparator)

  /** The scenario jobs alone, without the package: how a job runs on a machine where the package was never installed. */
  val WithoutPackage: String = new File("python/tests").getAbsolutePath

  val javaCommand: String = slashes(new File(System.getProperty("java.home"), "bin/java").toPath)

  /** An argument file (`java @file`) giving the checker this test run's own classpath, so the checker under test is the code under test. */
  lazy val checkerClasspath: Path = {
    val file = Files.createTempFile("beam-python-checker-", ".args")
    file.toFile.deleteOnExit()
    val classpath = System.getProperty("java.class.path").replace("\\", "/")
    Files.write(file, s"""-cp "$classpath"""".getBytes(StandardCharsets.UTF_8))
  }

  /** `Right(version)` when the Python environment can run a Beam pipeline with the package, else why not. */
  lazy val environment: Either[String, String] =
    scala.util.Try(python(List("-c", "import apache_beam, pyarrow, invaract_beam; print(apache_beam.__version__)"))).toEither match {
      case Right((0, version)) => Right(version.trim)
      case Right((status, out)) => Left(s"$Python cannot import apache_beam, pyarrow and invaract_beam (exit status $status): ${out.trim.split("\n").lastOption.getOrElse("")}")
      case Left(e)              => Left(s"no Python to run Beam pipelines with ($Python: ${e.getMessage}); set INVARACT_TEST_PYTHON")
    }

  /** Runs `python args`, with the package on its path; returns the exit status and everything it printed. */
  def python(args: List[String], pythonPath: String = PythonPath): (Int, String) = {
    val builder = new ProcessBuilder((Python :: args).asJava).redirectErrorStream(true)
    builder.environment().put("PYTHONPATH", pythonPath)
    val process = builder.start()
    val output = new String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
    if (!process.waitFor(5, TimeUnit.MINUTES)) { process.destroyForcibly(); throw new IllegalStateException(s"the Python job did not finish:\n$output") }
    (process.exitValue(), output)
  }

  def write(root: Path, name: String, text: String): Path = Files.write(root.resolve(name), text.getBytes(StandardCharsets.UTF_8))

  def slashes(path: Path): String = path.toAbsolutePath.toString.replace('\\', '/')
}
