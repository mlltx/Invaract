// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import com.invaract.contract.{Contract, ContractParser, Dataset, LogicalSchema, LogicalType}
import com.invaract.ir.Plan
import com.invaract.testkit.{ColumnSource, ConformanceAdapter, EnvironmentUnavailable, ReferenceAdapter, ScenarioInput, ScenarioJob, ScenarioOutcome, ScenarioOutput}
import com.invaract.verification.{AdapterCapabilities, VerificationOptions}
import com.invaract.verification.notification.NotificationJson
import org.yaml.snakeyaml.Yaml

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Comparator
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

/** PythonIo as an adapter for the conformance kit, through the real package: each neutral `ScenarioJob` becomes a real Python
  * job (`python/tests/conformance_job.py`) that reads and writes through the `invaract` package, run as a team's job would be.
  * Its settings are `INVARACT_*` environment variables, as a platform would give them, and its checker is `PythonIoCheck` from
  * this test run's own classpath, so the checker under test is the code under test.
  *
  * The job materializes each input first, with a few rows: one with every column set, and, for each column the input lets be
  * empty, one where only that column is empty. The package judges a required column by the empty values the data holds
  * (decision 3), so an input the kit lets be empty must actually hold an empty value for the scenario to mean the same here.
  *
  * What happened is read back from the job's exit status and the validation events its file sink wrote. The Python environment
  * is `INVARACT_TEST_PYTHON` (default `python3`) with pyarrow installed; without it every scenario is `EnvironmentUnavailable`
  * (canceled, or failed under `INVARACT_CONFORMANCE_STRICT=true`, as CI runs it). `PythonIoConformanceAdapter` plays the same
  * scenarios in-process, with no Python, for the checker's own mutation testing.
  */
class PythonIoPackageConformanceAdapter extends ConformanceAdapter {
  import PythonIoPackageConformanceAdapter._

  override def capabilities: AdapterCapabilities =
    PythonIoCapabilities.declared.getOrElse(throw new IllegalStateException("PythonIo's capability declaration did not load"))

  override def run(scenarioId: String, contract: Contract, job: ScenarioJob, options: VerificationOptions): ScenarioOutcome = {
    environment.left.foreach(why => throw new EnvironmentUnavailable(why))
    if (job.output.registeredAs.isDefined) iceberg.left.foreach(why => throw new EnvironmentUnavailable(why))
    val root = Files.createTempDirectory("pythonio-conformance-")
    try {
      val events = root.resolve("events.jsonl")
      val settings = Map(
        "INVARACT_CHECKER" -> s"$javaCommand @${slashes(checkerArguments)} ${PythonIoCheck.getClass.getName.stripSuffix("$")}",
        "INVARACT_CONTRACT" -> slashes(write(root, "contract.yaml", ContractParser.write(registeredAsIceberg(contract, job)))),
        "INVARACT_NOTIFY_CONFIG" -> slashes(write(root, "sink.properties",
          s"sink.enabled=true\nsink.class=com.invaract.verification.notification.FileNotificationSink\nsink.property.path=${slashes(events)}\n"))
      ) ++ optionVariables(options) ++ (if (job.output.registeredAs.isDefined) icebergCatalog(root) else Map.empty)
      val (status, output) = python(List(new File("python/tests/conformance_job.py").getAbsolutePath, slashes(write(root, "job.json", jobJson(scenarioId, contract, job)))), root, settings)
      val validations = readValidations(events)
      val statuses = validations.map(_.status)
      val engines = validations.map(_.engine)
      status match {
        case 0       => ScenarioOutcome.Passed(statuses, eventEngines = engines)
        case Refused => ScenarioOutcome.Rejected(validations.lastOption.map(_.violationTypes).getOrElse(Set.empty), statuses, eventEngines = engines)
        case other   => throw new IllegalStateException(s"the Python job stopped with exit status $other (not a refusal by its contract):\n$output")
      }
    } finally deleteQuietly(root)
  }

  /** The package hands the checker no plan, only reads and writes; lineage and sensitivity are declared unsupported. */
  override def translation(scenarioId: String, job: ScenarioJob): Option[Plan] = None
}

private[pythonioadapter] object PythonIoPackageConformanceAdapter {

  /** What the job script exits with when the contract refuses it (`conformance_job.REFUSED`). */
  val Refused = 10

  final case class Validation(status: String, violationTypes: Set[String], engine: Option[String])

  private def asMap(o: AnyRef): Map[String, AnyRef] =
    Option(o).collect { case m: java.util.Map[_, _] => m.asInstanceOf[java.util.Map[String, AnyRef]].asScala.toMap }.getOrElse(Map.empty)
  private def asList(o: AnyRef): List[AnyRef] =
    Option(o).collect { case l: java.util.List[_] => l.asScala.toList.map(_.asInstanceOf[AnyRef]) }.getOrElse(Nil)

  /** The validation events the checker published, in order (JSON lines, which the YAML reader reads). */
  def readValidations(events: Path): List[Validation] =
    if (!Files.exists(events)) Nil
    else
      Files.readAllLines(events, StandardCharsets.UTF_8).asScala.toList.filter(_.trim.nonEmpty).map(line => asMap(new Yaml().load[AnyRef](line)))
        .filter(_.get("eventType").contains("CONTRACT_VALIDATION"))
        .map(e => Validation(e("status").toString, asList(e.getOrElse("violations", null)).map(v => asMap(v)("type").toString).toSet, asMap(e.getOrElse("job", null)).get("engine").map(_.toString)))

  /** The environment variables for the verification options a scenario runs with, as a platform would set them. */
  def optionVariables(options: VerificationOptions): Map[String, String] =
    List(
      "rejectUndeclaredInputs" -> options.rejectUndeclaredInputs,
      "rejectUndeclaredFields" -> options.rejectUndeclaredFields,
      "computeFingerprint" -> options.computeFingerprint,
      "staticDataQuality" -> options.staticDataQuality,
      "roleConsistency" -> options.roleConsistency,
      "runtimeDataQuality" -> options.runtimeDataQuality
    ).collect { case (name, true) => PythonIoConfigSource.variable(name) -> "true" }.toMap

  /** An output the scenario registers in a catalog is, for a Python job, an Iceberg table: its location names the catalog,
    * namespace and table, as Beam's adapter makes it a BigQuery table. The contract's other requirements are unchanged. */
  def icebergTable(table: String): String =
    s"${PythonIoConformanceAdapter.IcebergCatalog}.${PythonIoConformanceAdapter.IcebergNamespace}.$table"

  def registeredAsIceberg(contract: Contract, job: ScenarioJob): Contract =
    job.output.registeredAs.fold(contract)(table =>
      contract.copy(outputs = contract.outputs.map(o => if (o.location == job.output.location) o.copy(location = icebergTable(table)) else o)))

  /** The catalog `conformance`: SQLite, its warehouse under `root`, configured by environment as a platform would. */
  def icebergCatalog(root: Path): Map[String, String] = {
    val name = PythonIoConformanceAdapter.IcebergCatalog.toUpperCase
    Files.createDirectories(root.resolve("warehouse"))
    Map(
      s"PYICEBERG_CATALOG__${name}__TYPE" -> "sql",
      s"PYICEBERG_CATALOG__${name}__URI" -> s"sqlite:///${slashes(root.resolve("catalog.db"))}",
      s"PYICEBERG_CATALOG__${name}__WAREHOUSE" -> s"file://${slashes(root.resolve("warehouse"))}"
    )
  }

  /** The name the job reads or writes a dataset by: the declared one's, or, for a location the contract does not declare, the
    * location itself, which the contract refuses. */
  private def nameOf(datasets: List[Dataset], location: String): String = datasets.find(_.location == location).fold(location)(_.name)

  /** A type as `conformance_job.py` builds the pyarrow type for it. */
  def typeJson(t: LogicalType): Map[String, Any] = {
    import LogicalType._
    t match {
      case DecimalType(p, s)  => Map("kind" -> "decimal", "precision" -> p, "scale" -> s)
      case ArrayType(e)       => Map("kind" -> "array", "element" -> typeJson(e))
      case MapType(k, v)      => Map("kind" -> "map", "key" -> typeJson(k), "value" -> typeJson(v))
      case StructType(fields) => Map("kind" -> "struct", "fields" -> fields.map(f => Map("name" -> f.name, "type" -> typeJson(f.dataType), "nullable" -> f.nullable)))
      case s: Scalar          => Map("kind" -> s.typeName)
      case other              => throw new IllegalArgumentException(s"no Python frame holds $other")
    }
  }

  private def fields(schema: LogicalSchema): List[Map[String, Any]] =
    schema.fields.map(f => Map("name" -> f.name, "type" -> typeJson(f.dataType), "nullable" -> f.nullable))

  /** `job` as `conformance_job.py` reads it: each dataset by the name the job uses for it, inputs with the format their
    * contract declares (the data a job reads is in the format it was written in). */
  def jobJson(scenarioId: String, contract: Contract, job: ScenarioJob): String = {
    def input(i: ScenarioInput) =
      Map("name" -> nameOf(contract.inputs, i.location), "location" -> i.location,
        "format" -> contract.inputs.find(_.location == i.location).flatMap(_.format).getOrElse("parquet"), "fields" -> fields(i.schema)) ++
        (if (i.rows.isEmpty) Map.empty else Map("rows" -> i.rows)) ++
        (if (i.csvOptions.isEmpty) Map.empty else Map("csv" -> i.csvOptions))
    def output(o: ScenarioOutput) =
      Map("name" -> nameOf(contract.outputs, o.location), "format" -> (if (o.registeredAs.isDefined) "iceberg" else o.format), "saveMode" -> o.saveMode,
        "partitionBy" -> o.partitionBy, "partitionOverwriteMode" -> o.partitionOverwriteMode) ++
        (if (o.csvOptions.isEmpty) Map.empty else Map("csv" -> o.csvOptions))
    def source(s: ColumnSource): Map[String, Any] = s match {
      case ColumnSource.FromInput(i, c)    => Map("kind" -> "FromInput", "input" -> i, "column" -> c)
      case ColumnSource.CastInput(i, c, t) => Map("kind" -> "CastInput", "input" -> i, "column" -> c, "to" -> typeJson(t))
      case ColumnSource.NonNullLong(v)     => Map("kind" -> "NonNullLong", "value" -> v)
      case ColumnSource.UniqueId           => Map("kind" -> "UniqueId")
      case ColumnSource.RowCount           => Map("kind" -> "RowCount")
    }
    NotificationJson.anyToJson(Map(
      "scenario" -> scenarioId,
      "inputs" -> job.inputs.map(input),
      "columns" -> job.columns.map(c => Map("name" -> c.name, "source" -> source(c.source))),
      "outputFields" -> fields(ReferenceAdapter.outputSchema(job)),
      "output" -> output(job.output),
      "join" -> job.join.map(j => Map("left" -> j.leftColumn, "right" -> j.rightColumn)),
      "crossJoin" -> job.crossJoin,
      "filterColumn" -> job.filterColumn,
      "groupBy" -> job.groupBy,
      "rejoins" -> job.rejoins,
      "untranslatableWrite" -> job.untranslatableWrite,
      "extraWrites" -> job.extraWrites.map(e => Map("input" -> input(e.input), "output" -> output(e.output)))
    ))
  }

  // --- the Python environment, and the checker's JVM ---

  val Python: String = sys.env.getOrElse("INVARACT_TEST_PYTHON", "python3")

  /** The package, found by the job as a team's job finds an installed package. */
  val PythonPath: String = new File("python").getAbsolutePath

  val javaCommand: String = slashes(new File(System.getProperty("java.home"), "bin/java").toPath)

  /** An argument file (`java @file`) giving the checker this test run's own classpath. */
  lazy val checkerArguments: Path = {
    val file = Files.createTempFile("pythonio-checker-", ".args")
    file.toFile.deleteOnExit()
    Files.write(file, s"""-cp "${System.getProperty("java.class.path").replace("\\", "/")}"""".getBytes(StandardCharsets.UTF_8))
  }

  /** `Right(version)` when the Python environment can run a job through the package, else why not. */
  lazy val environment: Either[String, String] =
    scala.util.Try(python(List("-c", "import pyarrow, invaract; print(pyarrow.__version__)"), new File(".").toPath, Map.empty)).toEither match {
      case Right((0, version))  => Right(version.trim)
      case Right((status, out)) => Left(s"$Python cannot import pyarrow and invaract (exit status $status): ${out.trim.split("\n").lastOption.getOrElse("")}")
      case Left(e)              => Left(s"no Python to run jobs with ($Python: ${e.getMessage}); set INVARACT_TEST_PYTHON")
    }

  /** `Right(version)` when the Python environment can also write Iceberg tables (pyiceberg, with its SQL catalog), else why not:
    * only a scenario that registers its output needs it. */
  lazy val iceberg: Either[String, String] =
    scala.util.Try(python(List("-c", "import pyiceberg, sqlalchemy; print(pyiceberg.__version__)"), new File(".").toPath, Map.empty)).toEither match {
      case Right((0, version))  => Right(version.trim)
      case Right((status, out)) => Left(s"$Python cannot import pyiceberg and sqlalchemy (exit status $status), which a registered output needs: ${out.trim.split("\n").lastOption.getOrElse("")}")
      case Left(e)              => Left(s"no Python to run jobs with ($Python: ${e.getMessage})")
    }

  /** Runs `python args` in `directory` with the package on its path and `variables` set (and no other `INVARACT_*` setting);
    * returns the exit status and everything it printed. */
  def python(args: List[String], directory: Path, variables: Map[String, String]): (Int, String) = {
    val builder = new ProcessBuilder((Python :: args).asJava).redirectErrorStream(true).directory(directory.toFile)
    val env = builder.environment()
    env.keySet().removeIf(_.startsWith("INVARACT_"))
    env.put("PYTHONPATH", PythonPath)
    variables.foreach { case (k, v) => env.put(k, v) }
    val process = builder.start()
    val output = new String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
    if (!process.waitFor(5, TimeUnit.MINUTES)) { process.destroyForcibly(); throw new IllegalStateException(s"the Python job did not finish:\n$output") }
    (process.exitValue(), output)
  }

  def write(root: Path, name: String, text: String): Path = Files.write(root.resolve(name), text.getBytes(StandardCharsets.UTF_8))

  def slashes(path: Path): String = path.toAbsolutePath.toString.replace('\\', '/')

  def deleteQuietly(root: Path): Unit =
    scala.util.Try(Files.walk(root).sorted(Comparator.reverseOrder[Path]()).iterator().asScala.foreach(p => Files.deleteIfExists(p)))
}
