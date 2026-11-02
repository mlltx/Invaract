// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import com.invaract.contract.{Contract, Dataset, LogicalField, LogicalSchema, LogicalType}
import com.invaract.ir.Plan
import com.invaract.testkit.{ConformanceAdapter, RecordingSink, ReferenceAdapter, ScenarioInput, ScenarioJob, ScenarioOutcome, ScenarioOutput}
import com.invaract.verification.{AdapterCapabilities, ContractViolationException, VerificationOptions}
import com.invaract.verification.notification.{JobInfo, NotificationJson}

/** PythonIo as an adapter for the conformance kit (docs/MULTI_ENGINE_ADAPTERS.md, Stage 4).
  *
  * A Python job's enforcement is the checker, `PythonIoCheck`, driven by the `invaract` package: a `read` for each input the job
  * reads, then for each table it writes a `write` describing the table before it is staged and a `written` with the empty values
  * each column turned out to hold. This adapter plays the package's part. It sends the checker the protocol lines the package
  * will send, with each column's type as pyarrow names it, through `Run.answer`, the code a real run's requests go through, and
  * turns a refusal reply into `sink.rejected`. The one shortcut is the start of the run: the kit hands the contract and options
  * over directly, as it does to every adapter, so the run is opened on them (`Run.start`) instead of on settings that name a
  * file.
  *
  * A column the kit's job can leave empty is sent with one empty value: the package counts what the data holds (decision 3), and
  * the kit's job is taken to produce one. A job the kit marks untranslatable hands `invaract.write` a table with no Arrow stream.
  *
  * `PythonIoPackageConformanceAdapter` runs the same scenarios as real Python jobs through the package itself; this one needs no
  * Python, so the checker's own mutation testing runs it.
  */
class PythonIoConformanceAdapter extends ConformanceAdapter {

  override def capabilities: AdapterCapabilities =
    PythonIoCapabilities.declared.getOrElse(throw new IllegalStateException("PythonIo's capability declaration did not load"))

  override def run(scenarioId: String, contract: Contract, job: ScenarioJob, options: VerificationOptions): ScenarioOutcome = {
    import PythonIoConformanceAdapter._
    val sink = new RecordingSink
    val run = new Run
    try {
      run.start(contract, options, Some(sink), JobInfo(name = Some(scenarioId), engine = Some(PythonIoCheck.Engine)))
      val main = job.inputs.map(readRequest(contract, _)) ++ (
        if (job.untranslatableWrite) List(unverifiableWrite(contract, job.output))
        else writeRequests(contract, job.output, ReferenceAdapter.outputSchema(job))
      )
      // a further write reads its own input first, unless the job has read it already
      val extra = job.extraWrites.foldLeft((job.inputs.map(_.location).toSet, List.empty[String])) { case ((read, lines), w) =>
        val readIt = if (read.contains(w.input.location)) Nil else List(readRequest(contract, w.input))
        (read + w.input.location, lines ++ readIt ++ writeRequests(contract, w.output, w.input.schema))
      }._2
      val replies = (main ++ extra :+ """{"op": "finish", "completed": true}""").iterator.map(run.answer)
      replies.find(_("ok") != true) match {
        case None                                                   => sink.passed
        case Some(reply) if reply.get("status").contains("refused") => sink.rejected(reply("violations").asInstanceOf[List[String]].toSet)
        case Some(reply)                                            => throw new IllegalStateException(s"the checker did not check scenario '$scenarioId': ${reply.get("reason").getOrElse(reply)}")
      }
    } catch {
      case e: ContractViolationException => sink.rejected(e.result.violations.map(_.violationType).toSet)
    }
  }

  /** The package hands the checker no plan, only reads and writes; lineage and sensitivity are declared unsupported. */
  override def translation(scenarioId: String, job: ScenarioJob): Option[Plan] = None
}

object PythonIoConformanceAdapter {

  /** The name the package would send for a dataset at `location`: the declared one's, or, for a location the contract does not
    * declare, the location itself, which the checker treats as an undeclared dataset. */
  private def nameOf(datasets: List[Dataset], location: String): String = datasets.find(_.location == location).fold(location)(_.name)

  private def json(fields: (String, Any)*): String = NotificationJson.anyToJson(fields.toMap)

  /** A column as the package describes it: pyarrow's type name, Arrow's nullable flag (pandas sets it on every column), and, for
    * data already seen, the empty values it holds. */
  private def column(f: LogicalField, withNulls: Boolean): Map[String, Any] =
    Map("name" -> f.name, "type" -> arrowName(f.dataType), "nullable" -> true) ++ (if (withNulls) Map("nulls" -> nullsOf(f)) else Map.empty)

  private def nullsOf(f: LogicalField): Long = if (f.nullable) 1L else 0L

  def readRequest(contract: Contract, input: ScenarioInput): String =
    json("op" -> "read", "name" -> nameOf(contract.inputs, input.location), "schema" -> input.schema.fields.map(column(_, withNulls = true)))

  /** The `write` describing a table with `schema` before it is staged, and the `written` once it is. */
  def writeRequests(contract: Contract, output: ScenarioOutput, schema: LogicalSchema): List[String] = {
    val name = nameOf(contract.outputs, output.location)
    List(
      json("op" -> "write", "name" -> name, "format" -> output.format, "saveMode" -> output.saveMode, "schema" -> schema.fields.map(column(_, withNulls = false))),
      json("op" -> "written", "name" -> name, "nulls" -> schema.fields.map(f => f.name -> nullsOf(f)).toMap)
    )
  }

  def unverifiableWrite(contract: Contract, output: ScenarioOutput): String =
    json("op" -> "write", "name" -> nameOf(contract.outputs, output.location), "unverifiable" -> "it offers no __arrow_c_stream__")

  /** The type as pyarrow names it (`str(pa_type)`), the inverse of `ArrowSchemas.dataType` for every type a frame can hold. */
  def arrowName(t: LogicalType): String = {
    import LogicalType._
    t match {
      case StringType           => "string"
      case BooleanType          => "bool"
      case ByteType             => "int8"
      case ShortType            => "int16"
      case IntegerType          => "int32"
      case LongType             => "int64"
      case FloatType            => "float"
      case DoubleType           => "double"
      case DateType             => "date32[day]"
      case TimestampType        => "timestamp[us, tz=UTC]"
      case TimestampNtzType     => "timestamp[us]"
      case BinaryType           => "binary"
      case TimeType             => "time64[us]"
      case IntervalType         => "month_day_nano_interval"
      case DecimalType(p, s)    => s"decimal128($p, $s)"
      case ArrayType(e)         => s"list<item: ${arrowName(e)}>"
      case MapType(k, v)        => s"map<${arrowName(k)}, ${arrowName(v)}>"
      case StructType(fields)   => fields.map(f => s"${f.name}: ${arrowName(f.dataType)}${if (f.nullable) "" else " not null"}").mkString("struct<", ", ", ">")
      case OtherType(name, _)   => name
      case other                => throw new IllegalArgumentException(s"no Arrow type holds $other")
    }
  }
}
