// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import com.invaract.contract.{Contract, Dataset, LogicalSchema}
import com.invaract.ir.Plan
import com.invaract.testkit.{ConformanceAdapter, RecordingSink, ReferenceAdapter, ScenarioJob, ScenarioOutcome, ScenarioOutput}
import com.invaract.verification.{AdapterCapabilities, ContractViolationException, VerificationOptions}
import com.invaract.verification.notification.{JobInfo, NotificationJson}

/** PythonIo as an adapter for the conformance kit (docs/MULTI_ENGINE_ADAPTERS.md, Stage 4).
  *
  * A Python job's enforcement is the checker, `PythonIoCheck`, driven by the `invaract` package one request per write:
  * the package reads the written table's columns from its Arrow schema and names the declared output and every input the
  * run has read so far (it cannot tell which of them fed the write). This adapter plays the package's part. It sends the checker the same protocol lines the package will
  * (`write` with the columns of the table the scenario's job produces, then `finish`), through `Run.answer`, the code a
  * real run's requests go through, and turns a refusal reply into `sink.rejected`. The one shortcut is the start of the
  * run: the kit hands the contract and options over directly, as it does to every adapter, so the run is opened on them
  * (`Run.start`) instead of on settings that name a file.
  *
  * The package is not built yet (docs/PYTHON_IO_ADAPTER_DESIGN.md, milestone 1); when it is, its own tests prove it
  * sends what this adapter sends.
  */
class PythonIoConformanceAdapter extends ConformanceAdapter {

  override def capabilities: AdapterCapabilities =
    PythonIoCapabilities.declared.getOrElse(throw new IllegalStateException("PythonIo's capability declaration did not load"))

  override def run(scenarioId: String, contract: Contract, job: ScenarioJob, options: VerificationOptions): ScenarioOutcome = {
    val sink = new RecordingSink
    val run = new Run
    try {
      run.start(contract, options, Some(sink), JobInfo(name = Some(scenarioId), engine = Some(PythonIoCheck.Engine)))
      // the package cannot tell which reads fed a write, so each write carries every read the run has made before it
      val mainReads = job.inputs.map(i => i.location -> i.schema)
      val extraReads = job.extraWrites.scanLeft(mainReads)((before, w) => before :+ (w.input.location -> w.input.schema)).tail
      val writes = PythonIoConformanceAdapter.writeRequest(contract, job.output, mainReads, ReferenceAdapter.outputSchema(job)) ::
        job.extraWrites.zip(extraReads).map { case (w, reads) => PythonIoConformanceAdapter.writeRequest(contract, w.output, reads.distinct, w.input.schema) }
      val replies = (writes :+ """{"op": "finish", "completed": true}""").iterator.map(run.answer)
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

  /** The name the package would send for a dataset at `location`: the declared one's, or, for a location the contract does
    * not declare, the location itself, which the checker treats as an undeclared dataset. */
  private def nameOf(datasets: List[Dataset], location: String): String = datasets.find(_.location == location).fold(location)(_.name)

  private def columns(schema: LogicalSchema): List[Map[String, Any]] =
    schema.fields.map(f => Map("name" -> f.name, "type" -> f.dataType.catalogString, "nullable" -> f.nullable))

  /** The `write` request the package sends for a table with `schema` written to `output`, after reading `inputs`. */
  def writeRequest(contract: Contract, output: ScenarioOutput, inputs: List[(String, LogicalSchema)], schema: LogicalSchema): String =
    NotificationJson.anyToJson(
      Map(
        "op" -> "write",
        "name" -> nameOf(contract.outputs, output.location),
        "format" -> output.format,
        "saveMode" -> output.saveMode,
        "schema" -> columns(schema),
        "reads" -> inputs.map { case (location, s) => Map("name" -> nameOf(contract.inputs, location), "schema" -> columns(s)) }
      )
    )
}
