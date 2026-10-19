// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.verification.notification.JobInfo
import com.invaract.verification.{ConfigSource, ContractViolationException}

import java.io.PrintStream
import java.nio.file.{Files, Paths}
import scala.util.control.NonFatal

/** The checker a Beam pipeline written in Python is checked by: the program the Python package (`invaract_beam`, in this module's
  * `python/` folder) runs, once, after the job has built its pipeline and before it hands it to the real runner
  * (docs/BEAM_PYTHON_ADAPTER.md, D1 and D4).
  *
  * {{{
  * java -jar invaract-beam-checker.jar --pipeline=<file> [--sdk_version=2.77.0] [--runner=DataflowRunner] [--job_name=...]
  *     [--invaract_contract=...] [--invaract_dry_run=true] [--invaract_<setting>=...]
  * }}}
  *
  * `--pipeline` is the pipeline's portable form, as the Python SDK serializes it; the Invaract settings are the engine-neutral ones
  * (`BeamSettings`), spelled as the Python package spells its flags. The check is the Java adapter's own (`BeamCheck`), over the same
  * translation, with the Python adapter's declaration (`BeamCapabilities.python`) and events naming the engine `beam-python`.
  *
  * The exit status is what the Python package acts on: 0, submit the pipeline; 1, the contract refuses it (the explanation is
  * printed to standard output); 2, it could not be checked (an unknown setting, a file that is not a pipeline, a contract that
  * cannot be read), which the package also treats as a refusal - the job does not start unchecked.
  */
object PortableCheck {

  /** The engine every event this checker publishes names (`job.engine`). */
  val Engine = "beam-python"

  val Passed = 0
  val Refused = 1
  val NotChecked = 2

  def main(args: Array[String]): Unit = {
    val status = run(args, System.out)
    System.out.flush()
    // a sink that keeps a thread alive must not keep the job from starting, or from being refused
    System.exit(status)
  }

  /** Checks the pipeline `args` name, printing the reason to `out` when it is not let through; returns the exit status. */
  def run(args: Array[String], out: PrintStream): Int =
    try {
      val given = arguments(args)
      val settings = given.collect { case (k, v) if k.startsWith(PythonConfigSource.Prefix) => k.stripPrefix(PythonConfigSource.Prefix) -> v }
      val unknownSettings = settings.keySet -- PythonConfigSource.Flags
      require(unknownSettings.isEmpty, s"this checker does not know the setting(s) ${unknownSettings.toList.sorted.map("--" + PythonConfigSource.Prefix + _).mkString(", ")}")
      val unknown = given.keySet.filterNot(_.startsWith(PythonConfigSource.Prefix)) -- Arguments
      require(unknown.isEmpty, s"this checker does not know the argument(s) ${unknown.toList.sorted.map("--" + _).mkString(", ")}")
      val pipeline = given.getOrElse("pipeline", throw new IllegalArgumentException("--pipeline=<file> must name the pipeline to check"))
      val job = JobInfo(
        runId = given.get("job_name"),
        name = given.get("job_name"),
        engine = Some(Engine),
        engineVersion = given.get("sdk_version"),
        engineDetails = given.get("runner").map("runner" -> _).toMap,
        startTimeMs = Some(System.currentTimeMillis())
      )
      val check = InvaractBeam.fromConfig(new PythonConfigSource(settings), job, BeamCapabilities.python)
      check.verifyTranslation(BeamTranslator.translate(PortablePipeline.steps(Files.readAllBytes(Paths.get(pipeline))), PythonSteps))
      Passed
    } catch {
      case e: ContractViolationException =>
        out.println(e.getMessage)
        Refused
      case NonFatal(e) =>
        out.println(s"Invaract could not check the pipeline, so it is not submitted: ${e.getClass.getSimpleName}: ${e.getMessage}")
        NotChecked
    }

  /** The arguments other than the Invaract settings. */
  private val Arguments = Set("pipeline", "sdk_version", "runner", "job_name")

  /** `--name=value` arguments, by name; anything else is a mistake in how the checker was called. */
  private def arguments(args: Array[String]): Map[String, String] =
    args.toList.map { arg =>
      val at = arg.indexOf('=')
      require(arg.startsWith("--") && at > 2, s"an argument must be --name=value, not '$arg'")
      arg.substring(2, at) -> arg.substring(at + 1)
    }.toMap
}

/** The Python package's spelling of the engine-neutral settings: `--invaract_<name in snake case>` (`--invaract_dry_run`), the way
  * a Python pipeline's own options are spelled. The package forwards each flag it was given to the checker unchanged. */
private[beamadapter] final class PythonConfigSource(settings: Map[String, String]) extends ConfigSource {
  override def get(name: String): Option[String] = settings.get(PythonConfigSource.flag(name)).filter(_.trim.nonEmpty)
  override def describe(name: String): String = "--" + PythonConfigSource.Prefix + PythonConfigSource.flag(name)
}

private[beamadapter] object PythonConfigSource {
  val Prefix = "invaract_"

  /** `dryRun` as the Python package spells it after the prefix: `dry_run`. */
  def flag(name: String): String = name.flatMap(c => if (c.isUpper) "_" + c.toLower else c.toString)

  /** Every setting a Python pipeline can be given, as spelled after the prefix. */
  val Flags: Set[String] = BeamSettings.Names.map(flag).toSet
}
