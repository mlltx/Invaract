// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import org.apache.beam.sdk.metrics.{MetricQueryResults, MetricResults, MetricsFilter}
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.runners.PipelineRunnerRegistrar
import org.apache.beam.sdk.{Pipeline, PipelineResult, PipelineRunner}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/** A runner that executes nothing itself: it receives the finished pipeline in `run`, before anything has started, has
  * `BeamCheck` verify it against the contract (throwing, so the pipeline is never submitted, when it breaks it), and then
  * hands it to the real runner.
  *
  * Named on a job's command line it is how Invaract attaches to a job nobody here owns: `--runner=Invaract` (the name it
  * registers with Beam, `InvaractRunnerRegistrar`; the full class name works too), `--invaractContract=...`, and
  * `--invaractDelegate=<runner>` for the runner the pipeline should really go to (default `DirectRunner`). Beam builds it
  * through the static `fromOptions` below.
  *
  * With `--invaractCheckOnly=true` there is no real runner: the pipeline is checked and `run` returns at once (`CheckedOnly`),
  * so a CI build can check a job with its production command line and one more flag, and nothing runs. The delegate is not
  * built - building one can need what CI does not have (Dataflow's checks a project and credentials) - but its name is still
  * resolved, so a misspelt one fails there too.
  */
final class InvaractRunner private (check: BeamCheck, delegate: Option[PipelineRunner[PipelineResult]]) extends PipelineRunner[PipelineResult] {

  override def run(pipeline: Pipeline): PipelineResult = {
    check.verify(pipeline)
    delegate match {
      case Some(runner) => runner.run(pipeline)
      case None =>
        InvaractRunner.Log.info("Invaract: the pipeline was checked against its contract and, as --invaractCheckOnly asks, not run")
        CheckedOnly
    }
  }
}

object InvaractRunner {

  /** Beam calls this (by reflection, as the static method of this class) to build the runner `--runner` names. */
  def fromOptions(options: PipelineOptions): InvaractRunner = {
    val check = InvaractBeam.fromOptions(options)
    if (InvaractBeamOptions.checkOnly(options)) {
      resolve(delegateName(options))
      new InvaractRunner(check, None)
    } else new InvaractRunner(check, Some(delegateFor(options)))
  }

  /** A runner that verifies with `check` and then hands the pipeline to `delegate` - for code that already holds both. */
  def enforcing(check: BeamCheck, delegate: PipelineRunner[PipelineResult]): InvaractRunner = new InvaractRunner(check, Some(delegate))

  private val Log = LoggerFactory.getLogger(classOf[InvaractRunner])

  private def delegateName(options: PipelineOptions): String =
    Option(options.as(classOf[InvaractBeamOptions]).getInvaractDelegate).map(_.trim).filter(_.nonEmpty).getOrElse("DirectRunner")

  /** The runner the pipeline is handed to: `--invaractDelegate`, by the simple name a runner registers with Beam or by its
    * class name. The name is a configured class, so it is loaded without initialising it and refused unless it really is a
    * `PipelineRunner` (docs/ADDING_AN_ENGINE_ADAPTER.md, "Loading a class named in configuration"); Beam then builds it exactly
    * as it builds the runner of `--runner`. */
  private def delegateFor(options: PipelineOptions): PipelineRunner[PipelineResult] = {
    val runnerClass = resolve(delegateName(options))
    val own = options.getRunner
    try {
      options.setRunner(runnerClass)
      PipelineRunner.fromOptions(options).asInstanceOf[PipelineRunner[PipelineResult]]
    } finally options.setRunner(own)
  }

  private def resolve(name: String): Class[_ <: PipelineRunner[_]] = {
    val runner = resolveAny(name)
    // a pipeline handed to this runner would be verified again and handed to itself, without end
    if (runner == classOf[InvaractRunner])
      throw new IllegalArgumentException(s"--invaractDelegate=$name names the Invaract runner itself; name the runner the pipeline should run on.")
    runner
  }

  private def resolveAny(name: String): Class[_ <: PipelineRunner[_]] = {
    val registered = java.util.ServiceLoader.load(classOf[PipelineRunnerRegistrar]).asScala.toList
      .flatMap(_.getPipelineRunners.asScala).find(c => c.getSimpleName == name || c.getName == name)
    registered.getOrElse {
      val loaded =
        try Class.forName(name, false, Thread.currentThread.getContextClassLoader)
        catch {
          case e: ClassNotFoundException =>
            throw new IllegalArgumentException(s"--invaractDelegate=$name names no runner: it is neither a registered runner nor a class on the classpath.", e)
        }
      if (!classOf[PipelineRunner[_]].isAssignableFrom(loaded))
        throw new IllegalArgumentException(s"--invaractDelegate=$name is ${loaded.getName}, which is not a Beam PipelineRunner.")
      loaded.asSubclass(classOf[PipelineRunner[_]])
    }
  }
}

/** Registers `InvaractRunner` with Beam (through `META-INF/services`), so a command line can name it `--runner=Invaract` or
  * `--runner=InvaractRunner` as it names Beam's own runners, instead of by its full class name. */
final class InvaractRunnerRegistrar extends PipelineRunnerRegistrar {
  override def getPipelineRunners: java.lang.Iterable[Class[_ <: PipelineRunner[_]]] =
    java.util.Collections.singletonList[Class[_ <: PipelineRunner[_]]](classOf[InvaractRunner])
}

/** What `run` returns for a pipeline that was only checked (`--invaractCheckOnly=true`): nothing ran, so it is finished at once,
  * cannot be cancelled, and has no metrics. A job that waits for its pipeline and reads its metrics goes on as after a run that
  * did nothing. The empty metrics are made by Beam's own factory, called by reflection because the number of kinds of metric it
  * takes has grown across the releases the adapter supports. */
private[beamadapter] object CheckedOnly extends PipelineResult {
  override def getState: PipelineResult.State = PipelineResult.State.DONE
  override def cancel(): PipelineResult.State = PipelineResult.State.DONE
  override def waitUntilFinish(duration: org.joda.time.Duration): PipelineResult.State = PipelineResult.State.DONE
  override def waitUntilFinish(): PipelineResult.State = PipelineResult.State.DONE

  override def metrics(): MetricResults = new MetricResults {
    override def queryMetrics(filter: MetricsFilter): MetricQueryResults = NoMetrics
  }

  private lazy val NoMetrics: MetricQueryResults = {
    val create = classOf[MetricQueryResults].getMethods
      .find(m => m.getName == "create" && java.lang.reflect.Modifier.isStatic(m.getModifiers))
      .getOrElse(throw new IllegalStateException("this Beam release has no MetricQueryResults.create"))
    create.invoke(null, Seq.fill(create.getParameterCount)(java.util.Collections.emptyList[AnyRef]()): _*).asInstanceOf[MetricQueryResults]
  }
}
