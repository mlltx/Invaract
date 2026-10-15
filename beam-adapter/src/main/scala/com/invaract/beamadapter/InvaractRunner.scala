// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.runners.PipelineRunnerRegistrar
import org.apache.beam.sdk.{Pipeline, PipelineResult, PipelineRunner}

import scala.collection.JavaConverters._

/** A runner that executes nothing itself: it receives the finished pipeline in `run`, before anything has started, has
  * `BeamCheck` verify it against the contract (throwing, so the pipeline is never submitted, when it breaks it), and then
  * hands it to the real runner.
  *
  * Named on a job's command line it is how Invaract attaches to a job nobody here owns: `--runner=Invaract` (the name it
  * registers with Beam, `InvaractRunnerRegistrar`; the full class name works too), `--invaractContract=...`, and
  * `--invaractDelegate=<runner>` for the runner the pipeline should really go to (default `DirectRunner`). Beam builds it
  * through the static `fromOptions` below.
  */
final class InvaractRunner private (check: BeamCheck, delegate: PipelineRunner[PipelineResult]) extends PipelineRunner[PipelineResult] {

  override def run(pipeline: Pipeline): PipelineResult = {
    check.verify(pipeline)
    delegate.run(pipeline)
  }
}

object InvaractRunner {

  /** Beam calls this (by reflection, as the static method of this class) to build the runner `--runner` names. */
  def fromOptions(options: PipelineOptions): InvaractRunner =
    new InvaractRunner(InvaractBeam.fromOptions(options), delegateFor(options))

  /** A runner that verifies with `check` and then hands the pipeline to `delegate` - for code that already holds both. */
  def enforcing(check: BeamCheck, delegate: PipelineRunner[PipelineResult]): InvaractRunner = new InvaractRunner(check, delegate)

  /** The runner the pipeline is handed to: `--invaractDelegate`, by the simple name a runner registers with Beam or by its
    * class name. The name is a configured class, so it is loaded without initialising it and refused unless it really is a
    * `PipelineRunner` (docs/ADDING_AN_ENGINE_ADAPTER.md, "Loading a class named in configuration"); Beam then builds it exactly
    * as it builds the runner of `--runner`. */
  private def delegateFor(options: PipelineOptions): PipelineRunner[PipelineResult] = {
    val setting = Option(options.as(classOf[InvaractBeamOptions]).getInvaractDelegate).map(_.trim).filter(_.nonEmpty).getOrElse("DirectRunner")
    val runnerClass = resolve(setting)
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
