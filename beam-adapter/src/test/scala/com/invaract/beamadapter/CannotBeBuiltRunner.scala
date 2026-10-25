// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.{Pipeline, PipelineResult, PipelineRunner}

/** A runner that cannot be built where the tests run, as a production runner cannot be in a CI build without its project and
  * credentials (Dataflow's checks both when it is built): a check-only run must never build it. */
final class CannotBeBuiltRunner private () extends PipelineRunner[PipelineResult] {
  override def run(pipeline: Pipeline): PipelineResult = throw new IllegalStateException("never built, so never run")
}

object CannotBeBuiltRunner {
  def fromOptions(options: PipelineOptions): CannotBeBuiltRunner =
    throw new IllegalStateException("this runner needs a project and credentials that a CI build does not have")
}
