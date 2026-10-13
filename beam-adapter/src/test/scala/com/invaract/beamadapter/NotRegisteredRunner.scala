// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.{Pipeline, PipelineResult, PipelineRunner}

/** A runner no registrar lists, which a platform names by class: it counts the pipelines it is handed and passes them on. */
final class NotRegisteredRunner(options: PipelineOptions) extends PipelineRunner[PipelineResult] {
  override def run(pipeline: Pipeline): PipelineResult = {
    NotRegisteredRunner.runs += 1
    DirectRunner.fromOptions(options).run(pipeline)
  }
}

object NotRegisteredRunner {
  @volatile var runs = 0
  def fromOptions(options: PipelineOptions): NotRegisteredRunner = new NotRegisteredRunner(options)
}
