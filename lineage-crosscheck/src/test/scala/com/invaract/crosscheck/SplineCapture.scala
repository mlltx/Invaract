// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.crosscheck

import org.apache.commons.configuration.Configuration
import za.co.absa.spline.harvester.dispatcher.AbstractJsonLineageDispatcher

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

/** A Spline lineage dispatcher that appends each execution plan (producer API 1.2 JSON) to the file
  * `spark.spline.lineageDispatcher.capture.path` names, one per line, for `Captured.fromSpline`. Spline's own console
  * dispatcher writes the same JSON to standard output, where it would be mixed with everything else. */
class SplineCapture(conf: Configuration) extends AbstractJsonLineageDispatcher {
  private val path = Paths.get(conf.getString("path"))
  override def name: String = "capture"
  // what AbstractJsonLineageDispatcher hands over is "<entity> (apiVersion: 1.2): <json>"
  override def send(json: String): Unit = synchronized {
    val body = json.substring(json.indexOf('{'))
    Files.write(path, (body.replace('\n', ' ') + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  }
}
