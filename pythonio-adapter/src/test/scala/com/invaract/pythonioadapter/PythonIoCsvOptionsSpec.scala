// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

import org.scalatest.funsuite.AnyFunSuite

/** A dry run's draft contract declares the CSV options each read and write reported (`check.csvOptions`), and only the
  * latest read of a location counts. */
class PythonIoCsvOptionsSpec extends AnyFunSuite {

  private def dryRun(requests: String*): String = {
    val dir = Files.createTempDirectory("invaract-csv-options")
    val events = dir.resolve("events.jsonl")
    val notify = dir.resolve("notify.properties")
    Files.write(notify, List("sink.enabled=true", "sink.class=com.invaract.verification.notification.FileNotificationSink",
      s"sink.property.path=$events").asJava, StandardCharsets.UTF_8)
    val run = new Run
    val open = s"""{"op": "open", "settings": {"dryRun": "true", "notifyConfig": "${notify.toString.replace("\\", "\\\\")}"}, "job": {"name": "csv.main", "runId": "run-1"}}"""
    val replies = (open +: requests :+ """{"op": "finish", "completed": true}""").map(run.answer)
    assert(replies.forall(_("ok") == true), replies.mkString("\n"))
    summary(events)
  }

  private def summary(events: Path): String = {
    val line = Files.readAllLines(events, StandardCharsets.UTF_8).asScala.filter(_.contains("mergedContractYaml")).last
    // the YAML is a JSON string: its line breaks are written \n
    "\"mergedContractYaml\"\\s*:\\s*\"((?:[^\"\\\\]|\\\\.)*)\"".r.findFirstMatchIn(line).get.group(1).replace("\\n", "\n").replace("\\\"", "\"")
  }

  private val schema = """"schema": [{"name": "id", "type": "int64", "nullable": true, "nulls": 0}]"""
  private def read(csv: String) = s"""{"op": "read", "name": "orders", "location": "in/orders.csv", "format": "csv", $schema$csv}"""
  private val write = s"""{"op": "write", "name": "report", "location": "out/report", "format": "csv", "saveMode": "overwrite", "schema": [{"name": "id", "type": "int64", "nullable": true}], "csv": {"delimiter": "|"}}"""
  private val written = """{"op": "written", "name": "report", "nulls": {"id": 0}}"""

  test("the draft declares the options a read and a write reported, and the format the input was read in") {
    val draft = dryRun(read(""", "csv": {"delimiter": ";", "encoding": "ISO-8859-1"}"""), write, written)
    assert(draft.contains("- name: orders\n  location: in/orders.csv\n  format: csv\n  schema:"), draft)
    assert(draft.contains("    nullable: false\n  csv:\n    delimiter: ;\n    encoding: ISO-8859-1\noutputs:"), draft)
    assert(draft.contains("  - orders\n  csv:\n    delimiter: '|'\n"), draft)
  }

  test("a location read again with no options is drafted with none") {
    val draft = dryRun(read(""", "csv": {"delimiter": ";"}"""), read(""), write, written)
    assert(!draft.contains("delimiter: ;"), draft)
    assert(draft.contains("delimiter: '|'"), draft)
  }
}
