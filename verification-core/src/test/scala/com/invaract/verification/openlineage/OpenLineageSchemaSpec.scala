// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.openlineage

import org.scalatest.funsuite.AnyFunSuite

/** The schema checker the other OpenLineage tests rely on: it must find what is wrong, or their passing means nothing. */
class OpenLineageSchemaSpec extends AnyFunSuite {

  private val runId = "c3cef2ba-db3b-5736-bfcc-1ec79ef4f4d7"
  private val producer = OpenLineageMapper.Producer
  private val schemaUrl = OpenLineageMapper.EventSchemaUrl

  private def event(eventTime: String = "2026-01-01T00:00:00Z", id: String = runId, extra: String = "", facets: String = "{}"): String =
    s"""{"eventType":"START","eventTime":"$eventTime","producer":"$producer","schemaURL":"$schemaUrl",""" +
      s""""run":{"runId":"$id","facets":$facets},"job":{"namespace":"n","name":"j"}$extra}"""

  private val engine = s"""{"_producer":"$producer","_schemaURL":"${OpenLineageMapper.standard("ProcessingEngineRunFacet", "1-1-1")}","version":"3.5.9","name":"spark"}"""

  test("a valid event, with a valid standard facet, has no errors") {
    assert(OpenLineageSchema.errors(event()) == Nil)
    assert(OpenLineageSchema.errors(event(facets = s"""{"processing_engine":$engine}""")) == Nil)
  }

  test("a missing required field, a bad run id and a bad time are each found") {
    assert(OpenLineageSchema.errors(event().replace(""","job":{"namespace":"n","name":"j"}""", "")).exists(_.contains("'job' is required")))
    assert(OpenLineageSchema.errors(event(id = "not-a-uuid")).exists(_.contains("is not a uuid")))
    assert(OpenLineageSchema.errors(event(eventTime = "yesterday")).exists(_.contains("is not a date-time")))
    assert(OpenLineageSchema.errors(event().replace("\"START\"", "\"BEGIN\"")).exists(_.contains("is not one of")))
  }

  test("a facet is checked against its own schema: a wrong field type, a missing _schemaURL, an unvendored version") {
    assert(OpenLineageSchema.errors(event(facets = s"""{"processing_engine":${engine.replace("\"3.5.9\"", "3")}}""")).exists(_.contains("expected string")))
    assert(OpenLineageSchema.errors(event(facets = s"""{"processing_engine":{"_producer":"$producer","version":"1"}}""")).exists(_.contains("has no _schemaURL")))
    val drifted = engine.replace("1-1-1", "1-0-0")
    assert(OpenLineageSchema.errors(event(facets = s"""{"processing_engine":$drifted}""")).exists(_.contains("the vendored spec has")))
  }

  test("dataset facets, input and output side, are checked too") {
    val schema = s"""{"_producer":"$producer","_schemaURL":"${OpenLineageMapper.standard("SchemaDatasetFacet", "1-2-0")}","fields":[{"name":1}]}"""
    val errors = OpenLineageSchema.errors(event(extra = s""","outputs":[{"namespace":"n","name":"d","facets":{"schema":$schema}}]"""))
    assert(errors.exists(e => e.startsWith("$.outputs[0].facets.schema") && e.contains("expected string")))
  }
}
