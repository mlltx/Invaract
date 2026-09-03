// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.networknt.schema.{JsonSchemaFactory, SpecVersion}
import org.scalatest.funsuite.AnyFunSuite
import org.yaml.snakeyaml.Yaml

import java.io.{File, FileInputStream}
import scala.collection.JavaConverters._

/** Validates contract/schema/invaract-contract.schema.json — the public,
  * language-agnostic contract for authoring Invaract contracts — against
  * the same real fixtures ContractParser/ContractValidator are tested
  * against. This schema is a separate, standalone artifact (nothing in the
  * contract module's own runtime consults it); this spec is what keeps it
  * from silently drifting out of sync with the Scala implementation it
  * documents as that implementation evolves.
  *
  * Deliberately not exhaustive of every ContractValidator rule: see the
  * schema file's own top-level `description` and docs/CONTRACT_MODEL.md's
  * "JSON Schema" section for what's schema-level (structural shape) versus
  * what only the real parser/validator can check (duplicate names,
  * cross-field business rules).
  */
class ContractSchemaSpec extends AnyFunSuite {
  private val objectMapper = new ObjectMapper()
  private val schema = {
    val schemaNode = objectMapper.readTree(new File("schema/invaract-contract.schema.json"))
    JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(schemaNode)
  }

  private def yamlAsJson(file: File): JsonNode = {
    val stream = new FileInputStream(file)
    try {
      val raw = new Yaml().load[Any](stream)
      objectMapper.valueToTree(raw)
    } finally {
      stream.close()
    }
  }

  private def fixture(name: String): File = new File(s"src/test/resources/fixtures/$name")
  private def demoContract(name: String): File = new File(s"../demo/contracts/$name")

  private def assertConformant(file: File): Unit = {
    val errors = schema.validate(yamlAsJson(file)).asScala
    assert(errors.isEmpty, s"${file.getPath} should conform to the schema but didn't: ${errors.mkString(", ")}")
  }

  private def assertRejected(file: File, expectedMissingProperty: String): Unit = {
    val errors = schema.validate(yamlAsJson(file)).asScala
    assert(errors.nonEmpty, s"${file.getPath} should have been rejected by the schema but wasn't")
    assert(
      errors.exists(_.getMessage.contains(expectedMissingProperty)),
      s"expected a violation mentioning '$expectedMissingProperty', got: ${errors.mkString(", ")}"
    )
  }

  test("schema itself is a valid Draft 2020-12 document") {
    // getSchema(...) above already throws if the schema is malformed; this
    // test exists so that failure surfaces with a clear name rather than
    // as a mysterious static-initializer failure in every other test here.
    assert(schema != null)
  }

  test("every valid contract fixture conforms to the schema") {
    assertConformant(fixture("customer_orders_v1.yaml"))
    assertConformant(fixture("customer_orders_v1_1_compatible.yaml"))
    assertConformant(fixture("customer_orders_v2_breaking.yaml"))
  }

  test("a fixture with only ContractValidator warnings (not errors) still conforms to the schema") {
    // warnings_field_issues.yaml has a duplicate field name and an
    // unrecognized field type - both real ContractValidator findings, but
    // Warning severity, not Error. The schema doesn't (and shouldn't)
    // reject on either: duplicate-name detection is out of JSON Schema's
    // reach, and an unrecognized type is explicitly not a schema-level
    // enum violation (see the schema's "type" field description).
    assertConformant(fixture("warnings_field_issues.yaml"))
  }

  test("both real demo contracts conform to the schema") {
    assertConformant(demoContract("invaract_output.yaml"))
    // The "broken" demo contract is only broken relative to what the demo
    // plugin actually produces (StructuralVerifier catches that at
    // verification time) - as a document, it's a perfectly well-formed
    // contract, so it should conform too.
    assertConformant(demoContract("invaract_output_broken_example.yaml"))
  }

  test("a contract missing the required 'id' is rejected by the schema") {
    assertRejected(fixture("invalid_missing_id.yaml"), "id")
  }

  test("a contract with no outputs is rejected by the schema") {
    // ContractParser itself would accept this (outputs defaults to an
    // empty list); ContractValidator is what actually rejects it
    // ("Contract must declare at least one output dataset"). The schema
    // requires 'outputs' up front so it doesn't accept a document
    // ContractValidator would immediately reject anyway - see the
    // schema's top-level description for why this is deliberately
    // stricter than the bare parser.
    assertRejected(fixture("invalid_no_outputs.yaml"), "outputs")
  }
}
