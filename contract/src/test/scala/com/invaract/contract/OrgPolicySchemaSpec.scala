// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.networknt.schema.{JsonSchemaFactory, SpecVersion}
import org.scalatest.funsuite.AnyFunSuite
import org.yaml.snakeyaml.Yaml

import java.io.{File, FileInputStream}
import scala.collection.JavaConverters._

/** Validates contract/schema/invaract-org-policy.schema.json against the
  * same real fixtures OrgPolicyParser/OrgPolicyValidator are tested against
  * — the org-policy counterpart to `ContractSchemaSpec`. See that class's
  * own doc for why this exists as a separate spec from the parser/validator
  * unit tests: nothing in the `contract` module's own runtime consults this
  * schema file, so this is what keeps it from silently drifting out of sync
  * with the Scala implementation it documents.
  */
class OrgPolicySchemaSpec extends AnyFunSuite {
  private val objectMapper = new ObjectMapper()
  private val schema = {
    val schemaNode = objectMapper.readTree(new File("schema/invaract-org-policy.schema.json"))
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

  private def fixture(name: String): File = new File(s"src/test/resources/fixtures/org-policy/$name")

  private def assertConformant(file: File): Unit = {
    val errors = schema.validate(yamlAsJson(file)).asScala
    assert(errors.isEmpty, s"${file.getPath} should conform to the schema but didn't: ${errors.mkString(", ")}")
  }

  private def assertRejected(file: File, expectedMention: String): Unit = {
    val errors = schema.validate(yamlAsJson(file)).asScala
    assert(errors.nonEmpty, s"${file.getPath} should have been rejected by the schema but wasn't")
    assert(
      errors.exists(_.getMessage.contains(expectedMention)),
      s"expected a violation mentioning '$expectedMention', got: ${errors.mkString(", ")}"
    )
  }

  test("schema itself is a valid Draft 2020-12 document") {
    assert(schema != null)
  }

  test("every valid fixture conforms to the schema") {
    assertConformant(fixture("valid_basic.yaml"))
    assertConformant(fixture("valid_with_condition_and_injection.yaml"))
    assertConformant(fixture("valid_with_exemptions.yaml"))
    assertConformant(fixture("valid_with_custom_policy_type.yaml"))
  }

  // Structurally well-formed as documents (a schema-conformant "policies"/
  // "exemptions" shape), even though OrgPolicyValidator would reject their
  // content - duplicate-id and unknown-policy-id-reference detection is a
  // cross-field business rule, out of JSON Schema's reach, the same
  // "conforms to the schema, fails ContractValidator" split
  // ContractSchemaSpec's own "warnings_field_issues.yaml" case documents.
  test("a fixture with only OrgPolicyValidator-level issues (not schema-level) still conforms") {
    assertConformant(fixture("invalid_duplicate_policy_id.yaml"))
    assertConformant(fixture("invalid_malformed_require_field.yaml"))
    assertConformant(fixture("invalid_bad_naming_pattern.yaml"))
    assertConformant(fixture("invalid_exemption_unknown_policy.yaml"))
  }

  test("a policy document missing the required 'version' is rejected by the schema") {
    assertRejected(fixture("invalid_missing_version.yaml"), "version")
  }

  test("a policy rule missing the required 'id' is rejected by the schema") {
    assertRejected(fixture("invalid_policy_missing_id.yaml"), "id")
  }

  test("a policy rule missing the required 'type' is rejected by the schema") {
    assertRejected(fixture("invalid_policy_missing_type.yaml"), "type")
  }

  test("an exemption with a malformed reviewBy date is rejected by the schema") {
    assertRejected(fixture("invalid_exemption_bad_date.yaml"), "reviewBy")
  }
}
