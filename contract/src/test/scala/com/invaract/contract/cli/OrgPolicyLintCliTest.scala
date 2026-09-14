// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract.cli

import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

class OrgPolicyLintCliTest extends AnyFunSuite {

  private def tempDir(): Path = Files.createTempDirectory("org-policy-lint-test")

  private def writeFile(dir: Path, name: String, content: String): Path = {
    val path = dir.resolve(name)
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
    path
  }

  private val compliantContract =
    """id: compliant
      |version: "1.0.0"
      |outputs:
      |  - name: out
      |    location: gold.compliant
      |    schema:
      |      fields:
      |        - name: id
      |          type: string
      |    catalog:
      |      required: true
      |""".stripMargin

  private val violatingContract =
    """id: violating
      |version: "1.0.0"
      |outputs:
      |  - name: out
      |    location: gold.violating
      |    schema:
      |      fields:
      |        - name: id
      |          type: string
      |""".stripMargin // no catalog block

  private val requireCatalogPolicy =
    """version: "1.0"
      |policies:
      |  - id: catalog-required
      |    type: require_catalog
      |    scope: outputs
      |""".stripMargin

  /** Runs the lint, capturing stdout/stderr separately rather than letting
    * them hit the real streams - `run` is `sys.exit`-free specifically so
    * this is possible.
    */
  private def runCapturing(args: Array[String]): (Int, String, String) = {
    val outBuf = new ByteArrayOutputStream()
    val errBuf = new ByteArrayOutputStream()
    val code = OrgPolicyLintCli.run(args, new PrintStream(outBuf, true, "UTF-8"), new PrintStream(errBuf, true, "UTF-8"))
    (code, outBuf.toString("UTF-8"), errBuf.toString("UTF-8"))
  }

  test("too few arguments: exit code 2, usage on stderr") {
    val (code, _, err) = runCapturing(Array("only-one-arg"))
    assert(code == 2)
    assert(err.contains("Usage:"))
  }

  test("a policy file that fails to parse: exit code 1, error on stderr") {
    val dir = tempDir()
    val policyPath = writeFile(dir, "policy.yaml", "policies: []\n") // missing required 'version'
    val (code, _, err) = runCapturing(Array(policyPath.toString, dir.toString))
    assert(code == 1)
    assert(err.contains("Failed to parse"))
  }

  test("a structurally invalid policy document (OrgPolicyValidator errors): exit code 1, errors listed on stderr") {
    val dir = tempDir()
    val policyPath = writeFile(
      dir,
      "policy.yaml",
      """version: "1.0"
        |policies:
        |  - id: dup
        |    type: require_catalog
        |  - id: dup
        |    type: field_naming_convention
        |    pattern: "^[a-z]+$"
        |""".stripMargin
    )
    val (code, _, err) = runCapturing(Array(policyPath.toString, dir.toString))
    assert(code == 1)
    assert(err.contains("is invalid"))
    assert(err.contains("Duplicate policy id"))
  }

  test("no contract files found under the given targets: exit code 1") {
    val dir = tempDir()
    val policyPath = writeFile(dir, "policy.yaml", requireCatalogPolicy)
    val emptyDir = Files.createTempDirectory(dir, "empty")
    val (code, _, err) = runCapturing(Array(policyPath.toString, emptyDir.toString))
    assert(code == 1)
    assert(err.contains("No contract files"))
  }

  test("every contract compliant: exit code 0, every file marked OK") {
    val dir = tempDir()
    val policyPath = writeFile(dir, "policy.yaml", requireCatalogPolicy)
    writeFile(dir, "compliant.yaml", compliantContract)
    val (code, out, _) = runCapturing(Array(policyPath.toString, dir.toString))
    assert(code == 0)
    assert(out.contains("[ OK ]"))
    assert(!out.contains("[FAIL]"))
  }

  test("a violating contract among a directory of contracts: exit code 1, both OK and FAIL reported") {
    val dir = tempDir()
    val policyPath = writeFile(dir, "policy.yaml", requireCatalogPolicy)
    writeFile(dir, "compliant.yaml", compliantContract)
    writeFile(dir, "violating.yaml", violatingContract)
    val (code, out, _) = runCapturing(Array(policyPath.toString, dir.toString))
    assert(code == 1)
    assert(out.contains("[ OK ]"))
    assert(out.contains("[FAIL]"))
    assert(out.contains("catalog-required"))
  }

  test("a single contract file target (not a directory) is linted directly") {
    val dir = tempDir()
    val policyPath = writeFile(dir, "policy.yaml", requireCatalogPolicy)
    val contractPath = writeFile(dir, "violating.yaml", violatingContract)
    val (code, out, _) = runCapturing(Array(policyPath.toString, contractPath.toString))
    assert(code == 1)
    assert(out.contains("[FAIL]"))
  }

  test("warn-mode violation never fails the run, but is still reported") {
    val dir = tempDir()
    val policyPath = writeFile(
      dir,
      "policy.yaml",
      """version: "1.0"
        |policies:
        |  - id: catalog-required
        |    type: require_catalog
        |    scope: outputs
        |    mode: warn
        |""".stripMargin
    )
    writeFile(dir, "violating.yaml", violatingContract)
    val (code, out, _) = runCapturing(Array(policyPath.toString, dir.toString))
    assert(code == 0, "a Warn-mode violation must never fail the lint run")
    assert(out.contains("[WARN]"))
    assert(out.contains("catalog-required"))
  }

  test("a contract file that fails to parse contributes a failure without crashing the whole run") {
    val dir = tempDir()
    val policyPath = writeFile(dir, "policy.yaml", requireCatalogPolicy)
    writeFile(dir, "compliant.yaml", compliantContract)
    writeFile(dir, "broken.yaml", "this: [is, not, a, valid, contract\n") // malformed YAML
    val (code, out, _) = runCapturing(Array(policyPath.toString, dir.toString))
    assert(code == 1)
    assert(out.contains("[ OK ]"), "the compliant contract must still be linted despite the broken one")
    assert(out.contains("[FAIL]"))
    assert(out.contains("could not parse"))
  }

  test("recurses into nested directories") {
    val dir = tempDir()
    val policyPath = writeFile(dir, "policy.yaml", requireCatalogPolicy)
    val nested = Files.createDirectories(dir.resolve("nested/deeper"))
    writeFile(nested, "violating.yaml", violatingContract)
    val (code, out, _) = runCapturing(Array(policyPath.toString, dir.toString))
    assert(code == 1)
    assert(out.contains("[FAIL]"))
  }

  test("an exemption suppresses the violation through the full CLI path") {
    val dir = tempDir()
    val policyPath = writeFile(
      dir,
      "policy.yaml",
      """version: "1.0"
        |policies:
        |  - id: catalog-required
        |    type: require_catalog
        |    scope: outputs
        |exemptions:
        |  - contractId: violating
        |    policyIds: [catalog-required]
        |    reason: "test"
        |""".stripMargin
    )
    writeFile(dir, "violating.yaml", violatingContract)
    val (code, out, _) = runCapturing(Array(policyPath.toString, dir.toString))
    assert(code == 0)
    assert(out.contains("[ OK ]"))
  }
}
