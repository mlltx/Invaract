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

  // -- exemption expiry look-ahead -----------------------------------------

  private def policyWithExemptionExpiring(daysFromNow: Int): String =
    s"""version: "1.0"
       |policies:
       |  - id: catalog-required
       |    type: require_catalog
       |    scope: outputs
       |exemptions:
       |  - contractId: violating
       |    policyIds: [catalog-required]
       |    reason: "migration pending"
       |    reviewBy: "${java.time.LocalDate.now().plusDays(daysFromNow.toLong)}"
       |""".stripMargin

  test("an exemption expiring within the default 30-day window is reported, without affecting the exit code") {
    val dir = tempDir()
    val policyPath = writeFile(dir, "policy.yaml", policyWithExemptionExpiring(10))
    writeFile(dir, "violating.yaml", violatingContract)
    val (code, out, _) = runCapturing(Array(policyPath.toString, dir.toString))
    assert(code == 0, "a look-ahead warning must never fail the run - the exemption is still fully active")
    assert(out.contains("expires"))
    assert(out.contains("contract 'violating'"))
    assert(out.contains("catalog-required"))
    assert(out.contains("migration pending"))
  }

  test("an exemption expiring well outside the default window is not reported") {
    val dir = tempDir()
    val policyPath = writeFile(dir, "policy.yaml", policyWithExemptionExpiring(90))
    writeFile(dir, "violating.yaml", violatingContract)
    val (_, out, _) = runCapturing(Array(policyPath.toString, dir.toString))
    assert(!out.contains("expires"))
  }

  test("--warn-expiring-within-days widens the window to catch an exemption the default would miss") {
    val dir = tempDir()
    val policyPath = writeFile(dir, "policy.yaml", policyWithExemptionExpiring(90))
    writeFile(dir, "violating.yaml", violatingContract)
    val (code, out, _) = runCapturing(Array("--warn-expiring-within-days", "120", policyPath.toString, dir.toString))
    assert(code == 0)
    assert(out.contains("expires"))
  }

  test("--warn-expiring-within-days works regardless of where it appears among the other arguments") {
    val dir = tempDir()
    val policyPath = writeFile(dir, "policy.yaml", policyWithExemptionExpiring(90))
    writeFile(dir, "violating.yaml", violatingContract)
    val (_, out, _) = runCapturing(Array(policyPath.toString, dir.toString, "--warn-expiring-within-days", "120"))
    assert(out.contains("expires"))
  }

  test("--warn-expiring-within-days with a non-integer value is a usage error, exit code 2") {
    val (code, _, err) = runCapturing(Array("--warn-expiring-within-days", "soon", "policy.yaml", "contracts/"))
    assert(code == 2)
    assert(err.contains("--warn-expiring-within-days"))
  }

  test("--warn-expiring-within-days with no following value is a usage error, exit code 2") {
    val (code, _, err) = runCapturing(Array("policy.yaml", "contracts/", "--warn-expiring-within-days"))
    assert(code == 2)
    assert(err.contains("--warn-expiring-within-days"))
  }

  test("no exemptions at all: no expiry warning, ordinary run unaffected") {
    val dir = tempDir()
    val policyPath = writeFile(dir, "policy.yaml", requireCatalogPolicy)
    writeFile(dir, "compliant.yaml", compliantContract)
    val (code, out, _) = runCapturing(Array(policyPath.toString, dir.toString))
    assert(code == 0)
    assert(!out.contains("expires"))
  }

  // -- --overlays (policy layering/inheritance) ------------------------------

  private val requireDescriptionOverlay =
    """version: "1.0"
      |policies:
      |  - id: bu-description-required
      |    type: require_dataset_description
      |    scope: outputs
      |""".stripMargin

  private val describedCompliantContract = compliantContract.stripSuffix("\n") + "\n    description: \"A compliant dataset.\"\n"

  // violatingContract has no catalog block *and* no description - not
  // suitable for isolating "only the base's rule fires," since it would
  // also violate the overlay's require_dataset_description. This adds a
  // description while keeping the missing catalog block, so it violates
  // require_catalog alone.
  private val describedButNoCatalogContract = violatingContract.stripSuffix("\n") + "\n    description: \"Not catalog-registered yet.\"\n"

  test("--overlays with no violations in any layer: exit code 0, every file marked OK") {
    val dir = tempDir()
    val basePath = writeFile(dir, "base.yaml", requireCatalogPolicy)
    val overlayPath = writeFile(dir, "overlay.yaml", requireDescriptionOverlay)
    writeFile(dir, "compliant.yaml", describedCompliantContract)
    val (code, out, _) = runCapturing(Array("--overlays", overlayPath.toString, basePath.toString, dir.toString))
    assert(code == 0, out)
    assert(out.contains("[ OK ]"))
  }

  test("--overlays: a violation from the overlay alone still fails the run, base is satisfied") {
    val dir = tempDir()
    val basePath = writeFile(dir, "base.yaml", requireCatalogPolicy)
    val overlayPath = writeFile(dir, "overlay.yaml", requireDescriptionOverlay)
    writeFile(dir, "compliant.yaml", compliantContract) // satisfies the base (has catalog), but declares no description
    val (code, out, _) = runCapturing(Array("--overlays", overlayPath.toString, basePath.toString, dir.toString))
    assert(code == 1)
    assert(out.contains("[FAIL]"))
    assert(out.contains("bu-description-required"))
  }

  test("--overlays: a violation from the base alone still fails the run, even if the overlay is fully satisfied") {
    val dir = tempDir()
    val basePath = writeFile(dir, "base.yaml", requireCatalogPolicy)
    val overlayPath = writeFile(dir, "overlay.yaml", requireDescriptionOverlay)
    writeFile(dir, "violating.yaml", describedButNoCatalogContract) // no catalog (violates base), has a description (satisfies overlay)
    val (code, out, _) = runCapturing(Array("--overlays", overlayPath.toString, basePath.toString, dir.toString))
    assert(code == 1)
    assert(out.contains("[FAIL]"))
    assert(out.contains("catalog-required"))
    assert(!out.contains("bu-description-required"), "the overlay's own rule is satisfied and must not fire")
  }

  test("--overlays: policy files from both layers are excluded from the contract sweep, not just the base") {
    val dir = tempDir()
    val basePath = writeFile(dir, "base.yaml", requireCatalogPolicy)
    val overlayPath = writeFile(dir, "overlay.yaml", requireDescriptionOverlay)
    // No actual contract in the directory - if either policy file were
    // mistakenly swept in as a contract to lint, ContractParser.parseFile
    // would reject it (no 'id'/'outputs') and this would report a parse
    // failure instead of "no contract files found".
    val (code, _, err) = runCapturing(Array("--overlays", overlayPath.toString, basePath.toString, dir.toString))
    assert(code == 1)
    assert(err.contains("No contract files"))
  }

  test("--overlays: a malformed overlay is reported by its own path, and the run fails before any contract is linted") {
    val dir = tempDir()
    val basePath = writeFile(dir, "base.yaml", requireCatalogPolicy)
    val overlayPath = writeFile(
      dir,
      "overlay.yaml",
      """version: "1.0"
        |policies:
        |  - id: dup
        |    type: require_catalog
        |  - id: dup
        |    type: field_naming_convention
        |    pattern: "^[a-z]+$"
        |""".stripMargin
    )
    writeFile(dir, "compliant.yaml", compliantContract)
    val (code, _, err) = runCapturing(Array("--overlays", overlayPath.toString, basePath.toString, dir.toString))
    assert(code == 1)
    assert(err.contains(overlayPath.toString))
    assert(err.contains("is invalid"))
    assert(err.contains("Duplicate policy id"))
  }

  test("--overlays: an overlay's own exemption suppresses only its own violation") {
    val dir = tempDir()
    val basePath = writeFile(dir, "base.yaml", requireCatalogPolicy)
    val overlayPath = writeFile(
      dir,
      "overlay.yaml",
      """version: "1.0"
        |policies:
        |  - id: bu-description-required
        |    type: require_dataset_description
        |    scope: outputs
        |exemptions:
        |  - contractId: compliant
        |    policyIds: [bu-description-required]
        |    reason: "legacy BU dataset"
        |""".stripMargin
    )
    writeFile(dir, "compliant.yaml", compliantContract) // satisfies the base, no description, but exempted by the overlay
    val (code, out, _) = runCapturing(Array("--overlays", overlayPath.toString, basePath.toString, dir.toString))
    assert(code == 0, out)
    assert(out.contains("[ OK ]"))
  }

  test("--overlays: an overlay's exemption cannot reach the base's own rule - fails loudly as an invalid policy") {
    val dir = tempDir()
    val basePath = writeFile(dir, "base.yaml", requireCatalogPolicy)
    val overlayPath = writeFile(
      dir,
      "overlay.yaml",
      """version: "1.0"
        |exemptions:
        |  - contractId: violating
        |    policyIds: [catalog-required]
        |    reason: "the BU wants this exempted"
        |""".stripMargin
    )
    writeFile(dir, "violating.yaml", violatingContract)
    val (code, _, err) = runCapturing(Array("--overlays", overlayPath.toString, basePath.toString, dir.toString))
    assert(code == 1)
    assert(err.contains("is invalid"))
    assert(err.contains("unknown policy id 'catalog-required'"))
  }

  test("--overlays accepts a comma-separated list of more than one overlay, applied in order") {
    val dir = tempDir()
    val basePath = writeFile(dir, "base.yaml", requireCatalogPolicy)
    val overlay1Path = writeFile(dir, "overlay1.yaml", requireDescriptionOverlay)
    val overlay2Path = writeFile(
      dir,
      "overlay2.yaml",
      """version: "1.0"
        |policies:
        |  - id: team-owner-required
        |    type: require_extension
        |    key: owner
        |""".stripMargin
    )
    writeFile(dir, "violating.yaml", violatingContract) // violates the base, and both overlays
    val (code, out, _) = runCapturing(Array("--overlays", s"$overlay1Path,$overlay2Path", basePath.toString, dir.toString))
    assert(code == 1)
    assert(out.contains("catalog-required"))
    assert(out.contains("bu-description-required"))
    assert(out.contains("team-owner-required"))
  }

  test("a policy id repeated across layers is a warning, not a failure") {
    val dir = tempDir()
    val basePath = writeFile(dir, "base.yaml", requireCatalogPolicy) // id "catalog-required"
    val overlayPath = writeFile(dir, "overlay.yaml", requireCatalogPolicy) // same id, independent rule
    writeFile(dir, "compliant.yaml", compliantContract)
    val (code, out, _) = runCapturing(Array("--overlays", overlayPath.toString, basePath.toString, dir.toString))
    assert(code == 0)
    assert(out.contains("more than one layer"))
  }

  test("--overlays with no following value is a usage error, exit code 2") {
    val (code, _, err) = runCapturing(Array("policy.yaml", "contracts/", "--overlays"))
    assert(code == 2)
    assert(err.contains("--overlays"))
  }

  test("with no --overlays at all, behavior is unchanged from a single-document run") {
    val dir = tempDir()
    val policyPath = writeFile(dir, "policy.yaml", requireCatalogPolicy)
    writeFile(dir, "violating.yaml", violatingContract)
    val (code, out, _) = runCapturing(Array(policyPath.toString, dir.toString))
    assert(code == 1)
    assert(out.contains("[FAIL]"))
    assert(out.contains("catalog-required"))
  }
}
