// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract.cli

import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

class CrossContractLintCliTest extends AnyFunSuite {

  private def tempDir(): Path = Files.createTempDirectory("cross-contract-lint-test")

  private def writeFile(dir: Path, name: String, content: String): Path = {
    val path = dir.resolve(name)
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
    path
  }

  private val producerContract =
    """id: customer_master_pipeline
      |version: "1.0.0"
      |outputs:
      |  - name: customer_master
      |    location: gold.customer_master
      |    type: DATA_ASSET
      |    schema:
      |      fields:
      |        - name: id
      |          type: string
      |""".stripMargin

  private val consistentConsumerContract =
    """id: downstream_pipeline
      |version: "1.0.0"
      |inputs:
      |  - name: customer_master
      |    location: gold.customer_master
      |    type: DATA_ASSET
      |outputs:
      |  - name: out
      |    location: gold.downstream
      |    schema:
      |      fields:
      |        - name: id
      |          type: string
      |""".stripMargin

  private val contradictingConsumerContract =
    """id: downstream_pipeline
      |version: "1.0.0"
      |inputs:
      |  - name: customer_master
      |    location: gold.customer_master
      |    type: SOURCE
      |outputs:
      |  - name: out
      |    location: gold.downstream
      |    schema:
      |      fields:
      |        - name: id
      |          type: string
      |""".stripMargin

  /** Runs the lint, capturing stdout/stderr separately rather than letting
    * them hit the real streams - `run` is `sys.exit`-free specifically so
    * this is possible, mirroring `OrgPolicyLintCliTest`'s identical helper.
    */
  private def runCapturing(args: Array[String]): (Int, String, String) = {
    val outBuf = new ByteArrayOutputStream()
    val errBuf = new ByteArrayOutputStream()
    val code = CrossContractLintCli.run(args, new PrintStream(outBuf, true, "UTF-8"), new PrintStream(errBuf, true, "UTF-8"))
    (code, outBuf.toString("UTF-8"), errBuf.toString("UTF-8"))
  }

  test("no arguments: exit code 2, usage on stderr") {
    val (code, _, err) = runCapturing(Array.empty)
    assert(code == 2)
    assert(err.contains("Usage:"))
  }

  test("a target with no contract files at all: exit code 1, error on stderr") {
    val dir = tempDir()
    val (code, _, err) = runCapturing(Array(dir.toString))
    assert(code == 1)
    assert(err.contains("No contract files"))
  }

  test("a contract file that fails to parse: exit code 1, error on stdout") {
    val dir = tempDir()
    writeFile(dir, "broken.yaml", "outputs: []\n") // missing required 'id'/'version'
    val (code, out, _) = runCapturing(Array(dir.toString))
    assert(code == 1)
    assert(out.contains("could not parse"))
  }

  test("every contract found, no cross-contract issue: exit code 0, OK on stdout") {
    val dir = tempDir()
    writeFile(dir, "producer.yaml", producerContract)
    writeFile(dir, "consumer.yaml", consistentConsumerContract)
    val (code, out, _) = runCapturing(Array(dir.toString))
    assert(code == 0)
    assert(out.contains("[ OK ]"))
    assert(out.contains("2 contract(s) checked"))
  }

  test("a real DATA_ASSET-vs-SOURCE contradiction: exit code 1, the issue printed on stdout") {
    val dir = tempDir()
    writeFile(dir, "producer.yaml", producerContract)
    writeFile(dir, "consumer.yaml", contradictingConsumerContract)
    val (code, out, _) = runCapturing(Array(dir.toString))
    assert(code == 1)
    assert(out.contains("[FAIL]"))
    assert(out.contains("gold.customer_master"))
    assert(out.contains("DATA_ASSET"))
    assert(out.contains("SOURCE"))
  }

  test("multiple directory targets are all scanned together") {
    val dirA = tempDir()
    val dirB = tempDir()
    writeFile(dirA, "producer.yaml", producerContract)
    writeFile(dirB, "consumer.yaml", contradictingConsumerContract)
    val (code, out, _) = runCapturing(Array(dirA.toString, dirB.toString))
    assert(code == 1)
    assert(out.contains("[FAIL]"))
  }

  test("a single file target works the same as a directory") {
    val dir = tempDir()
    val producerPath = writeFile(dir, "producer.yaml", producerContract)
    val consumerPath = writeFile(dir, "consumer.yaml", consistentConsumerContract)
    val (code, out, _) = runCapturing(Array(producerPath.toString, consumerPath.toString))
    assert(code == 0)
    assert(out.contains("[ OK ]"))
  }

  // -- --org-policy / TypeGuaranteeValidator -----------------------------------

  private val mismatchedSchemaConsumerContract =
    """id: downstream_pipeline
      |version: "1.0.0"
      |inputs:
      |  - name: customer_master
      |    location: gold.customer_master
      |    type: DATA_ASSET
      |    schema:
      |      fields:
      |        - name: id
      |          type: long
      |outputs:
      |  - name: out
      |    location: gold.downstream
      |    schema:
      |      fields:
      |        - name: id
      |          type: string
      |""".stripMargin

  private def policyEnabling(checkType: String, mode: String = "enforce"): String =
    s"""version: "1.0"
       |typeGuarantees:
       |  enabled: [$checkType]
       |  mode: $mode
       |""".stripMargin

  test("--org-policy with no value: exit code 2") {
    val (code, _, err) = runCapturing(Array("--org-policy"))
    assert(code == 2)
    assert(err.contains("--org-policy"))
  }

  test("--org-policy pointing to an unparseable policy: exit code 1, error on stderr") {
    val dir = tempDir()
    writeFile(dir, "producer.yaml", producerContract)
    val policyPath = writeFile(dir, "policy.yaml", "typeGuarantees: {}\n") // missing required 'version'
    val (code, _, err) = runCapturing(Array("--org-policy", policyPath.toString, dir.toString))
    assert(code == 1)
    assert(err.contains("Failed to parse organizational policy"))
  }

  test("--org-policy omitted entirely: no type guarantee checking happens at all") {
    val dir = tempDir()
    writeFile(dir, "producer.yaml", producerContract)
    writeFile(dir, "consumer.yaml", mismatchedSchemaConsumerContract)
    val (code, out, _) = runCapturing(Array(dir.toString))
    assert(code == 0, out)
    assert(!out.contains("data_asset_schema_consistency"))
  }

  test("--org-policy enabling data_asset_schema_consistency: a real mismatch blocks (exit 1, [FAIL])") {
    val dir = tempDir()
    writeFile(dir, "producer.yaml", producerContract) // id: string
    writeFile(dir, "consumer.yaml", mismatchedSchemaConsumerContract) // id: long
    val policyPath = writeFile(dir, "policy.yaml", policyEnabling("data_asset_schema_consistency"))
    val (code, out, _) = runCapturing(Array("--org-policy", policyPath.toString, dir.toString))
    assert(code == 1)
    assert(out.contains("[FAIL] [data_asset_schema_consistency]"))
    assert(out.contains("gold.customer_master"))
  }

  test("--org-policy enabling data_asset_schema_consistency in warn mode: reported but never blocks (exit 0, [WARN])") {
    val dir = tempDir()
    writeFile(dir, "producer.yaml", producerContract)
    writeFile(dir, "consumer.yaml", mismatchedSchemaConsumerContract)
    val policyPath = writeFile(dir, "policy.yaml", policyEnabling("data_asset_schema_consistency", mode = "warn"))
    val (code, out, _) = runCapturing(Array("--org-policy", policyPath.toString, dir.toString))
    assert(code == 0, out)
    assert(out.contains("[WARN] [data_asset_schema_consistency]"))
  }

  test("--org-policy enabling data_asset_downstream_consumption: a consumed DATA_ASSET Conforms (exit 0, [ OK ])") {
    val dir = tempDir()
    writeFile(dir, "producer.yaml", producerContract)
    writeFile(dir, "consumer.yaml", consistentConsumerContract)
    val policyPath = writeFile(dir, "policy.yaml", policyEnabling("data_asset_downstream_consumption"))
    val (code, out, _) = runCapturing(Array("--org-policy", policyPath.toString, dir.toString))
    assert(code == 0, out)
    assert(out.contains("[ OK ] [data_asset_downstream_consumption]"))
  }

  test("--org-policy enabling data_asset_downstream_consumption: an unconsumed DATA_ASSET is CannotDetermine, never blocks") {
    val dir = tempDir()
    writeFile(dir, "producer.yaml", producerContract)
    val policyPath = writeFile(dir, "policy.yaml", policyEnabling("data_asset_downstream_consumption"))
    val (code, out, _) = runCapturing(Array("--org-policy", policyPath.toString, dir.toString))
    assert(code == 0, out)
    assert(out.contains("[WARN] [data_asset_downstream_consumption]"))
  }
}
