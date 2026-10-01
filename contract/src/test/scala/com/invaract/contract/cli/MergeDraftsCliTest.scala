// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract.cli

import com.invaract.contract.{ContractParser, ContractValidator, ContractVersion}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

class MergeDraftsCliTest extends AnyFunSuite {

  private def tempDir(): Path = Files.createTempDirectory("merge-drafts-cli-test")

  private def writeFile(dir: Path, name: String, content: String): Path = {
    val path = dir.resolve(name)
    Files.createDirectories(path.getParent)
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
    path
  }

  /** A single-write draft as dry-run prints it. */
  private def draft(inputLocation: Option[String], outputLocation: String, inputType: String = "integer", derivedFrom: Option[String] = None): String = {
    val inputs = inputLocation.map(loc =>
      s"""inputs:
         |- name: input
         |  location: $loc
         |  schema:
         |    fields:
         |    - name: id
         |      type: $inputType
         |      required: true
         |      nullable: true
         |""".stripMargin).getOrElse("")
    val derived = derivedFrom.orElse(inputLocation.map(_ => "input")).map(n => s"  derivedFrom:\n  - $n\n").getOrElse("  derivedFrom: []\n")
    s"""id: inferred_contract
       |version: 0.1.0
       |status: draft
       |$inputs
       |outputs:
       |- name: output
       |  location: $outputLocation
       |  format: parquet
       |  schema:
       |    fields:
       |    - name: id
       |      type: integer
       |      required: true
       |      nullable: true
       |  saveMode: overwrite
       |$derived""".stripMargin
  }

  private def run(args: String*): (Int, String, String) = {
    val out = new ByteArrayOutputStream
    val err = new ByteArrayOutputStream
    val code = MergeDraftsCli.run(args.toArray, new PrintStream(out, true, "UTF-8"), new PrintStream(err, true, "UTF-8"))
    (code, out.toString("UTF-8"), err.toString("UTF-8"))
  }

  test("merges draft files given in order and prints a valid merged contract to standard output") {
    val dir = tempDir()
    val a = writeFile(dir, "a.yaml", draft(Some("/data/in.csv"), "/out/one.parquet"))
    val b = writeFile(dir, "b.yaml", draft(Some("/data/in.csv"), "/out/two.parquet"))

    val (code, out, err) = run("--id", "pipeline", "--version", "2.1", a.toString, b.toString)

    assert(code == 0, err)
    val merged = ContractParser.parse(out)
    assert(merged.id == "pipeline" && merged.version == ContractVersion(2, 1, 0))
    assert(merged.inputs.map(_.name) == List("in"))
    assert(merged.outputs.map(o => o.name -> o.derivedFrom) == List("one" -> Some(List("in")), "two" -> Some(List("in"))))
    assert(ContractValidator.validate(merged).errors.isEmpty)
  }

  test("defaults: id merged_contract, version 0.1.0") {
    val dir = tempDir()
    val (code, out, _) = run(writeFile(dir, "a.yaml", draft(None, "/out/one.parquet")).toString)

    assert(code == 0)
    val merged = ContractParser.parse(out)
    assert(merged.id == "merged_contract" && merged.version == ContractVersion(0, 1, 0))
    assert(merged.outputs.head.derivedFrom == Some(Nil))
  }

  test("a directory is walked, its drafts merged in PATH order regardless of the filesystem's listing order") {
    val dir = tempDir()
    writeFile(dir, "z_last.yaml", draft(None, "/out/zzz.parquet"))
    writeFile(dir, "sub/m_middle.yml", draft(None, "/out/mmm.parquet"))
    writeFile(dir, "a_first.yaml", draft(None, "/out/aaa.parquet"))
    writeFile(dir, "notes.txt", "not a draft")

    val (code, out, err) = run(dir.toString)

    assert(code == 0, err)
    assert(ContractParser.parse(out).outputs.map(_.name) == List("aaa", "mmm", "zzz"))
  }

  test("--output writes the merged contract to a file and says so; standard output carries no YAML") {
    val dir = tempDir()
    val a = writeFile(dir, "a.yaml", draft(Some("/data/in.csv"), "/out/one.parquet"))
    val target = dir.resolve("merged.yaml")

    val (code, out, _) = run("--output", target.toString, a.toString)

    assert(code == 0)
    assert(out.trim == s"merged 1 draft(s) into $target")
    assert(ContractParser.parseFile(target.toString).outputs.map(_.name) == List("one"))
  }

  test("conflicting drafts: every conflict is printed, exit 1, and nothing is written") {
    val dir = tempDir()
    val a = writeFile(dir, "a.yaml", draft(Some("/data/in.csv"), "/out/one.parquet"))
    val b = writeFile(dir, "b.yaml", draft(Some("/data/in.csv"), "/out/two.parquet", inputType = "string"))
    val target = dir.resolve("merged.yaml")

    val (code, out, err) = run("--output", target.toString, a.toString, b.toString)

    assert(code == 1 && out.isEmpty)
    assert(err.contains("conflict: input '/data/in.csv' has 2 different schemas"), err)
    assert(!Files.exists(target))
  }

  test("a malformed draft is reported by FILE name") {
    val dir = tempDir()
    val bad = writeFile(dir, "bad.yaml", draft(Some("/data/in.csv"), "/out/one.parquet", derivedFrom = Some("ghost")))

    val (code, _, err) = run(bad.toString)

    assert(code == 1)
    assert(err.contains(s"conflict: ${bad.toString}: ") && err.contains("'ghost'"), err)
  }

  test("a file that is not a contract: exit 1, naming the problem") {
    val dir = tempDir()
    val (code, _, err) = run(writeFile(dir, "broken.yaml", "id: [unterminated").toString)

    assert(code == 1)
    assert(err.startsWith("could not read a draft: "), err)
  }

  test("nothing found at the given path: exit 1") {
    val (code, _, err) = run(tempDir().resolve("missing").toString)

    assert(code == 1)
    assert(err.contains("no draft files found under:"), err)
  }

  test("usage errors exit 2 with the usage line: no drafts, a flag with no value, a malformed version") {
    val dir = tempDir()
    val a = writeFile(dir, "a.yaml", draft(None, "/out/one.parquet")).toString

    val none = run()
    val noValue = run(a, "--id")
    val badVersion = run("--version", "one.two", a)

    assert(none._1 == 2 && none._3.contains("no draft files given") && none._3.contains("Usage:"))
    assert(noValue._1 == 2 && noValue._3.contains("--id requires a value"))
    assert(badVersion._1 == 2 && badVersion._3.contains("Invalid contract version") && badVersion._3.contains("Usage:"))
    for (flag <- List("--version", "--output")) assert(run(a, flag)._3.contains(s"$flag requires a value"))
  }
}
