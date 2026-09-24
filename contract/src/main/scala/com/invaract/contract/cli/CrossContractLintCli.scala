// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract.cli

import com.invaract.contract._

import java.io.File

/** Standalone command-line lint: parses every contract document found under
  * one or more files/directories and reports every `DATA_ASSET`/`SOURCE`
  * cross-contract contradiction `CrossContractValidator` finds between them
  * — no Spark involved at all, the same fast pre-commit/CI shape
  * `OrgPolicyLintCli` already establishes for org-policy linting. See
  * docs/CONTRACT_MODEL.md's "Input and Output Types" section, "Cross-contract
  * validation."
  *
  * {{{
  * sbt "contract/runMain com.invaract.contract.cli.CrossContractLintCli contracts/"
  * sbt "contract/runMain com.invaract.contract.cli.CrossContractLintCli contracts/team-a/ contracts/team-b/"
  * }}}
  *
  * Exit codes: `0` — every discovered contract parsed and no cross-contract
  * issue was found between any pair; `1` — a contract failed to parse, no
  * contract files were found, or at least one cross-contract issue was
  * found; `2` — usage error (no targets given).
  */
object CrossContractLintCli {
  def main(args: Array[String]): Unit = sys.exit(run(args, System.out, System.err))

  private val Usage = "Usage: CrossContractLintCli <contract-file-or-directory>..."

  /** Deliberately `sys.exit`-free so it's directly unit-testable (a real
    * `System.exit` would kill the test JVM); `out`/`err` are injected for
    * the same reason, mirroring `OrgPolicyLintCli.run`'s exact shape.
    */
  private[cli] def run(args: Array[String], out: java.io.PrintStream, err: java.io.PrintStream): Int = {
    if (args.isEmpty) {
      err.println(Usage)
      return 2
    }

    val contractFiles = args.toList.flatMap(findContractFiles).distinct.sorted
    if (contractFiles.isEmpty) {
      err.println(s"No contract files (*.yaml/*.yml) found under: ${args.mkString(", ")}")
      return 1
    }

    var hadParseFailure = false
    val contracts = contractFiles.flatMap { path =>
      try {
        Some(ContractParser.parseFile(new File(path)))
      } catch {
        case e: ContractParseException =>
          hadParseFailure = true
          out.println(s"[FAIL] $path: could not parse - ${e.getMessage}")
          None
      }
    }

    // A set containing a contract that failed to parse can't be
    // meaningfully cross-checked at all (a missing contract's own
    // outputs/inputs are simply absent from `contracts`, which could hide
    // a real cross-contract issue involving it) - reported as a failure
    // immediately, the same "fail loudly rather than silently check a
    // partial set" principle `OrgPolicyLintCli` applies to an invalid
    // policy document.
    if (hadParseFailure) return 1

    val issues = CrossContractValidator.validate(contracts)
    if (issues.isEmpty) {
      out.println(s"[ OK ] ${contracts.size} contract(s) checked, no cross-contract issues found")
      0
    } else {
      issues.foreach(issue => out.println(s"[FAIL] ${issue.location}: ${issue.message}"))
      1
    }
  }

  /** `target` itself if it's a single file; every `.yaml`/`.yml` file found
    * recursively if it's a directory; empty if it's neither — the identical
    * walk `OrgPolicyLintCli.findContractFiles` already implements,
    * duplicated here rather than shared: both are small, self-contained,
    * and `OrgPolicyLintCli`'s version is `private` to that object.
    */
  private def findContractFiles(target: String): List[String] = {
    val file = new File(target)
    if (file.isDirectory) {
      def walk(dir: File): List[File] =
        Option(dir.listFiles()).toList.flatten.flatMap { f =>
          if (f.isDirectory) walk(f)
          else if (f.getName.endsWith(".yaml") || f.getName.endsWith(".yml")) List(f)
          else Nil
        }
      walk(file).map(_.getPath)
    } else if (file.isFile) {
      List(file.getPath)
    } else {
      Nil
    }
  }
}
