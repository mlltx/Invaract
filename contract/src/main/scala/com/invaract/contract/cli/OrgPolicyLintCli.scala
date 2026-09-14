// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract.cli

import com.invaract.contract._

import java.io.File

/** Standalone command-line lint: evaluates every contract document under a
  * directory (or a single file) against an organizational policy, with no
  * Spark involved at all — `OrgPolicyEvaluator` only ever touches the
  * `Contract` object model, so this runs as a fast pre-commit/CI check
  * against a contracts repository, giving a contract author feedback at
  * *authoring* time rather than only discovering a violation when a job
  * next runs. See docs/CONTRACT_MODEL.md's "Organizational Policy" section,
  * "Linting contracts before they run."
  *
  * {{{
  * sbt "contract/runMain com.invaract.contract.cli.OrgPolicyLintCli org-policy.yaml contracts/"
  * }}}
  *
  * Exit codes: `0` — every discovered contract satisfies every Enforce-mode
  * policy rule (Warn-mode violations are printed but never fail the run);
  * `1` — the policy document itself is invalid, a contract fails to parse,
  * no contract files were found, or any Enforce-mode violation was found;
  * `2` — usage error (too few arguments).
  */
object OrgPolicyLintCli {
  def main(args: Array[String]): Unit = sys.exit(run(args, System.out, System.err))

  /** The lint's actual logic, deliberately `sys.exit`-free so it's directly
    * unit-testable (a real `System.exit` would kill the test JVM); `out`/`err`
    * are injected for the same reason, so a test can capture output without
    * redirecting the real streams.
    */
  private[cli] def run(args: Array[String], out: java.io.PrintStream, err: java.io.PrintStream): Int = {
    if (args.length < 2) {
      err.println("Usage: OrgPolicyLintCli <policy.yaml> <contract-file-or-directory>...")
      return 2
    }

    val policyPath = args(0)
    val targets = args.drop(1).toList

    val policy =
      try OrgPolicyParser.parseFile(policyPath)
      catch {
        case e: OrgPolicyParseException =>
          err.println(s"Failed to parse organizational policy '$policyPath': ${e.getMessage}")
          return 1
      }

    val policyValidation = OrgPolicyValidator.validate(policy)
    if (!policyValidation.isValid) {
      err.println(s"Organizational policy '$policyPath' is invalid:")
      policyValidation.errors.foreach(issue => err.println(s"  $issue"))
      return 1
    }
    policyValidation.warnings.foreach(issue => out.println(s"[WARN] policy: $issue"))

    // Excludes the policy document itself: a directory target commonly
    // holds the org-policy file alongside the contracts it governs, and
    // without this a *.yaml scan would sweep the policy file in as if it
    // were a contract to lint - it isn't one, and ContractParser.parseFile
    // would always reject it (no 'id'/'outputs'), turning a correct policy
    // + a fully compliant set of contracts into a spurious failure. Compared
    // by canonical path so this holds regardless of how policyPath/targets
    // were spelled (relative vs. absolute, a trailing slash, ".").
    val policyFile = new File(policyPath).getCanonicalFile
    val contractFiles = targets
      .flatMap(findContractFiles)
      .distinct
      .filterNot(p => new File(p).getCanonicalFile == policyFile)
      .sorted
    if (contractFiles.isEmpty) {
      err.println(s"No contract files (*.yaml/*.yml) found under: ${targets.mkString(", ")}")
      return 1
    }

    var hadFailure = false
    contractFiles.foreach { path =>
      try {
        val contract = ContractParser.parseFile(new File(path))
        val evaluation = OrgPolicyEvaluator.evaluate(contract, policy)
        evaluation.warnViolations.foreach(v => out.println(s"[WARN] $path: [${v.policyId}] ${v.message}"))
        if (evaluation.hasBlockingViolations) {
          hadFailure = true
          evaluation.enforceViolations.foreach { v =>
            out.println(s"[FAIL] $path: [${v.policyId}] ${v.message}")
            out.println(s"       remediation: ${v.remediation}")
          }
        } else {
          out.println(s"[ OK ] $path")
        }
      } catch {
        case e: ContractParseException =>
          hadFailure = true
          out.println(s"[FAIL] $path: could not parse - ${e.getMessage}")
      }
    }

    if (hadFailure) 1 else 0
  }

  /** `target` itself if it's a single file; every `.yaml`/`.yml` file found
    * recursively if it's a directory; empty if it's neither (doesn't exist,
    * or is some other kind of filesystem entry).
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
