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
  * sbt "contract/runMain com.invaract.contract.cli.CrossContractLintCli --org-policy org-policy.yaml contracts/"
  * }}}
  *
  * `--org-policy` additionally runs `TypeGuaranteeValidator` (docs/CONTRACT_MODEL.md's
  * "Type guarantee checks" section, the spec's own deferred "stronger
  * semantic and guarantee validation" phase) against the same discovered
  * contracts, using that document's own `typeGuarantees` block to decide
  * *which* checks run and whether a `Contradicts` verdict blocks this run at
  * all — omitted entirely, this behaves exactly as it always has: no type
  * guarantee checking happens. Only `typeGuarantees` is consulted from the
  * named policy here — its `policies`/`exemptions`/etc. remain
  * `OrgPolicyLintCli`'s own concern (single-contract linting), not
  * re-evaluated by this cross-contract-scoped CLI.
  *
  * Exit codes: `0` — every discovered contract parsed, no cross-contract
  * issue was found between any pair, and (if `--org-policy` was given) no
  * enabled type guarantee check produced a blocking result; `1` — a
  * contract or the named policy failed to parse/validate, no contract files
  * were found, a cross-contract issue was found, or a blocking type
  * guarantee result was found; `2` — usage error (no targets given, or
  * `--org-policy` with no value).
  */
object CrossContractLintCli {
  def main(args: Array[String]): Unit = sys.exit(run(args, System.out, System.err))

  private val Usage = "Usage: CrossContractLintCli [--org-policy org-policy.yaml] <contract-file-or-directory>..."

  /** Deliberately `sys.exit`-free so it's directly unit-testable (a real
    * `System.exit` would kill the test JVM); `out`/`err` are injected for
    * the same reason, mirroring `OrgPolicyLintCli.run`'s exact shape.
    */
  private[cli] def run(args: Array[String], out: java.io.PrintStream, err: java.io.PrintStream): Int = {
    val (orgPolicyPathResult, remaining) = CliSupport.extractStringFlag(args, "--org-policy")
    val orgPolicyPath: Option[String] = orgPolicyPathResult match {
      case Left(()) =>
        err.println("--org-policy requires a path")
        return 2
      case Right(value) => value
    }

    if (remaining.isEmpty) {
      err.println(Usage)
      return 2
    }

    // Excludes the named org-policy file itself, the same way
    // OrgPolicyLintCli excludes its own policy document(s) from the
    // contracts it scans: a directory target commonly holds the policy
    // file alongside the contracts it governs, and without this a
    // *.yaml scan would sweep it in as if it were a contract to lint - it
    // isn't one, and ContractParser.parseFile would always reject it
    // (no 'id'/'outputs'), turning a correct policy + a fully compliant
    // set of contracts into a spurious failure. Compared by canonical
    // path so this holds regardless of how the path was spelled.
    val orgPolicyCanonicalPath = orgPolicyPath.map(p => new File(p).getCanonicalFile)
    val contractFiles = remaining.toList
      .flatMap(CliSupport.findContractFiles)
      .distinct
      .filterNot(p => orgPolicyCanonicalPath.contains(new File(p).getCanonicalFile))
      .sorted
    if (contractFiles.isEmpty) {
      err.println(s"No contract files (*.yaml/*.yml) found under: ${remaining.mkString(", ")}")
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
    issues.foreach(issue => out.println(s"[FAIL] ${issue.location}: ${issue.message}"))

    val typeGuaranteeBlocking: List[TypeGuaranteeResult] = orgPolicyPath match {
      case None => Nil
      case Some(policyPath) =>
        val policy =
          try {
            OrgPolicyParser.parseFile(policyPath)
          } catch {
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

        val evaluation = TypeGuaranteeValidator.evaluate(contracts, policy)
        val blockingResults = evaluation.blocking.toSet
        evaluation.results.foreach { r =>
          // "[FAIL]" is reserved for a result that actually blocks this run
          // (a real Contradicts *and* typeGuarantees.mode is Enforce) - a
          // Contradicts under a Warn-mode policy prints "[WARN]" instead,
          // the same "the tag reflects what actually happens, not just the
          // raw verdict" principle every other severity tag in this CLI
          // already follows, so a "[FAIL]" line is never paired with a
          // clean (0) exit code.
          val tag =
            if (blockingResults.contains(r)) "[FAIL]"
            else if (r.verdict == TypeGuaranteeVerdict.Conforms) "[ OK ]"
            else "[WARN]"
          out.println(s"$tag [${r.checkType}] ${r.location}: ${r.message}")
        }
        evaluation.blocking
    }

    if (issues.isEmpty && typeGuaranteeBlocking.isEmpty) {
      out.println(s"[ OK ] ${contracts.size} contract(s) checked, no cross-contract issues found")
      0
    } else {
      1
    }
  }

}
