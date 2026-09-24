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
  * sbt "contract/runMain com.invaract.contract.cli.OrgPolicyLintCli --warn-expiring-within-days 14 org-policy.yaml contracts/"
  * sbt "contract/runMain com.invaract.contract.cli.OrgPolicyLintCli --overlays bu-finance.yaml,team-payments.yaml org-policy.yaml contracts/"
  * }}}
  *
  * `--overlays` lints against the full policy-layering stack a governed job
  * would actually run under (see docs/CONTRACT_MODEL.md's "Policy layering"
  * section and `ContractEnforcementRule.OrgPolicyOverlaysConfKey`), rather
  * than just the org-wide base — the same base-first, comma-separated,
  * ordered-list convention that conf key uses, so a business unit's own CI
  * can lint its contracts against org-wide + its own overlay with the exact
  * same argument shape spark-submit would take. Omitted entirely, this
  * behaves exactly as it always has: the single named `<policy.yaml>` is
  * the whole policy.
  *
  * Exit codes: `0` — every discovered contract satisfies every Enforce-mode
  * policy rule, in every layer (Warn-mode violations are printed but never
  * fail the run); `1` — the base policy or any overlay is invalid, a
  * contract fails to parse, no contract files were found, or any
  * Enforce-mode violation was found in any layer; `2` — usage error (too
  * few arguments, a malformed `--warn-expiring-within-days` value, or
  * `--overlays` with no value).
  */
object OrgPolicyLintCli {
  def main(args: Array[String]): Unit = sys.exit(run(args, System.out, System.err))

  private val Usage =
    "Usage: OrgPolicyLintCli [--warn-expiring-within-days N] [--overlays overlay1.yaml,overlay2.yaml] <policy.yaml> <contract-file-or-directory>..."

  /** Default look-ahead window for `expiringExemptions` when
    * `--warn-expiring-within-days` isn't given — on by default (not an
    * opt-in flag nobody remembers to pass) since it only ever prints
    * something when an exemption genuinely is about to lapse; a policy
    * with none, or all comfortably far out, sees no extra output at all.
    */
  private val DefaultWarnExpiringWithinDays = 30

  /** The lint's actual logic, deliberately `sys.exit`-free so it's directly
    * unit-testable (a real `System.exit` would kill the test JVM); `out`/`err`
    * are injected for the same reason, so a test can capture output without
    * redirecting the real streams.
    */
  private[cli] def run(args: Array[String], out: java.io.PrintStream, err: java.io.PrintStream): Int = {
    val (warnExpiringWithinDaysOpt, afterWarnFlag) = extractIntFlag(args, "--warn-expiring-within-days")
    val warnExpiringWithinDays = warnExpiringWithinDaysOpt match {
      case Left(malformedValue) if malformedValue.isEmpty =>
        err.println("--warn-expiring-within-days requires an integer value")
        return 2
      case Left(malformedValue) =>
        err.println(s"--warn-expiring-within-days requires an integer, got '$malformedValue'")
        return 2
      case Right(value) => value.getOrElse(DefaultWarnExpiringWithinDays)
    }

    val (overlaysOpt, remaining) = CliSupport.extractStringFlag(afterWarnFlag, "--overlays")
    val overlayPaths = overlaysOpt match {
      case Left(())    => err.println("--overlays requires a comma-separated list of policy paths"); return 2
      case Right(None) => Nil
      case Right(Some(raw)) => raw.split(",").map(_.trim).filter(_.nonEmpty).toList
    }

    if (remaining.length < 2) {
      err.println(Usage)
      return 2
    }

    val basePath = remaining(0)
    val targets = remaining.drop(1).toList
    val layerPaths = basePath :: overlayPaths

    val layers = layerPaths.map { path =>
      try path -> OrgPolicyParser.parseFile(path)
      catch {
        case e: OrgPolicyParseException =>
          err.println(s"Failed to parse organizational policy '$path': ${e.getMessage}")
          return 1
      }
    }

    // A single pass over the whole stack: validateLayers already covers
    // each layer's own issues (path-prefixed with its own file path) *and*
    // the cross-layer duplicate-id check in one call, rather than this CLI
    // hand-rolling a per-layer loop that duplicates what validateLayers
    // itself already does.
    val layersValidation = OrgPolicyValidator.validateLayers(layers)
    if (!layersValidation.isValid) {
      err.println("Organizational policy is invalid:")
      layersValidation.errors.foreach(issue => err.println(s"  $issue"))
      return 1
    }
    layersValidation.warnings.foreach(issue => out.println(s"[WARN] policy: $issue"))

    // Printed once, up front, per layer - an exemption's expiry is a
    // policy-level concern independent of which contracts happen to be
    // scanned this run, so it doesn't belong interleaved with the
    // per-contract lines below. Never affects the exit code: this is a
    // look-ahead, not a violation - the exemption is still fully in force
    // today.
    layers.foreach { case (path, layer) =>
      OrgPolicyEvaluator.expiringExemptions(layer, warnExpiringWithinDays).foreach { exemption =>
        val daysRemaining = java.time.temporal.ChronoUnit.DAYS.between(java.time.LocalDate.now(), exemption.reviewBy.get)
        out.println(
          s"[WARN] policy ($path): exemption for contract '${exemption.contractId}' (policies: ${exemption.policyIds.mkString(", ")}) " +
            s"expires ${exemption.reviewBy.get} (in $daysRemaining day${if (daysRemaining == 1) "" else "s"}): ${exemption.reason}"
        )
      }
    }

    // Excludes every policy document itself (base and overlays alike): a
    // directory target commonly holds the policy file(s) alongside the
    // contracts they govern, and without this a *.yaml scan would sweep a
    // policy file in as if it were a contract to lint - it isn't one, and
    // ContractParser.parseFile would always reject it (no 'id'/'outputs'),
    // turning a correct policy + a fully compliant set of contracts into a
    // spurious failure. Compared by canonical path so this holds regardless
    // of how the paths were spelled (relative vs. absolute, a trailing
    // slash, ".").
    val policyFiles = layerPaths.map(p => new File(p).getCanonicalFile).toSet
    val contractFiles = targets
      .flatMap(CliSupport.findContractFiles)
      .distinct
      .filterNot(p => policyFiles.contains(new File(p).getCanonicalFile))
      .sorted
    if (contractFiles.isEmpty) {
      err.println(s"No contract files (*.yaml/*.yml) found under: ${targets.mkString(", ")}")
      return 1
    }

    val policies = layers.map(_._2)
    var hadFailure = false
    contractFiles.foreach { path =>
      try {
        val contract = ContractParser.parseFile(new File(path))
        val evaluation = OrgPolicyEvaluator.evaluateLayers(contract, policies)
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

  /** `CliSupport.extractStringFlag`, with the extracted value additionally
    * parsed as an integer: `Right(Some(v))` on a valid integer value,
    * `Right(None)` when the flag isn't present, `Left(rawValue)` when it's
    * present but the following token isn't a valid integer (`""` when the
    * flag was the very last argument, with no value at all). Scala 2.12 has
    * no `String.toIntOption` (a 2.13+ addition), hence `scala.util.Try`.
    */
  private def extractIntFlag(args: Array[String], flagName: String): (Either[String, Option[Int]], Array[String]) = {
    val (rawResult, remaining) = CliSupport.extractStringFlag(args, flagName)
    val result: Either[String, Option[Int]] = rawResult match {
      case Left(())         => Left("")
      case Right(None)      => Right(None)
      case Right(Some(raw)) =>
        scala.util.Try(raw.toInt).toOption match {
          case Some(value) => Right(Some(value))
          case None        => Left(raw)
        }
    }
    (result, remaining)
  }
}
