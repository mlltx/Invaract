// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import java.util.regex.Pattern

/** The steps of a job a platform trusts not to move governed data, by name: a metrics step, a logging step, a step that
  * notifies someone. An adapter refuses a step that ends a job without being a write it recognises, because it cannot tell
  * what such a step does with the data it is given (it may write it somewhere); a step named here is let through instead.
  *
  * Only that refusal is lifted. A write the adapter recognises is checked as always, and a write to a destination it cannot
  * verify is still refused: trusting a step is a statement about what it does with data, not a way past the contract.
  *
  * Read from `InvaractConf.TrustedSteps` (`VerificationSetup.trustedSteps`), so a platform names them without touching the
  * job: comma-separated step names, each matched against the whole name an engine gives the step, with `*` standing for any
  * run of characters (`Notify*`, or `*` then `/LogCounts` for a LogCounts step inside any other).
  */
final case class TrustedSteps(patterns: List[String]) {

  /** Whether `step` (the engine's full name for it) is one of the trusted steps. */
  def trusts(step: String): Boolean = patterns.exists(TrustedSteps.matches(_, step))
}

object TrustedSteps {

  /** No step trusted: the default. */
  val Nobody: TrustedSteps = TrustedSteps(Nil)

  /** The steps `config` names, blanks and surrounding spaces ignored. */
  def from(config: ConfigSource): TrustedSteps =
    TrustedSteps(config.get(InvaractConf.TrustedSteps).toList.flatMap(_.split(',')).map(_.trim).filter(_.nonEmpty))

  /** Whether `pattern` names all of `step`, `*` standing for any run of characters and everything else for itself. */
  def matches(pattern: String, step: String): Boolean =
    Pattern.matches(pattern.split("\\*", -1).map(Pattern.quote).mkString(".*"), step)
}
