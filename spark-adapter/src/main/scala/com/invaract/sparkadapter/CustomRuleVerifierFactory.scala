// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.ReflectivePluginResolver

import scala.util.Try

/** Reflectively resolves the `CustomRuleVerifier` a
  * `Contract.customRuleTypes` entry names — the same "class name in
  * config, public no-arg constructor, loaded once" mechanism
  * `NotificationSinkFactory` established for `NotificationSink`, and
  * `com.invaract.contract.CustomPolicyEvaluatorFactory` established for
  * organizational policy's `CustomPolicyEvaluator`. The actual
  * resolve/cache logic is generic (`com.invaract.contract.ReflectivePluginResolver`,
  * shared with `CustomPolicyEvaluatorFactory`) and lives in `contract`,
  * since the `contract` -> `spark-adapter` dependency runs one way; this
  * object exists to give `CustomRuleVerifier` (a `spark-adapter`-only
  * trait, which returns this module's own `Violation`) its own cache and
  * its own narrowly-scoped `tryResolve`.
  */
object CustomRuleVerifierFactory {

  private val resolver = new ReflectivePluginResolver[CustomRuleVerifier]

  /** Resolves and instantiates `className`, throwing `IllegalArgumentException`
    * on any failure (missing class, no public no-arg constructor, doesn't
    * implement `CustomRuleVerifier`) — the "fail loudly, at validation
    * time" treatment `ContractEnforcementRule.requireValidContract` uses
    * this for (alongside `ContractValidator`'s own, Spark-independent
    * checks on `Contract.customRuleTypes`'s shape), mirroring
    * `NotificationSinkFactory.create`'s identical failure handling for
    * `sink.class`. A failed resolution is not cached, so a subsequent
    * call (e.g. a corrected contract re-validated) always re-attempts
    * reflection rather than remembering a stale failure.
    */
  def resolve(className: String): CustomRuleVerifier = resolver.resolve(className)

  /** Same resolution as `resolve`, without throwing — `Failure` covers
    * every case `resolve` would throw for. `RuleVerifier` uses this: it
    * must never throw merely because a contract is misconfigured (that's
    * `requireValidContract`'s job), so a `customRuleTypes` entry naming an
    * unresolvable class is treated the same as any other malformed rule
    * type: no violation produced, not a thrown exception.
    */
  private[sparkadapter] def tryResolve(className: String): Try[CustomRuleVerifier] = resolver.tryResolve(className)
}
