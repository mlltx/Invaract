// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import scala.util.Try

/** Reflectively resolves the `CustomPolicyEvaluator` an
  * `OrgPolicy.customPolicyTypes` entry names — the same "class name in
  * config, public no-arg constructor, loaded once" mechanism
  * `spark-adapter`'s `NotificationSinkFactory` already established for
  * `NotificationSink`. The actual resolve/cache logic is generic (see
  * `ReflectivePluginResolver`, shared with `spark-adapter`'s
  * `CustomRuleVerifierFactory`); this object exists to give
  * `CustomPolicyEvaluator` its own cache and its own narrowly-scoped
  * `tryResolve`, the same way it always has.
  */
object CustomPolicyEvaluatorFactory {

  private val resolver = new ReflectivePluginResolver[CustomPolicyEvaluator]

  /** Resolves and instantiates `className`, throwing `IllegalArgumentException`
    * on any failure (missing class, no public no-arg constructor, doesn't
    * implement `CustomPolicyEvaluator`) — the "fail loudly, at validation
    * time" treatment `OrgPolicyValidator` uses this for, mirroring
    * `NotificationSinkFactory.create`'s identical failure handling for
    * `sink.class`. A failed resolution is not cached, so a subsequent call
    * (e.g. `OrgPolicyValidator` fixing the entry and re-validating) always
    * re-attempts reflection rather than remembering a stale failure.
    */
  def resolve(className: String): CustomPolicyEvaluator = resolver.resolve(className)

  /** Same resolution as `resolve`, without throwing — `Failure` covers every
    * case `resolve` would throw for. `OrgPolicyEvaluator` uses this: it must
    * never throw merely because a policy is misconfigured (that's
    * `OrgPolicyValidator`'s job — see `OrgPolicyEvaluator.evaluateRule`'s own
    * doc on staying total), so a `customPolicyTypes` entry naming an
    * unresolvable class is treated the same as any other malformed policy
    * type: no violation produced, not a thrown exception.
    */
  private[contract] def tryResolve(className: String): Try[CustomPolicyEvaluator] = resolver.tryResolve(className)
}
