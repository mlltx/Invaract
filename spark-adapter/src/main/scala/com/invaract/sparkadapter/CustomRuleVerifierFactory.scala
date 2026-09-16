// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import scala.collection.concurrent.TrieMap
import scala.util.Try

/** Reflectively resolves the `CustomRuleVerifier` a
  * `Contract.customRuleTypes` entry names — the same "class name in
  * config, public no-arg constructor, loaded once" mechanism
  * `NotificationSinkFactory` established for `NotificationSink`, and
  * `com.invaract.contract.CustomPolicyEvaluatorFactory` established for
  * organizational policy's `CustomPolicyEvaluator` — reimplemented here
  * (not shared) since it resolves a `spark-adapter`-only trait
  * (`CustomRuleVerifier`, which returns this module's own `Violation`),
  * unlike `CustomPolicyEvaluatorFactory`, which lives in `contract`
  * because everything it resolves is Spark-independent.
  */
object CustomRuleVerifierFactory {

  // Resolved instances are cached by class name - CustomRuleVerifier is
  // required to be stateless, so reusing one instance across every rule/
  // mutation that names it is always safe, and avoids repeated
  // Class.forName/reflective construction for what's typically a handful
  // of distinct classes checked on every governed row-level DML write.
  private val cache = TrieMap.empty[String, CustomRuleVerifier]

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
  def resolve(className: String): CustomRuleVerifier =
    cache.getOrElseUpdate(className, construct(className))

  /** Same resolution as `resolve`, without throwing — `Failure` covers
    * every case `resolve` would throw for. `RuleVerifier` uses this: it
    * must never throw merely because a contract is misconfigured (that's
    * `requireValidContract`'s job), so a `customRuleTypes` entry naming an
    * unresolvable class is treated the same as any other malformed rule
    * type: no violation produced, not a thrown exception.
    */
  private[sparkadapter] def tryResolve(className: String): Try[CustomRuleVerifier] = Try(resolve(className))

  private def construct(className: String): CustomRuleVerifier =
    try {
      Class.forName(className).getDeclaredConstructor().newInstance().asInstanceOf[CustomRuleVerifier]
    } catch {
      case e: ClassCastException =>
        throw new IllegalArgumentException(s"'$className' does not implement CustomRuleVerifier", e)
      case e: ReflectiveOperationException =>
        throw new IllegalArgumentException(
          s"Could not instantiate custom rule verifier '$className' (it needs a public no-arg constructor)",
          e
        )
    }
}
