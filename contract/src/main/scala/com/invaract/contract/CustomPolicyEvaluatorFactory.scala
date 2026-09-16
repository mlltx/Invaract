// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import scala.collection.concurrent.TrieMap
import scala.util.Try

/** Reflectively resolves the `CustomPolicyEvaluator` an
  * `OrgPolicy.customPolicyTypes` entry names — the same "class name in
  * config, public no-arg constructor, loaded once" mechanism
  * `spark-adapter`'s `NotificationSinkFactory` already established for
  * `NotificationSink`, reimplemented here rather than shared: this needs to
  * run from the `contract` module itself (org-policy evaluation is
  * Spark-independent — see `OrgPolicyEvaluator`'s own doc), and
  * `Class.forName` reflection is plain JVM, not a `spark-adapter`/Spark
  * dependency `contract` would have to take on to reuse that file.
  */
object CustomPolicyEvaluatorFactory {

  // Resolved instances are cached by class name — CustomPolicyEvaluator is
  // documented as required to be stateless, so reusing one instance across
  // every rule/contract that names it (rather than reflecting a fresh one
  // per PolicyRule per Contract, e.g. once per file in OrgPolicyLintCli's
  // own loop) is always safe, and avoids repeated Class.forName/reflective
  // construction for what's typically a handful of distinct classes shared
  // across many rules and many evaluate() calls.
  private val cache = TrieMap.empty[String, CustomPolicyEvaluator]

  /** Resolves and instantiates `className`, throwing `IllegalArgumentException`
    * on any failure (missing class, no public no-arg constructor, doesn't
    * implement `CustomPolicyEvaluator`) — the "fail loudly, at validation
    * time" treatment `OrgPolicyValidator` uses this for, mirroring
    * `NotificationSinkFactory.create`'s identical failure handling for
    * `sink.class`. A failed resolution is not cached, so a subsequent call
    * (e.g. `OrgPolicyValidator` fixing the entry and re-validating) always
    * re-attempts reflection rather than remembering a stale failure.
    */
  def resolve(className: String): CustomPolicyEvaluator =
    cache.getOrElseUpdate(className, construct(className))

  /** Same resolution as `resolve`, without throwing — `Failure` covers every
    * case `resolve` would throw for. `OrgPolicyEvaluator` uses this: it must
    * never throw merely because a policy is misconfigured (that's
    * `OrgPolicyValidator`'s job — see `OrgPolicyEvaluator.evaluateRule`'s own
    * doc on staying total), so a `customPolicyTypes` entry naming an
    * unresolvable class is treated the same as any other malformed policy
    * type: no violation produced, not a thrown exception.
    */
  private[contract] def tryResolve(className: String): Try[CustomPolicyEvaluator] = Try(resolve(className))

  private def construct(className: String): CustomPolicyEvaluator =
    try {
      Class.forName(className).getDeclaredConstructor().newInstance().asInstanceOf[CustomPolicyEvaluator]
    } catch {
      case e: ClassCastException =>
        throw new IllegalArgumentException(s"'$className' does not implement CustomPolicyEvaluator", e)
      case e: ReflectiveOperationException =>
        throw new IllegalArgumentException(
          s"Could not instantiate custom policy evaluator '$className' (it needs a public no-arg constructor)",
          e
        )
    }
}
