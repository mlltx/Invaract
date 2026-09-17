// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag
import scala.util.Try

/** Generic "class name in config, public no-arg constructor, cached once"
  * reflective plugin resolution, shared by every plugin mechanism built on
  * this exact shape — `CustomPolicyEvaluatorFactory` (this module, for
  * `OrgPolicy.customPolicyTypes`) and `spark-adapter`'s
  * `CustomRuleVerifierFactory` (for `Contract.customRuleTypes`) each wrap
  * one instance of this, parameterized by their own plugin trait. Lives
  * here, in `contract`, rather than in `spark-adapter`: the
  * `contract` -> `spark-adapter` dependency runs one way, so a
  * `spark-adapter`-only trait can still be resolved by a generic helper
  * defined here, but the reverse isn't possible.
  *
  * `NotificationSinkFactory` (`spark-adapter`) predates this and isn't
  * folded in — it also handles a `configure(properties)` call and
  * `SafeNotificationSink` wrapping this simple resolve/cache shape doesn't
  * cover, so sharing it would mean bending this generic helper's contract
  * to a second, different shape rather than reusing it as-is.
  */
final class ReflectivePluginResolver[T: ClassTag] {

  // Resolved instances are cached by class name - every plugin type this
  // resolves is documented as required to be stateless, so reusing one
  // instance across every call that names it is always safe, and avoids
  // repeated Class.forName/reflective construction for what's typically a
  // handful of distinct classes named repeatedly.
  private val cache = TrieMap.empty[String, T]

  /** Resolves and instantiates `className`, throwing `IllegalArgumentException`
    * on any failure (missing class, no public no-arg constructor, doesn't
    * implement `T`) — the "fail loudly, at validation time" treatment
    * every caller of this uses. A failed resolution is not cached, so a
    * subsequent call (e.g. a corrected config re-validated) always
    * re-attempts reflection rather than remembering a stale failure.
    */
  def resolve(className: String): T = cache.getOrElseUpdate(className, construct(className))

  /** Same resolution as `resolve`, without throwing — `Failure` covers
    * every case `resolve` would throw for. Every caller of this uses it
    * where reflection failing must never crash a real job merely because a
    * config entry is misconfigured — that's whatever validator/factory
    * eagerly calls `resolve` instead, elsewhere, that's responsible for
    * surfacing it loudly.
    */
  def tryResolve(className: String): Try[T] = Try(resolve(className))

  // `T` is erased at runtime, so a plain `.asInstanceOf[T]` here would
  // never actually throw ClassCastException at this call site - the JVM
  // has no reified check to perform against an erased type parameter. The
  // real cast only happens later, wherever the caller's own erasure
  // boundary assigns this method's result to a concrete-typed location
  // (e.g. CustomRuleVerifierFactory.resolve's `CustomRuleVerifier` return
  // type) - by then it's outside this method's own try/catch, so a
  // mis-implemented plugin class would surface a raw, unwrapped
  // ClassCastException instead of the intended IllegalArgumentException.
  // `ClassTag[T].runtimeClass.isInstance(...)` performs a real, reified
  // check right here instead, using the one piece of runtime type
  // information about T that erasure doesn't remove.
  private def construct(className: String): T = {
    val instance =
      try Class.forName(className).getDeclaredConstructor().newInstance()
      catch {
        case e: ReflectiveOperationException =>
          throw new IllegalArgumentException(
            s"Could not instantiate '$className' (it needs a public no-arg constructor)",
            e
          )
      }
    val expectedClass = implicitly[ClassTag[T]].runtimeClass
    if (!expectedClass.isInstance(instance)) {
      throw new IllegalArgumentException(s"'$className' does not implement ${expectedClass.getSimpleName}")
    }
    instance.asInstanceOf[T]
  }
}
