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
  * `NotificationSinkFactory.instantiate` (`verification-core`) loads its sink through a fresh
  * instance of this, so the load-without-initializing, check, then construct order lives in one place;
  * it adds its own `configure(properties)` call and `SafeNotificationSink` wrapping on top.
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
  def resolve(className: String): T = cache.getOrElseUpdate(className, ReflectivePluginResolver.instantiate[T](className))

  /** Same resolution as `resolve`, without throwing — `Failure` covers
    * every case `resolve` would throw for. Every caller of this uses it
    * where reflection failing must never crash a real job merely because a
    * config entry is misconfigured — that's whatever validator/factory
    * eagerly calls `resolve` instead, elsewhere, that's responsible for
    * surfacing it loudly.
    */
  def tryResolve(className: String): Try[T] = Try(resolve(className))
}

object ReflectivePluginResolver {

  /** A new instance of `className`, which must implement `T` and have a public no-arg constructor. Throws
    * `IllegalArgumentException` for anything else, which is the "fail loudly, at validation time" treatment every
    * caller of this uses. Not cached: use a `ReflectivePluginResolver` for a stateless plugin resolved repeatedly,
    * and this directly for a stateful one (a notification sink) that must not be shared.
    *
    * The class is loaded WITHOUT initializing it, checked against `T`, and only then constructed. A class name comes
    * from a contract (`Contract.customRuleTypes`) or an organizational policy, which can be authored or fetched from
    * somewhere other than the job, so naming a class must not by itself run that class's code: `Class.forName(name)`
    * initializes the class (its static initializer runs), and constructing it before the type check would run its
    * constructor, for any class on the classpath with a public no-arg constructor. Loading with `initialize = false`
    * and checking assignability first means a class that is not a `T` is never initialized or constructed.
    *
    * `T` is erased at runtime, so a plain `.asInstanceOf[T]` would never throw ClassCastException at this call site.
    * `ClassTag[T].runtimeClass.isAssignableFrom(...)` performs a real, reified check right here instead.
    */
  def instantiate[T: ClassTag](className: String): T = {
    def cannotInstantiate(e: Throwable) =
      new IllegalArgumentException(s"Could not instantiate '$className' (it needs a public no-arg constructor)", e)
    val expectedClass = implicitly[ClassTag[T]].runtimeClass
    val loaded =
      try Class.forName(className, false, getClass.getClassLoader)
      catch {
        case e: ReflectiveOperationException => throw cannotInstantiate(e)
        case e: LinkageError                 => throw new IllegalArgumentException(s"Could not load '$className': ${e.getMessage}", e)
      }
    if (!expectedClass.isAssignableFrom(loaded)) {
      throw new IllegalArgumentException(s"'$className' does not implement ${expectedClass.getSimpleName}")
    }
    try loaded.getDeclaredConstructor().newInstance().asInstanceOf[T]
    catch { case e: ReflectiveOperationException => throw cannotInstantiate(e) }
  }
}
