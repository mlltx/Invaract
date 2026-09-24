// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import scala.util.Try

/** Reflectively resolves the `TypeGuaranteeCheck` an
  * `OrgPolicy.typeGuarantees.customTypeGuaranteeTypes` entry names — the
  * same "class name in config, public no-arg constructor, loaded once"
  * mechanism `CustomPolicyEvaluatorFactory`/`spark-adapter`'s
  * `CustomRuleVerifierFactory` already establish. The actual resolve/cache
  * logic is generic (see `ReflectivePluginResolver`); this object exists to
  * give `TypeGuaranteeCheck` its own cache and its own narrowly-scoped
  * `tryResolve`, the same way every other reflectively-resolved plugin
  * trait in this module already does.
  */
object TypeGuaranteeCheckFactory {

  private val resolver = new ReflectivePluginResolver[TypeGuaranteeCheck]

  /** Resolves and instantiates `className`, throwing `IllegalArgumentException`
    * on any failure (missing class, no public no-arg constructor, doesn't
    * implement `TypeGuaranteeCheck`) — the "fail loudly, at validation time"
    * treatment `OrgPolicyValidator` uses this for.
    */
  def resolve(className: String): TypeGuaranteeCheck = resolver.resolve(className)

  /** Same resolution as `resolve`, without throwing — `Failure` covers every
    * case `resolve` would throw for. `TypeGuaranteeValidator` uses this: it
    * must never throw merely because a policy is misconfigured (that's
    * `OrgPolicyValidator`'s job), so a `customTypeGuaranteeTypes` entry
    * naming an unresolvable class is treated the same as any other
    * malformed config: no result produced, not a thrown exception.
    */
  private[contract] def tryResolve(className: String): Try[TypeGuaranteeCheck] = resolver.tryResolve(className)
}
