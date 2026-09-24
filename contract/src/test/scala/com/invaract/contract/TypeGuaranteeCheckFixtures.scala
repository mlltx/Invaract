// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

/** Real, permanent `TypeGuaranteeCheck` fixtures for
  * `TypeGuaranteeCheckFactoryTest`/`TypeGuaranteeValidatorTest`/
  * `OrgPolicyValidatorTest` — a class name reflectively resolved against the
  * real test classpath, the same "real fixture, not a mock" approach
  * `CustomPolicyEvaluatorFixtures.scala` already establishes for
  * `CustomPolicyEvaluator`.
  */
/** One deterministic `Conforms` result per contract given, naming the
  * contract's own id in `message` — used to confirm a custom check actually
  * ran (and saw the real contract list) rather than merely resolving.
  */
class AlwaysConformsTypeGuaranteeCheck extends TypeGuaranteeCheck {
  override def check(contracts: List[Contract]): List[TypeGuaranteeResult] =
    contracts.map { c =>
      TypeGuaranteeResult(
        "always_conforms_test_check",
        TypeGuaranteeVerdict.Conforms,
        "n/a",
        s"custom check ran for contract '${c.id}'",
        "No action needed."
      )
    }
}

/** Always produces exactly one `Contradicts` result, regardless of
  * `contracts` — used to confirm a custom check's `Contradicts` result
  * becomes blocking under `TypeGuaranteeValidator.evaluate` the same way a
  * built-in check's does.
  */
class AlwaysContradictsTypeGuaranteeCheck extends TypeGuaranteeCheck {
  override def check(contracts: List[Contract]): List[TypeGuaranteeResult] =
    List(
      TypeGuaranteeResult(
        "always_contradicts_test_check",
        TypeGuaranteeVerdict.Contradicts,
        "n/a",
        "this custom check always contradicts",
        "n/a"
      )
    )
}

/** Throws from `check` itself (not construction) — used to confirm a bug in
  * a custom check's own logic propagates out of `TypeGuaranteeValidator`
  * rather than being silently swallowed.
  */
class ThrowingTypeGuaranteeCheck extends TypeGuaranteeCheck {
  override def check(contracts: List[Contract]): List[TypeGuaranteeResult] = throw new RuntimeException("boom")
}

/** No public no-arg constructor — used to confirm
  * `TypeGuaranteeCheckFactory.resolve` fails loudly for a class shaped this
  * way, the same failure `CustomPolicyEvaluatorFactory`/`NotificationSinkFactory`
  * report for their own plugin classes without one.
  */
class NoNoArgConstructorTypeGuaranteeCheck(ignored: Int) extends TypeGuaranteeCheck {
  override def check(contracts: List[Contract]): List[TypeGuaranteeResult] = Nil
}

/** Does not implement `TypeGuaranteeCheck` at all — used to confirm
  * `TypeGuaranteeCheckFactory.resolve` rejects it with a
  * `ClassCastException`-backed `IllegalArgumentException`.
  */
class NotATypeGuaranteeCheck
