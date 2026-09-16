// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

/** Real, permanent `CustomPolicyEvaluator` fixtures for
  * `CustomPolicyEvaluatorFactoryTest`/`OrgPolicyEvaluatorTest`/
  * `OrgPolicyValidatorTest` — a class name reflectively resolved against
  * the real test classpath, the same "real fixture, not a mock" approach
  * `NotificationSinkFactory`'s own tests use for `sink.class`.
  */
/** Deterministic, side-effect-free: violates whenever `contract.id` isn't
  * already all-lowercase — exercises the full round trip (properties are
  * ignored; `rule.id`/`rule.ruleType`/`rule.mode` are threaded through the
  * returned `PolicyViolation` exactly like a built-in check does).
  */
class ContractIdMustBeLowercaseEvaluator extends CustomPolicyEvaluator {
  override def evaluate(contract: Contract, rule: PolicyRule): List[PolicyViolation] =
    if (contract.id == contract.id.toLowerCase) Nil
    else
      List(
        PolicyViolation(
          rule.id,
          rule.ruleType,
          rule.mode,
          dataset = None,
          s"organizational policy '${rule.id}' requires contract id '${contract.id}' to be all-lowercase, but it is not.",
          s"Rename contract '${contract.id}' to an all-lowercase id."
        )
      )
}

/** Always violates, once per dataset in `rule.scope` — used to confirm a
  * custom evaluator is free to implement dataset-level scoping itself
  * (nothing in `CustomPolicyEvaluator`'s single-method shape does it for
  * the implementation, unlike a built-in `DatasetPolicy`).
  */
class AlwaysViolatesPerDatasetInScopeEvaluator extends CustomPolicyEvaluator {
  override def evaluate(contract: Contract, rule: PolicyRule): List[PolicyViolation] = {
    val datasets = rule.scope match {
      case PolicyScope.Inputs  => contract.inputs
      case PolicyScope.Outputs => contract.outputs
      case PolicyScope.All     => contract.inputs ++ contract.outputs
    }
    datasets.map { d =>
      PolicyViolation(rule.id, rule.ruleType, rule.mode, Some(d.name), s"always violates for dataset '${d.name}'", "n/a")
    }
  }
}

/** Throws from `evaluate` itself (not construction) — used to confirm a
  * bug in a custom evaluator's own check logic propagates out of
  * `OrgPolicyEvaluator.evaluate` rather than being silently swallowed
  * (fail-closed: a broken custom check must not quietly pass every
  * contract it was supposed to be checking).
  */
class ThrowingCustomPolicyEvaluator extends CustomPolicyEvaluator {
  override def evaluate(contract: Contract, rule: PolicyRule): List[PolicyViolation] =
    throw new RuntimeException("boom")
}

/** No public no-arg constructor — used to confirm
  * `CustomPolicyEvaluatorFactory.resolve` fails loudly (not silently) for
  * a class shaped this way, the same failure `NotificationSinkFactory`
  * reports for a sink class without one.
  */
class NoNoArgConstructorCustomPolicyEvaluator(ignored: Int) extends CustomPolicyEvaluator {
  override def evaluate(contract: Contract, rule: PolicyRule): List[PolicyViolation] = Nil
}

/** Does not implement `CustomPolicyEvaluator` at all — used to confirm
  * `CustomPolicyEvaluatorFactory.resolve` rejects it with a
  * `ClassCastException`-backed `IllegalArgumentException`, the same check
  * `NotificationSinkFactory.create` performs for `sink.class`.
  */
class NotACustomPolicyEvaluator
