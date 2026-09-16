// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.ContractRule
import com.invaract.ir.RowMutation

/** Real, permanent `CustomRuleVerifier` fixtures for
  * `CustomRuleVerifierFactoryTest`/`RuleVerifierSpec`/
  * `ContractEnforcementRuleCustomRuleTypesSpec` — a class name reflectively
  * resolved against the real test classpath, the same "real fixture, not a
  * mock" approach `NotificationSinkFactory`'s and
  * `CustomPolicyEvaluatorFactory`'s own tests use.
  */
/** Deterministic, side-effect-free: violates an UPDATE that assigns a
  * `password` column, regardless of what the contract's own
  * `allowed_update_columns` rule (if any) says — a rule type Invaract's
  * built-in set has no equivalent for. `appliesTo` only answers `true` for
  * `MutationKind.Update`, exercising the same kind-scoping a built-in rule
  * type's own `appliesTo` performs.
  */
class ForbidPasswordColumnUpdateVerifier extends CustomRuleVerifier {
  override def appliesTo(kind: MutationKind): Boolean = kind == MutationKind.Update

  override def verify(rule: ContractRule, mutation: RowMutation): List[Violation] =
    if (mutation.updatedColumns.contains("password"))
      List(
        Violation(
          ViolationType.InvalidContract,
          s"organizational rule '${rule.ruleType}' forbids updating the 'password' column directly.",
          remediation = "Route password changes through the dedicated credential-rotation job instead."
        )
      )
    else Nil
}

/** Always violates - used to confirm a custom verifier's own violation
  * genuinely blocks the write, the same way a built-in rule's violation
  * does, independent of what it actually checks.
  */
class AlwaysViolatesRuleVerifier extends CustomRuleVerifier {
  override def appliesTo(kind: MutationKind): Boolean = true
  override def verify(rule: ContractRule, mutation: RowMutation): List[Violation] =
    List(Violation(ViolationType.InvalidContract, s"'${rule.ruleType}' always violates.", remediation = "n/a"))
}

/** Throws from `verify` itself (not construction) — used to confirm a bug
  * in a custom rule's own check logic propagates out of `RuleVerifier.verify`
  * rather than being silently swallowed (fail-closed: a broken custom check
  * must not quietly pass every mutation it was supposed to be checking).
  */
class ThrowingCustomRuleVerifier extends CustomRuleVerifier {
  override def appliesTo(kind: MutationKind): Boolean = true
  override def verify(rule: ContractRule, mutation: RowMutation): List[Violation] = throw new RuntimeException("boom")
}

/** No public no-arg constructor — used to confirm
  * `CustomRuleVerifierFactory.resolve` fails loudly (not silently) for a
  * class shaped this way, the same failure `NotificationSinkFactory`/
  * `CustomPolicyEvaluatorFactory` report for their own plugin classes.
  */
class NoNoArgConstructorCustomRuleVerifier(ignored: Int) extends CustomRuleVerifier {
  override def appliesTo(kind: MutationKind): Boolean = false
  override def verify(rule: ContractRule, mutation: RowMutation): List[Violation] = Nil
}

/** Does not implement `CustomRuleVerifier` at all — used to confirm
  * `CustomRuleVerifierFactory.resolve` rejects it with a
  * `ClassCastException`-backed `IllegalArgumentException`.
  */
class NotACustomRuleVerifier
