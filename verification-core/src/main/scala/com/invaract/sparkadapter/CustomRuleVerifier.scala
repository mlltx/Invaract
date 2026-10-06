// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.ContractRule
import com.invaract.ir.RowMutation

/** Extension point for a DML rule type Invaract's own built-in `RuleType`
  * set (`com.invaract.contract.RuleType`) doesn't cover — resolved
  * reflectively via `Contract.customRuleTypes`
  * (`CustomRuleVerifierFactory`), the same "class name named in config,
  * public no-arg constructor, loaded once" mechanism
  * `NotificationSinkFactory` established for `NotificationSink` and
  * `CustomPolicyEvaluatorFactory` established for `CustomPolicyEvaluator`
  * (organizational policy's own equivalent — see
  * docs/CONTRACT_MODEL.md's "Custom policy types" section). See
  * docs/SPARK_ADAPTER.md's "Custom rule types" section.
  *
  * Unlike `CustomPolicyEvaluator`, which only ever needs a contract's own
  * declared shape, a DML rule inherently needs to know what a real write
  * actually *did* — `verify` receives the same `ir.RowMutation`
  * `RuleVerifier`'s own built-in checks (`merge_condition`,
  * `forbid_unconditional_delete`, `allowed_update_columns`) are checked
  * against, extracted from the real Spark plan by (private)
  * `RowMutationSupport`.
  *
  * `appliesTo` mirrors `RuleVerifier.appliesTo`'s own role for a built-in
  * `InterpretedRule`: `ContractEnforcementRule` calls it when a plan is
  * recognized as `kind`-shaped row-level DML but this module couldn't
  * fully extract the facts a rule needs (`RowMutationSupport.Classification.Unverifiable`)
  * — answering `true` means a contract declaring this rule type must fail
  * closed on that plan rather than silently let it through unchecked, the
  * same "unverifiable, not passed" principle every other fail-closed
  * check in this module already applies. A rule type that never applies
  * to a given kind (e.g. a rule only ever relevant to `Merge`) should
  * answer `false` for `Update`/`Delete`, so an operation this rule type
  * doesn't care about is never spuriously failed closed over it.
  */
trait CustomRuleVerifier {
  def appliesTo(kind: MutationKind): Boolean
  def verify(rule: ContractRule, mutation: RowMutation): List[Violation]
}
