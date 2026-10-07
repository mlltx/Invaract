// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

/** Thrown by an engine adapter's enforcement hook (for Spark,
  * `ContractEnforcementRule`) to abort a write that violates its contract,
  * before the engine executes it. `result` carries the full
  * `VerificationResult`; `getMessage` is a complete, human-readable
  * explanation (see `ContractEnforcementRule.explain`) — a developer
  * reading only the exception text, with no other context, should be able
  * to answer all four of: what the contract expected, what the plan
  * contains, why it violates the contract, and how to correct it.
  */
class ContractViolationException(val result: VerificationResult, message: String) extends RuntimeException(message)
