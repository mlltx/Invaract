// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter.notification

import com.example.sparkadapter.Violation

/** One thing worth telling an external system about, published through a
  * `NotificationSink` when one is configured and enabled (see
  * `NotificationConfig`/`NotificationSinkFactory`). Two kinds exist, matching
  * the two moments Invaract's engine has something to report about a write:
  *
  *   - `ContractValidationEvent` — a write (or state-changing CALL) was
  *     checked against a contract. Published by `ContractEnforcementRule`
  *     for every check it performs, PASS or FAIL — this is "the contract was
  *     evaluated," not "the write happened."
  *   - `WriteEvent` — a write actually completed. Published by
  *     `SparkAdapterListener`, which (unlike the check rule) only observes
  *     Spark after real execution succeeds — see that class's doc for why
  *     that's a structurally different moment.
  *
  * Plain case classes crossing a JSON boundary to an external system, the
  * same "closed vocabulary, not an open trait hierarchy a sink must
  * pattern-match exhaustively" reasoning `ViolationType` already uses for
  * violations.
  */
sealed trait NotificationEvent {
  def eventType: String
  def timestamp: Long

  /** Copied verbatim from the active contract's own `extensions` bag (see
    * `com.example.contract.Contract.extensions`) at the moment the event is
    * published. Whatever a contract author already recorded there — owner,
    * team, upstream system, anything ODCS or this project doesn't itself
    * interpret — rides along on every event this contract's enforcement
    * produces, without Invaract needing a separate, parallel metadata
    * vocabulary of its own.
    */
  def metadata: Map[String, Any]
}

/** A contract check `ContractEnforcementRule` performed — `status` is
  * `"PASSED"` or `"FAILED"`, matching `VerificationResult.status` exactly
  * (this event is built directly from one). Published *before* a FAILED
  * result's `ContractViolationException` is thrown, so a subscriber sees
  * the rejection at the same moment the writing job does, not only once
  * some later retry succeeds.
  */
case class ContractValidationEvent(
  contract: String,
  status: String,
  violations: List[Violation],
  timestamp: Long,
  metadata: Map[String, Any]
) extends NotificationEvent {
  val eventType: String = "CONTRACT_VALIDATION"
}

/** One field of a `WriteEvent`'s schema — a deliberately minimal projection
  * of Spark's `StructField` (name/type-name/nullable only), so this package
  * (and any external sink deserializing its JSON) never needs a Spark
  * dependency to represent a `WriteEvent`.
  */
case class WriteFieldInfo(name: String, dataType: String, nullable: Boolean)

/** A write Spark actually executed successfully — `contract` is `None` only
  * when the session that captured this write was never given a contract at
  * all (dry-run mode); a write rejected by `ContractEnforcementRule` never
  * reaches this event, since Spark never executes it.
  */
case class WriteEvent(
  contract: Option[String],
  location: String,
  format: Option[String],
  saveMode: Option[String],
  schema: List[WriteFieldInfo],
  timestamp: Long,
  metadata: Map[String, Any]
) extends NotificationEvent {
  val eventType: String = "WRITE"
}
