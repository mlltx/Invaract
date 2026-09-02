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
  *
  * `applicationId` is the checking session's `SparkContext.applicationId`
  * — always present for a real, running Spark session (`Option` here only
  * because `verifyOrThrow` is also exercised directly in tests without a
  * session in scope).
  */
case class ContractValidationEvent(
  contract: String,
  status: String,
  violations: List[Violation],
  timestamp: Long,
  metadata: Map[String, Any],
  applicationId: Option[String] = None
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
  *
  * `durationMs` is `SparkAdapterListener.onSuccess`'s own `durationNs`
  * parameter, converted — always present. `rowCount`/`bytesWritten`/
  * `fileCount` come from Spark's own `SQLMetric`s
  * (`qe.executedPlan.metrics`) and are populated only when that specific
  * executed-plan node actually carries them — confirmed empirically (not
  * assumed) present for a plain V1 write (`InsertIntoHadoopFsRelationCommand`,
  * i.e. ordinary Parquet/CSV/JSON/ORC/Hive), and confirmed *absent* for
  * every Delta/Iceberg write shape probed (`SaveIntoDataSourceCommand`,
  * `AppendDataExecV1`, DSv2 `AppendDataExec`) — the physical node Spark
  * executes for those never populates `numOutputRows`/`numOutputBytes`/
  * `numFiles` at all. `None` there is an honest "not available through
  * this mechanism," not a bug — Delta and Iceberg both track equivalent
  * counts through their own connector-specific commit metadata instead
  * (Delta's `CommitInfo.operationMetrics`, Iceberg's
  * `Table.currentSnapshot().summary()`/`MetricsReporter`) — row/byte/file
  * counts through that route remain unattempted, but the commit identity
  * itself (which `deltaVersion`/`icebergSnapshotId` below capture) does
  * not need it. `applicationId` is
  * `qe.sparkSession.sparkContext.applicationId` — always present for a
  * real write.
  *
  * `deltaVersion`/`icebergSnapshotId` are the connector's own identifier
  * for the commit this write just produced — `Some` only for a write of
  * that connector's format, `None` otherwise (including for each other:
  * a Delta write never populates `icebergSnapshotId`, and vice versa).
  * `SparkAdapterListener.onSuccess` reaches these via reflection (this
  * module has no compile-time dependency on Delta or Iceberg — see
  * `WriteCommandSupport`'s existing `deltaRowLevelDml`/Hive cases for the
  * same convention): `DeltaLog.forTable(session, path).snapshot.version`
  * for Delta, and — for Iceberg — a *fresh*
  * `TableCatalog.loadTable(identifier)` call followed by
  * `SparkTable.table().refresh().currentSnapshot().snapshotId()`.
  *
  * Both are deliberately fresh lookups (by path for Delta, by catalog
  * identifier for Iceberg), never a reflective read on some previously-
  * captured `Table`/`DeltaLog` object from earlier in the write's analyzed
  * plan. Confirmed empirically, the hard way, via a real cross-suite test
  * failure: a `Table` object captured at analysis time can fail to see a
  * just-committed Iceberg snapshot even after an explicit `.refresh()`
  * call on it, specifically once a prior suite in the same JVM has already
  * exercised Iceberg's own catalog/table caching — while a plain SQL query
  * against the same table, and a fresh `loadTable` call, both see it
  * correctly. Delta's `.snapshot.version` read was never affected by the
  * same class of problem for exactly this reason: `DeltaLog.forTable` was
  * already a fresh-by-path lookup, not a captured object, before this was
  * even a known risk for Iceberg.
  */
case class WriteEvent(
  contract: Option[String],
  location: String,
  format: Option[String],
  saveMode: Option[String],
  schema: List[WriteFieldInfo],
  timestamp: Long,
  metadata: Map[String, Any],
  durationMs: Long = 0L,
  rowCount: Option[Long] = None,
  bytesWritten: Option[Long] = None,
  fileCount: Option[Long] = None,
  applicationId: Option[String] = None,
  deltaVersion: Option[Long] = None,
  icebergSnapshotId: Option[Long] = None
) extends NotificationEvent {
  val eventType: String = "WRITE"
}
