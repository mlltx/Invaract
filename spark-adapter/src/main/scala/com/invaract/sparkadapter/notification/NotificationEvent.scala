// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.notification

import com.invaract.fingerprint.TransformationFingerprint
import com.invaract.sparkadapter.Violation

/** One thing worth telling an external system about, published through a
  * `NotificationSink` when one is configured and enabled (see
  * `NotificationConfig`/`NotificationSinkFactory`). Three kinds exist:
  *
  *   - `ContractValidationEvent` — a write (or state-changing CALL) was
  *     checked against a contract. Published by `ContractEnforcementRule`
  *     for every check it performs, PASS or FAIL — this is "the contract was
  *     evaluated," not "the write happened."
  *   - `WriteEvent` — a write actually completed. Published by
  *     `SparkAdapterListener`, which (unlike the check rule) only observes
  *     Spark after real execution succeeds — see that class's doc for why
  *     that's a structurally different moment.
  *   - `JobSummaryEvent` — an aggregate over however many of the two events
  *     above a job produced, published once, on demand, by
  *     `SummarizingNotificationSink` (see its own doc for why this is a
  *     sink decorator rather than something `ContractEnforcementRule`/
  *     `SparkAdapterListener` publish themselves).
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
    * `com.invaract.contract.Contract.extensions`) at the moment the event is
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
  *
  * `fingerprints` is `result.fingerprints` carried straight through from
  * the `VerificationResult` this event is built from — `None` unless the
  * check ran with `VerificationOptions.computeFingerprint = true` and had
  * a real plan to fingerprint (see that field's own doc). Reaches every
  * configured sink, PASS or FAILED alike, the same as every other field
  * here — see docs/SEMANTIC_LINEAGE_FINGERPRINTING.md §14.5.
  */
case class ContractValidationEvent(
  contract: String,
  status: String,
  violations: List[Violation],
  timestamp: Long,
  metadata: Map[String, Any],
  applicationId: Option[String] = None,
  fingerprints: Option[TransformationFingerprint] = None
) extends NotificationEvent {
  val eventType: String = "CONTRACT_VALIDATION"
}

/** One field of a `WriteEvent`'s schema — a deliberately minimal projection
  * of Spark's `StructField` (name/type-name/nullable only), so this package
  * (and any external sink deserializing its JSON) never needs a Spark
  * dependency to represent a `WriteEvent`.
  */
case class WriteFieldInfo(name: String, dataType: String, nullable: Boolean)

/** A write's actual catalog registration, if any — a deliberately minimal
  * mirror of `ir.CatalogIdentity`'s shape (same five fields, same
  * meaning), kept as its own package-local case class the same way
  * `WriteFieldInfo` mirrors `StructField`: this is a notification-package
  * type in its own right, with a stable JSON shape a `NotificationSink`
  * can rely on independent of whatever internal shape `ir.CatalogIdentity`
  * evolves into, not merely a type alias for it.
  *
  * `None` on `WriteEvent.catalog` (not an absent field) means "this write
  * has no catalog entry" — an explicit, honest signal a downstream
  * consumer can act on directly (e.g. "flag this job's output as
  * undiscoverable"), not something indistinguishable from "catalog
  * identity wasn't checked at all." See docs/CONTRACT_MODEL.md's
  * `catalog` field and ROADMAP.md's catalog-registration addendum for the
  * org-wide policy this supports.
  */
case class CatalogInfo(
  technology: Option[String] = None,
  catalogName: Option[String] = None,
  location: Option[String] = None,
  namespace: List[String] = Nil,
  table: Option[String] = None
) {
  def toMap: Map[String, Any] =
    Map("namespace" -> namespace) ++
      technology.map("technology" -> _) ++
      catalogName.map("catalogName" -> _) ++
      location.map("location" -> _) ++
      table.map("table" -> _)
}

object CatalogInfo {
  def from(identity: com.invaract.ir.CatalogIdentity): CatalogInfo =
    CatalogInfo(identity.technology, identity.catalogName, identity.location, identity.namespace, identity.table)
}

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
  *
  * `operation` distinguishes which row-level DML this write actually was —
  * `"merge"`/`"update"`/`"delete"` — for the three write shapes where
  * `saveMode` is `None` because in-place mutation isn't append/overwrite/
  * ignore/error. `None` for a plain append/overwrite/create, where
  * `saveMode` already conveys the operation and this would only duplicate
  * it.
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
  icebergSnapshotId: Option[Long] = None,
  operation: Option[String] = None,
  catalog: Option[CatalogInfo] = None
) extends NotificationEvent {
  val eventType: String = "WRITE"
}

/** An aggregate over every `ContractValidationEvent`/`WriteEvent` a
  * `SummarizingNotificationSink` observed since it was constructed (or
  * since the last `publishSummary()` call — see that class's doc for
  * exactly when this is built and published, since nothing publishes it
  * automatically). `checksPassed`/`checksFailed` and `totalViolations`
  * count `ContractValidationEvent`s; `totalWrites` counts `WriteEvent`s;
  * `durationMs` is wall-clock time since the sink was constructed, not a
  * sum of individual write durations (those can overlap under
  * concurrent writes, wall-clock time can't).
  *
  * Exists for exactly the case `WriteEvent`/`ContractValidationEvent`
  * don't cover well: a job with many writes produces many events, useful
  * for a stream processor but noisy for a human — one `JobSummaryEvent`
  * gives a single Slack message or email per run instead.
  */
case class JobSummaryEvent(
  totalWrites: Long,
  checksPassed: Long,
  checksFailed: Long,
  totalViolations: Long,
  durationMs: Long,
  timestamp: Long,
  metadata: Map[String, Any],
  applicationId: Option[String] = None
) extends NotificationEvent {
  val eventType: String = "JOB_SUMMARY"
}
