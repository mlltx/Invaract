// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.notification

/** Renders a `NotificationEvent` as JSON for the built-in sinks
  * (`LoggingNotificationSink`/`FileNotificationSink`/`HttpNotificationSink`)
  * — a small, dependency-free encoder in the same hand-rolled style
  * `runner.DemoJobHarness.reportToJson`/`anyToJson` already use for
  * `demo/output/report.json`, rather than pulling in a JSON library this
  * module has never otherwise needed (see CLAUDE.md's dependency
  * discipline). Public, not `private[invaract]`: a custom
  * `NotificationSink` — including one living in a separate module/jar,
  * like `invaract-notification-kafka`'s `KafkaNotificationSink` — is free
  * to reuse this rather than reinventing an event's JSON rendering, or to
  * ignore it entirely and serialize `NotificationEvent` however its own
  * destination expects.
  */
object NotificationJson {

  /** The version of the JSON shape every event below renders. A receiver can branch on
    * it instead of guessing from which fields are present. Additive changes (a new
    * field, a new `status` value) do NOT bump it - a receiver must ignore fields it
    * does not know; only a change that could break such a receiver (a removed or
    * renamed field, a changed type) does. The published JSON Schema
    * (`docs-site/public/schemas/notification/v1/`) is versioned the same way.
    */
  val SchemaVersion = 1

  /** Each event type's own fixed field order — deliberately not routed
    * through a generic case-class-to-map reflection, so the JSON shape is
    * an explicit, reviewable contract rather than whatever field order the
    * compiler happens to produce.
    *
    * Every event also carries `schemaVersion` and `eventId`. `eventId` is the SHA-256
    * of the event's own content (keys sorted, so it does not depend on map ordering),
    * which makes it deterministic: re-sending the same event - a retry, a replay from
    * a dead-letter file - yields the same id, so a receiver can deduplicate on it.
    * Two different events never share one in practice, because `timestamp` is part of
    * the content.
    */
  def toJson(event: NotificationEvent): String = {
    val content = fields(event)
    anyToJson(content + ("schemaVersion" -> SchemaVersion, "eventId" -> sha256Hex(canonicalJson(content))))
  }

  /** `eventId` for `event`, exactly as `toJson` embeds it. */
  def eventIdOf(event: NotificationEvent): String = sha256Hex(canonicalJson(fields(event)))

  private def sha256Hex(text: String): String =
    java.security.MessageDigest.getInstance("SHA-256").digest(text.getBytes("UTF-8")).map("%02x".format(_)).mkString

  private def fields(event: NotificationEvent): Map[String, Any] = event match {
    case e: ContractValidationEvent =>
      Map(
        "eventType" -> e.eventType,
        "timestamp" -> e.timestamp,
        "contract" -> e.contract,
        "status" -> e.status,
        "violations" -> e.violations.map(_.toMap),
        "metadata" -> e.metadata,
        "applicationId" -> e.applicationId,
        "fingerprints" -> e.fingerprints.map(_.toMap),
        "dataQuality" -> e.dataQuality.map(_.toMap),
        "roleConformance" -> e.roleConformance.map(_.toMap),
        "unverifiableInputs" -> e.unverifiableInputs.map(_.toMap)
      )
    case e: WriteEvent =>
      Map(
        "eventType" -> e.eventType,
        "timestamp" -> e.timestamp,
        "contract" -> e.contract,
        "location" -> e.location,
        "format" -> e.format,
        "saveMode" -> e.saveMode,
        "schema" -> e.schema.map(f => Map("name" -> f.name, "type" -> f.dataType, "nullable" -> f.nullable)),
        "metadata" -> e.metadata,
        "durationMs" -> e.durationMs,
        "rowCount" -> e.rowCount,
        "bytesWritten" -> e.bytesWritten,
        "fileCount" -> e.fileCount,
        "applicationId" -> e.applicationId,
        "deltaVersion" -> e.deltaVersion,
        "icebergSnapshotId" -> e.icebergSnapshotId,
        "operation" -> e.operation,
        "catalog" -> e.catalog.map(_.toMap),
        "partitionColumns" -> e.partitionColumns,
        "datasetType" -> e.datasetType.map(_.name)
      )
    case e: JobSummaryEvent =>
      Map(
        "eventType" -> e.eventType,
        "timestamp" -> e.timestamp,
        "totalWrites" -> e.totalWrites,
        "checksPassed" -> e.checksPassed,
        "checksFailed" -> e.checksFailed,
        "totalViolations" -> e.totalViolations,
        "durationMs" -> e.durationMs,
        "metadata" -> e.metadata,
        "applicationId" -> e.applicationId
      )
    case e: ContractInferenceEvent =>
      Map(
        "eventType" -> e.eventType,
        "timestamp" -> e.timestamp,
        "status" -> e.status,
        "reason" -> e.reason,
        "writeLocation" -> e.writeLocation,
        "contractYaml" -> e.contractYaml,
        "contractDigest" -> e.contractYaml.map(sha256Hex),
        "selfCheck" -> e.selfCheck,
        "selfCheckViolations" -> e.selfCheckViolations.map(_.toMap),
        "diagnostics" -> e.diagnostics,
        "fingerprints" -> e.fingerprints.map(_.toMap),
        "job" -> e.job.toMap,
        "metadata" -> e.metadata
      )
    case e: DryRunSummaryEvent =>
      Map(
        "eventType" -> e.eventType,
        "timestamp" -> e.timestamp,
        "statusCounts" -> e.statusCounts,
        "mergeStatus" -> e.mergeStatus,
        "mergedContractYaml" -> e.mergedContractYaml,
        "mergedContractDigest" -> e.mergedContractYaml.map(sha256Hex),
        "mergeConflicts" -> e.mergeConflicts,
        "job" -> e.job.toMap,
        "metadata" -> e.metadata
      )
  }

  /** Recursively renders any value `fields` above can produce — `Map`/`List`/
    * `Option`/`String`/`Number`/`Boolean`/`null` — the same value shapes
    * `Violation.toMap` and `Contract.extensions` (SnakeYAML-sourced) ever
    * carry.
    */
  def anyToJson(obj: Any): String = render(obj, sortKeys = false)

  /** Same rendering with every object's keys sorted, so equal content renders to equal
    * text regardless of how a `Map` happened to be built - what `eventId` hashes.
    */
  private def canonicalJson(obj: Any): String = render(obj, sortKeys = true)

  private def render(obj: Any, sortKeys: Boolean): String = obj match {
    case m: Map[_, _] =>
      val entries = if (sortKeys) m.toList.sortBy(_._1.toString) else m.toList
      "{" + entries.map { case (k, v) => s""""${escape(k.toString)}": ${render(v, sortKeys)}""" }.mkString(", ") + "}"
    case it: Iterable[_] =>
      "[" + it.map(render(_, sortKeys)).mkString(", ") + "]"
    case Some(v) => render(v, sortKeys)
    case None => "null"
    case s: String => quote(s)
    case n: Number => n.toString
    case b: Boolean => b.toString
    case null => "null"
    case other => quote(other.toString)
  }

  private def quote(s: String): String = "\"" + escape(s) + "\""

  private def escape(s: String): String =
    s.replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t")
}
