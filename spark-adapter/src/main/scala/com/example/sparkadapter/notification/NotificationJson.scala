// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter.notification

/** Renders a `NotificationEvent` as JSON for the built-in sinks
  * (`LoggingNotificationSink`/`FileNotificationSink`/`HttpNotificationSink`)
  * — a small, dependency-free encoder in the same hand-rolled style
  * `runner.DemoJobHarness.reportToJson`/`anyToJson` already use for
  * `demo/output/report.json`, rather than pulling in a JSON library this
  * module has never otherwise needed (see CLAUDE.md's dependency
  * discipline). Public, not `private[sparkadapter]`: a custom
  * `NotificationSink` — including one living in a separate module/jar,
  * like `invaract-notification-kafka`'s `KafkaNotificationSink` — is free
  * to reuse this rather than reinventing an event's JSON rendering, or to
  * ignore it entirely and serialize `NotificationEvent` however its own
  * destination expects.
  */
object NotificationJson {

  /** Each event type's own fixed field order — deliberately not routed
    * through a generic case-class-to-map reflection, so the JSON shape is
    * an explicit, reviewable contract rather than whatever field order the
    * compiler happens to produce.
    */
  def toJson(event: NotificationEvent): String = anyToJson(fields(event))

  private def fields(event: NotificationEvent): Map[String, Any] = event match {
    case e: ContractValidationEvent =>
      Map(
        "eventType" -> e.eventType,
        "timestamp" -> e.timestamp,
        "contract" -> e.contract,
        "status" -> e.status,
        "violations" -> e.violations.map(_.toMap),
        "metadata" -> e.metadata,
        "applicationId" -> e.applicationId
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
        "icebergSnapshotId" -> e.icebergSnapshotId
      )
  }

  /** Recursively renders any value `fields` above can produce — `Map`/`List`/
    * `Option`/`String`/`Number`/`Boolean`/`null` — the same value shapes
    * `Violation.toMap` and `Contract.extensions` (SnakeYAML-sourced) ever
    * carry.
    */
  def anyToJson(obj: Any): String = obj match {
    case m: Map[_, _] =>
      "{" + m.map { case (k, v) => s""""${escape(k.toString)}": ${anyToJson(v)}""" }.mkString(", ") + "}"
    case it: Iterable[_] =>
      "[" + it.map(anyToJson).mkString(", ") + "]"
    case Some(v) => anyToJson(v)
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
