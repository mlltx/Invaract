// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.notification

import com.fasterxml.jackson.databind.ObjectMapper
import com.invaract.contract.DatasetType
import com.invaract.fingerprint.{Fingerprint, OutputFingerprint, TransformationFingerprint}
import com.invaract.sparkadapter._

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

/** The published JSON Schema against what the engine really emits, and the envelope
  * (`schemaVersion`, `eventId`, digests) every event carries.
  *
  * The strongest check is not in this file: `TestNotificationSink` validates every event
  * published anywhere in the suite, from real writes and real checks, against the same schema.
  */
class EventSchemaSpec extends AnyFunSuite {
  private val mapper = new ObjectMapper()

  private def sha256(text: String): String =
    java.security.MessageDigest.getInstance("SHA-256").digest(text.getBytes("UTF-8")).map("%02x".format(_)).mkString

  private val fingerprint = Fingerprint(1, "SHA-256", "ab" * 32)
  private val transformation = TransformationFingerprint(
    version = 1,
    overall = fingerprint,
    inputs = Map("file:/in#0" -> fingerprint),
    outputs = Map("id" -> OutputFingerprint(fingerprint, fingerprint, fingerprint, Some(false))),
    rowMutation = Some(fingerprint)
  )
  private val violation = Violation(
    ViolationType.MissingOutputField, "missing 'x'", "add 'x'", column = Some("x"), location = Some("/out"),
    expected = Some("long"), actual = Some("string")
  )
  private val job = JobInfo(
    Some("app-1"), Some("orders"), Some("nightly"), Some("3.5.1"), Some("local[*]"), Some("client"), Some("svc"), Some(1700L),
    Map("team" -> "data-eng")
  )

  /** One of every event, with every optional field both present and (in the second set) absent. */
  private val fullEvents: List[NotificationEvent] = List(
    ContractValidationEvent(
      "demo@1.0.0", "FAILED", List(violation), 1L, Map("owner" -> "team-a", "n" -> 3), Some("app-1"), Some(transformation),
      List(DataQualityCheckResult("id", "NOT NULL", DataQualityVerdict.Guaranteed)),
      List(RoleConformanceCheckResult("orders", DatasetType.Source, RoleConformanceVerdict.Conforms, "read and used")),
      List(UnverifiableInput("orders", "/in", List("LogicalRDD")))
    ),
    WriteEvent(
      Some("demo@1.0.0"), "file:/out", Some("parquet"), Some("overwrite"), List(WriteFieldInfo("id", "long", nullable = true)), 2L,
      Map("owner" -> "team-a"), 12L, Some(10L), Some(1024L), Some(1L), Some("app-1"), Some(3L), Some(99L), Some("merge"),
      Some(CatalogInfo(Some("iceberg"), Some("local"), Some("/wh"), List("db"), Some("t"))), List("dt"), Some(DatasetType.Control)
    ),
    JobSummaryEvent(1L, 2L, 3L, 4L, 5L, 6L, Map("k" -> "v"), Some("app-1")),
    ContractInferenceEvent(
      "INFERRED_DEGRADED", Some("why"), Some("/out"), Some("id: x\nversion: 0.1.0"), Some("FAILED"), List(violation), List("d"),
      Some(transformation), job, 7L, Map("team" -> "data-eng")
    ),
    DryRunSummaryEvent(Map("INFERRED" -> 2L, "SKIPPED_UNRECOGNIZED" -> 1L), "MERGED", Some("id: merged"), List("clash"), job, 8L, Map("team" -> "data-eng"))
  )

  private val sparseEvents: List[NotificationEvent] = List(
    ContractValidationEvent("demo@1.0.0", "PASSED", Nil, 1L, Map.empty),
    WriteEvent(None, "x", None, None, Nil, 2L, Map.empty),
    JobSummaryEvent(0L, 0L, 0L, 0L, 0L, 0L, Map.empty),
    ContractInferenceEvent("SKIPPED_UNSUPPORTED", None, None, None, None, Nil, Nil, None, JobInfo(), 7L, Map.empty),
    DryRunSummaryEvent(Map("NO_WRITES_OBSERVED" -> 1L), "NO_DRAFTS", None, Nil, JobInfo(), 8L, Map.empty)
  )

  test("every event type validates against the published schema, with all optionals present and with none") {
    (fullEvents ++ sparseEvents).foreach { event =>
      val problems = EventSchema.errors(NotificationJson.toJson(event))
      assert(problems.isEmpty, s"${event.eventType}: ${problems.mkString("; ")}")
    }
    assert((fullEvents ++ sparseEvents).map(_.eventType).toSet ==
      Set("CONTRACT_VALIDATION", "WRITE", "JOB_SUMMARY", "CONTRACT_INFERENCE", "DRY_RUN_SUMMARY"))
  }

  test("the checker is not vacuous: it rejects a missing envelope field, a wrong version, an unknown type, and a wrong field type") {
    val good = mapper.readTree(NotificationJson.toJson(sparseEvents.head)).asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
    def without(field: String) = { val c = good.deepCopy(); c.remove(field); c.toString }
    def withField(field: String, value: String) = { val c = good.deepCopy(); c.set[com.fasterxml.jackson.databind.JsonNode](field, mapper.readTree(value)); c.toString }

    assert(EventSchema.errors(good.toString).isEmpty)
    assert(EventSchema.errors(without("eventId")).nonEmpty)
    assert(EventSchema.errors(without("schemaVersion")).nonEmpty)
    assert(EventSchema.errors(without("violations")).nonEmpty)
    assert(EventSchema.errors(withField("schemaVersion", "2")).nonEmpty)
    assert(EventSchema.errors(withField("eventId", "\"NOT-A-HASH\"")).nonEmpty)
    assert(EventSchema.errors(withField("eventType", "\"SOMETHING_ELSE\"")).nonEmpty)
    assert(EventSchema.errors(withField("timestamp", "\"yesterday\"")).nonEmpty)
    assert(EventSchema.errors(withField("violations", "{}")).nonEmpty)
    assert(EventSchema.errors(withField("violations", "[{\"type\":\"X\"}]")).nonEmpty, "a violation missing message/remediation")
    assert(EventSchema.errors(withField("applicationId", "5")).nonEmpty)
  }

  test("a receiver-side extension is allowed: unknown extra fields are ignored, as the schema promises") {
    val good = mapper.readTree(NotificationJson.toJson(fullEvents.head)).asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
    good.put("aFieldAddedInSomeFutureVersion", "x")
    assert(EventSchema.errors(good.toString).isEmpty)
  }

  test("the schema file only uses keywords the checker implements (it throws on any other), so nothing is silently unchecked") {
    // Validating any event walks the whole reachable schema; an unimplemented keyword would have thrown above.
    // Make that explicit for every definition, including ones no sparse event reaches.
    val defs = EventSchema.schema.get("$defs").fieldNames().asScala.toList
    assert(defs.toSet == Set(
      "Envelope", "Violation", "Fingerprint", "OutputFingerprint", "TransformationFingerprint", "DataQualityCheckResult",
      "RoleConformanceCheckResult", "UnverifiableInput", "CatalogInfo", "JobInfo", "ContractValidationEvent", "WriteEvent",
      "JobSummaryEvent", "ContractInferenceEvent", "DryRunSummaryEvent"
    ))
  }

  test("every file in the published examples directory validates, and together they cover every event type") {
    val dir = EventSchema.repoFile("docs-site/public/schemas/notification/v1/examples")
    val files = dir.listFiles().filter(_.getName.endsWith(".json")).toList.sortBy(_.getName)
    assert(files.nonEmpty, "no examples published")
    val types = files.map { f =>
      val text = new String(java.nio.file.Files.readAllBytes(f.toPath), "UTF-8")
      val problems = EventSchema.errors(text)
      assert(problems.isEmpty, s"${f.getName}: ${problems.mkString("; ")}")
      mapper.readTree(text).get("eventType").asText()
    }.toSet
    assert(types == Set("CONTRACT_VALIDATION", "WRITE", "JOB_SUMMARY", "CONTRACT_INFERENCE", "DRY_RUN_SUMMARY"), s"examples cover only $types")
  }

  test("schemaVersion is 1 on every event, and NotificationJson.SchemaVersion says so") {
    assert(NotificationJson.SchemaVersion == 1)
    (fullEvents ++ sparseEvents).foreach { e =>
      assert(mapper.readTree(NotificationJson.toJson(e)).get("schemaVersion").asInt() == 1)
    }
  }

  test("eventId is the SHA-256 of the event's content, 64 hex characters, and eventIdOf reports the same value") {
    (fullEvents ++ sparseEvents).foreach { e =>
      val id = mapper.readTree(NotificationJson.toJson(e)).get("eventId").asText()
      assert(id.matches("[0-9a-f]{64}"), id)
      assert(id == NotificationJson.eventIdOf(e))
    }
  }

  test("eventId is deterministic: the same event always yields the same id, so a retry or replay can be deduplicated") {
    val e = fullEvents.head
    assert(NotificationJson.eventIdOf(e) == NotificationJson.eventIdOf(e))
    assert(NotificationJson.toJson(e) == NotificationJson.toJson(e))
    // An equal event built separately.
    assert(NotificationJson.eventIdOf(sparseEvents.head) == NotificationJson.eventIdOf(sparseEvents.head.asInstanceOf[ContractValidationEvent].copy()))
  }

  test("eventId changes when anything in the event changes, and ignores how a Map happened to be ordered") {
    val base = ContractValidationEvent("demo@1.0.0", "PASSED", Nil, 1L, Map("a" -> 1, "b" -> 2, "c" -> 3))
    val id = NotificationJson.eventIdOf(base)
    assert(NotificationJson.eventIdOf(base.copy(timestamp = 2L)) != id)
    assert(NotificationJson.eventIdOf(base.copy(status = "FAILED")) != id)
    assert(NotificationJson.eventIdOf(base.copy(contract = "other@1.0.0")) != id)
    assert(NotificationJson.eventIdOf(base.copy(metadata = Map("a" -> 1, "b" -> 2))) != id)
    assert(NotificationJson.eventIdOf(base.copy(metadata = Map("c" -> 3, "b" -> 2, "a" -> 1))) == id, "key order must not matter")
    // Nested maps too.
    val nested1 = base.copy(metadata = Map("o" -> Map("x" -> 1, "y" -> 2)))
    val nested2 = base.copy(metadata = Map("o" -> Map("y" -> 2, "x" -> 1)))
    assert(NotificationJson.eventIdOf(nested1) == NotificationJson.eventIdOf(nested2))
  }

  test("eventId does not include itself, schemaVersion, or any other envelope addition: it hashes the content alone") {
    val e = sparseEvents.head
    val json = mapper.readTree(NotificationJson.toJson(e)).asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
    json.remove("eventId")
    json.remove("schemaVersion")
    val content = json.fieldNames().asScala.toList.sorted
    assert(content == List("applicationId", "contract", "dataQuality", "eventType", "fingerprints", "metadata", "roleConformance", "status", "timestamp", "unverifiableInputs", "violations"))
  }

  test("contractDigest is the SHA-256 of contractYaml (null without one), and identical drafts have identical digests") {
    val yaml = "id: x\nversion: 0.1.0"
    val withYaml = fullEvents(3).asInstanceOf[ContractInferenceEvent]
    val json = mapper.readTree(NotificationJson.toJson(withYaml))
    assert(json.get("contractDigest").asText() == sha256(yaml))
    assert(mapper.readTree(NotificationJson.toJson(withYaml.copy(timestamp = 99L))).get("contractDigest").asText() == sha256(yaml))
    assert(mapper.readTree(NotificationJson.toJson(withYaml.copy(contractYaml = Some("id: y")))).get("contractDigest").asText() == sha256("id: y"))
    assert(mapper.readTree(NotificationJson.toJson(withYaml.copy(contractYaml = None))).get("contractDigest").isNull)
  }

  test("mergedContractDigest is the SHA-256 of mergedContractYaml (null without one)") {
    val summary = fullEvents(4).asInstanceOf[DryRunSummaryEvent]
    assert(mapper.readTree(NotificationJson.toJson(summary)).get("mergedContractDigest").asText() == sha256("id: merged"))
    assert(mapper.readTree(NotificationJson.toJson(summary.copy(mergedContractYaml = None))).get("mergedContractDigest").isNull)
  }
}
