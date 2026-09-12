// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.notification

import com.invaract.sparkadapter.Violation

import org.scalatest.funsuite.AnyFunSuite

class NotificationJsonSpec extends AnyFunSuite {

  test("anyToJson renders every value shape a NotificationEvent can carry") {
    assert(NotificationJson.anyToJson("hello") == "\"hello\"")
    assert(NotificationJson.anyToJson(42) == "42")
    assert(NotificationJson.anyToJson(42L) == "42")
    assert(NotificationJson.anyToJson(true) == "true")
    assert(NotificationJson.anyToJson(false) == "false")
    assert(NotificationJson.anyToJson(null) == "null")
    assert(NotificationJson.anyToJson(None) == "null")
    assert(NotificationJson.anyToJson(Some("x")) == "\"x\"")
    assert(NotificationJson.anyToJson(List(1, 2, 3)) == "[1, 2, 3]")
    assert(NotificationJson.anyToJson(List.empty[Int]) == "[]")
    assert(NotificationJson.anyToJson(Map("a" -> 1, "b" -> "two")) == """{"a": 1, "b": "two"}""")
    assert(NotificationJson.anyToJson(Map.empty[String, Any]) == "{}")
    // A value with no dedicated case (the `other` fallback) is quoted via toString.
    case class Opaque(x: Int) { override def toString: String = s"opaque($x)" }
    assert(NotificationJson.anyToJson(Opaque(7)) == "\"opaque(7)\"")
  }

  test("anyToJson escapes backslash, quote, newline, carriage return, and tab") {
    val raw = "back\\slash quote\" newline\n cr\r tab\t end"
    val json = NotificationJson.anyToJson(raw)
    assert(json == "\"back\\\\slash quote\\\" newline\\n cr\\r tab\\t end\"")
  }

  test("toJson for ContractValidationEvent includes every field, contract violations as their own maps") {
    val violation = Violation(
      violationType = "MISSING_OUTPUT_FIELD",
      message = "missing 'x'",
      remediation = "add 'x'",
      column = Some("x")
    )
    val event = ContractValidationEvent(
      contract = "demo@1.0.0",
      status = "FAILED",
      violations = List(violation),
      timestamp = 12345L,
      metadata = Map("team" -> "data-platform")
    )
    val json = NotificationJson.toJson(event)

    assert(json.contains("\"eventType\": \"CONTRACT_VALIDATION\""))
    assert(json.contains("\"timestamp\": 12345"))
    assert(json.contains("\"contract\": \"demo@1.0.0\""))
    assert(json.contains("\"status\": \"FAILED\""))
    assert(json.contains("\"type\": \"MISSING_OUTPUT_FIELD\""))
    assert(json.contains("\"column\": \"x\""))
    assert(json.contains("\"team\": \"data-platform\""))
  }

  test("toJson for ContractValidationEvent renders an empty violations list as []") {
    val event = ContractValidationEvent("demo@1.0.0", "PASSED", Nil, 0L, Map.empty)
    val json = NotificationJson.toJson(event)
    assert(json.contains("\"violations\": []"))
    assert(json.contains("\"metadata\": {}"))
  }

  test("toJson for ContractValidationEvent renders applicationId, None as null and Some as a plain value") {
    val withoutAppId = ContractValidationEvent("demo@1.0.0", "PASSED", Nil, 0L, Map.empty)
    assert(NotificationJson.toJson(withoutAppId).contains("\"applicationId\": null"))

    val withAppId = ContractValidationEvent("demo@1.0.0", "PASSED", Nil, 0L, Map.empty, applicationId = Some("app-123"))
    assert(NotificationJson.toJson(withAppId).contains("\"applicationId\": \"app-123\""))
  }

  test("toJson for ContractValidationEvent renders fingerprints, None as null and Some via TransformationFingerprint.toMap") {
    val withoutFingerprints = ContractValidationEvent("demo@1.0.0", "PASSED", Nil, 0L, Map.empty)
    assert(NotificationJson.toJson(withoutFingerprints).contains("\"fingerprints\": null"))

    val orders = com.invaract.ir.Read(com.invaract.ir.DatasetRef("raw.orders"))
    val plan = com.invaract.ir.Write(
      com.invaract.ir.DatasetRef("gold.out"),
      com.invaract.ir.Project(
        orders,
        List(com.invaract.ir.NamedExpr("id", com.invaract.ir.ColumnReference(com.invaract.ir.ColumnRef("id", Some("raw.orders")))))
      )
    )
    val fingerprints = com.invaract.fingerprint.TransformationFingerprinter.fingerprint(plan)
    val withFingerprints = ContractValidationEvent("demo@1.0.0", "PASSED", Nil, 0L, Map.empty, fingerprints = Some(fingerprints))
    val json = NotificationJson.toJson(withFingerprints)
    assert(json.contains(s""""version": ${fingerprints.version}"""))
    assert(json.contains(s""""value": "${fingerprints.overall.value}""""))
    assert(json.contains("\"outputs\": {"))
  }

  test("toJson for WriteEvent includes location/format/saveMode/schema/contract, with None fields as null") {
    val event = WriteEvent(
      contract = None,
      location = "file:/tmp/out.parquet",
      format = None,
      saveMode = None,
      schema = List(WriteFieldInfo("id", "long", nullable = false)),
      timestamp = 999L,
      metadata = Map.empty
    )
    val json = NotificationJson.toJson(event)

    assert(json.contains("\"eventType\": \"WRITE\""))
    assert(json.contains("\"contract\": null"))
    assert(json.contains("\"location\": \"file:/tmp/out.parquet\""))
    assert(json.contains("\"format\": null"))
    assert(json.contains("\"saveMode\": null"))
    assert(json.contains("\"name\": \"id\""))
    assert(json.contains("\"type\": \"long\""))
    assert(json.contains("\"nullable\": false"))
  }

  test("toJson for WriteEvent carries Some(contract)/Some(format)/Some(saveMode) through as plain values, not wrapped") {
    val event = WriteEvent(
      contract = Some("demo@1.0.0"),
      location = "file:/tmp/out.parquet",
      format = Some("parquet"),
      saveMode = Some("overwrite"),
      schema = Nil,
      timestamp = 0L,
      metadata = Map.empty
    )
    val json = NotificationJson.toJson(event)
    assert(json.contains("\"contract\": \"demo@1.0.0\""))
    assert(json.contains("\"format\": \"parquet\""))
    assert(json.contains("\"saveMode\": \"overwrite\""))
    assert(json.contains("\"schema\": []"))
  }

  test("toJson for WriteEvent renders durationMs/rowCount/bytesWritten/fileCount/applicationId") {
    val withMetrics = WriteEvent(
      contract = None,
      location = "file:/tmp/out.parquet",
      format = None,
      saveMode = None,
      schema = Nil,
      timestamp = 0L,
      metadata = Map.empty,
      durationMs = 42L,
      rowCount = Some(5L),
      bytesWritten = Some(1024L),
      fileCount = Some(2L),
      applicationId = Some("app-123")
    )
    val json = NotificationJson.toJson(withMetrics)
    assert(json.contains("\"durationMs\": 42"))
    assert(json.contains("\"rowCount\": 5"))
    assert(json.contains("\"bytesWritten\": 1024"))
    assert(json.contains("\"fileCount\": 2"))
    assert(json.contains("\"applicationId\": \"app-123\""))
  }

  test("toJson for WriteEvent renders default durationMs=0 and None metrics as null, not omitted") {
    val defaults = WriteEvent(None, "file:/tmp/out.parquet", None, None, Nil, 0L, Map.empty)
    val json = NotificationJson.toJson(defaults)
    assert(json.contains("\"durationMs\": 0"))
    assert(json.contains("\"rowCount\": null"))
    assert(json.contains("\"bytesWritten\": null"))
    assert(json.contains("\"fileCount\": null"))
    assert(json.contains("\"applicationId\": null"))
    assert(json.contains("\"deltaVersion\": null"))
    assert(json.contains("\"icebergSnapshotId\": null"))
    assert(json.contains("\"operation\": null"))
    assert(json.contains("\"catalog\": null"))
  }

  test("toJson for WriteEvent renders a populated catalog as a nested object, only its declared sub-fields") {
    val event = WriteEvent(
      contract = None,
      location = "file:/tmp/out.parquet",
      format = Some("parquet"),
      saveMode = None,
      schema = Nil,
      timestamp = 0L,
      metadata = Map.empty,
      catalog = Some(
        CatalogInfo(
          technology = Some("hive"),
          catalogName = Some("spark_catalog"),
          location = Some("thrift://metastore1.example.com:9083"),
          namespace = List("default"),
          table = Some("sales")
        )
      )
    )
    val json = NotificationJson.toJson(event)
    assert(json.contains("\"technology\": \"hive\""))
    assert(json.contains("\"catalogName\": \"spark_catalog\""))
    assert(json.contains("\"location\": \"thrift://metastore1.example.com:9083\""))
    assert(json.contains("\"namespace\": [\"default\"]"))
    assert(json.contains("\"table\": \"sales\""))
  }

  test("toJson for WriteEvent's catalog omits unset sub-fields entirely rather than rendering them as null") {
    val event = WriteEvent(
      contract = None,
      location = "file:/tmp/out.parquet",
      format = None,
      saveMode = None,
      schema = Nil,
      timestamp = 0L,
      metadata = Map.empty,
      catalog = Some(CatalogInfo(technology = Some("delta")))
    )
    val json = NotificationJson.toJson(event)
    assert(json.contains("\"technology\": \"delta\""))
    assert(!json.contains("\"catalogName\""), s"unset catalogName shouldn't appear at all: $json")
    assert(!json.contains("\"table\""), s"unset table shouldn't appear at all: $json")
    assert(json.contains("\"namespace\": []"), "namespace always renders, even empty - it's the only non-Option field")
  }

  test("toJson for WriteEvent renders operation, None as null and Some as a plain string") {
    val merge = WriteEvent(None, "file:/tmp/out.parquet", Some("delta"), None, Nil, 0L, Map.empty, operation = Some("merge"))
    assert(NotificationJson.toJson(merge).contains("\"operation\": \"merge\""))

    val plainAppend = WriteEvent(None, "file:/tmp/out.parquet", Some("parquet"), Some("append"), Nil, 0L, Map.empty)
    assert(NotificationJson.toJson(plainAppend).contains("\"operation\": null"))
  }

  test("toJson for JobSummaryEvent includes every field") {
    val summary = JobSummaryEvent(
      totalWrites = 3L,
      checksPassed = 2L,
      checksFailed = 1L,
      totalViolations = 4L,
      durationMs = 1234L,
      timestamp = 999L,
      metadata = Map("team" -> "data-platform"),
      applicationId = Some("app-123")
    )
    val json = NotificationJson.toJson(summary)
    assert(json.contains("\"eventType\": \"JOB_SUMMARY\""))
    assert(json.contains("\"timestamp\": 999"))
    assert(json.contains("\"totalWrites\": 3"))
    assert(json.contains("\"checksPassed\": 2"))
    assert(json.contains("\"checksFailed\": 1"))
    assert(json.contains("\"totalViolations\": 4"))
    assert(json.contains("\"durationMs\": 1234"))
    assert(json.contains("\"team\": \"data-platform\""))
    assert(json.contains("\"applicationId\": \"app-123\""))
  }

  test("toJson for JobSummaryEvent renders a default (no applicationId) event with null, not omitted") {
    val summary = JobSummaryEvent(0L, 0L, 0L, 0L, 0L, 0L, Map.empty)
    val json = NotificationJson.toJson(summary)
    assert(json.contains("\"totalWrites\": 0"))
    assert(json.contains("\"applicationId\": null"))
  }

  test("toJson for WriteEvent renders deltaVersion/icebergSnapshotId, None as null and Some as a plain value") {
    val deltaWrite = WriteEvent(None, "file:/tmp/out.parquet", Some("delta"), None, Nil, 0L, Map.empty, deltaVersion = Some(7L))
    val deltaJson = NotificationJson.toJson(deltaWrite)
    assert(deltaJson.contains("\"deltaVersion\": 7"))
    assert(deltaJson.contains("\"icebergSnapshotId\": null"))

    val icebergWrite =
      WriteEvent(None, "file:/tmp/out.parquet", Some("iceberg"), None, Nil, 0L, Map.empty, icebergSnapshotId = Some(123456789L))
    val icebergJson = NotificationJson.toJson(icebergWrite)
    assert(icebergJson.contains("\"icebergSnapshotId\": 123456789"))
    assert(icebergJson.contains("\"deltaVersion\": null"))
  }
}
