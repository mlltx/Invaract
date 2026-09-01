// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.sparkadapter.notification

import com.example.sparkadapter.Violation

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
}
