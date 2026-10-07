// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter.notification

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import java.io.File
import scala.collection.JavaConverters._

/** Checks event JSON against the published schema
  * (`docs-site/public/schemas/notification/v1/event.schema.json`).
  *
  * This is a deliberately small validator, not a JSON Schema library: this module's Jackson
  * versions are pinned together with Spark's, and a third-party schema library (with
  * Jackson modules of its own) is exactly the kind of dependency that has broken that
  * pinning before. It implements only the keywords the schema file uses, and **throws on
  * any validation keyword it does not implement**, so the schema cannot grow a constraint
  * this checker would silently skip. The same schema and its examples are also checked by
  * Ajv, a real implementation of the specification, in the docs build
  * (`docs-site/scripts/check-schemas.mjs`).
  */
object EventSchema {
  private val mapper = new ObjectMapper()

  val SchemaPath = "docs-site/public/schemas/notification/v1/event.schema.json"

  /** Keywords that describe rather than constrain. */
  private val annotations = Set("$schema", "$id", "title", "description", "examples", "$defs", "$comment")
  private val implemented = Set("$ref", "type", "const", "enum", "required", "properties", "additionalProperties", "items", "oneOf", "allOf", "pattern")

  /** The repository root, found from wherever sbt was started (a module directory or the root). */
  def repoFile(relative: String): File =
    Iterator(new File("."), new File(".."), new File("../.."))
      .map(dir => new File(dir, relative))
      .find(_.exists())
      .getOrElse(throw new IllegalStateException(s"cannot find '$relative' from ${new File(".").getCanonicalPath}"))

  lazy val schema: JsonNode = mapper.readTree(repoFile(SchemaPath))

  /** Every violation found; empty means valid. */
  def errors(json: String): List[String] = validate(mapper.readTree(json), schema, "$")

  def assertValid(json: String): Unit = {
    val problems = errors(json)
    if (problems.nonEmpty) {
      throw new AssertionError(s"event violates $SchemaPath:\n${problems.mkString("\n")}\n--- event ---\n$json")
    }
  }

  def assertValid(event: NotificationEvent): Unit = assertValid(NotificationJson.toJson(event))

  private def validate(node: JsonNode, rule: JsonNode, path: String): List[String] = {
    val keywords = rule.fieldNames().asScala.toList
    val unsupported = keywords.filterNot(k => annotations(k) || implemented(k))
    if (unsupported.nonEmpty) {
      throw new IllegalStateException(s"EventSchema does not implement keyword(s) ${unsupported.mkString(", ")} used at $path")
    }

    def has(keyword: String): Boolean = rule.has(keyword)

    val fromRef =
      if (has("$ref")) {
        val ref = rule.get("$ref").asText()
        require(ref.startsWith("#/$defs/"), s"only local #/$$defs/ references are supported, got $ref")
        validate(node, schema.get("$defs").get(ref.stripPrefix("#/$defs/")), path)
      } else Nil

    val fromType =
      if (!has("type")) Nil
      else {
        val allowed = rule.get("type") match {
          case t if t.isArray => t.elements().asScala.map(_.asText()).toList
          case t              => List(t.asText())
        }
        if (allowed.exists(matchesType(node, _))) Nil else List(s"$path: expected ${allowed.mkString(" or ")}, got ${describe(node)}")
      }

    val fromConst =
      if (has("const") && rule.get("const") != node) List(s"$path: expected constant ${rule.get("const")}, got $node") else Nil

    val fromEnum =
      if (has("enum") && !rule.get("enum").elements().asScala.contains(node)) List(s"$path: ${node} is not one of ${rule.get("enum")}") else Nil

    val fromPattern =
      if (has("pattern") && node.isTextual && !java.util.regex.Pattern.compile(rule.get("pattern").asText()).matcher(node.asText()).find())
        List(s"$path: '${node.asText()}' does not match ${rule.get("pattern").asText()}")
      else Nil

    val fromObject =
      if (!node.isObject) Nil
      else {
        val required =
          if (has("required")) rule.get("required").elements().asScala.map(_.asText()).filterNot(node.has).map(k => s"$path: missing required property '$k'").toList
          else Nil
        val declared = if (has("properties")) rule.get("properties") else mapper.createObjectNode()
        val known = node.fieldNames().asScala.toList.filter(declared.has).flatMap(k => validate(node.get(k), declared.get(k), s"$path.$k"))
        val extra =
          if (!has("additionalProperties")) Nil
          else {
            val additional = rule.get("additionalProperties")
            node.fieldNames().asScala.toList.filterNot(declared.has).flatMap { k =>
              if (additional.isBoolean) { if (additional.asBoolean()) Nil else List(s"$path: unexpected property '$k'") }
              else validate(node.get(k), additional, s"$path.$k")
            }
          }
        required ++ known ++ extra
      }

    val fromArray =
      if (!node.isArray || !has("items")) Nil
      else node.elements().asScala.toList.zipWithIndex.flatMap { case (item, i) => validate(item, rule.get("items"), s"$path[$i]") }

    val fromAllOf =
      if (!has("allOf")) Nil else rule.get("allOf").elements().asScala.toList.flatMap(validate(node, _, path))

    val fromOneOf =
      if (!has("oneOf")) Nil
      else {
        val branches = rule.get("oneOf").elements().asScala.toList.map(validate(node, _, path))
        val matching = branches.count(_.isEmpty)
        if (matching == 1) Nil
        else List(s"$path: matched $matching of ${branches.size} oneOf branches" + (if (matching == 0) s" (${branches.flatten.take(3).mkString("; ")})" else ""))
      }

    fromRef ++ fromType ++ fromConst ++ fromEnum ++ fromPattern ++ fromObject ++ fromArray ++ fromAllOf ++ fromOneOf
  }

  private def matchesType(node: JsonNode, jsonType: String): Boolean = jsonType match {
    case "object"  => node.isObject
    case "array"   => node.isArray
    case "string"  => node.isTextual
    case "integer" => node.isIntegralNumber
    case "number"  => node.isNumber
    case "boolean" => node.isBoolean
    case "null"    => node.isNull
    case other     => throw new IllegalStateException(s"unknown JSON type '$other'")
  }

  private def describe(node: JsonNode): String = node.getNodeType.toString.toLowerCase
}
