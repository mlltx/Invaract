// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification.openlineage

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.invaract.verification.notification.EventSchema

import java.io.File
import scala.collection.JavaConverters._

/** Validates OpenLineage events against the official OpenLineage JSON Schema (vendored, `src/test/resources/openlineage/
  * spec`, release 1.55.0) and every facet in them against the schema its own `_schemaURL` names: a standard facet against
  * the vendored file of exactly that version (a version not vendored is an error, so a facet version in
  * `OpenLineageMapper` cannot drift from what is tested), an Invaract facet against the schema the docs site publishes
  * (`docs-site/public/schemas/openlineage/`).
  *
  * The same small-validator approach as `EventSchema` (this module's Jackson is pinned with Spark's): it implements the
  * keywords these schemas use and throws on any other, so a schema cannot grow a constraint this checker would skip.
  */
object OpenLineageSchema {
  private val mapper = new ObjectMapper()

  private val SpecDir = "verification-core/src/test/resources/openlineage/spec"
  private val InvaractDir = "docs-site/public/schemas/openlineage"
  private val EventUrl = "https://openlineage.io/spec/2-0-2/OpenLineage.json"

  private val annotations = Set("$schema", "$id", "$defs", "description", "example", "examples", "deprecated", "title", "documentation", "$comment")
  private val implemented = Set("$ref", "type", "enum", "required", "properties", "additionalProperties", "items", "allOf", "anyOf", "oneOf", "not", "format", "minimum")

  private val documents = new java.util.concurrent.ConcurrentHashMap[String, JsonNode]()

  /** The documents a URL (without fragment) stands for. */
  private def document(url: String): JsonNode = documents.computeIfAbsent(url, u => mapper.readTree(fileFor(u)))

  private def fileFor(url: String): File = {
    val StandardFacet = "https://openlineage.io/spec/facets/([0-9-]+)/(\\w+)\\.json".r
    val InvaractFacet = "https://mlltx.github.io/Invaract/schemas/openlineage/([0-9-]+)/(\\w+)\\.json".r
    url match {
      case EventUrl => EventSchema.repoFile(s"$SpecDir/OpenLineage.json")
      case StandardFacet(version, name) =>
        val file = EventSchema.repoFile(s"$SpecDir/facets/$name.json")
        val vendored = mapper.readTree(file).get("$id").asText()
        if (vendored != url) throw new IllegalStateException(s"facet $name is published as version $version, but the vendored spec has $vendored")
        file
      case InvaractFacet(version, name) => EventSchema.repoFile(s"$InvaractDir/$version/$name.json")
      case other => throw new IllegalStateException(s"no schema vendored for '$other'")
    }
  }

  /** What is wrong with `json` as an OpenLineage run event; empty when nothing is. */
  def errors(json: String): List[String] = {
    val event = mapper.readTree(json)
    val base = validate(event, s"$EventUrl#/$$defs/RunEvent", "$")
    val facets = facetsIn(event).flatMap { case (path, facet) =>
      Option(facet.get("_schemaURL")).map(_.asText()) match {
        case None => List(s"$path: has no _schemaURL")
        case Some(url) =>
          try validate(facet, url, path)
          catch { case e: IllegalStateException => List(s"$path: ${e.getMessage}") }
      }
    }
    base ++ facets
  }

  /** Every facet of the event, with where it is. */
  private def facetsIn(event: JsonNode): List[(String, JsonNode)] = {
    def under(node: JsonNode, path: String): List[(String, JsonNode)] =
      Option(node).filter(_.isObject).toList.flatMap(_.fields().asScala.map(e => s"$path.${e.getKey}" -> e.getValue))
    val run = under(event.path("run").get("facets"), "$.run.facets")
    val job = under(event.path("job").get("facets"), "$.job.facets")
    val datasets = List("inputs", "outputs").flatMap { side =>
      Option(event.get(side)).toList.flatMap(_.elements().asScala.zipWithIndex).flatMap { case (d, i) =>
        List("facets", "inputFacets", "outputFacets").flatMap(kind => under(d.get(kind), s"$$.$side[$i].$kind"))
      }
    }
    run ++ job ++ datasets
  }

  /** `node` against the schema at `ref` (an absolute URL with a JSON-pointer fragment). */
  private def validate(node: JsonNode, ref: String, path: String): List[String] = {
    val (url, pointer) = ref.split("#", 2) match {
      case Array(u, p) => (u, p)
      case Array(u)    => (u, "")
    }
    check(node, document(url).at(pointer), url, path)
  }

  private def check(node: JsonNode, schema: JsonNode, url: String, path: String): List[String] = {
    if (schema.isMissingNode) throw new IllegalStateException(s"a $$ref in $url points at nothing")
    if (schema.isBoolean) return if (schema.asBoolean()) Nil else List(s"$path: nothing is allowed here")
    val unknown = schema.fieldNames().asScala.toList.filterNot(k => annotations(k) || implemented(k))
    if (unknown.nonEmpty) throw new IllegalStateException(s"$url uses ${unknown.mkString(", ")}, which OpenLineageSchema does not implement")
    def sub(child: JsonNode, at: String): List[String] = check(node, child, url, at)
    val errors = List.newBuilder[String]
    Option(schema.get("$ref")).foreach { r =>
      val target = r.asText()
      errors ++= (if (target.startsWith("#")) check(node, document(url).at(target.substring(1)), url, path) else validate(node, target, path))
    }
    Option(schema.get("type")).foreach { t =>
      val types = if (t.isArray) t.elements().asScala.map(_.asText()).toList else List(t.asText())
      if (!types.exists(matchesType(node, _))) errors += s"$path: expected ${types.mkString(" or ")}, found ${kind(node)}"
    }
    Option(schema.get("enum")).foreach { e =>
      if (!e.elements().asScala.exists(_ == node)) errors += s"$path: ${node} is not one of ${e}"
    }
    Option(schema.get("minimum")).foreach { m =>
      if (node.isNumber && node.asDouble() < m.asDouble()) errors += s"$path: ${node} is below the minimum ${m}"
    }
    Option(schema.get("format")).foreach { f =>
      if (node.isTextual && !formatOk(f.asText(), node.asText())) errors += s"$path: '${node.asText()}' is not a ${f.asText()}"
    }
    if (node.isObject) {
      val properties = Option(schema.get("properties")).map(_.fields().asScala.map(e => e.getKey -> e.getValue).toMap).getOrElse(Map.empty)
      Option(schema.get("required")).foreach(_.elements().asScala.map(_.asText()).foreach { r =>
        if (!node.has(r)) errors += s"$path: '$r' is required"
      })
      node.fields().asScala.foreach { e =>
        properties.get(e.getKey) match {
          case Some(p) => errors ++= check(e.getValue, p, url, s"$path.${e.getKey}")
          case None =>
            Option(schema.get("additionalProperties")).foreach { ap =>
              if (ap.isBoolean) { if (!ap.asBoolean()) errors += s"$path: '${e.getKey}' is not allowed" }
              else errors ++= check(e.getValue, ap, url, s"$path.${e.getKey}")
            }
        }
      }
    }
    if (node.isArray) Option(schema.get("items")).foreach { items =>
      node.elements().asScala.zipWithIndex.foreach { case (item, i) => errors ++= check(item, items, url, s"$path[$i]") }
    }
    Option(schema.get("allOf")).foreach(_.elements().asScala.foreach(s => errors ++= sub(s, path)))
    Option(schema.get("anyOf")).foreach { options =>
      val results = options.elements().asScala.map(sub(_, path)).toList
      if (!results.exists(_.isEmpty)) errors += s"$path: matches none of anyOf: ${results.flatten.take(3).mkString("; ")}"
    }
    Option(schema.get("oneOf")).foreach { options =>
      val passing = options.elements().asScala.count(sub(_, path).isEmpty)
      if (passing != 1) errors += s"$path: matches $passing of oneOf, not exactly one"
    }
    Option(schema.get("not")).foreach { s => if (sub(s, path).isEmpty) errors += s"$path: matches what 'not' forbids" }
    errors.result()
  }

  private def matchesType(node: JsonNode, t: String): Boolean = t match {
    case "object"  => node.isObject
    case "array"   => node.isArray
    case "string"  => node.isTextual
    case "integer" => node.isIntegralNumber
    case "number"  => node.isNumber
    case "boolean" => node.isBoolean
    case "null"    => node.isNull
    case other     => throw new IllegalStateException(s"type '$other' is not implemented")
  }

  private def kind(node: JsonNode): String = node.getNodeType.toString.toLowerCase

  private def formatOk(format: String, value: String): Boolean = format match {
    case "uuid"      => scala.util.Try(java.util.UUID.fromString(value)).toOption.exists(_.toString == value.toLowerCase)
    case "date-time" => scala.util.Try(java.time.OffsetDateTime.parse(value)).isSuccess
    case "uri"       => scala.util.Try(new java.net.URI(value)).toOption.exists(_.isAbsolute)
    case other       => throw new IllegalStateException(s"format '$other' is not implemented")
  }
}
