// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.contract

import org.yaml.snakeyaml.Yaml

import java.io.{File, FileInputStream, InputStream}
import scala.collection.JavaConverters._

/** Parses contract documents (YAML) into the Invariant contract object model.
  *
  * The parser is deliberately strict about structure it must interpret
  * (id, version, dataset locations, schema fields) and deliberately
  * permissive about everything else: unrecognized top-level keys are kept
  * under `Contract.extensions` rather than rejected, so the parser does not
  * need to track every concept the underlying standard (ODCS) defines in
  * order to accept a valid document.
  */
object ContractParser {

  def parse(yamlText: String): Contract = {
    val raw = loadMap(new Yaml().load[Any](yamlText), "contract")
    parseContract(raw)
  }

  def parseFile(path: String): Contract = parseFile(new File(path))

  def parseFile(file: File): Contract = {
    if (!file.exists()) {
      throw new ContractParseException(s"Contract file not found: ${file.getPath}")
    }
    val stream = new FileInputStream(file)
    try {
      parseStream(stream)
    } catch {
      case e: ContractParseException => throw e
      case e: Exception =>
        throw new ContractParseException(s"Failed to parse contract file: ${file.getPath}", e)
    } finally {
      stream.close()
    }
  }

  def parseStream(stream: InputStream): Contract = {
    val raw = loadMap(new Yaml().load[Any](stream), "contract")
    parseContract(raw)
  }

  private def parseContract(raw: Map[String, Any]): Contract = {
    val knownKeys = Set("id", "version", "status", "inputs", "outputs", "rules", "extensions")

    val id = requireString(raw, "id", "contract")
    val version = ContractVersion.parse(requireString(raw, "version", "contract"))
    val status = optString(raw, "status").getOrElse("active")

    val inputs = parseDatasets(raw.get("inputs"), "inputs")
    val outputs = parseDatasets(raw.get("outputs"), "outputs")
    val rules = parseRules(raw.get("rules"))

    val declaredExtensions = raw.get("extensions") match {
      case Some(m) => loadMap(m, "contract.extensions")
      case None    => Map.empty[String, Any]
    }
    // Preserve any top-level keys Invariant doesn't interpret, so contracts
    // authored against a broader standard aren't rejected outright.
    val undeclaredExtensions = raw -- knownKeys
    val extensions = undeclaredExtensions ++ declaredExtensions

    Contract(id, version, status, inputs, outputs, rules, extensions)
  }

  private def parseDatasets(raw: Option[Any], field: String): List[Dataset] = raw match {
    case None => Nil
    case Some(value) =>
      loadList(value, field).zipWithIndex.map { case (item, idx) =>
        parseDataset(loadMap(item, s"$field[$idx]"), s"$field[$idx]")
      }
  }

  private def parseDataset(raw: Map[String, Any], context: String): Dataset = {
    val name = requireString(raw, "name", context)
    val location = requireString(raw, "location", context)
    val format = optString(raw, "format")
    val schemaRaw = raw.getOrElse(
      "schema",
      throw new ContractParseException(s"Missing 'schema' in $context")
    )
    val schema = parseSchema(loadMap(schemaRaw, s"$context.schema"), s"$context.schema")
    Dataset(name, location, format, schema)
  }

  private def parseSchema(raw: Map[String, Any], context: String): Schema = {
    val fieldsRaw = raw.getOrElse(
      "fields",
      throw new ContractParseException(s"Missing 'fields' in $context")
    )
    val fields = loadList(fieldsRaw, s"$context.fields").zipWithIndex.map { case (item, idx) =>
      parseField(loadMap(item, s"$context.fields[$idx]"), s"$context.fields[$idx]")
    }
    Schema(fields)
  }

  private def parseField(raw: Map[String, Any], context: String): Field = {
    val name = requireString(raw, "name", context)
    val fieldType = requireString(raw, "type", context)
    val required = optBoolean(raw, "required", context).getOrElse(false)
    val nullable = optBoolean(raw, "nullable", context).getOrElse(!required)

    val properties = raw.get("properties") match {
      case Some(value) =>
        loadList(value, s"$context.properties").zipWithIndex.map { case (item, idx) =>
          parseField(loadMap(item, s"$context.properties[$idx]"), s"$context.properties[$idx]")
        }
      case None => Nil
    }

    Field(name, fieldType, required, nullable, properties)
  }

  private def parseRules(raw: Option[Any]): List[ContractRule] = raw match {
    case None => Nil
    case Some(value) =>
      loadList(value, "rules").zipWithIndex.map { case (item, idx) =>
        val map = loadMap(item, s"rules[$idx]")
        val ruleType = requireString(map, "type", s"rules[$idx]")
        ContractRule(ruleType, map - "type")
      }
  }

  // -- YAML -> Scala coercion helpers -------------------------------------

  private def loadMap(value: Any, context: String): Map[String, Any] = value match {
    case null => throw new ContractParseException(s"Expected a mapping for '$context', but it was empty")
    case m: java.util.Map[_, _] =>
      m.asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    case other =>
      throw new ContractParseException(s"Expected a mapping for '$context', got: $other")
  }

  private def loadList(value: Any, context: String): List[Any] = value match {
    case l: java.util.List[_] => l.asScala.toList
    case other =>
      throw new ContractParseException(s"Expected a list for '$context', got: $other")
  }

  private def requireString(raw: Map[String, Any], key: String, context: String): String =
    raw.get(key) match {
      case Some(null) | None =>
        throw new ContractParseException(s"Missing required field '$key' in $context")
      case Some(value) => String.valueOf(value)
    }

  private def optString(raw: Map[String, Any], key: String): Option[String] =
    raw.get(key).filter(_ != null).map(String.valueOf)

  private def optBoolean(raw: Map[String, Any], key: String, context: String): Option[Boolean] =
    raw.get(key).filter(_ != null).map {
      case b: java.lang.Boolean => b.booleanValue()
      case s: String            => s.toBoolean
      case other =>
        throw new ContractParseException(s"Expected boolean for '$context.$key', got: $other")
    }
}
