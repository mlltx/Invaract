// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.contract

import org.yaml.snakeyaml.Yaml

import java.io.{File, FileInputStream, InputStream}
import scala.collection.JavaConverters._

/** Parses contract documents (YAML) into the Invaract contract object model.
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
    // Preserve any top-level keys Invaract doesn't interpret, so contracts
    // authored against a broader standard aren't rejected outright.
    val undeclaredExtensions = raw -- knownKeys
    val extensions = undeclaredExtensions ++ declaredExtensions

    Contract(id, version, status, inputs, outputs, rules, extensions)
  }

  private def parseDatasets(raw: Option[Any], field: String): List[Dataset] = raw match {
    case None        => Nil
    case Some(value) => parseListOf(value, field)(parseDataset)
  }

  private def parseDataset(raw: Map[String, Any], context: String): Dataset = {
    val name = requireString(raw, "name", context)
    val location = requireString(raw, "location", context)
    val format = optString(raw, "format")
    val saveMode = optString(raw, "saveMode")
    val schemaRaw = raw.getOrElse(
      "schema",
      throw new ContractParseException(s"Missing 'schema' in $context")
    )
    val schema = parseSchema(loadMap(schemaRaw, s"$context.schema"), s"$context.schema")
    Dataset(name, location, format, schema, saveMode)
  }

  private def parseSchema(raw: Map[String, Any], context: String): Schema = {
    val fieldsRaw = raw.getOrElse(
      "fields",
      throw new ContractParseException(s"Missing 'fields' in $context")
    )
    Schema(parseListOf(fieldsRaw, s"$context.fields")(parseField))
  }

  private def parseField(raw: Map[String, Any], context: String): Field = {
    val name = requireString(raw, "name", context)
    val fieldType = requireString(raw, "type", context)
    val required = optBoolean(raw, "required", context).getOrElse(false)
    val nullable = optBoolean(raw, "nullable", context).getOrElse(!required)

    val properties = raw.get("properties") match {
      case Some(value) => parseListOf(value, s"$context.properties")(parseField)
      case None        => Nil
    }

    Field(name, fieldType, required, nullable, properties)
  }

  private def parseRules(raw: Option[Any]): List[ContractRule] = raw match {
    case None => Nil
    case Some(value) =>
      parseListOf(value, "rules") { (map, context) =>
        val ruleType = requireString(map, "type", context)
        ContractRule(ruleType, map - "type")
      }
  }

  // -- YAML -> Scala coercion helpers -------------------------------------

  /** Parses a YAML sequence of mappings, indexing each item's context as
    * `$context[idx]` for error messages. Shared by every list-of-object
    * field in the document (datasets, schema fields, nested properties,
    * rules).
    */
  private def parseListOf[T](raw: Any, context: String)(parseItem: (Map[String, Any], String) => T): List[T] =
    loadList(raw, context).zipWithIndex.map { case (item, idx) =>
      val itemContext = s"$context[$idx]"
      parseItem(loadMap(item, itemContext), itemContext)
    }

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

  /** Looks up `key`, treating an explicit YAML `null` the same as an absent
    * key. The one coercion primitive `requireString`/`optString`/`optBoolean`
    * all build on, so "is this key present" is decided in exactly one place.
    */
  private def optValue(raw: Map[String, Any], key: String): Option[Any] =
    raw.get(key).filter(_ != null)

  private def requireString(raw: Map[String, Any], key: String, context: String): String =
    optValue(raw, key)
      .map(String.valueOf)
      .getOrElse(throw new ContractParseException(s"Missing required field '$key' in $context"))

  private def optString(raw: Map[String, Any], key: String): Option[String] =
    optValue(raw, key).map(String.valueOf)

  private def optBoolean(raw: Map[String, Any], key: String, context: String): Option[Boolean] =
    optValue(raw, key).map {
      case b: java.lang.Boolean => b.booleanValue()
      case s: String            => s.toBoolean
      case other =>
        throw new ContractParseException(s"Expected boolean for '$context.$key', got: $other")
    }
}
