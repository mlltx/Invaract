// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.contract

import org.yaml.snakeyaml.{DumperOptions, Yaml}

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

  /** Serializes `contract` back to a YAML document `parse`/`parseFile` can
    * read — the inverse of this object's parsing direction. Round-trips
    * through the same document shape `parseContract` expects: a contract
    * parsed and then written back (with no changes in between) always
    * parses again to an equal `Contract`.
    *
    * Exists for callers that build a `Contract` programmatically and need
    * to hand a human something they can save and edit — e.g. `spark-adapter`'s
    * dry-run mode (`ContractEnforcementRule.dryRun`), which infers a
    * starting-point contract from a real transformation's actual
    * inputs/outputs and needs to print it as a document a user can copy
    * into a real contract file, not just describe it in prose.
    */
  def write(contract: Contract): String = {
    val options = new DumperOptions()
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
    new Yaml(options).dump(contractToJava(contract))
  }

  // -- Contract -> YAML-dumpable Java structures --------------------------
  //
  // SnakeYAML's `Yaml.dump` already knows how to render plain
  // java.util.Map/List/String/Number/Boolean values - the exact shape
  // `loadMap`/`loadList` decode YAML *into* above. So `ContractRule.properties`
  // and `Contract.extensions` (both `Map[String, Any]` populated straight
  // from a prior `Yaml().load`, never re-typed into Scala collections
  // beyond their own top-level map - see `loadMap`'s doc) can be hollowed
  // straight back out to `java.util.Map` and handed to the dumper as-is;
  // only the parts of `Contract` that are genuine Scala case classes
  // (Dataset/Schema/Field/ContractRule's `ruleType` itself, the contract's
  // own id/version/status) need building up here.

  private def contractToJava(contract: Contract): java.util.Map[String, Any] = {
    val doc = new java.util.LinkedHashMap[String, Any]()
    doc.put("id", contract.id)
    doc.put("version", contract.version.toString)
    doc.put("status", contract.status)
    if (contract.inputs.nonEmpty) doc.put("inputs", contract.inputs.map(datasetToJava).asJava)
    if (contract.outputs.nonEmpty) doc.put("outputs", contract.outputs.map(datasetToJava).asJava)
    if (contract.rules.nonEmpty) doc.put("rules", contract.rules.map(ruleToJava).asJava)
    if (contract.extensions.nonEmpty) doc.put("extensions", mapToJava(contract.extensions))
    doc
  }

  private def datasetToJava(dataset: Dataset): java.util.Map[String, Any] = {
    val m = new java.util.LinkedHashMap[String, Any]()
    m.put("name", dataset.name)
    m.put("location", dataset.location)
    dataset.format.foreach(m.put("format", _))
    m.put("schema", schemaToJava(dataset.schema))
    dataset.saveMode.foreach(m.put("saveMode", _))
    m
  }

  private def schemaToJava(schema: Schema): java.util.Map[String, Any] = {
    val m = new java.util.LinkedHashMap[String, Any]()
    m.put("fields", schema.fields.map(fieldToJava).asJava)
    m
  }

  private def fieldToJava(field: Field): java.util.Map[String, Any] = {
    val m = new java.util.LinkedHashMap[String, Any]()
    m.put("name", field.name)
    m.put("type", field.fieldType)
    m.put("required", Boolean.box(field.required))
    m.put("nullable", Boolean.box(field.nullable))
    if (field.properties.nonEmpty) m.put("properties", field.properties.map(fieldToJava).asJava)
    m
  }

  private def ruleToJava(rule: ContractRule): java.util.Map[String, Any] = {
    val m = new java.util.LinkedHashMap[String, Any]()
    m.put("type", rule.ruleType)
    rule.properties.foreach { case (k, v) => m.put(k, v) }
    m
  }

  private def mapToJava(m: Map[String, Any]): java.util.Map[String, Any] = {
    val out = new java.util.LinkedHashMap[String, Any]()
    m.foreach { case (k, v) => out.put(k, v) }
    out
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
