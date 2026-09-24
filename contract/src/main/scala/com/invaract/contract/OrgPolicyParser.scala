// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.contract

import org.yaml.snakeyaml.constructor.SafeConstructor
import org.yaml.snakeyaml.{LoaderOptions, Yaml}

import java.io.{File, FileInputStream, InputStream}
import java.time.LocalDate
import java.time.format.DateTimeParseException
import scala.collection.JavaConverters._

/** Parses organizational policy documents (YAML) into `OrgPolicy` — see its
  * own doc for what the document represents. Deliberately strict about the
  * structure needed to interpret the document at all (a policy rule's
  * `id`/`type`, an exemption's `contractId`/`policyIds`/`reason`, a
  * well-formed `reviewBy` date) and permissive about everything else (a
  * policy `type` this version of Invaract doesn't interpret still parses —
  * see `PolicyRule.interpret`) — the same fail-fast/permissive split
  * `ContractParser` uses for contracts. Deliberately self-contained rather
  * than reusing `ContractParser`'s private YAML-coercion helpers: the two
  * parsers are small, independently evolving documents, and duplicating a
  * few dozen lines of coercion logic is a smaller risk than reaching into
  * an already-published, MiMa-covered file for it.
  */
object OrgPolicyParser {

  def parse(yamlText: String): OrgPolicy = parseRaw(newSafeYaml().load[Any](yamlText))

  def parseFile(path: String): OrgPolicy = parseFile(new File(path))

  def parseFile(file: File): OrgPolicy = {
    if (!file.exists()) {
      throw new OrgPolicyParseException(s"Organizational policy file not found: ${file.getPath}")
    }
    val stream = new FileInputStream(file)
    try {
      parseStream(stream)
    } catch {
      case e: OrgPolicyParseException => throw e
      case e: Exception =>
        throw new OrgPolicyParseException(s"Failed to parse organizational policy file: ${file.getPath}", e)
    } finally {
      stream.close()
    }
  }

  def parseStream(stream: InputStream): OrgPolicy = parseRaw(newSafeYaml().load[Any](stream))

  // Same SafeConstructor-based defense-in-depth as ContractParser's own
  // newSafeYaml — see that method's doc for the full reasoning.
  private def newSafeYaml(): Yaml = new Yaml(new SafeConstructor(new LoaderOptions()))

  private def parseRaw(loaded: Any): OrgPolicy = {
    val raw = loadMap(loaded, "orgPolicy")

    val version = requireString(raw, "version", "orgPolicy")

    val policies = raw.get("policies") match {
      case Some(value) => parseListOf(value, "orgPolicy.policies")(parsePolicyRule)
      case None        => Nil
    }

    val inject = raw.get("inject") match {
      case Some(value) => parseInject(loadMap(value, "orgPolicy.inject"))
      case None        => InjectedDefaults()
    }

    val exemptions = raw.get("exemptions") match {
      case Some(value) => parseListOf(value, "orgPolicy.exemptions")(parseExemption)
      case None        => Nil
    }

    val customPolicyTypes = raw.get("customPolicyTypes") match {
      case Some(value) =>
        loadMap(value, "orgPolicy.customPolicyTypes").map { case (ruleType, className) =>
          ruleType -> String.valueOf(className)
        }
      case None => Map.empty[String, String]
    }

    val typeGuarantees = parseTypeGuarantees(raw)

    // Duplicate policy ids, a customPolicyTypes entry colliding with a
    // built-in PolicyType, or one naming a class that doesn't resolve are
    // all structurally sound documents (same as a Contract with duplicate
    // dataset names) — OrgPolicyValidator flags these as Errors/Warnings,
    // not this parser, the same split ContractValidator uses for duplicate
    // dataset/field names.
    OrgPolicy(version, policies, inject, exemptions, customPolicyTypes, typeGuarantees)
  }

  /** Parses an optional `typeGuarantees:` block — absent entirely means
    * `TypeGuaranteeConfig()`'s own defaults (nothing enabled, the same
    * opt-in-by-default shape every other capability in this feature uses).
    * `enabled: []`/an absent `enabled` key are treated identically (both
    * "nothing enabled"), the same "empty means malformed" the rest of this
    * parser does NOT apply here — unlike a required list
    * (`requireStringList`), an org genuinely choosing to enable nothing at
    * all is a normal, common starting point, not a mistake.
    */
  private def parseTypeGuarantees(raw: Map[String, Any]): TypeGuaranteeConfig =
    raw.get("typeGuarantees") match {
      case None => TypeGuaranteeConfig()
      case Some(value) =>
        val m = loadMap(value, "orgPolicy.typeGuarantees")
        val enabled = optStringList(m, "enabled", "orgPolicy.typeGuarantees").distinct
        val mode = optString(m, "mode").map(parseMode(_, "orgPolicy.typeGuarantees")).getOrElse(PolicyMode.Enforce)
        val customTypeGuaranteeTypes = m.get("customTypeGuaranteeTypes") match {
          case Some(v) =>
            loadMap(v, "orgPolicy.typeGuarantees.customTypeGuaranteeTypes").map { case (checkType, className) =>
              checkType -> String.valueOf(className)
            }
          case None => Map.empty[String, String]
        }
        TypeGuaranteeConfig(enabled, mode, customTypeGuaranteeTypes)
    }

  /** Like `requireStringList`, but for an optional list: `Nil` when `key`
    * is absent entirely, rather than throwing.
    */
  private def optStringList(raw: Map[String, Any], key: String, context: String): List[String] =
    optValue(raw, key) match {
      case None => Nil
      case Some(value) =>
        loadList(value, s"$context.$key").map {
          case s: String => s
          case other     => throw new OrgPolicyParseException(s"Expected a list of strings for '$context.$key', got: $other")
        }
    }

  private def parsePolicyRule(raw: Map[String, Any], context: String): PolicyRule = {
    val id = requireString(raw, "id", context)
    val ruleType = requireString(raw, "type", context)
    val description = optString(raw, "description")
    val scope = optString(raw, "scope").map(parseScope(_, context)).getOrElse(PolicyScope.All)
    val when = optValue(raw, "when").map(v => parseCondition(loadMap(v, s"$context.when"), context))
    val mode = optString(raw, "mode").map(parseMode(_, context)).getOrElse(PolicyMode.Enforce)
    val properties = raw -- Set("id", "type", "description", "scope", "when", "mode")
    PolicyRule(id, ruleType, properties, description, scope, when, mode)
  }

  private def parseScope(raw: String, context: String): PolicyScope = raw.trim.toLowerCase match {
    case "inputs"  => PolicyScope.Inputs
    case "outputs" => PolicyScope.Outputs
    case "all"     => PolicyScope.All
    case other     => throw new OrgPolicyParseException(s"Invalid 'scope' in $context: '$other' (expected inputs/outputs/all)")
  }

  private def parseMode(raw: String, context: String): PolicyMode = raw.trim.toLowerCase match {
    case "enforce" => PolicyMode.Enforce
    case "warn"    => PolicyMode.Warn
    case other     => throw new OrgPolicyParseException(s"Invalid 'mode' in $context: '$other' (expected enforce/warn)")
  }

  private def parseCondition(raw: Map[String, Any], context: String): PolicyCondition =
    PolicyCondition(requireString(raw, "sensitivityTag", s"$context.when"))

  private def parseInject(raw: Map[String, Any]): InjectedDefaults = {
    val rules = raw.get("rules") match {
      case Some(value) =>
        parseListOf(value, "orgPolicy.inject.rules") { (m, ctx) =>
          val ruleType = requireString(m, "type", ctx)
          ContractRule(ruleType, m - "type")
        }
      case None => Nil
    }
    val minVerificationOptions = raw.get("minVerificationOptions") match {
      case Some(value) =>
        loadMap(value, "orgPolicy.inject.minVerificationOptions").map { case (key, v) =>
          key -> parseBoolean(v, s"orgPolicy.inject.minVerificationOptions.$key")
        }
      case None => Map.empty[String, Boolean]
    }
    InjectedDefaults(rules, minVerificationOptions)
  }

  private def parseExemption(raw: Map[String, Any], context: String): PolicyExemption = {
    val contractId = requireString(raw, "contractId", context)
    val policyIds = requireStringList(raw, "policyIds", context)
    val reason = requireString(raw, "reason", context)
    val reviewBy = optString(raw, "reviewBy").map(parseDate(_, context))
    PolicyExemption(contractId, policyIds, reason, reviewBy)
  }

  private def parseDate(raw: String, context: String): LocalDate =
    try {
      LocalDate.parse(raw)
    } catch {
      case e: DateTimeParseException =>
        throw new OrgPolicyParseException(s"Invalid 'reviewBy' date in $context: '$raw' (expected YYYY-MM-DD)", e)
    }

  private def parseBoolean(value: Any, context: String): Boolean = value match {
    case b: java.lang.Boolean => b.booleanValue()
    case s: String            => s.toBoolean
    case other                => throw new OrgPolicyParseException(s"Expected boolean for '$context', got: $other")
  }

  // -- YAML -> Scala coercion helpers (mirrors ContractParser's own) ------

  private def parseListOf[T](raw: Any, context: String)(parseItem: (Map[String, Any], String) => T): List[T] =
    loadList(raw, context).zipWithIndex.map { case (item, idx) =>
      val itemContext = s"$context[$idx]"
      parseItem(loadMap(item, itemContext), itemContext)
    }

  private def loadMap(value: Any, context: String): Map[String, Any] = value match {
    case null => throw new OrgPolicyParseException(s"Expected a mapping for '$context', but it was empty")
    case m: java.util.Map[_, _] =>
      m.asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    case other =>
      throw new OrgPolicyParseException(s"Expected a mapping for '$context', got: $other")
  }

  private def loadList(value: Any, context: String): List[Any] = value match {
    case l: java.util.List[_] => l.asScala.toList
    case other =>
      throw new OrgPolicyParseException(s"Expected a list for '$context', got: $other")
  }

  private def optValue(raw: Map[String, Any], key: String): Option[Any] = raw.get(key).filter(_ != null)

  private def requireString(raw: Map[String, Any], key: String, context: String): String =
    optValue(raw, key)
      .map(String.valueOf)
      .getOrElse(throw new OrgPolicyParseException(s"Missing required field '$key' in $context"))

  private def optString(raw: Map[String, Any], key: String): Option[String] = optValue(raw, key).map(String.valueOf)

  private def requireStringList(raw: Map[String, Any], key: String, context: String): List[String] =
    optValue(raw, key) match {
      case None => throw new OrgPolicyParseException(s"Missing or empty required field '$key' in $context")
      case Some(value) =>
        val items = loadList(value, s"$context.$key").map {
          case s: String => s
          case other     => throw new OrgPolicyParseException(s"Expected a list of strings for '$context.$key', got: $other")
        }
        if (items.isEmpty) throw new OrgPolicyParseException(s"Missing or empty required field '$key' in $context")
        items
    }
}
