// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import com.invaract.contract.{LogicalField, LogicalSchema, LogicalType, Field => ContractField}

import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

/** Compares a contract's declared schema with the actual schema of one
  * dataset — the "Schema" check `StructuralVerifier` applies to both sides
  * (inputs and outputs are one rule set applied twice, differing only in which
  * violation types and wording each finding gets, hence `Side`).
  *
  * ## Engine-neutral
  *
  * The actual schema arrives as a `com.invaract.contract.LogicalSchema` — the
  * adapter (for Spark, `SparkSchemas`) has already mapped its own types into
  * the shared `LogicalType` vocabulary, and a declared nested type is parsed by
  * the contract's own `LogicalType.parse`. Nothing in this file knows which
  * engine produced the schema.
  *
  * ## Names follow `spark.sql.caseSensitive`
  *
  * Spark resolves column names case-insensitively unless
  * `spark.sql.caseSensitive` is set, so a plan whose column is `id` satisfies a
  * contract field `ID` in a default session. Matching by exact case reported
  * that as a missing field. `check` takes the session's setting
  * (`ContractEnforcementRule` reads it per check, so a runtime change is seen)
  * and applies it to every name comparison here — top-level fields, nested
  * struct fields, and the "undeclared column" check alike. Case-sensitive
  * sessions keep exact-case matching.
  *
  * ## Nested types
  *
  * A contract can describe nesting two ways, and both are now checked:
  *
  *   - `properties:` on a field (the contract model's own struct form — see
  *     `Field.properties`): the actual column must be a struct, and each declared
  *     nested field is checked recursively with the same rules (presence when
  *     `required`, type, nullability, and — under `rejectUndeclaredFields` —
  *     undeclared nested columns). A nested finding's `column` is the dotted path
  *     (`address.zip`).
  *   - A type written in the contract's type grammar (`LogicalType.parse`, the
  *     syntax Spark's DDL has always used for these forms) — `array<int>`,
  *     `map<string,long>`, `struct<a:int,b:string>`, arbitrarily nested — which
  *     is compared structurally against the actual type. Nullability *inside* a
  *     container (`containsNull`, `valueContainsNull`, a nested struct field's
  *     nullable flag) is not compared; the field's own top-level `nullable` is.
  *
  * Before, only the type's keyword was compared, which is just `array`,
  * `struct` or `map` for any container, so `array<int>` → `array<string>` or a
  * changed struct field passed silently. A bare `array`/`map`/`struct` with no
  * `properties` still means "any array/map/struct" — the existing, shallow
  * behavior, kept so no existing contract starts failing.
  */
private[invaract] object SchemaChecker {

  /** Which side of the contract a schema belongs to: only the violation types
    * and the wording differ between inputs and outputs.
    */
  sealed abstract class Side(
      val label: String,
      val noun: String,
      val missingField: String,
      val undeclaredColumn: String,
      val typeMismatch: String,
      val nullabilityMismatch: String
  )
  object Side {
    case object Input
        extends Side(
          "INPUT",
          "input",
          ViolationType.MissingInputField,
          ViolationType.UndeclaredInputColumn,
          ViolationType.InputFieldTypeMismatch,
          ViolationType.InputFieldNullabilityMismatch
        )
    case object Output
        extends Side(
          "OUTPUT",
          "output",
          ViolationType.MissingOutputField,
          ViolationType.UndeclaredOutputColumn,
          ViolationType.OutputFieldTypeMismatch,
          ViolationType.OutputFieldNullabilityMismatch
        )
  }

  def check(
      contractFields: List[ContractField],
      actualSchema: LogicalSchema,
      side: Side,
      location: String,
      rejectUndeclaredFields: Boolean,
      caseSensitive: Boolean
  ): List[Violation] =
    checkFields(contractFields, actualSchema.fields, "", side, location, rejectUndeclaredFields, caseSensitive)

  private def key(name: String, caseSensitive: Boolean): String = LogicalType.nameKey(name, caseSensitive)

  private def checkFields(
      contractFields: List[ContractField],
      actualFields: List[LogicalField],
      pathPrefix: String,
      side: Side,
      location: String,
      rejectUndeclaredFields: Boolean,
      caseSensitive: Boolean
  ): List[Violation] = {
    val actualByName = actualFields.map(f => key(f.name, caseSensitive) -> f).toMap
    val declaredNames = contractFields.map(f => key(f.name, caseSensitive)).toSet

    val fieldViolations = contractFields.flatMap { field =>
      val path = pathPrefix + field.name
      actualByName.get(key(field.name, caseSensitive)) match {
        case None =>
          if (field.required) List(Violations.missingField(side, location, path, field.fieldType))
          else Nil

        case Some(actualField) =>
          val typeViolation = typeMismatch(field, actualField.dataType, caseSensitive) match {
            case Some(actualDescription) => List(Violations.fieldTypeMismatch(side, location, path, field.fieldType, actualDescription))
            case None => Nil
          }

          // Compatible, not identical: a contract requiring non-null
          // (nullable = false) is violated by an actual column that
          // permits nulls; the reverse (contract allows null, actual
          // guarantees non-null) is a stricter-than-required guarantee,
          // not a violation.
          val nullabilityViolation =
            if (!field.nullable && actualField.nullable) List(Violations.fieldNullabilityMismatch(side, location, path))
            else Nil

          // Recurse only into a declared struct whose actual type really is one;
          // anything else was already reported as a type mismatch above.
          val nestedViolations = actualField.dataType match {
            case nested: LogicalType.StructType if field.isStruct =>
              checkFields(field.properties, nested.fields, path + ".", side, location, rejectUndeclaredFields, caseSensitive)
            case _ => Nil
          }

          typeViolation ++ nullabilityViolation ++ nestedViolations
      }
    }

    val undeclaredViolations =
      if (rejectUndeclaredFields)
        actualFields
          .filterNot(f => declaredNames.contains(key(f.name, caseSensitive)))
          .map(f => Violations.undeclaredColumn(side, location, pathPrefix + f.name, f.dataType.catalogString))
      else Nil

    fieldViolations ++ undeclaredViolations
  }

  /** `None` when the actual type satisfies the field's declared type; otherwise
    * how to describe the actual type in the violation — the bare `typeName` for
    * an ordinary scalar (unchanged wording), the `catalogString` whenever
    * either side is nested, since `array` alone says nothing about what differs.
    */
  private def typeMismatch(field: ContractField, actual: LogicalType, caseSensitive: Boolean): Option[String] = {
    val written = field.fieldType.trim
    val describeActual = if (actual.isNested) actual.catalogString else actual.typeName
    if (written.toLowerCase(Locale.ROOT) == actual.typeName) None
    else if (written.contains("<")) {
      // Parsed as written, not lower-cased: a nested struct member's name is part
      // of the type and its case is judged by the session's rule below.
      parseDeclared(written) match {
        case Some(parsed) if sameShape(parsed, actual, caseSensitive) => None
        case _                                                        => Some(describeActual)
      }
    } else Some(describeActual)
  }

  // A contract's declared types are few and fixed, but `check` runs on every
  // write the session performs: parse each distinct string once.
  private val parsedTypes = new ConcurrentHashMap[String, Option[LogicalType]]()

  private def parseDeclared(declared: String): Option[LogicalType] =
    parsedTypes.computeIfAbsent(declared, d => LogicalType.parse(d))

  /** Structural equality of two types ignoring every nullability flag, with
    * struct field names compared under the session's case rule.
    */
  private[invaract] def sameShape(declared: LogicalType, actual: LogicalType, caseSensitive: Boolean): Boolean =
    (declared, actual) match {
      case (LogicalType.ArrayType(de), LogicalType.ArrayType(ae)) => sameShape(de, ae, caseSensitive)
      case (LogicalType.MapType(dk, dv), LogicalType.MapType(ak, av)) =>
        sameShape(dk, ak, caseSensitive) && sameShape(dv, av, caseSensitive)
      case (LogicalType.StructType(df), LogicalType.StructType(af)) =>
        df.length == af.length && df.zip(af).forall { case (d, a) =>
          key(d.name, caseSensitive) == key(a.name, caseSensitive) && sameShape(d.dataType, a.dataType, caseSensitive)
        }
      case (d, a) => d == a
    }
}
