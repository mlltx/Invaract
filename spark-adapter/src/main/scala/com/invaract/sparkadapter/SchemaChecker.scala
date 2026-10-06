// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.{Field => ContractField}

import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}

import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

import scala.util.Try

/** Compares a contract's declared schema with the actual Spark schema of one
  * dataset — the "Schema" check `StructuralVerifier` applies to both sides
  * (inputs and outputs are one rule set applied twice, differing only in which
  * violation types and wording each finding gets, hence `Side`).
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
  *   - A type written in Spark's DDL syntax — `array<int>`,
  *     `map<string,long>`, `struct<a:int,b:string>`, arbitrarily nested — which
  *     is compared structurally against the actual type. Nullability *inside* a
  *     container (`containsNull`, `valueContainsNull`, a nested struct field's
  *     nullable flag) is not compared; the field's own top-level `nullable` is.
  *
  * Before, only `DataType.typeName` was compared, which is just `array`,
  * `struct` or `map` for any container, so `array<int>` → `array<string>` or a
  * changed struct field passed silently. A bare `array`/`map`/`struct` with no
  * `properties` still means "any array/map/struct" — the existing, shallow
  * behavior, kept so no existing contract starts failing.
  */
private[sparkadapter] object SchemaChecker {

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
      actualSchema: StructType,
      side: Side,
      location: String,
      rejectUndeclaredFields: Boolean,
      caseSensitive: Boolean
  ): List[Violation] =
    checkFields(contractFields, actualSchema.fields, "", side, location, rejectUndeclaredFields, caseSensitive)

  private def key(name: String, caseSensitive: Boolean): String =
    if (caseSensitive) name else name.toLowerCase(Locale.ROOT)

  private def checkFields(
      contractFields: List[ContractField],
      actualFields: Array[StructField],
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
            case nested: StructType if field.isStruct =>
              checkFields(field.properties, nested.fields, path + ".", side, location, rejectUndeclaredFields, caseSensitive)
            case _ => Nil
          }

          typeViolation ++ nullabilityViolation ++ nestedViolations
      }
    }

    val undeclaredViolations =
      if (rejectUndeclaredFields)
        actualFields.toList
          .filterNot(f => declaredNames.contains(key(f.name, caseSensitive)))
          .map(f => Violations.undeclaredColumn(side, location, pathPrefix + f.name, f.dataType.catalogString))
      else Nil

    fieldViolations ++ undeclaredViolations
  }

  /** `None` when the actual type satisfies the field's declared type; otherwise
    * how to describe the actual type in the violation — the bare `typeName` for
    * an ordinary scalar (unchanged wording), Spark's `catalogString` whenever
    * either side is nested, since `array` alone says nothing about what differs.
    */
  private def typeMismatch(field: ContractField, actual: DataType, caseSensitive: Boolean): Option[String] = {
    val written = field.fieldType.trim
    val describeActual = if (isNested(actual)) actual.catalogString else actual.typeName
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

  private def isNested(dt: DataType): Boolean = dt match {
    case _: ArrayType | _: MapType | _: StructType => true
    case _                                         => false
  }

  // A contract's declared types are few and fixed, but `check` runs on every
  // write the session performs: parse each distinct string once.
  private val parsedTypes = new ConcurrentHashMap[String, Option[DataType]]()

  private def parseDeclared(declared: String): Option[DataType] =
    parsedTypes.computeIfAbsent(declared, d => Try(DataType.fromDDL(d)).toOption)

  /** Structural equality of two types ignoring every nullability flag, with
    * struct field names compared under the session's case rule.
    */
  private[sparkadapter] def sameShape(declared: DataType, actual: DataType, caseSensitive: Boolean): Boolean =
    (declared, actual) match {
      case (ArrayType(de, _), ArrayType(ae, _)) => sameShape(de, ae, caseSensitive)
      case (MapType(dk, dv, _), MapType(ak, av, _)) =>
        sameShape(dk, ak, caseSensitive) && sameShape(dv, av, caseSensitive)
      case (StructType(df), StructType(af)) =>
        df.length == af.length && df.zip(af).forall { case (d, a) =>
          key(d.name, caseSensitive) == key(a.name, caseSensitive) && sameShape(d.dataType, a.dataType, caseSensitive)
        }
      case (d, a) => d == a
    }
}
