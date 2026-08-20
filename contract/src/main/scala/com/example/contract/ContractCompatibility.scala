// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invariant Contributors

package com.example.contract

sealed trait CompatibilityLevel extends Ordered[CompatibilityLevel] {
  protected def rank: Int
  def compare(that: CompatibilityLevel): Int = rank compare that.rank
}
object CompatibilityLevel {
  case object Patch extends CompatibilityLevel { protected val rank = 0 }
  case object Minor extends CompatibilityLevel { protected val rank = 1 }
  case object Breaking extends CompatibilityLevel { protected val rank = 2 }
}

/** A single detected difference between two versions of a contract. */
case class CompatibilityChange(level: CompatibilityLevel, path: String, description: String)

/** The full set of differences between two contract versions, and the
  * minimum version bump those differences require.
  */
case class CompatibilityReport(changes: List[CompatibilityChange]) {
  def requiredLevel: CompatibilityLevel =
    if (changes.isEmpty) CompatibilityLevel.Patch
    else changes.map(_.level).max

  def isBreaking: Boolean = changes.exists(_.level == CompatibilityLevel.Breaking)
  def breakingChanges: List[CompatibilityChange] = changes.filter(_.level == CompatibilityLevel.Breaking)
}

/** Compares two versions of the same contract and determines whether the
  * declared version bump matches the actual scope of change.
  *
  * Rules (mirroring docs/VERSIONING.md):
  *   - Removing a dataset, removing a field, narrowing a type, tightening
  *     nullability, or adding a new *required* field is a BREAKING change.
  *   - Adding a dataset or an optional field is a MINOR (additive) change.
  *   - Everything else (description/metadata-only changes) is a PATCH change.
  */
object ContractCompatibility {

  def diff(previous: Contract, next: Contract): CompatibilityReport = {
    val changes = List.newBuilder[CompatibilityChange]

    if (previous.id != next.id) {
      changes += CompatibilityChange(
        CompatibilityLevel.Breaking,
        "id",
        s"Contract id changed from '${previous.id}' to '${next.id}'"
      )
    }

    changes ++= diffDatasets("outputs", previous.outputs, next.outputs)
    changes ++= diffDatasets("inputs", previous.inputs, next.inputs)

    CompatibilityReport(changes.result())
  }

  /** Returns human-readable problems if `next.version` does not reflect the
    * scope of change relative to `previous`. An empty list means the bump
    * is consistent with (or more conservative than) the actual changes.
    */
  def verifyVersionBump(previous: Contract, next: Contract): List[String] = {
    val required = diff(previous, next).requiredLevel
    val actualBump = classifyBump(previous.version, next.version)

    if (actualBump < required) {
      val kind = required match {
        case CompatibilityLevel.Breaking => "a MAJOR"
        case CompatibilityLevel.Minor    => "at least a MINOR"
        case CompatibilityLevel.Patch    => "at least a PATCH"
      }
      List(
        s"Changes between ${previous.version} and ${next.version} require $kind version bump, " +
          s"but the declared version only reflects a ${describe(actualBump)} change"
      )
    } else {
      Nil
    }
  }

  private def describe(level: CompatibilityLevel): String = level match {
    case CompatibilityLevel.Breaking => "MAJOR"
    case CompatibilityLevel.Minor    => "MINOR"
    case CompatibilityLevel.Patch    => "PATCH"
  }

  private def classifyBump(oldV: ContractVersion, newV: ContractVersion): CompatibilityLevel = {
    if (newV.major != oldV.major) CompatibilityLevel.Breaking
    else if (newV.minor != oldV.minor) CompatibilityLevel.Minor
    else CompatibilityLevel.Patch
  }

  private def diffDatasets(kind: String, previous: List[Dataset], next: List[Dataset]): List[CompatibilityChange] = {
    val changes = List.newBuilder[CompatibilityChange]
    val prevByName = previous.map(d => d.name -> d).toMap
    val nextByName = next.map(d => d.name -> d).toMap

    (prevByName.keySet -- nextByName.keySet).foreach { name =>
      changes += CompatibilityChange(CompatibilityLevel.Breaking, s"$kind.$name", s"Dataset '$name' was removed")
    }

    (nextByName.keySet -- prevByName.keySet).foreach { name =>
      changes += CompatibilityChange(CompatibilityLevel.Minor, s"$kind.$name", s"Dataset '$name' was added")
    }

    (prevByName.keySet intersect nextByName.keySet).toList.sorted.foreach { name =>
      val prevDs = prevByName(name)
      val nextDs = nextByName(name)

      if (prevDs.location != nextDs.location) {
        changes += CompatibilityChange(
          CompatibilityLevel.Breaking,
          s"$kind.$name.location",
          s"Location changed from '${prevDs.location}' to '${nextDs.location}'"
        )
      }

      changes ++= diffSchema(s"$kind.$name.schema", prevDs.schema, nextDs.schema)
    }

    changes.result()
  }

  private def diffSchema(path: String, previous: Schema, next: Schema): List[CompatibilityChange] = {
    val changes = List.newBuilder[CompatibilityChange]
    val prevByName = previous.fields.map(f => f.name -> f).toMap
    val nextByName = next.fields.map(f => f.name -> f).toMap

    (prevByName.keySet -- nextByName.keySet).foreach { name =>
      changes += CompatibilityChange(CompatibilityLevel.Breaking, s"$path.$name", s"Field '$name' was removed")
    }

    (nextByName.keySet -- prevByName.keySet).foreach { name =>
      val field = nextByName(name)
      if (field.required) {
        changes += CompatibilityChange(
          CompatibilityLevel.Breaking,
          s"$path.$name",
          s"Required field '$name' was added without a default; existing producers will fail validation"
        )
      } else {
        changes += CompatibilityChange(CompatibilityLevel.Minor, s"$path.$name", s"Optional field '$name' was added")
      }
    }

    (prevByName.keySet intersect nextByName.keySet).toList.sorted.foreach { name =>
      val prevField = prevByName(name)
      val nextField = nextByName(name)

      if (prevField.fieldType != nextField.fieldType) {
        changes += CompatibilityChange(
          CompatibilityLevel.Breaking,
          s"$path.$name.type",
          s"Type changed from '${prevField.fieldType}' to '${nextField.fieldType}'"
        )
      }

      if (!prevField.required && nextField.required) {
        changes += CompatibilityChange(
          CompatibilityLevel.Breaking,
          s"$path.$name.required",
          s"Field '$name' changed from optional to required"
        )
      }

      if (prevField.nullable && !nextField.nullable) {
        changes += CompatibilityChange(
          CompatibilityLevel.Breaking,
          s"$path.$name.nullable",
          s"Field '$name' changed from nullable to non-nullable"
        )
      }

      changes ++= diffSchema(s"$path.$name", Schema(prevField.properties), Schema(nextField.properties))
    }

    changes.result()
  }
}
