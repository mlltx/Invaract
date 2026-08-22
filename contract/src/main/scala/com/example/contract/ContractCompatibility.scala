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
    val idChange =
      if (previous.id != next.id)
        List(CompatibilityChange(
          CompatibilityLevel.Breaking,
          "id",
          s"Contract id changed from '${previous.id}' to '${next.id}'"
        ))
      else Nil

    val changes = idChange ++
      diffDatasets("outputs", previous.outputs, next.outputs) ++
      diffDatasets("inputs", previous.inputs, next.inputs)

    CompatibilityReport(changes)
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

  /** Diffs two named collections by matching entries on `name`, classifying
    * each side-only entry via `onRemoved`/`onAdded` and each matched pair via
    * `onCommon`. Shared by `diffDatasets` and `diffSchema`, which differ only
    * in what "removed", "added", and "changed" mean for a `Dataset` vs. a
    * `Field`.
    */
  private def diffByName[T](previous: List[T], next: List[T])(
    name: T => String,
    onRemoved: (String, T) => CompatibilityChange,
    onAdded: (String, T) => CompatibilityChange,
    onCommon: (String, T, T) => List[CompatibilityChange]
  ): List[CompatibilityChange] = {
    val prevByName = previous.map(t => name(t) -> t).toMap
    val nextByName = next.map(t => name(t) -> t).toMap

    val removed = (prevByName.keySet -- nextByName.keySet).toList.sorted.map(n => onRemoved(n, prevByName(n)))
    val added = (nextByName.keySet -- prevByName.keySet).toList.sorted.map(n => onAdded(n, nextByName(n)))
    val common = (prevByName.keySet intersect nextByName.keySet).toList.sorted.flatMap { n =>
      onCommon(n, prevByName(n), nextByName(n))
    }

    removed ++ added ++ common
  }

  private def diffDatasets(kind: String, previous: List[Dataset], next: List[Dataset]): List[CompatibilityChange] =
    diffByName(previous, next)(
      name = _.name,
      onRemoved = (n, _) =>
        CompatibilityChange(CompatibilityLevel.Breaking, s"$kind.$n", s"Dataset '$n' was removed"),
      onAdded = (n, _) =>
        CompatibilityChange(CompatibilityLevel.Minor, s"$kind.$n", s"Dataset '$n' was added"),
      onCommon = (n, prevDs, nextDs) => {
        val locationChange =
          if (prevDs.location != nextDs.location)
            List(CompatibilityChange(
              CompatibilityLevel.Breaking,
              s"$kind.$n.location",
              s"Location changed from '${prevDs.location}' to '${nextDs.location}'"
            ))
          else Nil

        locationChange ++ diffSchema(s"$kind.$n.schema", prevDs.schema, nextDs.schema)
      }
    )

  private def diffSchema(path: String, previous: Schema, next: Schema): List[CompatibilityChange] =
    diffByName(previous.fields, next.fields)(
      name = _.name,
      onRemoved = (n, _) =>
        CompatibilityChange(CompatibilityLevel.Breaking, s"$path.$n", s"Field '$n' was removed"),
      onAdded = (n, field) =>
        if (field.required)
          CompatibilityChange(
            CompatibilityLevel.Breaking,
            s"$path.$n",
            s"Required field '$n' was added without a default; existing producers will fail validation"
          )
        else
          CompatibilityChange(CompatibilityLevel.Minor, s"$path.$n", s"Optional field '$n' was added"),
      onCommon = (n, prevField, nextField) => {
        val typeChange =
          if (prevField.fieldType != nextField.fieldType)
            List(CompatibilityChange(
              CompatibilityLevel.Breaking,
              s"$path.$n.type",
              s"Type changed from '${prevField.fieldType}' to '${nextField.fieldType}'"
            ))
          else Nil

        val requiredChange =
          if (!prevField.required && nextField.required)
            List(CompatibilityChange(
              CompatibilityLevel.Breaking,
              s"$path.$n.required",
              s"Field '$n' changed from optional to required"
            ))
          else Nil

        val nullableChange =
          if (prevField.nullable && !nextField.nullable)
            List(CompatibilityChange(
              CompatibilityLevel.Breaking,
              s"$path.$n.nullable",
              s"Field '$n' changed from nullable to non-nullable"
            ))
          else Nil

        // Only recurse when at least one side actually declares nested fields;
        // two leaf fields would otherwise pay for an empty diff every time.
        val nestedChange =
          if (prevField.properties.nonEmpty || nextField.properties.nonEmpty)
            diffSchema(s"$path.$n", Schema(prevField.properties), Schema(nextField.properties))
          else Nil

        typeChange ++ requiredChange ++ nullableChange ++ nestedChange
      }
    )
}
