// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors
package com.invaract.contract

/** The canonical vocabulary for how a write behaves toward data already at its target - the
  * values `Dataset.saveMode` is written in, and the values an engine adapter normalizes its own
  * write dispositions into when it fills in `ir.Write.saveMode`.
  *
  * The set is deliberately open (a contract may declare any string, and the check compares
  * case-insensitively): an engine can have a write behavior none of these names. But an
  * adapter must map every disposition that *does* correspond to one of these four onto it, so
  * that one contract means the same thing on every engine. Mapping tables for the engines we
  * know of are in docs/MULTI_ENGINE_ADAPTERS.md ("Write vocabulary").
  */
object SaveModes {

  /** Add to what is there. */
  val Append = "append"

  /** Replace what is there (a truncate, or an overwrite of the affected partitions). */
  val Overwrite = "overwrite"

  /** Write nothing if the target already has data. */
  val Ignore = "ignore"

  /** Fail if the target already has data. */
  val Error = "error"

  val All: Set[String] = Set(Append, Overwrite, Ignore, Error)

  /** Whether `mode` is one of the canonical names (case-insensitively, as the check compares). */
  def isCanonical(mode: String): Boolean = All.contains(mode.trim.toLowerCase)
}
