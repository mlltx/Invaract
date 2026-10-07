// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

/** Builds a complete capability declaration for tests: every capability `supported` unless overridden. */
object CapabilityFixtures {

  /** @param overrides capability id -> (status, note) replacing the default `supported`. */
  def yaml(
      adapter: String = "toy",
      engine: String = "Toy Engine 1.0",
      point: String = "pre-submit-gate",
      enforcementNote: Option[String] = None,
      overrides: Map[String, (String, Option[String])] = Map.empty,
      omit: Set[String] = Set.empty,
      extra: String = ""
  ): String = {
    val sb = new StringBuilder
    sb.append(s"adapter: $adapter\nengine: $engine\nenforcement:\n  point: $point\n")
    enforcementNote.foreach(n => sb.append(s"  note: $n\n"))
    sb.append("capabilities:\n")
    Capability.all.filterNot(c => omit.contains(c.id)).foreach { c =>
      val (status, note) = overrides.getOrElse(c.id, ("supported", None))
      sb.append(s"  ${c.id}:\n    status: $status\n")
      note.foreach(n => sb.append(s"    note: $n\n"))
    }
    sb.append(extra)
    sb.toString
  }

  def parsed(
      overrides: Map[String, (String, Option[String])] = Map.empty,
      adapter: String = "toy",
      point: String = "pre-submit-gate"
  ): AdapterCapabilities =
    AdapterCapabilities.parse(yaml(adapter = adapter, point = point, overrides = overrides)).fold(e => throw new AssertionError(e.mkString("; ")), identity)
}
