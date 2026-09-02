// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.example.ir

/** The physical location a transformation reads from or writes to (a table
  * name, path, topic, etc.). Deliberately the same shape as
  * `com.example.contract.Dataset.location`: the IR and the contract model
  * both need to name "where data lives" without redefining that concept
  * twice.
  */
case class DatasetRef(location: String)

/** A reference to a column, optionally qualified by the dataset or relation
  * alias it comes from (e.g. `customer_id` from `raw.orders`), and
  * optionally carrying an opaque per-translation identity.
  *
  * The IR has no symbol-resolution pass of its own — no Catalyst-style
  * `exprId` binding every attribute to a globally unique producer as a
  * first-class IR concept. A `ColumnRef`'s qualifier is how lineage
  * tracing decides which upstream relation a bare name belongs to,
  * particularly across a `Join`; an unqualified reference is resolved
  * structurally by walking the plan (see `Lineage`).
  *
  * `id` is the one deliberate exception to "no engine identity in the
  * model": a front-end translator that has access to a real per-attribute
  * identity (Spark's `exprId`, or any other engine's analogous concept)
  * may populate it as a plain opaque number — never an engine type, never
  * exposed as anything but an integer this IR doesn't interpret. This
  * exists for the one case name/qualifier genuinely can't disambiguate:
  * two attributes that are structurally the *same* qualifier (e.g. two
  * unaliased occurrences of the same source column reaching a join from
  * different paths) but are not actually the same value stream. Most
  * front-ends, and all hand-constructed IR (tests included), can safely
  * leave this `None` — name/qualifier equality remains the primary,
  * "stable and understandable" identity this IR exposes; `id` only
  * *strengthens* equality when a translator has real per-attribute
  * identity available, it never weakens it, since `None == None`.
  */
case class ColumnRef(name: String, qualifier: Option[String] = None, id: Option[Long] = None) {
  override def toString: String = qualifier.map(q => s"$q.$name").getOrElse(name)
}
