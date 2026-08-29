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
  * alias it comes from (e.g. `customer_id` from `raw.orders`).
  *
  * The IR has no symbol-resolution pass — no Catalyst-style exprIds binding
  * every attribute to a globally unique producer. A `ColumnRef`'s qualifier
  * is how lineage tracing decides which upstream relation a bare name
  * belongs to, particularly across a `Join`; an unqualified reference is
  * resolved structurally by walking the plan (see `Lineage`).
  */
case class ColumnRef(name: String, qualifier: Option[String] = None) {
  override def toString: String = qualifier.map(q => s"$q.$name").getOrElse(name)
}
