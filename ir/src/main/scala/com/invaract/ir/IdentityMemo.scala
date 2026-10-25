// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.ir

import scala.util.control.TailCalls._

/** Results remembered per object, told apart by reference: what every walk over a `Plan` (or an `Expr` built from
  * one) uses so that a node reached along several paths is worked out once. See `Plan`'s own doc for why plans
  * share nodes and why equality cannot be the key: a plan is a case class, so its `equals` and `hashCode` walk
  * every path themselves.
  *
  * One instance belongs to one call (one `Lineage.trace`, one fingerprint): what it remembers is only true for the
  * inputs that call was given.
  */
final class IdentityMemo[K <: AnyRef, V] {
  private val table = new java.util.IdentityHashMap[K, V]()

  /** What was remembered for `key`, if anything. */
  def get(key: K): Option[V] = if (table.containsKey(key)) Some(table.get(key)) else None

  /** What was remembered for `key`, or `compute`, remembered. */
  def getOrElseUpdate(key: K, compute: => V): V =
    if (table.containsKey(key)) table.get(key)
    else {
      val value = compute
      table.put(key, value)
      value
    }

  /** The same for a trampolined computation (`scala.util.control.TailCalls`, which the walks use so that a deep plan
    * cannot overflow the stack): a remembered result at once, or `compute`, remembered when it completes. A plan has
    * no cycles, so nothing asks for a key while that key is still being worked out.
    */
  def getOrElseUpdateT(key: K)(compute: => TailRec[V]): TailRec[V] =
    if (table.containsKey(key)) done(table.get(key))
    else compute.map { value => table.put(key, value); value }
}
