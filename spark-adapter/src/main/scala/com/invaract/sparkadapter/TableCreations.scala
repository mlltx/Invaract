// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.contract.Locations
import com.invaract.ir.{CatalogIdentity, Plan, Write}

/** The table a `.saveAsTable()` (or SQL `CREATE TABLE ... AS SELECT`) on a data-source table is creating, for the insert
  * Spark runs inside it.
  *
  * Spark checks such a write as two plans: the outer `CreateDataSourceTableAsSelectCommand`, which names the table, and,
  * once that runs, an inner `InsertIntoHadoopFsRelationCommand` writing the table's files - which, for a table that does not
  * exist yet, carries no catalog table at all (`DataSource.catalogTable` is set only for an existing one), so nothing in its
  * own plan says which table it is filling. A contract that names the table (`default/orders`, matched through
  * `LocationMatching.namesOf`) would match the outer write and reject the inner one.
  *
  * So when the outer write passes its check, `expect` notes the table's identity under the location it reported (the
  * table's files, where the inner insert writes); `claim` gives that identity to the next write landing there that has
  * none of its own, and forgets the note. A note is only made after the outer write passed - a rejected one runs nothing -
  * and the oldest notes are dropped past `Capacity`, so one left by a create that failed in Spark itself cannot pile up.
  */
private[sparkadapter] object TableCreations {

  /** How many unclaimed notes are kept; far more than creates can be in flight at once in one application. */
  val Capacity = 64

  private val pending = new java.util.LinkedHashMap[String, CatalogIdentity]()

  /** Notes that the write at `location` is filling the table `identity` names. */
  def expect(location: String, identity: CatalogIdentity): Unit = pending.synchronized {
    val key = Locations.canonical(location)
    pending.remove(key)
    pending.put(key, identity)
    while (pending.size > Capacity) pending.remove(pending.keySet.iterator.next())
  }

  /** `plan`, with the identity of the table being created at its location given to a root write that has none. */
  def claim(plan: Plan): Plan = plan match {
    case write: Write if write.catalog.isEmpty =>
      pending.synchronized(Option(pending.remove(Locations.canonical(write.dataset.location)))) match {
        case Some(identity) => write.copy(catalog = Some(identity))
        case None           => write
      }
    case other => other
  }

  /** How many notes are waiting to be claimed (for tests). */
  private[sparkadapter] def waiting: Int = pending.synchronized(pending.size)

  /** Forgets every note (for tests). */
  private[sparkadapter] def clear(): Unit = pending.synchronized(pending.clear())
}
