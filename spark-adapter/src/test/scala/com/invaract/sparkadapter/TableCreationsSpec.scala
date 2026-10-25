// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.ir.{CatalogIdentity, DatasetRef, Read, UnknownPlan, Write}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

class TableCreationsSpec extends AnyFunSuite with BeforeAndAfterEach {

  override def beforeEach(): Unit = TableCreations.clear()
  override def afterEach(): Unit = TableCreations.clear()

  private val orders = CatalogIdentity(catalogName = Some("spark_catalog"), namespace = List("default"), table = Some("orders"))
  private def write(location: String, catalog: Option[CatalogIdentity] = None) = Write(DatasetRef(location), UnknownPlan("q"), catalog = catalog)

  test("a write landing where a table is being created, with no table of its own, gets that table once") {
    TableCreations.expect("file:/warehouse/orders", orders)
    assert(TableCreations.claim(write("file:/warehouse/orders")) == write("file:/warehouse/orders", Some(orders)))
    assert(TableCreations.waiting == 0)
    assert(TableCreations.claim(write("file:/warehouse/orders")) == write("file:/warehouse/orders"), "the note is used up")
  }

  test("locations are compared in canonical form") {
    TableCreations.expect("file:/warehouse/orders", orders)
    assert(TableCreations.claim(write("/warehouse//orders/")).asInstanceOf[Write].catalog.contains(orders))
  }

  test("a write elsewhere, a write with its own table, and a plan that is not a write are left alone and leave the note") {
    TableCreations.expect("file:/warehouse/orders", orders)
    val own = CatalogIdentity(table = Some("other"))
    assert(TableCreations.claim(write("file:/warehouse/other")) == write("file:/warehouse/other"))
    assert(TableCreations.claim(write("file:/warehouse/orders", Some(own))) == write("file:/warehouse/orders", Some(own)))
    assert(TableCreations.claim(Read(DatasetRef("file:/warehouse/orders"))) == Read(DatasetRef("file:/warehouse/orders")))
    assert(TableCreations.waiting == 1)
  }

  test("a note made again replaces the earlier one for that location") {
    TableCreations.expect("file:/warehouse/orders", orders)
    TableCreations.expect("file:/warehouse/orders/", orders.copy(table = Some("orders2")))
    assert(TableCreations.waiting == 1)
    assert(TableCreations.claim(write("file:/warehouse/orders")).asInstanceOf[Write].catalog.flatMap(_.table).contains("orders2"))
  }

  test("past Capacity the oldest notes are dropped, and only those") {
    (0 to TableCreations.Capacity).foreach(i => TableCreations.expect(s"/w/t$i", orders))
    assert(TableCreations.waiting == TableCreations.Capacity)
    assert(TableCreations.claim(write("/w/t0")).asInstanceOf[Write].catalog.isEmpty, "the oldest went")
    assert(TableCreations.claim(write("/w/t1")).asInstanceOf[Write].catalog.contains(orders))
    assert(TableCreations.claim(write(s"/w/t${TableCreations.Capacity}")).asInstanceOf[Write].catalog.contains(orders))
  }
}
