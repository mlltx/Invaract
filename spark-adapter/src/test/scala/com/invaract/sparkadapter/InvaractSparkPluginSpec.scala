// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.apache.spark.sql.SparkSessionExtensions
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

/** `InvaractSparkPlugin` (H-07): what its driver half does to an application's configuration. A real job attached by
  * the plugin is `InvaractSparkPluginJobSpec`. */
class InvaractSparkPluginSpec extends AnyFunSuite {
  import InvaractSparkPlugin._

  private val ext = classOf[InvaractSparkSessionExtension].getName

  test("withInvaract: Invaract's extension is added to a list without it, and a list with it is unchanged") {
    assert(withInvaract("") == ext)
    assert(withInvaract("io.delta.sql.DeltaSparkSessionExtension") == s"io.delta.sql.DeltaSparkSessionExtension,$ext")
    assert(withInvaract(s"a.B,$ext") == s"a.B,$ext")
    assert(withInvaract(s"$ext,a.B") == s"$ext,a.B", "not moved, not doubled")
    assert(withInvaract(s" a.B , $ext ,") == s"a.B,$ext", "spaces around the commas and an empty entry")
    assert(withInvaract(" a.B ,c.D") == s"a.B,c.D,$ext")
  }

  private def applied(conf: Map[String, String], pins: Map[String, String] = Map.empty): (Map[String, String], List[String]) = {
    val live = mutable.Map(conf.toSeq: _*)
    val sets = mutable.ListBuffer.empty[String]
    attach(live.get, (k, v) => { sets += k; live(k) = v }, pins)
    (live.toMap, sets.toList)
  }

  test("attach: adds the extension when it is missing, and leaves the setting alone when it is there") {
    assert(applied(Map.empty) == (Map(ExtensionsConfKey -> ext), List(ExtensionsConfKey)))
    assert(applied(Map(ExtensionsConfKey -> "a.B")) == (Map(ExtensionsConfKey -> s"a.B,$ext"), List(ExtensionsConfKey)))
    assert(applied(Map(ExtensionsConfKey -> s"a.B, $ext")) == (Map(ExtensionsConfKey -> s"a.B, $ext"), Nil))
  }

  test("attach: puts back a pinned setting the job changed, sets one it left out, and touches one that already matches not at all") {
    val pins = Map("spark.invaract.dryRun" -> "false", "spark.invaract.contract" -> "/c.yaml", "spark.invaract.orgPolicy" -> "/p.yaml")
    val (conf, sets) = applied(Map(ExtensionsConfKey -> ext, "spark.invaract.dryRun" -> "true", "spark.invaract.orgPolicy" -> "/p.yaml"), pins)
    assert(conf == Map(ExtensionsConfKey -> ext) ++ pins)
    assert(sets == List("spark.invaract.contract", "spark.invaract.dryRun"))
  }

  test("the plugin has a driver half and no executor half") {
    val plugin = new InvaractSparkPlugin
    assert(plugin.driverPlugin().isInstanceOf[InvaractSparkPlugin.Driver])
    assert(plugin.executorPlugin() == null)
  }

  test("the extension installs its check rule once per session, however many times it is listed") {
    val first = new SparkSessionExtensions
    val second = new SparkSessionExtensions
    assert(InvaractSparkSessionExtension.firstTime(first))
    assert(!InvaractSparkSessionExtension.firstTime(first))
    assert(InvaractSparkSessionExtension.firstTime(second))
  }
}
