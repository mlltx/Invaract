// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.verification

import org.scalatest.funsuite.AnyFunSuite

class ConfigSourceSpec extends AnyFunSuite {

  test("empty has nothing, and describes a key by its neutral name") {
    assert(ConfigSource.empty.get(InvaractConf.LocationMap).isEmpty)
    assert(ConfigSource.empty.describe(InvaractConf.LocationMap) == "locationMap")
  }

  test("fromMap is keyed by neutral name") {
    val source = ConfigSource.fromMap(Map("orgPolicy" -> "/p.yaml"))
    assert(source.get(InvaractConf.OrgPolicy).contains("/p.yaml"))
    assert(source.get(InvaractConf.OrgPolicyOverlays).isEmpty)
    assert(source.describe(InvaractConf.OrgPolicy) == "orgPolicy")
  }

  test("prefixed looks every neutral name up under the engine's prefix and describes it that way") {
    val raw = Map("spark.invaract.locationMap" -> "/m.properties", "locationMap" -> "wrong")
    val source = ConfigSource.prefixed("spark.invaract.")(raw.get)
    assert(source.get(InvaractConf.LocationMap).contains("/m.properties"))
    assert(source.get(InvaractConf.OrgPolicy).isEmpty)
    assert(source.describe(InvaractConf.OrgPolicyOverlays) == "spark.invaract.orgPolicyOverlays")
  }

  test("flag is false when unset, reads true and false in any case with spaces around, and refuses anything else") {
    def flag(value: String) = ConfigSource.flag(ConfigSource.fromMap(Map("dryRun" -> value)), "dryRun")
    assert(!ConfigSource.flag(ConfigSource.empty, "dryRun"))
    assert(flag("true") && flag("TRUE") && flag(" True\t"))
    assert(!flag("false") && !flag("FALSE") && !flag(" false "))
    List("yes", "1", "", "truee", "on").foreach { bad =>
      val e = intercept[IllegalArgumentException](flag(bad))
      assert(e.getMessage == s"'dryRun' must be true or false, not '$bad'.", bad)
    }
    val spark = ConfigSource.prefixed("spark.invaract.")(Map("spark.invaract.dryRun" -> "y").get)
    assert(intercept[IllegalArgumentException](ConfigSource.flag(spark, "dryRun")).getMessage == "'spark.invaract.dryRun' must be true or false, not 'y'.")
  }

  test("All lists every setting InvaractConf names, once: an adapter's spelling of the settings is checked against it") {
    val declared = InvaractConf.getClass.getDeclaredMethods.toList
      .filter(m => m.getParameterCount == 0 && m.getReturnType == classOf[String])
      .map(_.invoke(InvaractConf).asInstanceOf[String])
    assert(declared.nonEmpty)
    assert(InvaractConf.All.sorted == declared.sorted)
    assert(InvaractConf.All.distinct == InvaractConf.All)
  }

  test("the attach settings are spelled as the existing spark.invaract.* keys are") {
    assert(
      List(InvaractConf.Contract, InvaractConf.DryRun, InvaractConf.NotifyConfig, InvaractConf.RegistryUrl, InvaractConf.RegistryClientClass) ==
        List("contract", "dryRun", "notifyConfig", "registryUrl", "registryClientClass")
    )
  }

  test("the neutral names are the suffixes every existing spark.invaract.* key is spelled with") {
    assert(
      List(
        InvaractConf.LocationMap, InvaractConf.RejectUndeclaredInputs, InvaractConf.RejectUndeclaredFields,
        InvaractConf.ComputeFingerprint, InvaractConf.StaticDataQuality, InvaractConf.RoleConsistency,
        InvaractConf.OrgPolicy, InvaractConf.OrgPolicyOverlays
      ) == List(
        "locationMap", "rejectUndeclaredInputs", "rejectUndeclaredFields", "computeFingerprint",
        "staticDataQuality", "roleConsistency", "orgPolicy", "orgPolicyOverlays"
      )
    )
  }
}
