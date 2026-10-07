// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

/** Every `spark.invaract.*` key `ContractEnforcementRule` has always exposed is, by construction, the
  * neutral `InvaractConf` name under Spark's one prefix. Deployed `--conf` settings depend on these
  * exact spellings, so this pins them: if a neutral name were ever renamed, this fails before a
  * deployed job silently stops getting a setting.
  */
class SparkConfigSourceSpec extends AnyFunSuite {

  test("every public spark.invaract.* key is the neutral name under Spark's prefix") {
    assert(SparkConfigSource.Prefix == "spark.invaract.")
    val pairs = List(
      ContractEnforcementRule.LocationMapConfKey -> InvaractConf.LocationMap,
      ContractEnforcementRule.RejectUndeclaredInputsConfKey -> InvaractConf.RejectUndeclaredInputs,
      ContractEnforcementRule.RejectUndeclaredFieldsConfKey -> InvaractConf.RejectUndeclaredFields,
      ContractEnforcementRule.ComputeFingerprintConfKey -> InvaractConf.ComputeFingerprint,
      ContractEnforcementRule.StaticDataQualityConfKey -> InvaractConf.StaticDataQuality,
      ContractEnforcementRule.RoleConsistencyConfKey -> InvaractConf.RoleConsistency,
      ContractEnforcementRule.OrgPolicyConfKey -> InvaractConf.OrgPolicy,
      ContractEnforcementRule.OrgPolicyOverlaysConfKey -> InvaractConf.OrgPolicyOverlays
    )
    pairs.foreach { case (sparkKey, neutral) => assert(sparkKey == SparkConfigSource.Prefix + neutral, neutral) }
  }

  test("a SparkConfigSource reads a live session's conf under the prefix, and describes a key by its full Spark name") {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkConfigSourceSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.invaract.locationMap", "/m.properties")
      .getOrCreate()
    try {
      val source = SparkConfigSource(spark)
      assert(source.get(InvaractConf.LocationMap).contains("/m.properties"))
      assert(source.get(InvaractConf.OrgPolicy).isEmpty)
      assert(source.describe(InvaractConf.OrgPolicyOverlays) == "spark.invaract.orgPolicyOverlays")
      // A later runtime change is seen: the source reads the live conf, not a snapshot.
      spark.conf.set("spark.invaract.orgPolicy", "/p.yaml")
      assert(source.get(InvaractConf.OrgPolicy).contains("/p.yaml"))
    } finally spark.stop()
  }
}
