// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter


import com.invaract.verification.InvaractConf
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
      ContractEnforcementRule.LocationUrlConfKey -> InvaractConf.LocationUrl,
      ContractEnforcementRule.LocationTokenEnvConfKey -> InvaractConf.LocationTokenEnv,
      ContractEnforcementRule.RejectUndeclaredInputsConfKey -> InvaractConf.RejectUndeclaredInputs,
      ContractEnforcementRule.RejectUndeclaredFieldsConfKey -> InvaractConf.RejectUndeclaredFields,
      ContractEnforcementRule.ComputeFingerprintConfKey -> InvaractConf.ComputeFingerprint,
      ContractEnforcementRule.StaticDataQualityConfKey -> InvaractConf.StaticDataQuality,
      ContractEnforcementRule.RoleConsistencyConfKey -> InvaractConf.RoleConsistency,
      ContractEnforcementRule.RuntimeDataQualityConfKey -> InvaractConf.RuntimeDataQuality,
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

  test("a pinned setting wins over the session's; an unpinned one is the session's; a pinned one the session lacks is found") {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkConfigSourceSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.invaract.dryRun", "true")
      .config("spark.invaract.locationMap", "/m.properties")
      .getOrCreate()
    try {
      PinnedSettings.withPinsForTest(Map("spark.invaract.dryRun" -> "false", "spark.invaract.orgPolicy" -> "/p.yaml")) {
        val source = SparkConfigSource(spark)
        assert(source.get(InvaractConf.DryRun).contains("false"))
        spark.conf.set("spark.invaract.dryRun", "true")
        assert(source.get(InvaractConf.DryRun).contains("false"), "nor can the job change it later")
        assert(source.get(InvaractConf.LocationMap).contains("/m.properties"))
        assert(source.get(InvaractConf.OrgPolicy).contains("/p.yaml"))
      }
      assert(SparkConfigSource(spark).get(InvaractConf.DryRun).contains("true"), "with nothing pinned, the session's")
    } finally spark.stop()
  }

  test("the settings Invaract reads are known; nothing else under spark.invaract. is") {
    assert(SparkSettingNames.unknown(Map.empty) == Nil)
    assert(SparkSettingNames.unknown(Map("spark.sql.shuffle.partitions" -> "2", "spark.invaractx" -> "1")) == Nil, "only the prefix is judged")
    val known = InvaractConf.All.map(n => SparkConfigSource.Prefix + n -> "x").toMap
    assert(SparkSettingNames.unknown(known) == Nil)
    assert(
      SparkSettingNames.unknown(Map("spark.invaract.job.metadata.team" -> "a", "spark.invaract.contract.metadata.owner" -> "b")) == Nil,
      "the two per-key families"
    )
    assert(SparkSettingNames.unknown(Map("spark.invaract." -> "x")) == List("spark.invaract." -> None), "the bare prefix is no setting")
  }

  test("an unknown key is offered the nearest known one, ignoring case, within three edits") {
    def unknownOf(key: String) = SparkSettingNames.unknown(Map(key -> "true"))
    assert(unknownOf("spark.invaract.rejectUndeclaredInput") == List("spark.invaract.rejectUndeclaredInput" -> Some("spark.invaract.rejectUndeclaredInputs")))
    assert(unknownOf("spark.invaract.dryrun") == List("spark.invaract.dryrun" -> Some("spark.invaract.dryRun")), "case alone")
    assert(unknownOf("spark.invaract.dryRunXYZ").head._2.contains("spark.invaract.dryRun"), "three edits away")
    assert(unknownOf("spark.invaract.dryRunWXYZ").head._2.isEmpty, "four edits away")
    assert(unknownOf("spark.invaract.somethingElseEntirely").head._2.isEmpty)
    assert(SparkSettingNames.unknown(Map("spark.invaract.zz" -> "1", "spark.invaract.aa" -> "1")).map(_._1) == List("spark.invaract.aa", "spark.invaract.zz"), "in key order")
  }

  test("the refusal names every unknown key, its suggestion when there is one, and where the settings are listed") {
    val text = SparkSettingNames.describe(List("spark.invaract.dryrun" -> Some("spark.invaract.dryRun"), "spark.invaract.foo" -> None))
    assert(
      text ==
        "Invaract does not read 'spark.invaract.dryrun'. Did you mean 'spark.invaract.dryRun'?\n" +
        "Invaract does not read 'spark.invaract.foo'.\n" +
        s"(The settings Invaract reads: ${SparkSettingNames.SettingsPage})"
    )
  }
}
