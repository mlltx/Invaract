// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import com.invaract.testkit.{Attestation, AttestedClaimsSpec}
import com.invaract.verification.{AdapterCapabilities, Capability}

/** The capabilities no neutral job can show (how Spark attaches to a job, what it applies before one exists) are
  * Spark's own to demonstrate. Each names the real-Spark test that does; the kit checks the test still exists, and
  * `AttestedClaims.requirements` says what it must show.
  */
class SparkAttestedClaimsSpec extends AttestedClaimsSpec {

  override protected def capabilities: AdapterCapabilities =
    SparkCapabilities.declared.getOrElse(throw new IllegalStateException("Spark's capability declaration did not load"))

  // IcebergConnectorSpec is compiled out below JDK 17 (the Iceberg test dependency needs 17; see build.sbt), so its
  // attestation is checked on the JDK 17 and 21 builds, which CI gates the same way.
  override protected def notCheckableHere: Map[Capability, String] =
    if (scala.util.Properties.isJavaAtLeast("17")) Map.empty
    else Map(Capability.WriteStateChange -> "the Iceberg test dependency needs JDK 17, so IcebergConnectorSpec is not compiled here")

  override protected def attestations: Map[Capability, Attestation] = Map(
    Capability.PolicyOrganizational ->
      Attestation("com.invaract.sparkadapter.ContractEnforcementRuleOrgPolicySpec", "enforce mode: a contract violating org policy throws before any plan is ever analyzed"),
    Capability.ConfigZeroCodeInstall ->
      Attestation("com.invaract.sparkadapter.InvaractSparkSessionExtensionSpec", "a write violating its conf-attached contract is aborted before any data is written"),
    Capability.ConfigLocationRefs ->
      Attestation("com.invaract.sparkadapter.ContractEnforcementRuleSpec", "forContract end-to-end: a real write against a ref:// output is resolved via spark.invaract.locationMap"),
    Capability.ConfigContractRegistry ->
      Attestation("com.invaract.sparkadapter.registry.ContractSourceSpec", "resolve fetches an exact version through the reflectively-loaded client when a real version is named"),
    Capability.ReportingDryRun ->
      Attestation("com.invaract.sparkadapter.InvaractSparkSessionExtensionSpec", "spark.invaract.dryRun=true never blocks a write, with no contract conf set at all"),
    Capability.WriteStateChange ->
      Attestation("com.invaract.sparkadapter.IcebergConnectorSpec", "FAIL: rollback_to_snapshot on a table whose current schema violates the contract is aborted before touching the table"),
    Capability.RulesCustom ->
      Attestation("com.invaract.sparkadapter.ContractEnforcementRuleCustomRuleTypesSpec", "FAIL: an UPDATE assigning the disallowed column is aborted before touching the table")
  )
}
