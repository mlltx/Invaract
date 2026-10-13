// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.testkit.{Attestation, AttestedClaimsSpec}
import com.invaract.verification.{AdapterCapabilities, Capability}

/** The capabilities no neutral job can show - how this adapter attaches to a job, what it applies before one exists - are
  * Beam's own to demonstrate. Each is shown on a real Beam job by `BeamAttachSpec`; the kit checks the named test exists and is
  * not ignored, and `AttestedClaims.requirements` says what it must show.
  *
  * `config.zeroCodeInstall` is declared `partial`: the same suite also records the two ways a job escapes it (it picks its own
  * runner in code, or it never reads its command line), and the explicit call that covers the first.
  */
class BeamAttestedClaimsSpec extends AttestedClaimsSpec {

  override protected def capabilities: AdapterCapabilities =
    BeamCapabilities.declared.getOrElse(throw new IllegalStateException("Beam's capability declaration did not load"))

  private def attach(test: String) = Attestation(classOf[BeamAttachSpec].getName, test)

  override protected def attestations: Map[Capability, Attestation] = Map(
    Capability.PolicyOrganizational -> attach(BeamAttachSpec.OrganizationalPolicy),
    Capability.ConfigZeroCodeInstall -> attach(BeamAttachSpec.ZeroCodeInstall),
    Capability.ConfigLocationRefs -> attach(BeamAttachSpec.LocationRefs),
    Capability.ConfigContractRegistry -> attach(BeamAttachSpec.ContractRegistry),
    Capability.ReportingDryRun -> attach(BeamAttachSpec.DryRunInference)
  )
}
