// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.testkit.{Attestation, AttestedClaimsSpec}
import com.invaract.verification.{AdapterCapabilities, Capability}

/** The capabilities no neutral job can show, for the Python adapter: each is shown by `BeamPythonAttachSpec`, on a real Python job
  * where it is about the job (switching Invaract on, dry-run mode) and with the checker the package runs where it is about what
  * the checker applies (location references, the registry, organizational policy). The kit checks the named test exists and is
  * not ignored, and `AttestedClaims.requirements` says what it must show. */
class BeamPythonAttestedClaimsSpec extends AttestedClaimsSpec {

  override protected def capabilities: AdapterCapabilities =
    BeamCapabilities.python.getOrElse(throw new IllegalStateException("the Beam Python declaration did not load"))

  private def attach(test: String) = Attestation(classOf[BeamPythonAttachSpec].getName, test)

  override protected def attestations: Map[Capability, Attestation] = Map(
    Capability.PolicyOrganizational -> attach(BeamPythonAttachSpec.OrganizationalPolicy),
    Capability.ConfigZeroCodeInstall -> attach(BeamPythonAttachSpec.ZeroCodeInstall),
    Capability.ConfigLocationRefs -> attach(BeamPythonAttachSpec.LocationRefs),
    Capability.ConfigContractRegistry -> attach(BeamPythonAttachSpec.ContractRegistry),
    Capability.ReportingDryRun -> attach(BeamPythonAttachSpec.DryRunInference)
  )
}
