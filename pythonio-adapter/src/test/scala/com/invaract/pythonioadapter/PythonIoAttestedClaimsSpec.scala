// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import com.invaract.testkit.{Attestation, AttestedClaimsSpec}
import com.invaract.verification.{AdapterCapabilities, Capability}

/** The capabilities no neutral job can show - how this adapter attaches to a job, what it applies before one exists - are
  * PythonIo's own to demonstrate. Each claimed one is shown by a recorded protocol case (`PythonIoProtocolCasesSpec`), the
  * requests the `invaract` package sends with its settings from `INVARACT_*` variables; the kit checks the named test exists
  * and is not ignored, and `AttestedClaims.requirements` says what it must show.
  */
class PythonIoAttestedClaimsSpec extends AttestedClaimsSpec {

  override protected def capabilities: AdapterCapabilities =
    PythonIoCapabilities.declared.getOrElse(throw new IllegalStateException("PythonIo's capability declaration did not load"))

  private def protocol(test: String) = Attestation(classOf[PythonIoProtocolCasesSpec].getName, test)

  override protected def attestations: Map[Capability, Attestation] = Map(
    Capability.PolicyOrganizational -> protocol(PythonIoProtocolCasesSpec.OrganizationalPolicy),
    Capability.ConfigLocationRefs -> protocol(PythonIoProtocolCasesSpec.LocationRefs),
    Capability.ConfigContractRegistry -> protocol(PythonIoProtocolCasesSpec.ContractRegistry)
  )
}
