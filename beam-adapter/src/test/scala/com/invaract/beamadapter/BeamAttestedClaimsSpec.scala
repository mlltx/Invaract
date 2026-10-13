// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.testkit.{Attestation, AttestedClaimsSpec}
import com.invaract.verification.{AdapterCapabilities, Capability}

/** The capabilities no neutral job can show - how this adapter attaches to a job, what it applies before one
  * exists - are Beam's own to demonstrate. For each one declared `supported` or `partial`, name the test that
  * does (`Attestation(suite, test)`); the kit checks the test exists, and `AttestedClaims.requirements` says what
  * it must show. Until a capability is claimed, nothing is attested.
  */
class BeamAttestedClaimsSpec extends AttestedClaimsSpec {

  override protected def capabilities: AdapterCapabilities =
    BeamCapabilities.declared.getOrElse(throw new IllegalStateException("Beam's capability declaration did not load"))

  override protected def attestations: Map[Capability, Attestation] = Map.empty
}
