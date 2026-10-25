// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.testkit.{AdapterConformanceSpec, ConformanceAdapter}

/** Beam through the adapter conformance kit: every engine-neutral scenario runs as a real Beam job under
  * the real enforcement, and each must come out the way `invaract-capabilities-beam.yaml` promises. A
  * scenario the declaration says does not apply is canceled with its reason, not silently skipped.
  */
class BeamConformanceSpec extends AdapterConformanceSpec {
  private val beamAdapter = new BeamConformanceAdapter

  override protected def adapter: ConformanceAdapter = beamAdapter
}
