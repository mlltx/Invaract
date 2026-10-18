// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.beamadapter

import com.invaract.testkit.{AdapterConformanceSpec, ConformanceAdapter}

/** Beam's Python SDK through the adapter conformance kit: every engine-neutral scenario runs as a real Python Beam pipeline under
  * the real enforcement (`BeamPythonConformanceAdapter`), and each must come out the way `invaract-capabilities-beam-python.yaml`
  * promises. Without a Python environment the scenarios are canceled; CI's Python job runs them strictly. */
class BeamPythonConformanceSpec extends AdapterConformanceSpec {
  private val pythonAdapter = new BeamPythonConformanceAdapter

  override protected def adapter: ConformanceAdapter = pythonAdapter
}
