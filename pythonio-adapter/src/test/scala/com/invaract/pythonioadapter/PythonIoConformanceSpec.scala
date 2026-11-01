// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import com.invaract.testkit.{AdapterConformanceSpec, ConformanceAdapter}

/** PythonIo through the adapter conformance kit: every engine-neutral scenario runs as a real PythonIo job under
  * the real enforcement, and each must come out the way `invaract-capabilities-pythonio.yaml` promises. A
  * scenario the declaration says does not apply is canceled with its reason, not silently skipped.
  */
class PythonIoConformanceSpec extends AdapterConformanceSpec {
  private val pythonioAdapter = new PythonIoConformanceAdapter

  override protected def adapter: ConformanceAdapter = pythonioAdapter
}
