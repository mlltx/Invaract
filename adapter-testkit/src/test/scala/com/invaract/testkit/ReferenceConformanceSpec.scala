// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.testkit

/** The reference adapter run through the same trait a real adapter mixes in: proves the trait itself (its
  * per-scenario tests and its unverified-claims test) works, with no engine involved.
  */
class ReferenceConformanceSpec extends AdapterConformanceSpec {
  private val reference = new ReferenceAdapter
  override protected def adapter: ConformanceAdapter = reference
}
