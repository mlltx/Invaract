// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import com.invaract.testkit.{AdapterConformanceSpec, ConformanceAdapter}

import java.io.File

/** The Python I/O adapter through the conformance kit with real Python jobs: every engine-neutral scenario runs as a job reading
  * and writing through the `invaract` package (`PythonIoPackageConformanceAdapter`), and each must come out the way
  * `invaract-capabilities-pythonio.yaml` promises, the same as `PythonIoConformanceSpec`, which plays the package in-process.
  * Without a Python environment the scenarios are canceled; CI's `pythonio-native-checker` job runs them strictly. */
class PythonIoPackageConformanceSpec extends AdapterConformanceSpec {
  private val packageAdapter = new PythonIoPackageConformanceAdapter

  override protected def adapter: ConformanceAdapter = packageAdapter

  /** Its own results, beside the in-process run's (both are the adapter `pythonio`). */
  override protected def resultsDirectory: Option[File] = Some(new File("target/conformance/package"))
}
