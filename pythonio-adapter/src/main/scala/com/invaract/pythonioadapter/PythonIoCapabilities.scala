// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.pythonioadapter

import com.invaract.verification.AdapterCapabilities

/** The PythonIo adapter's own declaration of what it verifies - loaded from
  * `invaract-capabilities-pythonio.yaml` (bundled in this module's jar), the same file the engine
  * capability matrix in the docs is generated from. `VerificationPipeline` consults it so that a contract
  * relying on something this adapter declares unsupported is rejected instead of passed unchecked.
  *
  * Loading never throws: a missing or malformed declaration (a repackaged jar that dropped resources) must
  * not take a job down, so it yields `None` - the capability check is skipped - and is logged at WARN.
  * `PythonIoCapabilitiesSpec` proves the shipped file parses, so that path is never taken in a real build.
  */
object PythonIoCapabilities {
  val ResourcePath = "invaract-capabilities-pythonio.yaml"

  lazy val declared: Option[AdapterCapabilities] = load(ResourcePath, PythonIoCapabilities.getClass.getClassLoader)

  /** The declaration at `path` on `classLoader`'s classpath, or `None` (logged) when it is absent or invalid. */
  def load(path: String, classLoader: ClassLoader): Option[AdapterCapabilities] =
    AdapterCapabilities.load(path, classLoader)
}
