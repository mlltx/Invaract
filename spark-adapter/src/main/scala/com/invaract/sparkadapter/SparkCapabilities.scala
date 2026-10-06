// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.slf4j.LoggerFactory

/** The Spark adapter's own declaration of what it verifies - loaded from
  * `invaract-capabilities-spark.yaml` (bundled in this module's jar), the same file the engine
  * capability matrix in the docs is generated from. `VerificationPipeline` consults it so that a
  * contract relying on something this adapter declares unsupported is rejected instead of passed
  * unchecked (see `CapabilityCheck`).
  *
  * Loading never throws: a missing or malformed declaration (a repackaged jar that dropped
  * resources) must not take a job down, so it yields `None` - the capability check is skipped - and
  * is logged at WARN. `SparkCapabilitiesSpec` proves the shipped file parses, so that path is never
  * taken in a real build.
  */
private[sparkadapter] object SparkCapabilities {
  private val logger = LoggerFactory.getLogger(SparkCapabilities.getClass)

  val ResourcePath = "invaract-capabilities-spark.yaml"

  lazy val declared: Option[AdapterCapabilities] = load(ResourcePath, SparkCapabilities.getClass.getClassLoader)

  /** The declaration at `path` on `classLoader`'s classpath, or `None` (logged) when it is absent or invalid. */
  private[sparkadapter] def load(path: String, classLoader: ClassLoader): Option[AdapterCapabilities] =
    AdapterCapabilities.fromResource(path, classLoader) match {
      case Some(Right(capabilities)) => Some(capabilities)
      case Some(Left(errors)) =>
        logger.warn(s"$path is invalid; the capability check is skipped: ${errors.mkString("; ")}")
        None
      case None =>
        logger.warn(s"$path is missing from the classpath; the capability check is skipped")
        None
    }
}
