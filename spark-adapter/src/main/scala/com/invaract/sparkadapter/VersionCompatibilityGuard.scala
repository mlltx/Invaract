// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.util.Try

/** Warns — never blocks — when `ContractEnforcementRule` is installed into a
  * `SparkSession` running a Spark, Delta, or Iceberg version this project has
  * no real CI evidence for. Called once per `SparkSession` construction from
  * each of `ContractEnforcementRule`'s entry points' outer closures (Spark
  * invokes that outer function once per session, not once per analyzed
  * plan — see `ContractEnforcementRule`'s own class doc).
  *
  * ## Scales, not hardcoded
  *
  * The verified-version lists live in exactly one place —
  * `src/main/resources/supported-versions.properties`, bundled onto this
  * module's own classpath (it's under `src/main/resources`, so `sbt
  * assembly` includes it automatically; no separate publishing step) — and
  * are read here at runtime, not compiled into this file. The same file also
  * drives `build.sbt`'s own `sparkVersion`/`deltaVersion`/`icebergVersion`
  * test defaults and `dev/generate-version-docs`'s generated tables (see
  * that file's own header comment for the full list of consumers). Adding a
  * newly-verified version is a one-line edit to that file; this class never
  * needs to change.
  *
  * ## Delta/Iceberg detection has no compile-time dependency on either
  *
  * Same reflective, `Try`-wrapped, `Class.forName`-based convention
  * `SparkAdapterListener.deltaVersionOf`/`icebergSnapshotIdOfTable` already
  * use (`delta-spark`/`iceberg-spark-runtime` are `test`-scope only in
  * `build.sbt`) — a job that never touches Delta or Iceberg simply never
  * finds either class on its classpath, so neither check ever runs, and
  * neither ever logs anything.
  *
  *   - **Delta**: confirmed by downloading the real
  *     `delta-spark_2.12-3.3.3.jar` and inspecting its manifest —
  *     `META-INF/MANIFEST.MF` carries a real `Implementation-Version: 3.3.3`
  *     attribute, readable via the ordinary JVM
  *     `Package.getImplementationVersion()` once a class loaded from that
  *     jar is in hand. This reads the *library's* own version — a different
  *     number from `deltaVersionOf`'s *table commit* version.
  *   - **Iceberg**: confirmed by downloading the real
  *     `iceberg-spark-runtime-3.5_2.12-1.11.0.jar` and running `javap` — it
  *     ships a real, public `org.apache.iceberg.IcebergBuild` class with a
  *     public static `version()` method, Iceberg's own official
  *     version-reporting API (its manifest has no `Implementation-Version` —
  *     it's a shaded jar — so the manifest approach doesn't apply here).
  */
private[sparkadapter] object VersionCompatibilityGuard {

  private val logger = LoggerFactory.getLogger(VersionCompatibilityGuard.getClass)

  private val docsUrl = "https://mlltx.github.io/Invaract/reference/spark-version-support/"

  /** Loaded once, from this module's own classpath (bundled by `sbt
    * assembly` since it lives under `src/main/resources`) — never from disk
    * by path, unlike `build.sbt`'s copy of this same read, since a running
    * Spark job has no guarantee of a working directory containing the
    * source tree. `None` (silently — see `check` below) if the resource is
    * missing or malformed, rather than throwing: this check is advisory
    * only and must never be the reason a real job fails.
    */
  private lazy val properties: Option[java.util.Properties] =
    Try {
      val in = getClass.getResourceAsStream("/supported-versions.properties")
      require(in != null, "supported-versions.properties not found on the classpath")
      try {
        val props = new java.util.Properties()
        props.load(in)
        props
      } finally in.close()
    }.toOption

  private[sparkadapter] def verifiedVersions(key: String): Set[String] =
    properties
      .flatMap(p => Option(p.getProperty(key)))
      .toList
      .flatMap(_.split(",").map(_.trim).filter(_.nonEmpty))
      .toSet

  /** Never throws: every step here is either a pure string comparison
    * against a `Try`-loaded properties file, or a `Try`-wrapped reflective
    * lookup, matching every other reflective probe in this module (see
    * `SparkAdapterListener.deltaVersionOf`'s class doc for the same
    * reasoning). A malformed or missing `supported-versions.properties`
    * produces an empty verified set, not an exception, so this degrades to
    * "compare against an empty list" (no warning) - not a crash.
    */
  private[sparkadapter] def check(session: SparkSession): Unit = {
    checkSpark(session)
    checkReflectiveLibrary(
      "org.apache.spark.sql.delta.DeltaLog",
      klass => Option(klass.getPackage.getImplementationVersion),
      "Delta Lake",
      verifiedVersions("delta.verified")
    )
    checkReflectiveLibrary(
      "org.apache.iceberg.IcebergBuild",
      klass => Try(klass.getMethod("version").invoke(null).asInstanceOf[String]).toOption,
      "Iceberg",
      verifiedVersions("iceberg.verified")
    )
  }

  private def checkSpark(session: SparkSession): Unit =
    Try(session.sparkContext.version).toOption.foreach { detected =>
      warnIfUnverified("Spark", detected, verifiedVersions("spark.verified"))
    }

  /** Shared by both Delta's and Iceberg's checks — and exposed directly for
    * tests — since both are exactly the same shape: look up a marker class
    * by name (skipping entirely, no warning, if it's not on the classpath —
    * a job that never touches this library shouldn't see a warning about
    * it), then pull a version string out of the `Class` some
    * library-specific way (`detectVersion`; the manifest for Delta, a
    * static method call for Iceberg — see this object's class doc) and
    * compare it. `className` isn't hardcoded into this method itself,
    * which is what lets a test exercise "class not on the classpath" (a
    * real scenario for any job that doesn't use that library) without
    * needing an actual second build that genuinely excludes Delta or
    * Iceberg — a bogus class name reaches the exact same `Class.forName`
    * failure a truly-absent library would.
    */
  private[sparkadapter] def checkReflectiveLibrary(
      className: String,
      detectVersion: Class[_] => Option[String],
      component: String,
      verified: Set[String]
  ): Option[String] =
    Try(Class.forName(className)).toOption.flatMap { klass =>
      Try(detectVersion(klass)).toOption.flatten.flatMap { detected =>
        val warning = unverifiedWarning(component, detected, verified)
        warning.foreach(logger.warn(_))
        warning
      }
    }

  private def warnIfUnverified(component: String, detected: String, verified: Set[String]): Unit =
    unverifiedWarning(component, detected, verified).foreach(logger.warn(_))

  /** The pure decision behind `warnIfUnverified`, split out and exposed
    * directly for tests (same reasoning as `ContractEnforcementRule.
    * verifyOrThrow`'s own doc comment: real logic, real assertions, without
    * needing to capture SLF4J/log4j2 output through a test-only logging
    * backend just to observe it) — `check`'s only use of this is via the
    * thin `warnIfUnverified` wrapper above, so a test exercising this
    * directly is exercising the exact same decision `check` makes, not a
    * parallel copy of it.
    *
    * `None` (not just "no warning" but no comparison attempted at all) when
    * `verified` is empty — a missing/unreadable properties file must never
    * manifest as "every version looks unverified," which would make this
    * check noisy exactly when it's least trustworthy.
    */
  private[sparkadapter] def unverifiedWarning(component: String, detected: String, verified: Set[String]): Option[String] =
    if (verified.nonEmpty && !verified.contains(detected)) {
      Some(
        s"Invaract is running against $component $detected, which is not in this project's verified-version " +
          s"list (verified: ${verified.toList.sorted.mkString(", ")}). This is advisory only - verification will " +
          s"continue normally - but $component $detected's compatibility with Invaract has not been checked by " +
          s"this project's own CI. See $docsUrl for the current verified-version list."
      )
    } else None
}
