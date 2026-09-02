// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

package com.invaract.sparkadapter

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class VersionCompatibilityGuardSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    // A plain session, deliberately without Delta's session extension/catalog
    // config (unlike ContractEnforcementRuleSpec/SparkPlanAdapterSpec's
    // shared session) - proves the guard's Delta/Iceberg detection depends
    // only on the JVM classpath (delta-spark/iceberg-spark-runtime are
    // test-scope dependencies of this module, so both are on it regardless
    // of session config - see VersionCompatibilityGuard's own class doc),
    // not on anything a real caller's session happens to configure.
    spark = SparkSession.builder().master("local[*]").appName("VersionCompatibilityGuardSpec").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()

  // --- unverifiedWarning: the pure decision `check` ultimately relies on ---

  test("unverifiedWarning: a verified version produces no warning") {
    assert(VersionCompatibilityGuard.unverifiedWarning("Spark", "3.5.7", Set("3.5.6", "3.5.7", "3.5.9")).isEmpty)
  }

  test("unverifiedWarning: an unverified version produces exactly one warning naming the detected and verified versions") {
    val warning = VersionCompatibilityGuard.unverifiedWarning("Spark", "3.5.1", Set("3.5.6", "3.5.7", "3.5.9"))
    assert(warning.isDefined)
    assert(warning.get.contains("Spark"))
    assert(warning.get.contains("3.5.1"))
    assert(warning.get.contains("3.5.6, 3.5.7, 3.5.9"))
  }

  test("unverifiedWarning: an empty verified set (e.g. an unreadable properties file) fails open - no warning") {
    // Deliberately the opposite of what "unverified" would otherwise imply:
    // a missing/malformed supported-versions.properties must never cause
    // this check to warn on literally every version it sees, since that
    // would make the warning noisy exactly when it's least trustworthy.
    assert(VersionCompatibilityGuard.unverifiedWarning("Spark", "anything-at-all", Set.empty).isEmpty)
  }

  test("unverifiedWarning: never throws on a malformed/unparseable-looking version string") {
    // No numeric parsing happens anywhere in this decision - it's a plain
    // set-membership string comparison - so an odd-looking version string
    // (a SNAPSHOT build, a vendor suffix, empty string) is just another
    // string that either is or isn't in the verified set, never a crash.
    noException should be thrownBy VersionCompatibilityGuard.unverifiedWarning("Spark", "", Set("3.5.7"))
    noException should be thrownBy VersionCompatibilityGuard.unverifiedWarning(
      "Spark",
      "3.5.7-SNAPSHOT+build.123~weird",
      Set("3.5.7")
    )
  }

  // --- checkReflectiveLibrary: the shared Delta/Iceberg detection path ---

  test("checkReflectiveLibrary: a class not on the classpath produces no warning") {
    val result = VersionCompatibilityGuard.checkReflectiveLibrary(
      "com.invaract.sparkadapter.ThisClassDefinitelyDoesNotExist",
      _ => Some("1.0.0"),
      "Delta Lake",
      Set("3.3.3")
    )
    assert(result.isEmpty)
  }

  test("checkReflectiveLibrary: a real class with an unverified detected version produces a warning") {
    // java.lang.String is guaranteed to be on any JVM's classpath - this
    // exercises the real Class.forName lookup, just with a synthetic
    // version-extractor standing in for Delta's manifest read/Iceberg's
    // static method call, so the test doesn't depend on which Delta/Iceberg
    // versions happen to be pinned in build.sbt right now.
    val result = VersionCompatibilityGuard.checkReflectiveLibrary(
      "java.lang.String",
      _ => Some("9.9.9-unverified"),
      "Delta Lake",
      Set("3.3.3")
    )
    assert(result.isDefined)
    assert(result.get.contains("9.9.9-unverified"))
  }

  test("checkReflectiveLibrary: a real class with a verified detected version produces no warning") {
    val result = VersionCompatibilityGuard.checkReflectiveLibrary(
      "java.lang.String",
      _ => Some("3.3.3"),
      "Delta Lake",
      Set("3.3.3")
    )
    assert(result.isEmpty)
  }

  test("checkReflectiveLibrary: never throws even if the version extractor itself throws") {
    // Simulates a reflective lookup failing in some way this method didn't
    // anticipate (a NoSuchMethodException, an IllegalAccessException, a
    // ClassCastException on an unexpected return type) - the class is
    // found, but pulling a version out of it blows up. Must degrade to "no
    // information, no warning," never propagate.
    val result = VersionCompatibilityGuard.checkReflectiveLibrary(
      "java.lang.String",
      _ => throw new RuntimeException("simulated reflective failure"),
      "Delta Lake",
      Set("3.3.3")
    )
    assert(result.isEmpty)
  }

  // --- Real, non-mocked integration: the actual classes/versions this build uses ---

  test("check: never throws against a real SparkSession") {
    noException should be thrownBy VersionCompatibilityGuard.check(spark)
  }

  test("the real running Spark version is itself in supported-versions.properties's verified list") {
    // Ties the properties file back to the actual session this suite (and
    // spark-version-matrix's CI leg) runs against - if build.sbt's
    // sparkVersion default and the properties file's spark.verified list
    // ever disagreed, this would fail here, independently of the CI-only
    // matrix-drift-check job in test.yml.
    val verified = VersionCompatibilityGuard.verifiedVersions("spark.verified")
    assert(verified.nonEmpty, "supported-versions.properties failed to load - spark.verified was empty")
    assert(VersionCompatibilityGuard.unverifiedWarning("Spark", spark.sparkContext.version, verified).isEmpty)
  }

  test("the real Delta Lake jar on this module's test classpath is itself in supported-versions.properties's verified list") {
    // delta-spark is a real test-scope dependency of this module (see
    // build.sbt) - this calls the exact same production code path `check`
    // uses, with the real class name, against whatever delta-spark version
    // sbt actually resolved for this test run (INVARACT_TEST_DELTA_VERSION,
    // defaulting to supported-versions.properties's own delta.primary).
    val verified = VersionCompatibilityGuard.verifiedVersions("delta.verified")
    assert(verified.nonEmpty, "supported-versions.properties failed to load - delta.verified was empty")
    val warning = VersionCompatibilityGuard.checkReflectiveLibrary(
      "org.apache.spark.sql.delta.DeltaLog",
      klass => Option(klass.getPackage.getImplementationVersion),
      "Delta Lake",
      verified
    )
    assert(warning.isEmpty, s"expected no warning, got: $warning")
  }

  test("the real Iceberg jar on this module's test classpath is itself in supported-versions.properties's verified list") {
    // Same reasoning as the Delta case above, for Iceberg's IcebergBuild.version().
    val verified = VersionCompatibilityGuard.verifiedVersions("iceberg.verified")
    assert(verified.nonEmpty, "supported-versions.properties failed to load - iceberg.verified was empty")
    val warning = VersionCompatibilityGuard.checkReflectiveLibrary(
      "org.apache.iceberg.IcebergBuild",
      klass => scala.util.Try(klass.getMethod("version").invoke(null).asInstanceOf[String]).toOption,
      "Iceberg",
      verified
    )
    assert(warning.isEmpty, s"expected no warning, got: $warning")
  }
}
