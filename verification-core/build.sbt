name := "invaract-verification-core"

// The engine-neutral verification core: everything an engine adapter reuses
// instead of re-implementing - the structural checkers (schema, input,
// output, catalog), rule/data-quality/role verifiers, the result model
// (Violation, VerificationResult), notification sinks and events, location
// resolution - plus the pieces an adapter customises (lineage-boundary
// types, `MutationKind`, `InferredWrite`). Extracted from `spark-adapter`
// (docs/MULTI_ENGINE_ADAPTERS.md, Stage 2): no Spark dependency, so a
// second engine's adapter depends on this, not on `spark-adapter`.
//
// Packages are `com.invaract.verification` (with `.notification` and
// `.location`) - neutral, not Spark's. Deployed configurations name built-in
// sinks and custom plug-ins by fully-qualified class name
// (`sink.class=com.invaract.verification.notification.FileNotificationSink`,
// `customRuleTypes`, ...); the rename from `com.invaract.sparkadapter` was made
// before any release, so there is no forwarding layer (docs/MULTI_ENGINE_ADAPTERS.md,
// Stage 2c).
//
// Wired up for Maven Central publishing below (sonatype.sbt/pgp.sbt, mirroring ir's own) and
// published by release.yml: `spark-adapter`'s published POM depends on it.
// 0.2.0 (main): Stage 2c moved every class to `com.invaract.verification`. The version
// has to change with the package: CI's api-compatibility job publishes the
// base ref's modules to the local Ivy cache by coordinate, and the base
// ref's spark-adapter (still importing `com.invaract.sparkadapter.*` from
// the core) must resolve the OLD core, not this one, under that coordinate.
// 0.3.0: Stages 4-5 (the adapter-testkit's SPI additions and the function
// catalog's capability) change this module again, so main's 0.2.0 stays the
// base ref's coordinate and this build is a new one.
// 0.4.0: the notification events went engine-neutral (`applicationId` -> `runId`, `JobInfo`
// reshaped) and HadoopFsNotificationSink moved to spark-adapter - source- and binary-breaking
// for the base ref's spark-adapter, hence a new coordinate again.
ThisBuild / version := "0.4.0"
scalaVersion := "2.12.18"
organization := "com.invaract"

// --- Maven Central publishing (Central Portal) ---
// Published with contract/ir/spark-adapter: spark-adapter's POM depends on this module, so a release
// that left it out would be unresolvable (docs/RELEASING.md). Everything below is the POM metadata and
// Central Portal settings Sonatype requires, copied from ir/build.sbt so the modules cannot drift.
publishMavenStyle := true
Test / publishArtifact := false
pomIncludeRepository := { _ => false }

homepage := Some(url("https://github.com/mlltx/Invaract"))
licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/mlltx/Invaract"),
    "scm:git@github.com:mlltx/Invaract.git"
  )
)
developers := List(
  // id/url are the real GitHub org this repo lives under; email is
  // GitHub's own noreply-alias convention, to avoid publishing a personal
  // address in a public POM. Update `name` with a real maintainer name
  // before the first real release.
  Developer(
    id = "mlltx",
    name = "mlltx",
    email = "mlltx@users.noreply.github.com",
    url = url("https://github.com/mlltx")
  )
)

// Pre-1.0 (docs/VERSIONING.md): a 0.x -> 0.(x+1) bump may be
// binary-breaking, so "early-semver" is the accurate scheme - the same
// convention this file's own mimaPreviousArtifacts comment already bumps
// MINOR (not PATCH) for a deliberate break under.
versionScheme := Some("early-semver")

// The OSSRH/Nexus staging host sbt-sonatype originally targeted is
// retired; Central Portal (central.sonatype.com) is the only route onto
// Maven Central now. See docs/RELEASING.md for credentials/CI wiring and
// the actual release commands (publishSigned, then sonatypeCentralRelease).
import xerial.sbt.Sonatype.sonatypeCentralHost
sonatypeCredentialHost := sonatypeCentralHost
publishTo := sonatypePublishToBundle.value

// Non-interactive PGP passphrase for CI (crazy-max/ghaction-import-gpg
// imports the key + configures gpg-agent; this just supplies the
// passphrase sbt-pgp needs when it shells out to gpg). Unset locally -
// sbt-pgp falls back to an interactive prompt, which is fine for a
// maintainer cutting a release by hand.
pgpPassphrase := sys.env.get("PGP_PASSPHRASE").map(_.toCharArray)

// New module: no previous artifact exists to compare against, so MiMa has
// nothing to check on this PR (CI's api-compatibility job skips a module
// that does not exist at the base commit). Once a version is released, point
// this at it the way fingerprint/build.sbt does.
mimaPreviousArtifacts := Set(
  "com.invaract" %% "invaract-verification-core" % sys.env.getOrElse("INVARACT_MIMA_BASELINE_VERSION", "0.3.0")
)

import com.typesafe.tools.mima.core._

// Deliberate break (docs/MULTI_ENGINE_ADAPTERS.md, Stage 2c): this module's packages moved from
// `com.invaract.sparkadapter` to `com.invaract.verification` before anything was released - there
// are no consumers, so no forwarding classes. Against a baseline built at the old package names
// every class is reported missing; excluding the whole old package is the one honest way to say
// "all of it moved". Inert once the base branch carries the rename.
mimaBinaryIssueFilters ++= Seq(
  ProblemFilters.exclude[Problem]("com.invaract.sparkadapter.*"),
  // Deliberate break, review pass 1 (docs/MULTI_ENGINE_ADAPTERS.md, "Conventions every adapter follows"):
  // the notification events went engine-neutral. `applicationId` is `runId` on every event, and
  // `JobInfo` lost its Spark fields (`appName`, `sparkVersion`, `master`, `deployMode`) for
  // `name`, `engine`, `engineVersion` and `engineDetails`. HadoopFsNotificationSink moved to
  // spark-adapter so this module carries no Hadoop dependency. No consumers exist; these are the
  // exact lines MiMa's own output suggests.
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.verification.notification.ContractValidationEvent.applicationId"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.verification.notification.JobInfo.appName"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.verification.notification.JobInfo.applicationId"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.verification.notification.JobInfo.deployMode"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.verification.notification.JobInfo.master"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.verification.notification.JobInfo.sparkVersion"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.verification.notification.JobSummaryEvent.applicationId"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.verification.notification.WriteEvent.applicationId"),
  ProblemFilters.exclude[IncompatibleMethTypeProblem]("com.invaract.verification.notification.JobInfo.apply"),
  ProblemFilters.exclude[IncompatibleMethTypeProblem]("com.invaract.verification.notification.JobInfo.copy"),
  ProblemFilters.exclude[IncompatibleMethTypeProblem]("com.invaract.verification.notification.JobInfo.this"),
  ProblemFilters.exclude[IncompatibleResultTypeProblem]("com.invaract.verification.notification.JobInfo.<init>$default$6"),
  ProblemFilters.exclude[IncompatibleResultTypeProblem]("com.invaract.verification.notification.JobInfo.apply$default$6"),
  ProblemFilters.exclude[IncompatibleResultTypeProblem]("com.invaract.verification.notification.JobInfo.copy$default$6"),
  ProblemFilters.exclude[MissingClassProblem]("com.invaract.verification.notification.HadoopFsNotificationSink")
)

libraryDependencies ++= Seq(
  "com.invaract" %% "invaract-contract" % "0.13.0",
  "com.invaract" %% "invaract-ir" % "0.6.0",
  "com.invaract" %% "invaract-fingerprint" % "0.4.0",
  // Logging is supplied by whatever engine hosts this module (Spark ships
  // slf4j), never bundled: `provided`. Deliberately no Hadoop here - the one
  // sink that needed it (HadoopFsNotificationSink) lives in spark-adapter, so
  // a non-Hadoop engine's classpath is never in question.
  "org.slf4j" % "slf4j-api" % "2.0.17" % "provided",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test",
  "org.scalatestplus" %% "scalacheck-1-17" % "3.2.18.0" % "test",
  "org.slf4j" % "slf4j-simple" % "2.0.7" % "test",
  // EventSchema (test helper validating published events against the JSON
  // Schema) reads/walks JSON.
  // Same version spark-adapter pins (its CVE-driven override block).
  "com.fasterxml.jackson.core" % "jackson-core" % "2.18.11" % "test",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.11" % "test",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.18.11" % "test"
)

// A snappy-java dependencyOverrides entry (CVE-2023-43642, GHSA-55g7-9cwv-5qfv)
// lived here briefly, pinning the org.xerial.snappy:snappy-java:1.1.8.2 that
// org.apache.hadoop:hadoop-client-api/hadoop-client-runtime transitively
// resolved. That Hadoop dependency - and the snappy-java exposure that came
// with it - left this module in the same review pass that moved
// HadoopFsNotificationSink to spark-adapter (see this file's own
// "Deliberately no Hadoop here" comment above): with no Hadoop dependency,
// there is nothing left for that override to pin. spark-adapter, which now
// hosts HadoopFsNotificationSink, already carries its own snappy-java
// override at the same 1.1.10.4 (see its build.sbt).
assembly / assemblyJarName := "invaract-verification-core-0.4.0.jar"

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature"
)

Test / fork := true
Test / javaOptions ++= Seq(
  "-Xmx512m",
  "-XX:+UseSerialGC",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)

// Mutation testing (Stryker4s), same convention as ir/spark-adapter/
// fingerprint (see CLAUDE.md's "Mutation Testing Requirement"). The code
// here moved from spark-adapter, where it scored well partly on the strength
// of spark-adapter's Spark-backed suites; the thresholds below are the same
// as spark-adapter's, checked against a real whole-module run on this module's
// own (Spark-free) tests: 87.89% of total / 91.16% of covered code (950 mutants, Stage 2b),
// and 100% on the Stage 2b SPI files (VerificationPipeline, VerificationSetup, ConfigSource).
// `StringLiteral` is excluded for the same documented reason as in
// spark-adapter (violation/remediation wording is human-readable text).
strykerMutate := Seq("src/main/scala/**/*.scala")
strykerExcludedMutations := Seq("StringLiteral")
strykerThresholdsHigh := 90
strykerThresholdsLow := 80
strykerThresholdsBreak := 70

// Line/branch coverage gating (sbt-scoverage) - see ir/build.sbt's matching
// comment for coverageScalacPluginVersion's own reasoning and the "measure
// first, then pin" discipline: measured stmt=95.39%, branch=92.75% via a real
// `sbt coverage test coverageReport` run on this module's own (Spark-free)
// suite (after the Stage 2b SPI and its tests landed; 91.54% / 89.88% before),
// pinned a few points below.
coverageScalacPluginVersion := "2.4.2"
coverageMinimumStmtTotal := 92
coverageMinimumBranchTotal := 89
coverageFailOnMinimum := true
coverageHighlighting := true
