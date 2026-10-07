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
// Not yet wired into Maven Central publishing or `release.yml` (the same
// disclosed gap `fingerprint` has - see CLAUDE.md "What's the product"):
// `spark-adapter`'s published POM depends on it, so that follow-up has to
// land before the next Maven Central release.
// 0.2.0: Stage 2c moved every class to `com.invaract.verification`. The version
// has to change with the package: CI's api-compatibility job publishes the
// base ref's modules to the local Ivy cache by coordinate, and the base
// ref's spark-adapter (still importing `com.invaract.sparkadapter.*` from
// the core) must resolve the OLD core, not this one, under that coordinate.
ThisBuild / version := "0.2.0"
scalaVersion := "2.12.18"
organization := "com.invaract"

// New module: no previous artifact exists to compare against, so MiMa has
// nothing to check on this PR (CI's api-compatibility job skips a module
// that does not exist at the base commit). Once a version is released, point
// this at it the way fingerprint/build.sbt does.
mimaPreviousArtifacts := Set(
  "com.invaract" %% "invaract-verification-core" % sys.env.getOrElse("INVARACT_MIMA_BASELINE_VERSION", "0.1.0")
)

import com.typesafe.tools.mima.core._

// Deliberate break (docs/MULTI_ENGINE_ADAPTERS.md, Stage 2c): this module's packages moved from
// `com.invaract.sparkadapter` to `com.invaract.verification` before anything was released - there
// are no consumers, so no forwarding classes. Against a baseline built at the old package names
// every class is reported missing; excluding the whole old package is the one honest way to say
// "all of it moved". Inert once the base branch carries the rename.
mimaBinaryIssueFilters ++= Seq(
  ProblemFilters.exclude[Problem]("com.invaract.sparkadapter.*")
)

versionScheme := Some("early-semver")

val hadoopVersion = "3.3.4"

libraryDependencies ++= Seq(
  "com.invaract" %% "invaract-contract" % "0.13.0",
  "com.invaract" %% "invaract-ir" % "0.5.0",
  "com.invaract" %% "invaract-fingerprint" % "0.3.0",
  // Logging and (for HadoopFsNotificationSink only) Hadoop's FileSystem are
  // supplied by whatever engine hosts this module (Spark ships both), never
  // bundled: `provided`, exactly as they were for spark-adapter.
  "org.slf4j" % "slf4j-api" % "2.0.17" % "provided",
  "org.apache.hadoop" % "hadoop-client-api" % hadoopVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test",
  "org.scalatestplus" %% "scalacheck-1-17" % "3.2.18.0" % "test",
  "org.apache.hadoop" % "hadoop-client-runtime" % hadoopVersion % "test",
  "org.slf4j" % "slf4j-simple" % "2.0.7" % "test",
  // EventSchema (test helper validating published events against the JSON
  // Schema) reads/walks JSON.
  // Same version spark-adapter pins (its CVE-driven override block).
  "com.fasterxml.jackson.core" % "jackson-core" % "2.18.11" % "test",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.11" % "test",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.18.11" % "test"
)

assembly / assemblyJarName := "invaract-verification-core-0.2.0.jar"

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
