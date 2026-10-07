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
// Package names are DELIBERATELY unchanged (`com.invaract.sparkadapter`,
// `.notification`, `.location`): deployed configurations name built-in
// sinks and custom plug-ins by fully-qualified class name
// (`sink.class=com.invaract.sparkadapter.notification.FileNotificationSink`,
// `customRuleTypes`, ...), so renaming a package would silently break every
// existing deployment's config - the External Attachability Requirement's
// whole point is that a platform team changes none of that. The relocation
// is invisible to a user of the `spark-adapter` jar, which bundles this
// module's classes (sbt-assembly, the same way it bundles `fingerprint`'s).
// Neutral package names, with FQN compatibility for existing configs, are a
// separate follow-up decision.
//
// Not yet wired into Maven Central publishing or `release.yml` (the same
// disclosed gap `fingerprint` has - see CLAUDE.md "What's the product"):
// `spark-adapter`'s published POM depends on it, so that follow-up has to
// land before the next Maven Central release.
ThisBuild / version := "0.1.0"
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

// Deliberate break (docs/MULTI_ENGINE_ADAPTERS.md, Stage 3): `VerificationPipeline.verifyWrite`
// gained a sixth parameter, the adapter's `AdapterCapabilities`, which the pipeline now checks the
// contract against (UNSUPPORTED_CONTRACT_FEATURE). A parameter added to a method changes its
// compiled signature even with a default. Nothing outside this repository calls the method yet
// (the module is unreleased), so there is no overload kept for the old shape. MiMa's own
// suggested filter, verbatim; inert once the base branch carries this change.
mimaBinaryIssueFilters ++= Seq(
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.sparkadapter.VerificationPipeline.verifyWrite")
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

assembly / assemblyJarName := "invaract-verification-core-0.1.0.jar"

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
