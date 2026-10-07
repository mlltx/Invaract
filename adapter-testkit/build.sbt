// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 Invaract Contributors

// The adapter conformance kit (docs/MULTI_ENGINE_ADAPTERS.md, Stage 4): a catalogue of
// engine-neutral scenarios plus the machinery that runs them through any engine adapter and
// compares the verdict with what the adapter's own capability declaration promises.
//
// It is a library an adapter depends on `% Test` - spark-adapter is the first - not part of the
// engine itself: nothing at runtime depends on it, so it is not bundled into any assembly jar.
// It depends only on verification-core (and scalatest, because it ships the ScalaTest trait an
// adapter's test mixes in), never on an engine.
// 0.3.0: ScenarioJob gained `untranslatableWrite` and Scenarios split `notCovered` into `attested` and
// `gaps` - a binary break of the kit's own API (declared below for MiMa), hence a new coordinate: the
// base ref's spark-adapter resolves the kit by coordinate in CI.
ThisBuild / version := "0.3.0"
scalaVersion := "2.12.18"
organization := "com.invaract"
name := "invaract-adapter-testkit"

libraryDependencies ++= Seq(
  "com.invaract" %% "invaract-contract" % "0.13.0",
  "com.invaract" %% "invaract-ir" % "0.6.0",
  "com.invaract" %% "invaract-verification-core" % "0.4.0",
  "org.scalatest" %% "scalatest" % "3.2.18",
  // verification-core's own `provided` dependencies, needed to run its classes here.
  "org.slf4j" % "slf4j-api" % "2.0.17" % "provided",
  "org.slf4j" % "slf4j-simple" % "2.0.7" % "test"
)

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature"
)

Test / fork := true

versionScheme := Some("early-semver")

// API compatibility (MiMa), the same gate the other engine modules have: the kit is what third-party
// adapters compile their tests against. The baseline is whatever the base branch published (CI sets
// INVARACT_MIMA_BASELINE_VERSION from its build.sbt); the fallback is the last released-to-main version.
import com.typesafe.tools.mima.core._
mimaPreviousArtifacts := Set(
  "com.invaract" %% "invaract-adapter-testkit" % sys.env.getOrElse("INVARACT_MIMA_BASELINE_VERSION", "0.2.0")
)

// Deliberate break (review pass 1): ScenarioJob gained the `untranslatableWrite` parameter (the fail-closed
// scenario's job shape). The kit has no consumers outside this repository yet; these are the exact lines
// MiMa's own output suggests.
mimaBinaryIssueFilters ++= Seq(
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.testkit.ScenarioJob.apply"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.testkit.ScenarioJob.copy"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.testkit.ScenarioJob.this"),
  ProblemFilters.exclude[MissingTypesProblem]("com.invaract.testkit.ScenarioJob$")
)

// Line/branch coverage gating (sbt-scoverage), same "measure first, then pin" discipline as the other
// modules: measured stmt=94.47%, branch=86.27% via `sbt coverage test coverageReport`, pinned a few points below.
coverageScalacPluginVersion := "2.4.2"
coverageMinimumStmtTotal := 91
coverageMinimumBranchTotal := 82
coverageFailOnMinimum := true
coverageHighlighting := true
