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
// 0.4.0: ScenarioJob gained `rowChange` (row-level DML scenarios), so rulesDml and write.rowLevelDml
// are verified rather than unchecked gaps; ScenarioOutput gained `registeredAs` (catalog-registration
// scenarios). The same deliberate ScenarioJob break, and the ScenarioOutput one, declared below.
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
  "com.invaract" %% "invaract-adapter-testkit" % sys.env.getOrElse("INVARACT_MIMA_BASELINE_VERSION", "0.3.0")
)

// Deliberate break (review passes 1 and 4): ScenarioJob gained the `untranslatableWrite` and `rowChange`
// parameters (the fail-closed and row-level DML scenarios' job shapes). The kit has no consumers outside this repository yet; these are the exact lines
// MiMa's own output suggests.
mimaBinaryIssueFilters ++= Seq(
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.testkit.ScenarioJob.apply"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.testkit.ScenarioJob.copy"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.testkit.ScenarioJob.this"),
  ProblemFilters.exclude[MissingTypesProblem]("com.invaract.testkit.ScenarioJob$"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.testkit.ScenarioOutput.apply"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.testkit.ScenarioOutput.copy"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.testkit.ScenarioOutput.this"),
  ProblemFilters.exclude[MissingTypesProblem]("com.invaract.testkit.ScenarioOutput$"),
  // Kit hardening (stage 1 of the adapter-skill preparation): Scenario gained the `analysis` expectations, ScenarioJob the
  // `boundary` and `streaming` shapes, and ScenarioOutcome the data-quality, role and unverifiable-input results, so
  // an adapter reports them and the kit judges them. Same reason as above: no consumer outside this repository yet.
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.testkit.Conformance.judge"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.testkit.Scenario.apply"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.testkit.Scenario.copy"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.testkit.Scenario.this"),
  ProblemFilters.exclude[IncompatibleResultTypeProblem]("com.invaract.testkit.Scenario.<init>$default$9"),
  ProblemFilters.exclude[IncompatibleResultTypeProblem]("com.invaract.testkit.Scenario.apply$default$9"),
  ProblemFilters.exclude[IncompatibleResultTypeProblem]("com.invaract.testkit.Scenario.copy$default$9"),
  ProblemFilters.exclude[MissingTypesProblem]("com.invaract.testkit.Scenario$"),
  ProblemFilters.exclude[ReversedMissingMethodProblem]("com.invaract.testkit.ScenarioOutcome.dataQuality"),
  ProblemFilters.exclude[ReversedMissingMethodProblem]("com.invaract.testkit.ScenarioOutcome.roles"),
  ProblemFilters.exclude[ReversedMissingMethodProblem]("com.invaract.testkit.ScenarioOutcome.unverifiableInputs"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.testkit.ScenarioOutcome#Passed.apply"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.testkit.ScenarioOutcome#Passed.copy"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.testkit.ScenarioOutcome#Passed.this"),
  ProblemFilters.exclude[MissingTypesProblem]("com.invaract.testkit.ScenarioOutcome$Passed$"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.testkit.ScenarioOutcome#Rejected.apply"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.testkit.ScenarioOutcome#Rejected.copy"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.testkit.ScenarioOutcome#Rejected.this"),
  ProblemFilters.exclude[MissingTypesProblem]("com.invaract.testkit.ScenarioOutcome$Rejected$")
)

// Line/branch coverage gating (sbt-scoverage), same "measure first, then pin" discipline as the other
// modules: measured stmt=94.47%, branch=86.27% via `sbt coverage test coverageReport`, pinned a few points below.
coverageScalacPluginVersion := "2.4.2"
coverageMinimumStmtTotal := 91
coverageMinimumBranchTotal := 82
coverageFailOnMinimum := true
coverageHighlighting := true

// Mutation testing (Stryker4s), same convention as the other engine modules (CLAUDE.md's "Mutation Testing
// Requirement"). The kit is what defines "conformant", so a scenario or judge that a mutant can change
// without ConformanceKitSpec noticing is a hole in every adapter's guarantee. `StringLiteral` is excluded for
// the reason it is in verification-core and spark-adapter: scenario descriptions and verdict messages are
// human-readable text. The break threshold is pinned a few points under a real whole-module run (see the
// measured figure in docs/MULTI_ENGINE_ADAPTERS.md, Stage 4).
// Scenarios.scala is left out: it is the declarative catalogue (one object initialiser building every
// scenario), and the code Stryker injects pushes that initialiser past the JVM's 64KB method limit
// ("Method too large"), so the module would not compile. Its content is already held by the reference adapter
// and by every real adapter's conformance run, which fail on a scenario whose expectation is wrong.
strykerMutate := Seq("src/main/scala/**/*.scala", "!src/main/scala/com/invaract/testkit/Scenarios.scala")
strykerExcludedMutations := Seq("StringLiteral")
strykerThresholdsHigh := 90
strykerThresholdsLow := 80
strykerThresholdsBreak := 70
