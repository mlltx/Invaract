name := "invaract-ir"
// 0.2.0 -> 0.3.0: the expression-algebra rework (FunctionCall split into
// Cast/Arithmetic/Comparison/BooleanExpr/Conditional/Function/UDF/Alias;
// Unsupported/UnsupportedExpr renamed to UnknownPlan/UnknownExpression;
// ColumnRef gained an `id` field) and the lineage rework (ColumnLineage's
// `aggregated: Boolean` replaced with `derivation`/`aggregations`) are
// both real binary breaks against the 0.2.0 baseline, confirmed by a real
// `sbt mimaReportBinaryIssues` run, not assumed. Pre-1.0,
// docs/VERSIONING.md's FAQ calls for bumping the MINOR digit (not MAJOR,
// pinned at 0 until 1.0.0) to signal a deliberate break - the same
// convention the 0.1.0 -> 0.2.0 rebrand itself used.
// ThisBuild-scoped, not a bare `version :=` - see contract/build.sbt's
// matching comment for why (sonatypePublishToBundle reads ThisBuild/version
// specifically; confirmed the gap directly with
// `sbt "show version" "show ThisBuild/version"` before fixing it there).
ThisBuild / version := "0.5.0"
scalaVersion := "2.12.18"
organization := "com.invaract"

// --- Maven Central publishing (Central Portal) ---
// This module is one of the three published to Maven Central (see
// CLAUDE.md's "What's the product": contract/ir/spark-adapter). No release
// has actually been published yet - the `organization` above is the
// intended groupId, contingent on completing Sonatype's namespace
// verification for it (a DNS TXT record proving control of the invaract.com
// domain; see docs/RELEASING.md). Everything below is the POM metadata and
// Central Portal settings Sonatype requires before it will accept any
// release at all, regardless of namespace.
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

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.18" % "test"
)

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature"
)

assembly / assemblyJarName := "invaract-ir-0.5.0.jar"

// Mutation testing (Stryker4s) config: see stryker4s.conf for reporters.
// `mutate`/`thresholds` are set here rather than in stryker4s.conf, whose
// equivalent keys were observed not to take effect via the config file in
// this sbt/plugin version combination - these sbt settings do work.
// Whole-module scope (widened from just Lineage.scala once the initial
// narrow pass's score was reviewed). `break` is what makes CI's
// mutation-testing job fail when the score regresses below it.
strykerMutate := Seq("src/main/scala/**/*.scala")
strykerThresholdsHigh := 80
strykerThresholdsLow := 60
strykerThresholdsBreak := 50

// Line/branch coverage gating (sbt-scoverage) - the guardrail CLAUDE.md's
// Testing Strategy section (via ARCHITECTURE.md) had disclosed as still
// outstanding: mutation testing above answers "does this code have tests
// that would catch a change," a different, complementary question from
// "does this code have tests at all." coverageScalacPluginVersion pins the
// exact scalac-scoverage-plugin runtime release known reachable from this
// environment (2.5.2, sbt-scoverage's own auto-selected default for this
// Scala version, repeatedly hit a stale/edge-cached 429 on Maven Central
// when resolved directly - confirmed by hand via direct HTTP HEAD requests
// against several published versions - 2.4.2 does not) rather than a
// version chosen for any technical reason over another; revisit if the
// pinned version ever stops resolving instead of assuming this one is
// special.
coverageScalacPluginVersion := "2.4.2"
// Measured via a real `sbt coverage test coverageReport` run against this
// module's actual suite (statement 86.13%, branch 80.90%), then set a few
// points below each - the same "measure first, then pin" discipline
// `strykerThresholdsBreak` above already follows, not a guessed round
// number, with headroom for minor fluctuation rather than pinning to the
// exact measured value.
coverageMinimumStmtTotal := 84
coverageMinimumBranchTotal := 78
coverageFailOnMinimum := true
coverageHighlighting := true

// API compatibility (MiMa) - see contract/build.sbt's comment for the full
// rationale (no Maven Central release yet, so CI's `api-compatibility` job
// compares against the PR's own base branch instead) and
// docs/TRANSFORMATION_IR.md's "API compatibility" section.
//
// Points at the base branch's own current published coordinate, which
// must always track base-ref's own live `version` above (CI's
// api-compatibility job runs `sbt publishLocal` against base-ref's own
// build.sbt, then resolves exactly this coordinate against it - see
// contract/build.sbt's matching comment for the general invariant this
// has to satisfy).
//
// The 0.3.0 -> 0.4.0 bump (this file's version above) previously required
// a manual FOLLOW-UP PR to flip this literal once the bump landed on the
// base branch - see contract/build.sbt's matching comment for the root
// cause (a hardcoded baseline drifting out of sync with base-ref's actual
// version breaks every OTHER PR's api-compatibility job in between) and
// the fix: CI now derives the comparison baseline from base-ref's own
// `ThisBuild / version` directly (INVARACT_MIMA_BASELINE_VERSION), so this
// hardcoded value only matters for a local `sbt mimaReportBinaryIssues`
// run and can no longer break CI by going stale.
mimaPreviousArtifacts := Set(
  "com.invaract" %% "invaract-ir" % sys.env.getOrElse("INVARACT_MIMA_BASELINE_VERSION", "0.4.0")
)

import com.typesafe.tools.mima.core._

// The real, deliberate break motivating the 0.4.0 -> 0.5.0 bump above:
// ColumnPropertyState gained a sixth constructor parameter (length, for
// string-constraint support - see docs/STATIC_DATA_QUALITY_VERIFICATION.md
// §3.9) - a case class's compiled apply/copy/constructor arity changes even
// when the new parameter carries a default and lands at the very end, the
// same real-but-unavoidable break contract/build.sbt's and
// spark-adapter/build.sbt's own filters document for their own case
// classes. These filters are load-bearing for this PR's own
// api-compatibility check (base-ref doesn't carry the bump yet) and become
// inert, matching nothing, once base-ref's own version reaches 0.5.0 or
// later - no future PR needs to remove them.
mimaBinaryIssueFilters ++= Seq(
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.ir.ColumnPropertyState.apply"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.ir.ColumnPropertyState.copy"),
  ProblemFilters.exclude[DirectMissingMethodProblem]("com.invaract.ir.ColumnPropertyState.this")
)
