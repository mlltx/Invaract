name := "invaract-fingerprint"
ThisBuild / version := "0.3.0"
scalaVersion := "2.12.18"
organization := "com.invaract"

// This module implements docs/SEMANTIC_LINEAGE_FINGERPRINTING.md: a pure,
// engine-independent canonicalisation/fingerprinting layer over `ir.Plan`/
// `ir.Expr`/`ir.Lineage` (see that document's §1 "Recommended fingerprinting
// architecture"). No Spark dependency, mirroring `ir`'s own independence
// from `contract` — this is meant to be usable by any future front end that
// produces `ir.Plan`, not just `spark-adapter`.
//
// Deliberately NOT wired up for Maven Central publishing (no sonatype.sbt/
// pgp.sbt) yet, unlike contract/ir/spark-adapter — no release metadata to
// carry until this module is ready to actually be published there.
// FOLLOW-UP, once it is: add sonatype.sbt/pgp.sbt (mirroring ir's own).
//
// API compatibility (MiMa) IS wired up below, same as contract/ir/
// spark-adapter - see contract/build.sbt's comment for the general
// invariant this has to satisfy (must always track base-ref's own live
// `version` above, since CI's api-compatibility job runs `sbt publishLocal`
// against base-ref's own build.sbt and then resolves exactly this
// coordinate against it).
//
// The 0.1.0 -> 0.2.0 bump (this file's version above) previously required
// a manual FOLLOW-UP PR to flip this literal once the bump landed on the
// base branch - the same "flip this once that PR lands" pattern
// contract/ir/spark-adapter's own version bumps have each needed and, more
// than once, forgotten (see contract/build.sbt's matching comment for the
// root cause: a hardcoded baseline drifting out of sync with base-ref's
// actual version breaks every OTHER PR's api-compatibility job in
// between, not just the one that bumped it). The fix: CI now derives the
// comparison baseline from base-ref's own `ThisBuild / version` directly
// (INVARACT_MIMA_BASELINE_VERSION), so this hardcoded value only matters
// for a local `sbt mimaReportBinaryIssues` run and can no longer break CI
// by going stale.
mimaPreviousArtifacts := Set(
  "com.invaract" %% "invaract-fingerprint" % sys.env.getOrElse("INVARACT_MIMA_BASELINE_VERSION", "0.2.0")
)

// The 0.2.0 -> 0.3.0 bump (this file's version above): NOT a MiMa break -
// this module's own compiled classes/public API are byte-for-byte
// unchanged (sbt mimaReportBinaryIssues stays clean either way). The real,
// deliberate reason is narrower: this file's own `invaract-ir` dependency
// pin below moved 0.4.0 -> 0.5.0 (following ir's own deliberate break for
// string-constraint support), and `versionScheme := Some("early-semver")`
// on `ir` makes sbt treat two different MINOR versions of it appearing
// together as a hard conflict, not a silent eviction. The
// `api-compatibility` CI job publishes base-ref's own module builds
// alongside the PR's, all into the same shared local Ivy cache under each
// module's own currently-declared version - base-ref's `fingerprint`
// still pins `ir % 0.4.0` (unchanged on main), so if PR-head's
// `fingerprint` published under the *same* "0.2.0" coordinate (now
// declaring `ir % 0.5.0`), whichever publish ran last would silently
// clobber the other's POM, and a downstream module resolving both
// `ir % 0.4.0` directly and `ir % 0.5.0` transitively via that clobbered
// `fingerprint` coordinate hits exactly this "found version conflict(s)"
// failure - confirmed directly against a real CI run, not assumed. Version
// coordinates for the SAME artifact name need to differ whenever what a
// POM declares differs, even with zero code change, precisely so two
// different dependency graphs can never collide under one coordinate like
// this.

// Pre-1.0 (docs/VERSIONING.md), same convention as contract/ir/
// spark-adapter: a 0.x -> 0.(x+1) bump may be binary-breaking, so
// "early-semver" is the accurate scheme.
versionScheme := Some("early-semver")

libraryDependencies ++= Seq(
  "com.invaract" %% "invaract-ir" % "0.5.0",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test",
  "org.scalatestplus" %% "scalacheck-1-17" % "3.2.18.0" % "test"
)

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature",
  // Load-bearing, not just hygiene: Canonicalizer's own doc claims it is
  // "pure and total over every node kind in ir" - true today only by
  // discipline, since a plain `expr match { ... }`/`plan match { ... }`
  // over a sealed trait compiles fine with an incomplete case list,
  // failing only at runtime (a scala.MatchError) the first time a real
  // Plan/Expr hits the missing case. Confirmed directly: removing one
  // existing case from canonicalizeExprT and recompiling with this flag
  // fails the build at that exact match with "match may not be
  // exhaustive"; without it, the same removal compiles clean with only a
  // warning easy to miss in normal build output. This is the only
  // practical way to make "every ir.Expr/ir.Plan case is handled" a
  // compile-time guarantee instead of a hand-checked one - genuinely
  // important here (unlike a typical -Xfatal-warnings adoption) because
  // this module's whole value proposition is never silently missing a
  // node kind.
  "-Xfatal-warnings"
)

assembly / assemblyJarName := "invaract-fingerprint-0.3.0.jar"

// Mutation testing (Stryker4s), same convention as ir/spark-adapter (see
// CLAUDE.md's "Mutation Testing Requirement"). Whole-module scope from the
// start, since this is a new module with no pre-existing coverage baseline
// to widen from the way ir/spark-adapter's own history did.
strykerMutate := Seq("src/main/scala/**/*.scala")
strykerThresholdsHigh := 80
strykerThresholdsLow := 60
strykerThresholdsBreak := 50

// Line/branch coverage gating (sbt-scoverage) - see ir/build.sbt's matching
// comment for coverageScalacPluginVersion's own reasoning and the "measure
// first, then pin" discipline behind the two thresholds below (measured
// stmt=93.30%, branch=89.10% via a real `sbt coverage test coverageReport`
// run).
coverageScalacPluginVersion := "2.4.2"
coverageMinimumStmtTotal := 91
coverageMinimumBranchTotal := 87
coverageFailOnMinimum := true
coverageHighlighting := true
