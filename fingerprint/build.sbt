name := "invaract-fingerprint"
ThisBuild / version := "0.2.0"
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
// spark-adapter. The 0.1.0 -> 0.2.0 bump above (this file's version) is
// this module's first real one, and its motivation is a real bug found via
// a genuine CI failure, not a break in this module's own public API
// surface: this PR's catalog-registration feature bumped `ir`'s own
// version (0.3.0 -> 0.4.0) and updated the `invaract-ir` dependency below
// to match, but left this module's own version at 0.1.0 - meaning
// "fingerprint 0.1.0 depending on ir 0.3.0" (already published locally/in
// CI caches from before this PR) and "fingerprint 0.1.0 depending on ir
// 0.4.0" (this PR's own state) both claim the exact same coordinate,
// which is exactly what CI's api-compatibility job hit: an Ivy "version
// conflict... suspected to be binary incompatible" resolving spark-adapter's
// base-ref build (which needs ir 0.3.0) against an already-cached
// fingerprint:0.1.0 that itself resolves to ir 0.4.0. The fix is the
// coordinate bump itself, not a MiMa filter - this module's own compiled
// classes (Canonicalizer's internal Read/Write pattern-match arity aside,
// which is source-only, not part of any public signature) didn't change
// shape, so `mimaPreviousArtifacts` stays at the pre-bump 0.1.0 baseline
// with nothing to filter, the same "no filters needed right now" outcome
// spark-adapter's own 0.2.0 -> 0.3.0 bump documented for an unrelated
// reason.
mimaPreviousArtifacts := Set("com.invaract" %% "invaract-fingerprint" % "0.1.0")

// Pre-1.0 (docs/VERSIONING.md), same convention as contract/ir/
// spark-adapter: a 0.x -> 0.(x+1) bump may be binary-breaking, so
// "early-semver" is the accurate scheme.
versionScheme := Some("early-semver")

libraryDependencies ++= Seq(
  "com.invaract" %% "invaract-ir" % "0.4.0",
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

assembly / assemblyJarName := "invaract-fingerprint-0.2.0.jar"

// Mutation testing (Stryker4s), same convention as ir/spark-adapter (see
// CLAUDE.md's "Mutation Testing Requirement"). Whole-module scope from the
// start, since this is a new module with no pre-existing coverage baseline
// to widen from the way ir/spark-adapter's own history did.
strykerMutate := Seq("src/main/scala/**/*.scala")
strykerThresholdsHigh := 80
strykerThresholdsLow := 60
strykerThresholdsBreak := 50
