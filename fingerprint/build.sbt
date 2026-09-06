name := "invaract-fingerprint"
ThisBuild / version := "0.1.0"
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
// spark-adapter, and in the same state their own very first introducing PR
// left them in: `mimaPreviousArtifacts` points at this module's own current
// coordinate, but this is the PR that first adds `fingerprint/` to the
// repository at all, so CI's api-compatibility job (.github/workflows/
// test.yml) will find no `base-ref/fingerprint` to compare against and skip
// this module gracefully this one time (see CLAUDE.md's API Compatibility
// Requirement: "A module that doesn't exist yet at the base commit is
// skipped gracefully"). Starting with the next PR that touches this module,
// the check runs for real.
mimaPreviousArtifacts := Set("com.invaract" %% "invaract-fingerprint" % "0.1.0")

// Pre-1.0 (docs/VERSIONING.md), same convention as contract/ir/
// spark-adapter: a 0.x -> 0.(x+1) bump may be binary-breaking, so
// "early-semver" is the accurate scheme.
versionScheme := Some("early-semver")

libraryDependencies ++= Seq(
  "com.invaract" %% "invaract-ir" % "0.3.0",
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

assembly / assemblyJarName := "invaract-fingerprint-0.1.0.jar"

// Mutation testing (Stryker4s), same convention as ir/spark-adapter (see
// CLAUDE.md's "Mutation Testing Requirement"). Whole-module scope from the
// start, since this is a new module with no pre-existing coverage baseline
// to widen from the way ir/spark-adapter's own history did.
strykerMutate := Seq("src/main/scala/**/*.scala")
strykerThresholdsHigh := 80
strykerThresholdsLow := 60
strykerThresholdsBreak := 50
