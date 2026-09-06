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
// pgp.sbt/sbom.sbt, no `mimaPreviousArtifacts`) yet, unlike contract/ir/
// spark-adapter: there is no previous version of this module to compare
// against or release metadata to carry — the same position contract/ir/
// spark-adapter were in for the PR that first introduced them (see
// CLAUDE.md's API Compatibility Requirement: "A module that doesn't exist
// yet at the base commit is skipped gracefully"). FOLLOW-UP, once this
// module is ready to join contract/ir/spark-adapter as a real published
// artifact: add sonatype.sbt/pgp.sbt/sbom.sbt/mima.sbt (mirroring ir's own),
// set `mimaPreviousArtifacts` once a 0.1.0 baseline actually exists to
// compare against, and fold it into CI's api-compatibility/mutation-testing
// jobs the way ir/spark-adapter already are.

libraryDependencies ++= Seq(
  "com.invaract" %% "invaract-ir" % "0.3.0",
  "org.scalatest" %% "scalatest" % "3.2.18" % "test",
  "org.scalatestplus" %% "scalacheck-1-17" % "3.2.18.0" % "test"
)

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature"
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
