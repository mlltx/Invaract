name := "invaract-ir"
version := "0.2.0"
scalaVersion := "2.12.18"
organization := "com.invaract"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.18" % "test"
)

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature"
)

assembly / assemblyJarName := "invaract-ir-0.2.0.jar"

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

// API compatibility (MiMa) - see contract/build.sbt's comment for the full
// rationale (no Maven Central release yet, so CI's `api-compatibility` job
// compares against the PR's own base branch instead) and
// docs/TRANSFORMATION_IR.md's "API compatibility" section.
//
// Renamed invariant-ir -> invaract-ir by the rebrand PR, which is now on the
// base branch - see contract/build.sbt's comment for why this coordinate
// must match base-ref's own published name.
mimaPreviousArtifacts := Set("com.example" %% "invaract-ir" % "0.1.0")

// com.example -> com.invaract namespace rebrand (this PR) - see
// contract/build.sbt's matching comment for the full rationale (deliberate
// break per CLAUDE.md's "API Compatibility Requirement" option 2, version
// bumped to 0.2.0 as the MAJOR-equivalent bump docs/VERSIONING.md's pre-1.0
// policy calls for).
//
// FOLLOW-UP (once this PR lands on the base branch): flip
// `mimaPreviousArtifacts` above to `Set("com.invaract" %% "invaract-ir" %
// "0.2.0")` and remove this filter - do not make that flip in this PR.
import com.typesafe.tools.mima.core._
mimaBinaryIssueFilters ++= Seq(
  ProblemFilters.exclude[Problem]("com.example.ir.*")
)
