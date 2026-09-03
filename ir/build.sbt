import com.typesafe.tools.mima.core._

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

// Two independent deliberate MAJOR-version breaks against the 0.1.0
// baseline, both documented rather than silently filtered:
//
// 1. com.example -> com.invaract namespace rebrand - see
//    contract/build.sbt's matching comment for the full rationale
//    (deliberate break per CLAUDE.md's "API Compatibility Requirement"
//    option 2, version bumped to 0.2.0 as the MAJOR-equivalent bump
//    docs/VERSIONING.md's pre-1.0 policy calls for). The wildcard below
//    subsumes every symbol that ever lived under the old package,
//    including the expression-algebra rework below (2) - once you rename
//    the package, MiMa has nothing left under com.example.ir to compare
//    non-wildcard filters against.
// 2. The "Spark Logical Plan -> Invariant IR" expression-level rework:
//    split the single `FunctionCall` node into `Cast`/`Arithmetic`/
//    `Comparison`/`BooleanExpr`/`Conditional`/`Function`/`UDF`/`Alias`,
//    renamed `Unsupported`/`UnsupportedExpr` to `UnknownPlan`/
//    `UnknownExpression` (adding a `sourceType` field to each), and added
//    an optional `id` field to `ColumnRef` for translator-assigned column
//    identity - see docs/TRANSFORMATION_IR.md's "Critical principle"
//    section for the full rationale.
//
// FOLLOW-UP (once this PR lands on the base branch): flip
// `mimaPreviousArtifacts` above to `Set("com.invaract" %% "invaract-ir" %
// "0.2.0")` and remove this filter - do not make that flip in this PR.
mimaBinaryIssueFilters ++= Seq(
  ProblemFilters.exclude[Problem]("com.example.ir.*")
)
